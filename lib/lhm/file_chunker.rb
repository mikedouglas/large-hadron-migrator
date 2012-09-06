# Copyright (c) 2012, Mike Douglas

require 'lhm/command'
require 'lhm/sql_helper'

require 'fileutils'

module Lhm
  class FileChunker
    include Command
    include SqlHelper

    attr_reader :connection

    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @outfile_dir = options[:outfile_dir]
      @connection = connection
      @stride = options[:stride] || 500_000
      @start = options[:start] || select_start
      @limit = options[:limit] || select_limit
    end

    def up_to(&block)
      1.upto(traversable_chunks_size) do |n|
        yield(bottom(n), top(n), n)
      end
    end

    def traversable_chunks_size
      @limit && @start ? ((@limit - @start + 1) / @stride.to_f).ceil : 0
    end

    def bottom(chunk)
      (chunk - 1) * @stride + @start
    end

    def top(chunk)
      [chunk * @stride + @start - 1, @limit].min
    end

    def write(lowest, highest, chunk)
      "select #{ columns } from `#{ origin_name }` " +
      "where `id` between #{ lowest } and #{ highest } " +
      "into outfile '#{ outfile(chunk) }'"
    end

    def read(chunk)
      "load data infile '#{ outfile(chunk) }' " +
      "into table `#{ destination_name }`(#{ columns })"
    end

    def select_start
      start = connection.select_value("select min(id) from #{ origin_name }")
      start ? start.to_i : nil
    end

    def select_limit
      limit = connection.select_value("select max(id) from #{ origin_name }")
      limit ? limit.to_i : nil
    end

  private

    def outfile(chunk)
      "#{ @outfile_dir }/#{ origin_name }.#{ chunk }"
    end

    def destination_name
      @migration.destination.name
    end

    def origin_name
      @migration.origin.name
    end

    def columns
      @columns ||= @migration.intersection.joined
    end

    def validate
      if @start && @limit && @start > @limit
        error("impossible chunk options (limit must be greater than start)")
      end
    end

    def execute
      # write origin to outfiles
      up_to do |lowest, highest, chunk|
        sql(write(lowest, highest, chunk))
        print "."
      end

      # temporarily remove indices
      indices = @migration.destination.indices
      sql("alter table `#{ @migration.destination.name }` " +
          indices.map {|idx_name, idx_cols| "drop index `#{ idx_name }`" }.join(', '))

      # read outfiles into destination
      1.upto(traversable_chunks_size) do |chunk|
        sql(read(chunk))
        print "*"
      end

      # re-add indices
      sql("alter table `#{ @migration.destination.name }` " +
          indices.map {|idx_name, idx_cols| "add unique index `#{ idx_name }` (#{ idx_cols.join(', ') })" }.join(', '))

      # remove outfiles
      FileUtils.rm((1..traversable_chunks_size).map {|chunk| outfile(chunk) })

      print "\n"
    end
  end
end
