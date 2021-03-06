# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/chunker'
require 'lhm/file_chunker'
require 'lhm/entangler'
require 'lhm/atomic_switcher'
require 'lhm/locked_switcher'
require 'lhm/migrator'

module Lhm
  # Copies an origin table to an altered destination table. Live activity is
  # synchronized into the destination table using triggers.
  #
  # Once the origin and destination tables have converged, origin is archived
  # and replaced by destination.
  class Invoker
    include SqlHelper

    attr_reader :migrator, :connection

    def initialize(origin, connection)
      @connection = connection
      @migrator = Migrator.new(origin, connection)
    end

    def run(options = {})
      if !options.include?(:atomic_switch)
        if supports_atomic_switch?
          options[:atomic_switch] = true
        else
          raise Error.new(
            "Using mysql #{version_string}. You must explicitly set " +
            "options[:atomic_switch] (re SqlHelper#supports_atomic_switch?)")
        end
      end

      if options[:outfile_dir] and not File.directory?(options[:outfile_dir])
        raise Error.new("Directory specified by options[:outfile_dir] doesn't exist.")
      end

      migration = @migrator.run

      Entangler.new(migration, @connection).run do
        if options[:outfile_dir]
          FileChunker.new(migration, @connection, options).run
        else
          Chunker.new(migration, @connection, options).run
        end

        if options[:atomic_switch]
          AtomicSwitcher.new(migration, @connection).run
        else
          LockedSwitcher.new(migration, @connection).run
        end
      end
    end
  end
end
