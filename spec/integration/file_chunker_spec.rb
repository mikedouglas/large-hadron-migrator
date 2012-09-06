# Copyright (c) 2012, Mike Douglas

require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

require 'lhm'
require 'lhm/table'
require 'lhm/migration'

require 'tmpdir'

describe Lhm::FileChunker do
  include IntegrationHelper

  before(:each) { connect_master! }

  describe "copying" do
    before(:each) do
      @origin = table_create(:origin)
      @destination = table_create(:destination)
      @migration = Lhm::Migration.new(@origin, @destination)
    end

    it "should copy 23 rows from origin to destination" do
      23.times { |n| execute("insert into origin set id = '#{ n * n + 23 }'") }

      outfile_dir = Dir.mktmpdir
      Lhm::FileChunker.new(@migration, connection, { :stride => 100, :outfile_dir => outfile_dir }).run

      slave do
        count_all(@destination.name).must_equal(23)
      end
    end
  end
end
