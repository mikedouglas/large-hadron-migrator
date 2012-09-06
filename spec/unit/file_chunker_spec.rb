require File.expand_path(File.dirname(__FILE__)) + '/unit_helper'

require 'lhm/table'
require 'lhm/migration'
require 'lhm/file_chunker'

require 'tmpdir'

describe Lhm::FileChunker do
  include UnitHelper

  before(:each) do
    @origin = Lhm::Table.new("origin")
    @destination = Lhm::Table.new("destination")
    @migration = Lhm::Migration.new(@origin, @destination)
    @outfile_dir = Dir.mktmpdir
    @chunker = Lhm::FileChunker.new(@migration, nil, { :start => 1, :limit => 10, :outfile_dir => @outfile_dir })
  end

  after(:each) do
    FileUtils.rm_rf @outfile_dir
  end

  describe "write outfiles" do
    before(:each) do
      @origin.columns["secret"] = { :metadata => "VARCHAR(255)" }
      @destination.columns["secret"] = { :metadata => "VARCHAR(255)" }
    end

    it "should copy the correct range and column" do
      @chunker.write(from = 1, to = 100, chunk = 1).must_equal(
        "select `secret` from `origin` " +
        "where `id` between 1 and 100 " +
        "into outfile '#{ @outfile_dir }/origin.1'"
      )
    end
  end

  describe "read outfiles" do
    before(:each) do
      @origin.columns["secret"] = { :metadata => "VARCHAR(255)" }
      @destination.columns["secret"] = { :metadata => "VARCHAR(255)" }
    end

    it "should read from the correct file into the correct table and columns" do
      @chunker.read(chunk = 14).must_equal(
        "load data infile '#{ @outfile_dir }/origin.14' " +
        "into table `destination`(`secret`)"
      )
    end
  end

  describe "invalid" do
    before do
      @chunker = Lhm::FileChunker.new(@migration, nil, { :start => 0, :limit => -1, :outfile_dir => @outfile_dir })
    end

    it "should have zero chunks" do
      @chunker.traversable_chunks_size.must_equal 0
    end

    it "should not iterate" do
      @chunker.up_to do |bottom, top, chunk|
        raise "should not iterate"
      end
    end
  end

  describe "two" do
    before do
      @chunker = Lhm::FileChunker.new(@migration, nil, {
        :stride => 100_000, :start => 2, :limit => 150_000, :outfile_dir => @outfile_dir
      })
    end

    it "should have two chunks" do
      @chunker.traversable_chunks_size.must_equal 2
    end

    it "should lower bound second chunk on 100_002" do
      @chunker.bottom(chunk = 2).must_equal 100_002
    end

    it "should upper bound second chunk on 150_000" do
      @chunker.top(chunk = 2).must_equal 150_000
    end
  end

  describe "iterating" do
    before do
      @chunker = Lhm::FileChunker.new(@migration, nil, {
        :stride => 100, :start => 53, :limit => 121, :outfile_dir => @outfile_dir
      })
    end

    it "should iterate" do
      @chunker.up_to do |bottom, top, chunk|
        bottom.must_equal 53
        top.must_equal 121
        chunk.must_equal 1
      end
    end
  end
end
