#
#  Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek
#

require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

require "migrate/add_new_column"

describe "LargeHadronMigrator", "integration" do
  include SpecHelper

  before(:each) { recreate }

  it "should add new column" do

    table("addscolumn") do |t|
      t.string :title
      t.integer :rating
      t.timestamps
    end

    truthiness_column "addscolumn", "title", "varchar"
    truthiness_column "addscolumn", "rating", "int"
    truthiness_column "addscolumn", "created_at", "datetime"
    truthiness_column "addscolumn", "updated_at", "datetime"

    ghost = AddNewColumn.up

    truthiness_column "addscolumn", "title", "varchar"
    truthiness_column "addscolumn", "rating", "int"
    truthiness_column "addscolumn", "spam", "tinyint"
    truthiness_column "addscolumn", "created_at", "datetime"
    truthiness_column "addscolumn", "updated_at", "datetime"
  end

  it "should have same row data" do
    table "addscolumn" do |t|
      t.string :text
      t.integer :number
      t.timestamps
    end

    1200.times do |i|
      random_string = (0...rand(25)).map{65.+(rand(25)).chr}.join
      sql "INSERT INTO `addscolumn` SET
            `id`         = #{i+1},
            `text`       = '#{random_string}',
            `number`     = '#{rand(255)}',
            `updated_at` = NOW(),
            `created_at` = NOW()"
    end

    ghost = AddNewColumn.up

    truthiness_rows "addscolumn", ghost
  end
end


describe "LargeHadronMigrator", "rename" do
  include SpecHelper

  before(:each) do
    recreate
  end

  it "should rename multiple tables" do
    table "renameme" do |t|
      t.string :text
    end

    table "renamemetoo" do |t|
      t.integer :number
    end

    LargeHadronMigrator.rename_tables("renameme" => "renameme_new", "renamemetoo" => "renameme")

    truthiness_column "renameme", "number", "int"
    truthiness_column "renameme_new", "text", "varchar"
  end

end

describe "LargeHadronMigrator", "triggers" do
  include SpecHelper

  before(:each) do
    recreate

    table "triggerme" do |t|
      t.string :text
      t.integer :number
      t.timestamps
    end

    LargeHadronMigrator.clone_table_for_changes \
      "triggerme",
      "triggerme_changes"
  end

  it "should create a table for triggered changes" do
    truthiness_column "triggerme_changes", "hadron_action", "enum"
    truthiness_index "triggerme_changes", "hadron_action", [ "hadron_action" ], false
  end

  it "should trigger on insert" do
    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "insert"

      # test
    sql("insert into triggerme values (111, 'hallo', 5, NOW(), NOW())")
    select_one("select * from triggerme_changes where id = 111").tap do |row|
      row["hadron_action"].should == "insert"
      row["text"].should == "hallo"
    end
  end

  it "should trigger on update" do

    # setup
    sql "insert into triggerme values (111, 'hallo', 5, NOW(), NOW())"
    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "update"

    # test
    sql("update triggerme set text = 'goodbye' where id = '111'")
    select_one("select * from triggerme_changes where id = 111").tap do |row|
      row["hadron_action"].should == "update"
      row["text"].should == "goodbye"
    end
  end

  it "should trigger on delete" do

    # setup
    sql "insert into triggerme values (111, 'hallo', 5, NOW(), NOW())"
    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "delete"

    # test
    sql("delete from triggerme where id = '111'")
    select_one("select * from triggerme_changes where id = 111").tap do |row|
      row["hadron_action"].should == "delete"
      row["text"].should == "hallo"
    end
  end

  it "should trigger on create and update" do
    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "insert"

    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "update"

    # test
    sql "insert into triggerme values (111, 'hallo', 5, NOW(), NOW())"
    sql("update triggerme set text = 'goodbye' where id = '111'")

    select_value("select count(*) from triggerme_changes where id = 111").should == "1"
  end

  it "should trigger on multiple update" do
    sql "insert into triggerme values (111, 'hallo', 5, NOW(), NOW())"
    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "update"

    # test
    sql("update triggerme set text = 'goodbye' where id = '111'")
    sql("update triggerme set text = 'hallo again' where id = '111'")

    select_value("select count(*) from triggerme_changes where id = 111").should == "1"
  end

  it "should trigger on inser, update and delete" do
    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "insert"

    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "update"

    LargeHadronMigrator.add_trigger_on_action \
      "triggerme",
      "triggerme_changes",
      "delete"

    # test
    sql "insert into triggerme values (111, 'hallo', 5, NOW(), NOW())"
    sql("update triggerme set text = 'goodbye' where id = '111'")
    sql("delete from triggerme where id = '111'")

    select_value("select count(*) from triggerme_changes where id = 111").should == "1"
  end

  it "should cleanup triggers" do
    %w(insert update delete).each do |action|
      LargeHadronMigrator.add_trigger_on_action \
        "triggerme",
        "triggerme_changes",
        action
    end

    LargeHadronMigrator.cleanup "triggerme"

    # test
    sql("insert into triggerme values (111, 'hallo', 5, NOW(), NOW())")
    sql("update triggerme set text = 'goodbye' where id = '111'")
    sql("delete from triggerme where id = '111'")

    select_value("select count(*) from triggerme_changes where id = 111").should == "0"
  end

end

describe "LargeHadronMigrator", "replaying changes" do
  include SpecHelper

  before(:each) do
    recreate

    table "source" do |t|
      t.string :text
      t.integer :number
      t.timestamps
    end

    table "source_changes" do |t|
      t.string :text
      t.integer :number
      t.string :hadron_action
      t.timestamps
    end
  end

  it "should replay inserts" do
    sql %Q{
      insert into source (id, text, number, created_at, updated_at)
           values (1, 'hallo', 5, NOW(), NOW())
    }

    sql %Q{
      insert into source_changes (id, text, number, created_at, updated_at, hadron_action)
           values (2, 'goodbye', 5, NOW(), NOW(), 'insert')
    }

    sql %Q{
      insert into source_changes (id, text, number, created_at, updated_at, hadron_action)
           values (3, 'goodbye', 5, NOW(), NOW(), 'delete')
    }

    LargeHadronMigrator.replay_insert_changes("source", "source_changes")

    select_value("select text from source where id = 2").should == "goodbye"
    select_value("select count(*) from source where id = 3").should == "0"
  end


  it "should replay updates" do
    sql %Q{
      insert into source (id, text, number, created_at, updated_at)
           values (1, 'hallo', 5, NOW(), NOW())
    }

    sql %Q{
      insert into source_changes (id, text, number, created_at, updated_at, hadron_action)
           values (1, 'goodbye', 5, NOW(), NOW(), 'update')
    }

    LargeHadronMigrator.replay_update_changes("source", "source_changes")

    select_value("select text from source where id = 1").should == "goodbye"
  end

  it "should replay deletes" do
    sql %Q{
      insert into source (id, text, number, created_at, updated_at)
           values (1, 'hallo', 5, NOW(), NOW()),
                  (2, 'schmu', 5, NOW(), NOW())
    }

    sql %Q{
      insert into source_changes (id, text, number, created_at, updated_at, hadron_action)
           values (1, 'goodbye', 5, NOW(), NOW(), 'delete')
    }

    LargeHadronMigrator.replay_delete_changes("source", "source_changes")

    select_value("select count(*) from source").should == "1"
  end

  it "doesn't replay delete if there are any" do
    LargeHadronMigrator.should_receive(:execute).never
    LargeHadronMigrator.replay_delete_changes("source", "source_changes")
  end

end

describe "LargeHadronMigrator", "units" do
  include SpecHelper

  it "should return correct schema" do
    recreate
    table "source" do |t|
      t.string :text
      t.integer :number
      t.timestamps
    end

    sql %Q{
      insert into source (id, text, number, created_at, updated_at)
           values (1, 'hallo', 5, NOW(), NOW()),
                  (2, 'schmu', 5, NOW(), NOW())
    }

    schema = LargeHadronMigrator.schema_sql("source", "source_changes", 1000)

    schema.should_not include('`source`')
    schema.should include('`source_changes`')
    schema.should include('1003')
  end
end
