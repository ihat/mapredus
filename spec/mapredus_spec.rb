require File.dirname(__FILE__) + '/helper'

describe "MapRedus" do
  # this is called before each test case
  before(:each) do
    MapRedus::FileSystem.flushall
    @process = GetWordCount.create
    MapRedus::FileSystem.setnx("wordstream:test", GetWordCount::TEST)

    @word_count = {"gunrest"=>1, "over"=>1, "still"=>1, "of"=>1, "him"=>2, "and"=>4, "bowl"=>1, "himself"=>1, "went"=>1, "friendly"=>1, "finger"=>1, "propped"=>1, "cheeks"=>1, "dipped"=>1, "down"=>1, "wearily"=>1, "up"=>1, "stepped"=>1, "dedalus"=>1, "to"=>2, "in"=>2, "sat"=>1, "the"=>6, "pointed"=>1, "as"=>1, "followed"=>1, "stephen"=>1, "laughing"=>1, "his"=>2, "he"=>2, "brush"=>1, "jest"=>1, "neck"=>1, "mirror"=>1, "edge"=>1, "on"=>2, "parapet"=>2, "lathered"=>1, "watching"=>1, "halfway"=>1}
  end

  it "creates a process successfully" do
    process = GetWordCount.open(@process.pid)
    
    process.inputter.should == WordStream
    process.mapper.should == WordCounter
    process.reducer.should == Adder
    process.finalizer.should == ToRedisHash
    process.outputter.should == MapRedus::RedisHasher
  end

  it "runs a map reduce process synchronously" do
    ##
    ## In general map reduce shouldn't be running operations synchronously
    ##
    @process.run("wordstream:test", synchronously = true)
    @process.map_keys.size.should == @word_count.size

    @process.map_keys.each do |key|
      reduce_values = @process.reduce_values(key)
      reduce_values.size.should == 1
    end

    @process.each_key_reduced_value do |key, value|
      @process.outputter.decode(@process.keyname, key).to_i.should == @word_count[key]
    end
  end

  it "runs a map reduce process asynchronously" do
    @process.run("wordstream:test", synchronously = false)
    work_off

    @process.map_keys.size.should == @word_count.size
    @process.map_keys.each do |key|
      reduce_values = @process.reduce_values(key)
      reduce_values.size.should == 1
    end

    @process.each_key_reduced_value do |key, value|
      @process.outputter.decode(@process.keyname, key).to_i.should == @word_count[key]
    end
  end
end
