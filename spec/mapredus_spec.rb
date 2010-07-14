require File.dirname(__FILE__) + '/helper'

describe "MapRedus" do
  # this is called before each test case
  before(:each) do
    MapRedus::FileSystem.flushall
    @process = GetWordCount.create
    MapRedus::FileSystem.setnx("wordstream:test", GetWordCount::TEST)
  end

  it "has sets up the correct default classes" do
    MapRedus::Process.inputter.should == MapRedus::WordStream
    MapRedus::Process.mapper.should == MapRedus::WordCounter
    MapRedus::Process.reducer.should == MapRedus::Adder
    MapRedus::Process.finalizer.should == MapRedus::ToRedisHash
    MapRedus::Process.outputter.should == MapRedus::RedisHasher

    GetWordCount.result_key.should == "test:result"
    GetWordCount.inputter.should == MapRedus::WordStream
    GetWordCount.mapper.should == MapRedus::WordCounter
    GetWordCount.reducer.should == MapRedus::Adder
    GetWordCount.finalizer.should == MapRedus::ToRedisHash
    GetWordCount.outputter.should == MapRedus::RedisHasher
    
    GetCharCount.inputter.should == CharStream
    GetCharCount.mapper.should == CharCounter
    GetCharCount.reducer.should == MapRedus::Adder
    GetCharCount.finalizer.should == MapRedus::ToRedisHash
    GetCharCount.outputter.should == MapRedus::RedisHasher
  end

  it "creates a process successfully" do
    process = GetWordCount.open(@process.pid)
    
    process.inputter.should == MapRedus::WordStream
    process.mapper.should == MapRedus::WordCounter
    process.reducer.should == MapRedus::Adder
    process.finalizer.should == MapRedus::ToRedisHash
    process.outputter.should == MapRedus::RedisHasher

    process = GetCharCount.create
    process.inputter.should == CharStream
    process.mapper.should == CharCounter
    process.reducer.should == MapRedus::Adder
    process.finalizer.should == MapRedus::ToRedisHash
    process.outputter.should == MapRedus::RedisHasher
  end

  it "runs a map reduce process synchronously" do
    ##
    ## In general map reduce shouldn't be running operations synchronously
    ##
    @process.class.should == GetWordCount
    @process.run("wordstream:test", synchronously = true)
    @process.map_keys.size.should == GetWordCount::EXPECTED_ANSWER.size

    @process.map_keys.each do |key|
      reduce_values = @process.reduce_values(key)
      reduce_values.size.should == 1
    end

    @process.each_key_reduced_value do |key, value|
      @process.outputter.decode(@process.result_key, key).to_i.should == GetWordCount::EXPECTED_ANSWER[key]
    end
  end

  it "runs a map reduce process asynchronously" do
    @process.run("wordstream:test", synchronously = false)
    work_off

    @process.map_keys.size.should == GetWordCount::EXPECTED_ANSWER.size
    @process.map_keys.each do |key|
      reduce_values = @process.reduce_values(key)
      reduce_values.size.should == 1
    end

    @process.each_key_reduced_value do |key, value|
      @process.outputter.decode(@process.result_key, key).to_i.should == GetWordCount::EXPECTED_ANSWER[key]
    end
  end

  it "runs the default process" do
    process = MapRedus::Process.create
    process.update(:key_args => [process.pid])
    process.result_key.should == "mapredus:process:#{process.pid}:result"
    process.run("wordstream:test")
    work_off
    
    process.map_keys.size.should == GetWordCount::EXPECTED_ANSWER.size
    process.map_keys.each do |key|
      reduce_values = process.reduce_values(key)
      reduce_values.size.should == 1
    end

    process.each_key_reduced_value do |key, value|
      process.outputter.decode(process.result_key, key).to_i.should == GetWordCount::EXPECTED_ANSWER[key]
    end
  end

  it "runs a process without result_key being set (using the default key location)" do
    process = CharCountTest.create
    process.update(:key_args => [process.pid])
    process.result_key.should == "mapredus:process:#{process.pid}:result"
    process.run("wordstream:test")
    work_off
    
    process.map_keys.size.should == GetCharCount::EXPECTED_ANSWER.size
    process.map_keys.each do |key|
      reduce_values = process.reduce_values(key)
      reduce_values.size.should == 1
    end

    process.each_key_reduced_value do |key, value|
      process.outputter.decode(process.result_key, key).to_i.should == GetCharCount::EXPECTED_ANSWER[key]
    end
  end

  it "runs a process where key arguments exist and extra arguments are used" do 
    process = TestResultKeyArguments.create
    process.result_key("extra_arg").should == "test:key_argument:test:extra_arg"
    process.run("wordstream:test")
    work_off
    
    process.map_keys.size.should == GetWordCount::EXPECTED_ANSWER.size
    process.map_keys.each do |key|
      reduce_values = process.reduce_values(key)
      reduce_values.size.should == 1
    end

    process.each_key_reduced_value do |key, value|
      process.outputter.decode(process.result_key("extra_arg"), key).to_i.should == GetWordCount::EXPECTED_ANSWER[key]
    end
  end
end

describe "MapRedus Process" do
  before(:each) do
    MapRedus::FileSystem.flushall
    @process = GetWordCount.create
  end

  it "saves a process" do
    @process.mapper = CharCounter
    @process.synchronous = true
    @process.save

    @process = MapRedus::Process.open(@process.pid)
    
    @process.mapper.should == CharCounter
    @process.synchronous.should == true
  end
  
  it "updates a process" do
    @process.update(:mapper => CharCounter, :ordered => true)
    @process = MapRedus::Process.open(@process.pid)
    
    @process.mapper.should == CharCounter
    @process.ordered.should == true
  end
  
  it "deletes a process" do
    @process.delete
    
    proc = MapRedus::Process.open(@process.pid)
    proc.should == nil
  end
  
  it "kills a process" do
    @process.run(GetWordCount::TEST)
    MapRedus::Process.kill(@process.pid)
    Resque.size(:mapredus).should == 0
  end

  it "kills a process that is started" do
    @process.run(GetWordCount::TEST)

    worker = Resque::Worker.new("*")
    worker.perform(worker.reserve)   # do some work
    
    MapRedus::Process.kill(@process.pid)
    Resque.size(:mapredus).should == 0
  end
  
  it "kills all process" do
    proc_1 = GetWordCount.create
    proc_2 = GetWordCount.create
    proc_3 = GetWordCount.create
    proc_4 = GetWordCount.create
    proc_5 = GetWordCount.create
    proc_6 = GetWordCount.create

    proc_1.run(GetWordCount::TEST)
    proc_2.run(GetWordCount::TEST)
    proc_3.run(GetWordCount::TEST)

    worker = Resque::Worker.new("*")
    6.times do 
      worker.perform(worker.reserve)
    end
    
    proc_4.run(GetWordCount::TEST)
    proc_5.run(GetWordCount::TEST)
    proc_6.run(GetWordCount::TEST)
    
    6.times do
      worker.perform(worker.reserve)
    end

    MapRedus::Process.kill_all
    Resque.peek(:mapredus, 0, 100) == []
  end

  it "responses to next state correctly" do
    @process.state.should == MapRedus::NOT_STARTED
    @process.next_state
    @process.state.should == MapRedus::INPUT_MAP_IN_PROGRESS
    work_off

    @process.next_state
    @process.state.should == MapRedus::REDUCE_IN_PROGRESS
    work_off

    @process.next_state
    @process.state.should == MapRedus::FINALIZER_IN_PROGRESS
    work_off
    
    @process.next_state
    @process.state.should == MapRedus::COMPLETE
  end

  it "emit_intermediate unordered successfully" do
    @process.emit_intermediate("hell", "yeah")
    result = []
    @process.each_key_nonreduced_value do |key, value|
      result << [key, value]
    end

    result.should == [["hell", "yeah"]]
  end

  it "emit_intermediate on an ordered process" do
    @process.update(:ordered => true)
    @process.emit_intermediate(2, "place", "two")
    @process.emit_intermediate(1, "number", "one")
    res = []
    @process.each_key_nonreduced_value do |key, value|
      res << [key, value]
    end
    
    res.should == [["number", "one"], ["place", "two"]]
  end

  it "emit successfully" do
    @process.emit_intermediate("something", "non_reduced_value")
    @process.emit("something", "reduced")
    result = []
    @process.each_key_reduced_value do |key, rv|
      result << [key, rv]
    end
    result.should == [["something", "reduced"]]
  end

  it "produces the correct map keys" do
    @process.emit_intermediate("map key 1", "value")
    @process.emit_intermediate("map key 1", "value")
    @process.emit_intermediate("map key 2", "value")

    @process.map_keys.sort.should == ["map key 1", "map key 2"]
  end

  it "produces the correct map/reduce values" do
    MapRedus::FileSystem.setnx("wordstream:test", GetWordCount::TEST)
    @process.run("wordstream:test")
    work_off
    @process.map_keys.sort.should == GetWordCount::EXPECTED_ANSWER.keys.sort
    
    @process.each_key_reduced_value do |key, reduced_value|
      reduced_value.to_i.should == GetWordCount::EXPECTED_ANSWER[key]
      @process.map_values(key).should == ["1"] * reduced_value.to_i
    end
  end
end

describe "MapRedus Master" do
  before(:each) do
    MapRedus::FileSystem.flushall
    MapRedus::FileSystem.setnx("test", "some data")
    @process = GetWordCount.create
  end

  it "handles slaves (enslaving) correctly" do
    MapRedus::Master.enslave(@process, MapRedus::WordCounter, @process.pid, "test")
    Resque.peek(:mapredus, 0, 1).should == {"args"=>[@process.pid, "test"], "class"=>"MapRedus::WordCounter"}
    MapRedus::Master.slaves(@process.pid).should == ["1"]
  end

  it "handles slaves (freeing) correctly" do
    MapRedus::Master.enslave(@process, MapRedus::WordCounter, @process.pid, "test")
    MapRedus::Master.enslave(@process, MapRedus::WordCounter, @process.pid, "test")

    MapRedus::Master.slaves(@process.pid).should == ["1", "1"]

    MapRedus::Master.free_slave(@process.pid)
    MapRedus::Master.free_slave(@process.pid)
    MapRedus::Master.slaves(@process.pid).should == []
  end

  it "handles redundant multiple workers (same output regardless of how many workers complete)"
end

describe "MapRedus Mapper/Reducer/Finalizer" do
  before(:each) do
    MapRedus::FileSystem.flushall
    MapRedus::FileSystem.setnx("wordstream:test", "data")
    @process = GetWordCount.create
  end

  it "runs a map correctly proceeding to the next state" do
    @process.update(:state => MapRedus::INPUT_MAP_IN_PROGRESS)
    @process.state.should == MapRedus::INPUT_MAP_IN_PROGRESS
    @process.inputter.perform(@process.pid, "wordstream:test")
    Resque.peek(:mapredus, 0, 1).should == {"args"=>[@process.pid, 0], "class"=>"MapRedus::WordCounter"}
    Resque.pop(:mapredus)
    @process.mapper.perform(@process.pid, 0)
    @process.reload
    @process.state.should == MapRedus::REDUCE_IN_PROGRESS
    Resque.peek(:mapredus, 0, 1).should == {"args"=>[@process.pid, "data"], "class"=>"MapRedus::Adder"}

    MapRedus::Process.open(@process.pid).state.should == MapRedus::REDUCE_IN_PROGRESS
  end

  it "runs a reduce correctly proceeding to the correct next state" do
    @process.update(:state => MapRedus::REDUCE_IN_PROGRESS)
    @process.state.should == MapRedus::REDUCE_IN_PROGRESS
    @process.emit_intermediate("data", "1")
    @process.reducer.perform(@process.pid, "data")
    @process.reload
    @process.state.should == MapRedus::FINALIZER_IN_PROGRESS
    Resque.peek(:mapredus, 0, 1).should == {"args"=>[@process.pid], "class"=>"MapRedus::ToRedisHash"}

    MapRedus::Process.open(@process.pid).state.should == MapRedus::FINALIZER_IN_PROGRESS
  end

  it "should test that the finalizer correctly saves" do
    @process.update(:state => MapRedus::FINALIZER_IN_PROGRESS)
    @process.state.should == MapRedus::FINALIZER_IN_PROGRESS
    @process.emit_intermediate("data", "1")
    @process.emit("data", "1")
    @process.finalizer.perform(@process.pid)
    @process.reload
    @process.state.should == MapRedus::COMPLETE
    Resque.peek(:mapredus, 0, 100).should == []
    @process.outputter.decode("test:result", "data").should == "1"

    MapRedus::Process.open(@process.pid).state.should == MapRedus::COMPLETE
  end
end

describe "MapRedus Support" do
  before(:each) do
    MapRedus::FileSystem.flushall
    @doc = Document.new(10)
    @other_doc = Document.new(15)
  end

  it "should be simple to create a mapredus as a part of a job" do
    MapRedus::FileSystem.setnx("wordstream:test", GetWordCount::TEST)
    MapRedus::FileSystem.setnx("charstream:test", "simpler test")
    other_answer = {" "=>1, "l"=>1, "m"=>1, "e"=>2, "p"=>1, "r"=>1, "s"=>2, "t"=>2, "i"=>1}

    @doc.calculate_chars("wordstream:test")
    @other_doc.calculate_chars("charstream:test")
    work_off

    GetCharCount::EXPECTED_ANSWER.keys.each do |char|
      @doc.mapreduce.char_count_result([@doc.id], char).should == GetCharCount::EXPECTED_ANSWER[char].to_s
    end
    
    other_answer.keys.each do |char|
      @other_doc.mapreduce.char_count_result([@other_doc.id], char).should == other_answer[char].to_s
    end
  end
end
