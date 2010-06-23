require File.dirname(__FILE__) + '/helper'

describe "MapRedus" do
  # this is called before each test case
  before(:each) do
    MapRedus::FileSystem.flushall
    @process = GetWordCount.create(["There was nothing so VERY remarkable in that; nor did Alice think it so
VERY much out of the", "way to hear the Rabbit say to itself, 'Oh dear!
Oh dear! I shall be late!'", "(when she thought it over afterwards, it
occurred to her that she ought", "to have wondered at this, but at the time
it all seemed quite natural);"])

    @word_count = {"natural"=>1, "over"=>1, "say"=>1, "it"=>4, "think"=>1, "all"=>1, "but"=>1, "of"=>1, "quite"=>1, "much"=>1, "alice"=>1, "very"=>2, "be"=>1, "shall"=>1, "oh"=>2, "hear"=>1, "out"=>1, "time"=>1, "way"=>1, "nothing"=>1, "seemed"=>1, "occurred"=>1, "she"=>2, "itself"=>1, "to"=>4, "in"=>1, "wondered"=>1, "ought"=>1, "rabbit"=>1, "the"=>3, "nor"=>1, "so"=>2, "thought"=>1, "at"=>2, "have"=>1, "when"=>1, "remarkable"=>1, "there"=>1, "afterwards"=>1, "i"=>1, "dear"=>2, "did"=>1, "that"=>2, "was"=>1, "this"=>1, "her"=>1, "late"=>1}
  end

  it "creates a process successfully" do
    process = GetWordCount.open(@process.pid)

    process.mapper.should == WordCounter
    process.reducer.should == Adder
    process.finalizer.should == ToHash
    process.outputter.should == MapRedus::JsonOutputter
  end

  it "runs a map reduce process synchronously" do
    ##
    ## In general map reduce shouldn't be running operations synchronously
    ##
    @process.run(synchronously = true)
    @process.get_saved_result.should == @word_count
  end

  it "runs a map reduce process asynchronously" do
    @process.run(synchronously = false)
    @process.get_saved_result.should == nil
    work_off

    result = @process.get_saved_result
    @process.get_saved_result.should == @word_count
  end
end

describe "MapRedus Support Runner" do
  before(:each) do
    MapRedus.redis.flushall
    @doc = Document.new
  end

  it "runs a process within a class" do
    @doc.run_word_count
    work_off
    @doc.get_word_count.should == @doc.word_answer
  end

  it "runs a second process within a class" do
    @doc.run_char_count
    work_off
    @doc.get_char_count.should == @doc.char_answer
  end

  it "runs separate class with the same mapreduce process name" do
    @job = Job.new
    @job.mapreduce.word_count(@doc.words)
    work_off
    @job.mapreduce.word_count_result.should == @doc.word_answer
  end

  it "runs a map reduce process with reduce recoverable fail" do
    @doc.mapreduce.recoverable_test(@doc.words)
    work_off

    # p x = Resque.info[:failed]
    # p Resque::Failure.all(0, x)
    @doc.mapreduce.recoverable_test_result.should == @doc.recoverable_answer
  end
end

describe "MapRedus Process" do
  before(:each) do
    MapRedus.redis.flushall
    @process = GetWordCount.create( Document::TEST )
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
    @process.run
    MapRedus::Process.kill(@process.pid)
    Resque.size(:mapredus).should == 0
  end

  it "kills a process that is started" do
    @process.run

    worker = Resque::Worker.new("*")
    worker.perform(worker.reserve)   # do some work
    
    MapRedus::Process.kill(@process.pid)
    Resque.size(:mapredus).should == 0
  end
  
  it "kills all process" do
    proc_1 = GetWordCount.create( Document::TEST )
    proc_2 = GetWordCount.create( Document::TEST )
    proc_3 = GetWordCount.create( Document::TEST )
    proc_4 = GetWordCount.create( Document::TEST )
    proc_5 = GetWordCount.create( Document::TEST )
    proc_6 = GetWordCount.create( Document::TEST )

    proc_1.run
    proc_2.run
    proc_3.run

    worker = Resque::Worker.new("*")
    6.times do 
      worker.perform(worker.reserve)
    end
    
    proc_4.run
    proc_5.run
    proc_6.run
    
    6.times do
      worker.perform(worker.reserve)
    end

    MapRedus::Process.kill_all
    Resque.peek(:mapredus, 0, -1) == []
  end

  it "responses to next state correctly" do
    @process.state.should == MapRedus::NOT_STARTED
    @process.next_state
    @process.state.should == MapRedus::MAP_IN_PROGRESS
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
    @process.emit_intermediate(1, "number", "one")
    @process.emit_intermediate(2, "place", "two")
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

  it "saves and deletes JSON results" do
    @process.save_result({"answer" => "value_json"})
    @process.get_saved_result.should == { "answer" => "value_json" }

    @process.delete_saved_result
    @process.get_saved_result.should == nil
  end

  it "produces the correct map keys" do
    @process.emit_intermediate("map key 1", "value")
    @process.emit_intermediate("map key 1", "value")
    @process.emit_intermediate("map key 2", "value")

    @process.map_keys.sort.should == ["map key 1", "map key 2"]
  end

  it "produces the correct map/reduce values" do
    @process.run 
    work_off
    expected_answer = Document.new.word_answer
    @process.map_keys.sort.should == expected_answer.keys.sort

    @process.each_key_reduced_value do |key, reduced_value|
      reduced_value.to_i.should == expected_answer[key]
      @process.map_values(key).should == ["1"] * reduced_value.to_i
    end
  end
end

describe "MapRedus Master" do
  before(:each) do
    MapRedus.redis.flushall
    @process = GetWordCount.create(Document::TEST)
  end

  it "handles slaves (enslaving) correctly" do
    MapRedus::Master.enslave(@process, WordCounter, @process.pid, ["some data"])
    Resque.peek(:mapredus, 0, -1).should == [{"args"=>[@process.pid, ["some data"]], "class"=>"WordCounter"}]
    MapRedus::Master.slaves(@process.pid).should == ["1"]
  end

  it "handles slaves (freeing) correctly" do
    MapRedus::Master.enslave(@process, WordCounter, @process.pid, ["some data"])
    MapRedus::Master.enslave(@process, WordCounter, @process.pid, ["more data"])

    MapRedus::Master.slaves(@process.pid).should == ["1", "1"]

    MapRedus::Master.free_slave(@process.pid)
    MapRedus::Master.free_slave(@process.pid)
    MapRedus::Master.slaves(@process.pid).should == []
  end

  it "handles redundant multiple workers (same output regardless of how many workers complete)"
end

describe "MapRedus Mapper/Reducer/Finalizer" do
  before(:each) do
    MapRedus.redis.flushall
    @process = GetWordCount.create(Document::TEST)
  end

  it "runs a map correctly proceeding to the next state" do
    @process.update(:state => MapRedus::MAP_IN_PROGRESS)
    @process.state.should == MapRedus::MAP_IN_PROGRESS
    @process.mapper.perform(@process.pid, ["data"])
    @process.reload
    @process.state.should == MapRedus::REDUCE_IN_PROGRESS
    Resque.peek(:mapredus, 0, -1).should == [{"args"=>[@process.pid, "data"], "class"=>"Adder"}]
  end

  it "runs a reduce correctly proceeding to the correct next state" do
    @process.update(:state => MapRedus::REDUCE_IN_PROGRESS)
    @process.state.should == MapRedus::REDUCE_IN_PROGRESS
    @process.emit_intermediate("data", "1")
    @process.reducer.perform(@process.pid, "data")
    @process.reload
    @process.state.should == MapRedus::FINALIZER_IN_PROGRESS
    Resque.peek(:mapredus, 0, -1).should == [{"args"=>[@process.pid], "class"=>"ToHash"}]
  end

  it "should test that the finalizer correctly saves" do
    @process.update(:state => MapRedus::FINALIZER_IN_PROGRESS)
    @process.state.should == MapRedus::FINALIZER_IN_PROGRESS
    @process.emit_intermediate("data", "1")
    @process.emit("data", "1")
    @process.finalizer.perform(@process.pid)
    @process.reload
    @process.state.should == MapRedus::COMPLETE
    Resque.peek(:mapredus, 0, -1).should == []
    @process.get_saved_result.should == {"data" => 1}
  end
end
