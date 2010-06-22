require File.dirname(__FILE__) + '/helper'

context "MapRedus" do

  # this is called before each test case
  setup do
    MapRedus::FileSystem.flushall
    @process = GetWordCount.create(["There was nothing so VERY remarkable in that; nor did Alice think it so
VERY much out of the", "way to hear the Rabbit say to itself, 'Oh dear!
Oh dear! I shall be late!'", "(when she thought it over afterwards, it
occurred to her that she ought", "to have wondered at this, but at the time
it all seemed quite natural);"])

    @word_count = {"natural"=>1, "over"=>1, "say"=>1, "it"=>4, "think"=>1, "all"=>1, "but"=>1, "of"=>1, "quite"=>1, "much"=>1, "alice"=>1, "very"=>2, "be"=>1, "shall"=>1, "oh"=>2, "hear"=>1, "out"=>1, "time"=>1, "way"=>1, "nothing"=>1, "seemed"=>1, "occurred"=>1, "she"=>2, "itself"=>1, "to"=>4, "in"=>1, "wondered"=>1, "ought"=>1, "rabbit"=>1, "the"=>3, "nor"=>1, "so"=>2, "thought"=>1, "at"=>2, "have"=>1, "when"=>1, "remarkable"=>1, "there"=>1, "afterwards"=>1, "i"=>1, "dear"=>2, "did"=>1, "that"=>2, "was"=>1, "this"=>1, "her"=>1, "late"=>1}
  end

  test "000 mapreduce process is created successfully" do
    process = GetWordCount.open(@process.pid)

    assert_equal WordCounter, process.mapper
    assert_equal Adder, process.reducer
    assert_equal ToHash, process.finalizer
    assert_equal MapRedus::JsonOutputter, process.outputter
  end

  test "000 running a map reduce process synchronously" do
    ##
    ## In general map reduce shouldn't be running operations synchronously
    ##
    @process.run(synchronously = true)
    assert_equal @word_count, @process.get_saved_result
  end

  test "000 running a map reduce process asynchronously" do
    @process.run(synchronously = false)
    assert_nil @process.get_saved_result
    work_off

    result = @process.get_saved_result
    assert_equal @word_count, @process.get_saved_result
  end
end

context "MapRedus Support Runner" do
  setup do
    MapRedus.redis.flushall
    @doc = Document.new
  end

  test "000 running a process within a class" do
    @doc.run_word_count
    work_off
    assert_equal @doc.word_answer, @doc.get_word_count
  end

  test "000 running a second process within a class" do
    @doc.run_char_count
    work_off
    assert_equal @doc.char_answer, @doc.get_char_count
  end

  test "000 separate class with the same mapreduce process name" do
    @job = Job.new
    @job.mapreduce.word_count(@doc.words)
    work_off
    assert_equal @doc.word_answer, @job.mapreduce.word_count_result
  end

  test "000 running a map reduce process with reduce recoverable fail" do
    @doc.mapreduce.recoverable_test(@doc.words)
    work_off

    # p x = Resque.info[:failed]
    # p Resque::Failure.all(0, x)
    assert_equal @doc.recoverable_answer, @doc.mapreduce.recoverable_test_result
  end
end

context "MapRedus Process" do
  setup do
    MapRedus.redis.flushall
    @process = GetWordCount.create( Document::TEST )
  end

  test "000 process save" do
    @process.mapper = CharCounter
    @process.synchronous = true
    @process.save

    @process = MapRedus::Process.open(@process.pid)
    
    assert_equal @process.mapper, CharCounter
    assert_equal @process.synchronous, true
  end
  
  test "000 process update" do
    @process.update(:mapper => CharCounter, :ordered => true)
    @process = MapRedus::Process.open(@process.pid)
    
    assert_equal @process.mapper, CharCounter
    assert_equal @process.ordered, true
  end
  
  test "000 process delete" do
    @process.delete
    
    proc = MapRedus::Process.open(@process.pid)
    assert_nil proc
  end
  
  test "000 process kill" do
    @process.run
    MapRedus::Process.kill(@process.pid)
    assert_equal 0, Resque.size(:mapredus)
  end

  test "000 process kill halfway" do
    @process.run

    worker = Resque::Worker.new("*")
    worker.perform(worker.reserve)   # do some work
    
    MapRedus::Process.kill(@process.pid)
    assert_equal 0, Resque.size(:mapredus)
  end

  test "000 process kill all" do
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
    assert_equal 0, Resque.size(:mapredus)
    assert Resque.peek(:mapredus, 0, -1).empty?
  end

  test "000 next state responds correctly" do
    assert_equal @process.state, MapRedus::NOT_STARTED
    @process.next_state
    assert_equal @process.state , MapRedus::MAP_IN_PROGRESS
    work_off
    @process.next_state
    assert_equal @process.state , MapRedus::REDUCE_IN_PROGRESS
    work_off
    @process.next_state
    assert_equal @process.state , MapRedus::FINALIZER_IN_PROGRESS
    work_off
    @process.next_state
    assert_equal @process.state , MapRedus::COMPLETE
  end

  test "000 process emit_intermediate unordered" do
    @process.emit_intermediate("hell", "yeah")
    @process.each_key_nonreduced_value do |key, value|
      assert_equal "hell", key
      assert_equal "yeah", value
    end
  end

  test "000 process emit_intermediate ordered" do
    @process.update(:ordered => true)
    @process.emit_intermediate(1, "number", "one")
    @process.emit_intermediate(2, "place", "two")
    res = []
    @process.each_key_nonreduced_value do |key, value|
      res << [key, value]
    end
    
    assert_equal [["number", "one"], ["place", "two"]], res
  end

  test "000 process emit" do 
    @process.emit("something", "reduced")
    @process.each_key_reduced_value do |key, rv|
      assert_equal "something", key
      assert_equal "reduced", rv
    end
  end

  test "000 process save and delete result" do
    @process.save_result({"answer" => "value_json"})
    assert_equal( ({ "answer" => "value_json" }), @process.get_saved_result )

    @process.delete_saved_result
    assert_nil @process.get_saved_result
  end

  test "000 correct map keys produced in an emit_intermediate" do
    @process.emit_intermediate("map key 1", "value")
    @process.emit_intermediate("map key 1", "value")
    @process.emit_intermediate("map key 2", "value")
    assert_equal ["map key 1", "map key 2"], @process.map_keys.map { |k| k }.sort
  end
  
  test "000 correct map/reduce values produced in an emit"
end

context "MapRedus Master" do
  setup do
    "some shit here"
  end

  test "000 handles workers correctly"
  test "000 redundant multiple workers handled correctly"
end

context "MapRedus Mapper" do
  setup do
    "some shit here"
  end

  test "000 maps correctly"
end

context "MapRedus Reducer" do
  setup do
    "some shit here"
  end

  test "000 reduce recoverable fail"
end

context "MapRedus Finalizer" do
  setup do
    "some shit here"
  end

  test "000 finalizes correctly saves"
end
