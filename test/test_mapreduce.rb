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

  test "001 mapreduce process is created successfully" do
    process = GetWordCount.open(@process.pid)

    assert_equal WordCounter, process.mapper
    assert_equal Adder, process.reducer
    assert_equal ToHash, process.finalizer
    assert_equal MapRedus::JsonOutputter, process.outputter
  end

  test "002 running a map reduce process synchronously" do
    ##
    ## In general map reduce shouldn't be running operations synchronously
    ##
    @process.run(synchronously = true)
    assert_equal @word_count, @process.get_saved_result
  end

  test "003 running a map reduce process asynchronously" do
    @process.run(synchronously = false)
    assert_nil @process.get_saved_result
    work_off

    result = @process.get_saved_result
    assert_equal @word_count, @process.get_saved_result
  end

  test "000 running a map reduce process with reduce recoverable fail" do
    assert_equal true, false
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
    result = @doc.get_word_count
    assert_equal @doc.word_answer, @doc.get_word_count
  end

  test "000 running a second process within a class" do
    @doc.run_char_count
    work_off
    result = @doc.get_char_count
    assert_equal @doc.char_answer, @doc.get_char_count
  end
end

context "MapRedus Process" do
  setup do
    "some shit here"
  end

  test "000 process save"
  test "000 process update"
  test "000 process delete"
  test "000 process kill"
  test "000 process kill all"
  test "000 next state responds correctly"
  test "000 process emit_intermediate"
  test "000 process emit"
  test "000 process save result"
  test "000 delete saved result"
  test "000 correct map keys produced"
  test "000 correct map/reduce values produced"
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
