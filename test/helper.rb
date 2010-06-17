require 'rubygems'
require 'test/unit'

dir = File.dirname(__FILE__)
$LOAD_PATH.unshift(File.join(dir, '..', 'lib'))
$LOAD_PATH.unshift(dir)
require 'mapreduce'

##
# test/spec/mini 3
# http://gist.github.com/25455
# chris@ozmm.org
#
def context(*args, &block)
  return super unless (name = args.first) && block
  require 'test/unit'
  klass = Class.new(defined?(ActiveSupport::TestCase) ? ActiveSupport::TestCase : Test::Unit::TestCase) do
    def self.test(name, &block)
      define_method("test_#{name.gsub(/\W/,'_')}", &block) if block
    end
    def self.xtest(*args) end
    def self.setup(&block) define_method(:setup, &block) end
    def self.teardown(&block) define_method(:teardown, &block) end
  end
  (class << klass; self end).send(:define_method, :name) { name.gsub(/\W/,'_') }
  klass.class_eval &block
end

#
# make sure we can run redis
#
if !system("which redis-server")
  puts '', "** can't find `redis-server` in your path"
  puts "** try running `sudo rake install`"
  abort ''
end

#
# start our own redis when the tests start,
# kill it when they end
#
at_exit do
  next if $!

  if defined?(MiniTest)
    exit_code = MiniTest::Unit.new.run(ARGV)
  else
    exit_code = Test::Unit::AutoRunner.run
  end

  pid = `ps -A -o pid,command | grep [r]edis-test`.split(" ")[0]
  puts "Killing test redis server..."
  `rm -f #{dir}/dump.rdb`
  Process.kill("KILL", pid.to_i)
  # exit exit_code
end

puts "Starting redis for testing at localhost:9736..."
`/Users/john/main/code/redis/redis-server #{dir}/redis-test.conf`

MapRedus.redis = 'localhost:9736:0'
Resque.redis = MapRedus.redis
require 'resque/failure/redis'
Resque::Failure.backend = Resque::Failure::Redis

require 'helper_classes'
class GetWordCount < MapRedus::Job
  def self.specification(data)
    {
      :mapper => WordCounter,
      :reducer => Adder,
      :finalizer => ToHash,
      :data => data,
      :ordered => false,
      :extra_data => {}
    }
  end
end

class WordCounter < MapRedus::Mapper
  def self.partition_size; 1; end
  def self.map(map_data)
    map_data.join(" ").split(/\W/).each do |word|
      next if word.empty?
      yield(word.downcase, 1)
    end
  end
end

class Adder < MapRedus::Reducer
  def self.reduce(value_list)
    yield( value_list.reduce(0) { |r, v| r += v.to_i } )
  end
end

class ToHash < MapRedus::Finalizer
  def self.finalize(pid)
    result = {}
    each_key_value(pid) do |key, value|
      result[key] = value.to_i
    end
    MapRedus::Job.save_result(MapReduce::Support.encode(result), pid, "test:result")
    MapRedus::Job.delete(pid)
    result
  end
end
