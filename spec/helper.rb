require 'rubygems'
require 'spec'

puts "clearly does not work, does not load the redis server.."

dir = File.dirname(__FILE__)
$LOAD_PATH.unshift(File.join(dir, '..', 'lib'))
$LOAD_PATH.unshift(dir)
require 'mapredus'

#
# make sure we can run redis
#
if !system("which redis-server")
  puts '', "** can't find `redis-server` in your path"
  abort ''
end

#
# start our own redis when the tests start,
# kill it when they end (redis is run as a daemon)
#
puts "Starting redis for testing at localhost:9736..."
`redis-server #{dir}/redis-test.conf`

at_exit do
  #
  # hope that no other processes have redis-test in the name...
  # TODO: fixme
  #
  pid = `ps -A -o pid,command | grep [r]edis-test`.split(" ")[0]
  puts "Killing test redis server..."
  `rm -f #{dir}/dump.rdb`
  Process.kill("KILL", pid.to_i)
end

#
# Set the redis server
#
MapRedus.redis = 'localhost:9736:0'
Resque.redis = MapRedus.redis
require 'resque/failure/redis'
Resque::Failure.backend = Resque::Failure::Redis

require 'helper_classes'

def work_off
  Resque::Worker.new("*").work(0)
end
