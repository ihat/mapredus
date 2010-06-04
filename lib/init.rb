require 'redis'
require 'resque'
require 'resque_scheduler'
require 'resque/server'
require 'json'
require 'mapreduce'

# this is where we would start some server shit and get map reduce classes loaded
# and shit in the mapreduce server

# setup the redis server
#

# setup the resque server with the correct class environment
#
use Rack::ShowExceptions
use Rack::ShowStatus
use Rack::Auth::Basic do |u,p|  
  "password" == p
end     

run Resque::Server.new

