$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "/"))
require 'mapreduce'

# this is where we would start some server shit and get map reduce classes loaded
# and shit in the mapreduce server

# setup the redis server
#
MapRedus.redis = 'localhost:6379:0'
Resque.redis = MapRedus.redis
# setup the resque server with the correct class environment
#
# use Rack::ShowExceptions
# use Rack::ShowStatus
# use Rack::Auth::Basic do |u,p|  
#   "password" == p
# end     

# run Resque::Server.new

