# requirements:
#   Redis
#   Resque
#   Resque-scheduler
#   json parser
# 
# Notes:
#   - Instead of calling "emit_intermediate"/"emit" in your map/reduce to
#     produce a key value pair/value you call yield, which will call
#     emit_intermediate/emit for you.  This gives flexibility in using
#     Mapper/Reducer classes especially in testing.
#
# not necessarily in the given order
# TODO: * if a job fails we do what we are supposed to do
#         i.e. add a failure_hook which does something if your job fails
#       * add an on finish hook or something to notify the user when the program
#         finishes
#       * include functionality for a partitioner
#       * include functionality for a combiner 
#       * implement this shit so that we can run mapreduce commands from
#         the command line.  Defining any arbitrary mapper and reducer.
#      ** implement redundant workers (workers doing the same work in case one of them fails)
# ******* edit emit for when we have multiple workers doing the same reduce
#         (redundant workers for fault tolerance might need to change
#         the rpush to a lock and setting of just a value)
#         even if other workers do work on the same answer, want to make sure
#         that the final reduced thing is the same every time
#   ***** Add fault tolerance, better tracking of which workers fail, especially
#         when we have multiple workers doing the same work
#         ... currently is handled by Resque failure auto retry
#     *** if a perform operation fails then we need to have worker recover
#       * make use of finish_metrics somewhere so that we can have statistics on how
#         long map reduce jobs take
#    **** better tracking of work being assigned so we can know when a job is finished
#         or in progress and have a trigger to do things when shit finishes
#         - in resque there is functionality for an after hook
#           which performs something after your job does it's work
#       * ensure reducers only do a fixed amount of work
#         See section 3.2 of paper. bookkeeping
#         that tells the master when tasks are in-progress or completed.
#         this will be important for better paralleziation of tasks
#  ****** think about the following logic
#         if a reducer starts working on a key after all maps have finished
#           then when it is done the work on that key is finished forerver
#         this would imply a job finishes when all map tasks have finished
#           and all reduce tasks that start after the map tasks have finished
#         if a reducer started before all map tasks were finished, then
#           load its reduced result back onto the value list
#         if the reducer started after all map tasks finished, then emit
#           the result
#     *** refactor manager/master so that master talks to a "filesystem" (we'll create this filesystem module/class)
#
module MapRedus
  class InvalidProcess < Exception
    def initialize; super("MapRedus QueueProcess: need to have perform method defined");end
  end

  class InvalidMapper < Exception
    def initialize; super("MapRedus Mapper: need to have map method defined");end
  end

  class InvalidReducer < Exception
    def initialize; super("MapRedus Reducer: need to have reduce method defined");end
  end

  class InvalidJob < Exception
    def initialize; super("MapRedus Job Creation Failed: Specifications were not specified");end
  end

  class RecoverableFail < Exception
    def initialize; super("MapRedus Operation Failed: but it is recoverable") ;end
  end
  
  # All Queue Processes should have a function called perform
  # ensuring that when the class is put on the resque queue it can perform its work
  # 
  # Caution: defines redis, which is also defined in RedisSupport
  # 
  class QueueProcess
    include Resque::Helpers
    def self.queue; :mapreduce; end
    def self.perform(*args); raise InvalidProcess; end
  end  

  # TODO: When you send work to a worker using a mapper you define, 
  # the worker won't have that class name defined, unless it was started up
  # with the class loaded
  #
  def self.register_reducer(klass); end;
  def self.register_mapper(klass); end;

  class Support
    # Defines a hash by taking the absolute value of ruby's string
    # hash to rid the dashes since redis keys should not contain any.
    #
    # key - The key to be hashed.
    #
    # Examples
    #
    #   Support::hash( key )
    #   # => '8dd8hflf8dhod8doh9hef'
    #
    # Returns the hash.
    def self.hash( key )
      key.to_s.hash.abs.to_s(16)
    end

    # Returns the classname of the namespaced class.
    #
    # The full name of the class.
    #
    # Examples
    #
    #   Support::class_get( Super::Long::Namespace::ClassName )
    #   # => 'ClassName'
    #
    # Returns the class name.
    def self.class_get(string)
      string.is_a?(String) ? string.split("::").inject(Object) { |r, n| r.const_get(n) } : string
    end
  end
end

require 'mapreduce/job'
require 'mapreduce/job/manager'
require 'mapreduce/job/master'
require 'mapreduce/mapper'
require 'mapreduce/reducer'
require 'mapreduce/finalizer'
