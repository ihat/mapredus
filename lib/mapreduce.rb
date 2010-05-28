# requirements:
#   Redis
#   Resque
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
#       
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
    def self.perform(*args); raise InvalidProcess ;end
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

  # This is what keeps track of our map reduce processes
  #  
  # We use a redis key to identify the id of map reduce process
  # the value of the redis object is a json object which contains: 
  # 
  #   {
  #     mapper : mapclass,
  #     reducer : reduceclass,
  #     finalizer : finalizerclass,
  #     partitioner : <not supported>,
  #     combiner : <not supported>,
  #     ordered : true_or_false   ## ensures ordering keys from the map output --> [ order, key, value ],
  #     synchronous : true_or_false   ## runs the process synchronously or not (generally used for testing)
  #     extra_data : extra data in a hash that the job possibly needs
  #   }
  # 
  class Job
    
    # Manages the book keeping of redis keys and redis usage
    # All interaction with redis should go through this class
    # 
    class Manager < QueueProcess
      include RedisSupport

      redis_key :pid, "map_reduce_job:PID"

      # Holds the current map reduce processes that are either running or which still have data lyrin around
      #
      redis_key :jobs, "map_reduce:jobs"

      # All the keys that the map produced
      #
      redis_key :keys, "map_reduce_job:PID:keys"

      # The hashed key to actual string value of key
      #
      redis_key :hash_to_key, "map_reduce_job:PID:keys:HASHED_KEY" # to ACTUAL KEY
            
      # The key generated by our map function.
      # When a reduce is run it takes elements from this key and pushes them to :reduce
      #
      # key - list of values
      #
      redis_key :map, "map_reduce_job:PID:map_key:HASHED_KEY"
      redis_key :reduce, "map_reduce_job:PID:map_key:HASHED_KEY:reduce"
      
      # Temporary redis space for reduce functions to use
      #
      redis_key :temp, "map_reduce_job:PID:temp_reduce_key:HASHED_KEY:UNIQUE_REDUCE_HOSTNAME:UNIQUE_REDUCE_PROCESS_ID"

      # If we want to hold on to our final data we have a key to put that data in
      # In normal map reduce we would just be outputting files
      #
      redis_key :result, "map_reduce_job:PID:result"
      redis_key :result_custom_key, "map_reduce:result:KEYNAME"

      def self.all_info(pid)
        redis.keys(Keys.pid(pid) + "*")
      end
      
      def self.authorize(pid, spec)
        redis.sadd(Keys.jobs, pid)
        redis.set(Keys.pid(pid), spec.to_json)
      end

      def self.get_job(pid)
        redis.get(Keys.pid(pid))
      end

      # Keys that the map operation produced
      #
      # pid  - The process id
      #
      # Examples
      #   Job::Manager::get_keys(pid)
      #   # =>
      #
      # Returns the Keys.
      def self.get_keys(pid)
        job = Job.open(pid)
        return unless job
        
        if( not job.ordered )
          redis.smembers( Keys.keys(pid) )
        else
          redis.zrange( Keys.keys(pid), 0, -1 )
        end
      end

      def self.num_values(pid, key)
        hashed_key = Support.hash(key)
        redis.llen( Keys.map(pid, hashed_key) )
      end

      def self.get_values(pid, key)
        hashed_key = Support.hash(key)
        redis.lrange( Keys.map(pid, hashed_key), 0, -1 )
      end

      
      def self.get_reduced_values(pid, key)
        hashed_key = Support.hash(key)
        redis.lrange( Keys.reduce(pid, hashed_key), 0, -1 )
      end

      # Emissions, when we get map/reduce results back we emit these 
      # to be stored in our file system (redis)
      #
      # pid_or_job - The process or job id
      # key_value  - The key, value
      #
      # Examples
      #   Job::Manager::emit_intermediate(pid, [key, value])
      #   # =>
      #
      # Returns the true on success.
      def self.emit_intermediate(pid_or_job, key_value)
        job = Job.open(pid_or_job)
        return unless job

        if( not job.ordered )
          key, value = key_value
          redis.sadd( Keys.keys(job.pid), key )
          hashed_key = Support.hash(key)
          redis.rpush( Keys.map(job.pid, hashed_key), value )
        else
          # if there's an order for the job then we should use a zset above
          # ordered job's map emits [rank, key, value]
          #
          rank, key, value = key_value
          redis.zadd( Keys.keys(job.pid), rank, key )
          hashed_key = Support.hash(key)
          redis.rpush( Keys.map(job.pid, hashed_key), value )
        end
        raise "Key Collision: key:#{key}, #{key.class} => hashed key:#{hashed_key}" if key_collision?(job.pid, hashed_key, key)
        true
      end

      def self.emit(pid, key, reduce_val)
        hashed_key = Support.hash(key)
        redis.rpush( Keys.reduce(pid, hashed_key), reduce_val )
      end

      def self.reduced?(pid, key)
        hashed_key = Support.hash(key)
        redis.exists( Keys.reduce(pid, hashed_key) )
      end

      def self.key_collision?(pid, hashed_key, key)
        not ( redis.setnx( Keys.hash_to_key(pid, hashed_key), key ) ||
              redis.get( Keys.hash_to_key(pid, hashed_key) ) == key.to_s )
      end
      
      # Filing a report saves it in the appropriate place, given the info to do so
      #
      # Returns nothing.
      def self.file_report(finalizer, result, pid, keyname)
        rep = finalizer.serialize(result)
        redis.set(Keys.result(pid), rep) if pid
        if keyname
          redis.set(Keys.result_custom_key(keyname), rep) 
          redis.expire(Keys.result_custom_key(keyname), 3600 * 24)
        end
      end

      def self.retrieve_report(keyname)
        redis.get(Keys.result_custom_key(keyname))
      end
      
      def self.shred_report(keyname)
        redis.del(Keys.result_custom_key(keyname))
      end

      # Setup locks on results
      #
      # Examples
      #   Job::Manager::has_lock?(keyname)
      #   # => true or false 
      #
      # Returns true if there's a lock
      def self.has_lock?(keyname)
        "1" == redis.get(lock_key(Keys.result_custom_key(keyname)))
      end
      
      def self.acquire_lock(keyname)
        redis.setnx( lock_key(Keys.result_custom_key(keyname)), 1)
      end
      
      def self.release_lock(keyname)
        redis.del(lock_key(Keys.result_custom_key(keyname)))
      end

      def self.lock_key( key_to_lock )
        "lock.#{key_to_lock}"
      end

      # This will not delete if the master is working
      # It can't get ahold of the files to shred while the master is working
      #
      # Examples
      #   Job::Manager::shred(pid)
      #   # => true or false
      #
      # Returns true as long as the master is not working.
      def self.shred(pid)
        return false if Master.working?(pid)
        redis.keys("map_reduce_job:#{pid}*").each { |k| redis.del(k) }
        redis.srem(Keys.jobs, pid)        
        true
      end

      def self.destroy_all
        redis.smembers(Keys.jobs).each do |pid|
          Job.kill(pid)
        end
        redis.del(Keys.jobs)
      end

      # Find out what map reduce processes are out there
      # 
      # Examples
      #   Job::Manager::get_available_pid
      #
      # Returns an avilable pid.
      def self.get_available_pid
        ( redis.smembers(Keys.jobs).map{|pid| pid.to_i}.max || 0 ) + 1 + rand(20)
      end

      # Find out what map reduce processes are out there
      # 
      # Examples
      #   Job::Manager::ps
      #
      # Returns the process?? maybe?
      def self.ps
        redis.smembers(Keys.jobs)
      end

      # These temp functions are meant to allocate local space
      # for reduce operations to operate on values without having to worry about other workers
      # operating on the same information, i.e. the manager provides support workers (temps)
      # who help do work until it is reclaimed by the manager.
      # 
      # DEPRECATED: No longer in use, because we don't make the assumption that a
      # single reducer does all the work on a single key.
      #
      # Example
      #   Job::Manager::hire_temp(pid, key, agency, name, work)
      #   # => [hashed_key, tempid]
      #
      # Returns an array of the hashed_key and the temp_id.
      def self.hire_temp(pid, key, agency, name, work)
        hashed_key = Support.hash(key)
        temp_id = Keys.temp(pid, hashed_key, agency, name)
        assign_temp( work, Keys.map(pid, hashed_key), temp_id)
        return hashed_key, temp_id
      end

      def self.assign_temp( orig_key, temp_id )
        redis.llen(orig_key).times do 
          redis.rpoplpush( orig_key, temp_id ) 
        end
      end

      def self.get_temp_work(temp_id)
        redis.lrange(temp_id, 0, -1)
      end

      def self.fire_temp(temp_id)
        redis.del(temp_id)
      end
    end

    # Note: Instead of using Resque directly within the job, we implement
    # a master interface with Resque
    #
    # Does bookkeeping to keep track of how many slaves are doing work. If we have
    # no slaves doing work for a job then the job is done. While there is work available
    # the slaves will always be doing work.
    #
    class Master < Manager
      redis_key :slaves, "map_reduce_job:PID:master:slaves"
      redis_key :started_at, "map_reduce_job:PID:started_at"
      redis_key :finished_at, "map_reduce_job:PID:finished_at"
      redis_key :requested_at, "map_reduce_job:PID:request_at"
      redis_key :recent_time_to_complete, "map_reduce_job:recent_time_to_complete"
      
      def self.partition_data( arr, partition_size )
        0.step(arr.size, partition_size) do |i|
          yield arr[i...(i + partition_size)]
        end
      end

      def self.perform( pid )
        free_slave(pid)
        enslave_mappers(pid)
      end
      
      def self.enslave_mappers( pid )
        job = Job.open(pid)
        return unless job
        start_metrics(pid)
        
        partition_data( job.data, job.mapper.partition_size ) do |data_chunk|
          unless data_chunk.empty?
            enslave_map( pid, data_chunk )
          end
        end
      end
      
      def self.enslave_reducers( pid )
        job = Job.open(pid)
        return unless job
        Manager.get_keys(pid).each do |key|
          enslave_reduce( pid, key )
        end
      end

      # Have these to match what the Mapper/Reducer perform function expects to see as arguments
      #
      def self.enslave_map(pid, data_chunk)
        job = Job.open(pid)
        return unless job
        enslave( pid, job.mapper.queue, job.mapper, pid, data_chunk )
      end
      
      def self.enslave_reduce(pid, key)
        job = Job.open(pid)
        return unless job
        enslave( pid, job.reducer.queue, job.reducer, pid, key )
      end

      def self.enslave_finalizer( pid )
        job = Job.open(pid)
        return unless job
        enslave( pid, job.finalizer.queue, job.finalizer, pid )
      end

      # Possible priorites
      #   :mapreduce_mapper > :mapreduce_reducer
      #
      def self.enslave( pid, priority, klass, *args )
        job = Job.open(pid)
        return unless job

        redis.rpush(Keys.slaves(pid), 1)
        
        if( job.synchronous )
          klass.perform(*args)
        else
          set_request_time(pid)
          Resque.push( priority, { :class => klass.to_s, :args => args } )
        end
      end

      def self.free_slave(pid)
        redis.lpop(Keys.slaves(pid))
      end

      def self.emancipate(pid)
        job = Job.open(pid)
        return unless job
        
        # Working on resque directly is dangerous
        #
        # FIXME:
        # returns the number of jobs killed on the queue
        #
        destroyed = 0
        qs = [queue, job.mapper.queue, job.reducer.queue, job.finalizer.queue].uniq
        qs.each do |q|
          q_key = "resque:queue:#{q}"
          redis.lrange(q_key, 0, -1).each do | string |
            json   = QueueProcess.new.decode(string)
            match  = json['class'] == "MapRedus::Job::Master"
            match |= json['class'] == job.mapper.to_s
            match |= json['class'] == job.reducer.to_s
            match |= json['class'] == job.finalizer.to_s
            match &= json['args'][0] == job.pid
            if match
              destroyed += redis.lrem(q_key, 0, string).to_i
            end
          end
        end

        redis.del(Keys.slaves(pid))
        destroyed
      end

      def self.working?(pid)
        0 < redis.llen(Keys.slaves(pid))
      end

      def self.set_request_time(pid)
        redis.set( Keys.requested_at(pid), Time.now.to_i )
      end

      def self.start_metrics(pid)
        job = Job.open(pid)
        return unless job

        started  = Keys.started_at( pid )
        redis.set started, Time.now.to_i
      end

      def self.finish_metrics(pid)
        job = Job.open(pid)
        return unless job

        started  = Keys.started_at( pid )
        finished = Keys.finished_at( pid )
        requested = Keys.requested_at( pid )
        
        completion_time = Time.now.to_i

        redis.set finished, completion_time
        time_to_complete = completion_time - redis.get(started).to_i

        recent_ttcs = Keys.recent_time_to_complete
        redis.lpush( recent_ttcs , time_to_complete )
        redis.ltrim( recent_ttcs , 0, 30 - 1)

        redis.expire finished, 60 * 60
        redis.expire started, 60 * 60
        redis.expire requested, 60 * 60
      end
    end

    # Public: Keep track of information that may show up as the redis json value
    #         This is so we know exactly what might show up in the json hash
    #
    # Returns the pid.
    attr_reader :pid
    
    
    # Public: Get the mapper.
    #
    # Returns the mapper.
    attr_reader :mapper
    
    # Set the mapper
    #
    # Returns nothing.
    attr_writer :mapper
    
    
    # Public: Get the reducer.
    #
    # Returns the reducer.    
    attr_reader :reducer
    
    # Sets the reducer.
    #
    # Returns nothing.
    attr_writer :reducer
    
    # Public: Get the finalizer.
    # 
    # Returns the finalizer.
    attr_reader :finalizer
    
    # Sets the finalizer.
    # 
    # Returns nothing.
    attr_writer :finalizer

    # Public: Get the data.
    # 
    # Returns the data.
    attr_reader :data
    
    # Sets the data
    # 
    # Returns nothing.
    attr_writer :data
    
    # Public: Get the extra_data.
    # 
    # Returns the extra_data.
    attr_reader :extra_data
    
    # Sets the extra_data.
    # 
    # Returns nothing.
    attr_writer :extra_data

    # Public: Get the ordered
    # 
    # Returns the ordered.
    attr_reader :ordered
    
    # Sets the ordered.
    # 
    # Returns nothing.
    attr_writer :ordered

    # Public: Get the the synchronous.
    # 
    # Retruns the synchronous.
    attr_reader :synchronous
    
    # Sets the synchronous.
    # 
    # Returns nothing.
    attr_writer :synchronous

    
    def initialize(pid, json_info)
      @pid = pid
      @json = json_info
      @mapper = Support.class_get(@json["mapper"])
      @reducer = Support.class_get(@json["reducer"])
      @finalizer = Support.class_get(@json["finalizer"])
      @data = @json["data"]
      @ordered = @json["ordered"]
      @extra_data = @json["extra_data"]
      @synchronous = @json["synchronous"]
    end
    def to_s; to_json; end
    def to_hash
      {
        :pid => @pid,
        :mapper => @mapper,
        :reducer => @reducer,
        :finalizer => @finalizer,
        :data => @data,
        :ordered => @ordered,
        :extra_data => @extra_data,
        :synchronous => @synchronous
      }
    end
    def to_json
      to_hash.to_json
    end
    def save; Manager.authorize(@pid, to_hash); end

    # Map and Reduce are strings naming the Mapper and Reducer
    # classes we want to run our map reduce with.
    # 
    # For instance
    #   Mapper = "Mapper"
    #   Reducer = "Reducer"
    # 
    # Data represents the initial input data, currently we assume
    # that data is an array.
    # 
    # Default finalizer
    #   "MapRedus::Finalizer"
    # 
    # The options will go into the extra_data section of the spec and
    # should be whatever extra information the job may need to have during running
    # 
    # Returns the new process id.
    def self.create( *args )
      new_pid = Manager.get_available_pid
      
      spec = specification( *args )
      return nil unless spec
      
      Manager.authorize(new_pid, spec)
      new_pid
    end
    
    def self.specification(*args)
      mapper_class, reducer_class, finalizer, data, ordered, opts = args
      
      {
        :mapper => mapper_class,
        :reducer => reducer_class,
        :finalizer => finalizer,
        :data => data,
        :ordered => ordered,
        :extra_data => opts
      }
    end

    def self.run( pid, synchronous = false )
      job = Job.open(pid)
      return unless job

      job.synchronous = synchronous
      job.save
      Master.enslave( job.pid, QueueProcess.queue, MapRedus::Job::Master, job.pid )
      true
    end

    # TODO:
    # Should also have some notion of whether the job is completed or not
    # since the master might not be working, but the job is not yet complete
    # so it is still running
    #
    def self.running?(pid)
      job = Job.open(pid)
      return false unless job
      Master.working?(pid)
    end

    def self.info(pid)
      Manager.all_info(pid)
    end
    
    def self.open(pid)
      return pid if pid.is_a?(Job) 
      job = Manager.get_job(pid)
      job && Job.new(pid, JSON.parse( job ))
    end

    # If the master is not working, runs your finalizer on the results of the mapreduce
    #
    def self.result(pid, keyname = nil)
      job = Job.open(pid)
      return unless job
      
      if( Master.working?(pid) ) 
        nil
      else
        job.finalizer.finalize(pid)
      end
    end

    # Saves the result to the specified keyname
    #
    # Example
    #   (map_reduce_job:result:KEYNAME)
    # OR
    #   job:pid:result
    #
    # The finalizer is needed so that we know how to correctly
    # serialize the result as a string.
    #
    # Returns true on success.
    def self.save_result(result, pid, keyname)
      job = Job.open(pid)
      return unless job
      Manager.file_report(job.finalizer, result, job.pid, keyname)
      true
    end

    def self.get_saved_result(keyname)
      Manager.retrieve_report(keyname)
    end

    def self.delete_saved_result(keyname)
      Manager.shred_report(keyname)
    end

    # Remove redis keys associated with this job if the Master isn't working.
    #
    # Example
    #   Master::kill(pid)
    #   # => true
    #
    # Returns true on success.
    def self.kill(pid)
      num_killed = Master.emancipate(pid)
      cleanup(pid)
      num_killed
    end
    
    def self.cleanup(pid)
      Manager.shred(pid)
    end

    # Iterates through the key, values
    # 
    # Example
    #   Master::each_key_value(pid)
    # 
    # Returns nothing.
    def self.each_key_value(pid)
      Manager.get_keys(pid).each do |key|
        Manager.get_reduced_values(pid, key).each do |value|
          yield key, value
        end
      end
    end

    def self.each_key_original_value(pid)
      Manager.get_keys(pid).each do |key|
        Manager.get_values(pid, key).each do |value|
          yield key, value
        end
      end
    end
  end

  # Map is a function that takes a data chunk
  # where each data chunk is a list of pieces of your raw data
  # and emits a list of key, value pairs.
  #
  # The output of th emap shall always be
  #   [ [key, value], [key, value], ... ]
  #
  # If the order is important change redis.sadd to use a zset.
  #
  # Note: Values must be string, integers, booleans, or floats.
  # i.e., They must be primitive types since these are the only
  # types that redis supports and since anything inputted into
  #
  # redis becomes a string.
  class Mapper < QueueProcess    
    def self.partition_size
      30
    end

    def self.map(data_chunk); raise InvalidMapper ;end
    
    def self.perform(pid, data_chunk)
      Job::Master.free_slave( pid )
      
      job = Job.open(pid)
      return unless job
      
      map_result = map( data_chunk ) do |*key_value|
        Job::Manager.emit_intermediate(pid, key_value)
      end
      
      if ( not Job::Master.working?(pid) )
        # This means all the map jobs have finished
        # so now we can start all of the reducers
        # TODO: wonky, should take this book keeping out of here
        #        
        Job::Master.enslave_reducers( pid )
      end
      
      map_result
    end
  end

  # Reduce is a function that takes in "all" the values for a single given key
  # and outputs a list of values or a single value that usually "reduces"
  # the initial given value set.
  #
  # The output of the reduce shall always be
  #   reduce(values) = [ reduced value, reduced value, ... ]
  # and it will often only be a single element array
  #
  # The input values and the output values of the reduce will always
  # be a string. As described in the paper, it is up to the client
  # to define how to deal with this restriction.
  #
  class Reducer < QueueProcess
    
    # Ensure all mappers finished their work before reducers started doing work./
    # Skewed distributions would be dealt with a combiner, help deal with reducers doing too much work.
    # Don't have to worry about associativity.
    # 
    # All the values for a given key are process by a single reduce.
    # Fault tolerance and redundancy are not handled.
    # 
    # When a reduce is made, if it finishes then the reduce operation for that key is complete.
    #
    def self.associative?; true; end
    def self.reduce_unwrap(value)
      JSON.parse(value)["_reduced"]
    end
    def self.reduce_wrap(value)
      associative? ? value : { :_reduced => value }.to_json
    end
    def self.reduced?(value)
      r = reduce_unwrap(value)
      r.is_a?(String) && r
    end

    def self.reduce(values); raise InvalidReducer ;end
    
    # Doesn't handle redundant workers and fault tolerance
    #
    def self.perform(pid, key)
      Job::Master.free_slave(pid)
      
      begin
        job = Job.open(pid)
        return unless job
        reduction = reduce(Job::Manager.get_values(pid, key)) do |reduce_val|
          Job::Manager.emit( pid, key, reduce_val )
        end
      rescue MapRedus::RecoverableFail
        Job::Master.enslave_reduce(pid, key)
      end

      if ( not Job::Master.working?(pid) )
        # This means all the reduce jobs have finished
        # so now we can start the finalization process
        # TODO: wonky, should take this book keeping out of here
        #
        Job::Master.enslave_finalizer( pid )
      end
      
      reduction
    end
  end

  # Run the stuff you want to run at the end of the job.
  # Define subclass which defines self.finalize and self.serialize
  # to do what is needed when you want to get the final output
  # out of redis and into ruby.
  #
  # This is basically the message back to the user program that a
  # job is completed storing the necessary info.
  #
  class Finalizer < QueueProcess
    def self.each_key_value(pid)
      Job.each_key_value(pid) do |key, value|
        yield key, value
      end
    end

    def self.each_key_original_value(pid)
      Job.each_key_original_value(pid) do |key, value|
        yield key, value
      end
    end
    
    # The default finalizer is to notify of job completion
    #
    # Example
    #   Finalizer::finalize(pid)
    #   # => "MapRedus Job : 111 : has completed"
    #
    # Returns a message notification
    def self.finalize(pid)
      "MapRedus Job : #{pid} : has completed"
    end

    def self.serialize(result); result; end
    def self.deserialize(serialized_result); serialized_result; end

    def self.perform(pid)
      Job::Master.free_slave(pid)
      
      job = Job.open(pid)
      return unless job
      result = finalize(pid)
      Job::Master.finish_metrics(pid)
      
      result
    end
  end
end
