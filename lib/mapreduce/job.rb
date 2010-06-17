module MapRedus
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
    # Public: Keep track of information that may show up as the redis json value
    #         This is so we know exactly what might show up in the json hash
    attr_reader :pid
    attr_accessor :mapper, :reducer, :finalizer, :data, :extra_data, :ordered, :synchronous

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
      Support.encode(to_hash)
    end

    def save
      FileSystem.sadd( JobInfo.jobs, @pid) 
      FileSystem.save( JobInfo.pid(@pid), to_json)
    end

    def update(attrs = {})
      attrs.each do |attr, val|
        send("#{attr}=", val)
      end
      save
    end

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
      new_pid = get_available_pid
      
      spec = specification( *args )
      return nil unless spec

      Job.new(new_pid, spec).save
      
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

      job.update(:synchronous => synchronous)
      Master.enslave( job.pid, QueueProcess.queue, MapRedus::Master, job.pid )
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
      FileSystem.keys(JobInfo.pid(pid) + "*")
    end
    
    def self.open(pid)
      return pid if pid.is_a?(Job) 
      job = FileSystem.get(JobInfo.pid(pid))
      job && Job.new(pid, Support.decode( job ))
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

      serialized_result = job.finalizer.serialize(result)

      FileSystem.save(JobInfo.result(pid), serialized_result) if pid
      FileSystem.save_temporary(JobInfo.result_custom_key(keyname), serialized_result, 3600 * 24) if keyname
      true
    end

    def self.get_saved_result(keyname)
      FileSystem.get( JobInfo.result_custom_key(keyname) )
    end

    def self.delete_saved_result(keyname)
      FileSystem.del( JobInfo.result_custom_key(keyname) )
    end

    # Find out what map reduce processes are out there
    # 
    # Examples
    #   FileSystem::ps
    #
    # Returns a list of the map reduce process ids
    def self.ps
      FileSystem.smembers(JobInfo.jobs)
    end

    # Find out what map reduce processes are out there
    # 
    # Examples
    #   FileSystem::get_available_pid
    #
    # Returns an avilable pid.
    def self.get_available_pid
      ( ps.map{|pid| pid.to_i}.max || 0 ) + 1 + rand(20)
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
      delete(pid)
      num_killed
    end

    def self.kill_all
      ps.each do |pid|
        kill(pid)
      end
      FileSystem.del(JobInfo.jobs)
    end
    
    # This will not delete if the master is working
    # It can't get ahold of the files to shred while the master is working
    #
    # Examples
    #   delete(pid)
    #   # => true or false
    #
    # Returns true as long as the master is not working.
    def self.delete(pid, safe = true)
      return false if Master.working?(pid)
      FileSystem.keys("mapreduce:job:#{pid}*").each do |k|
        FileSystem.del(k)
      end        
      FileSystem.srem(JobInfo.jobs, pid)
      true
    end

    # The following functions deal with items produced during the
    # running of a job
    
    # Keys that the map operation produced
    #
    # pid  - The process id
    #
    # Examples
    #   map_keys(pid)
    #   # =>
    #
    # Returns the Keys.
    def self.map_keys(pid)
      job = open(pid)
      return unless job
      
      if( not job.ordered )
        FileSystem.smembers( JobInfo.keys(pid) )
      else
        FileSystem.zrange( JobInfo.keys(pid), 0, -1 )
      end
    end

    def self.num_values(pid, key)
      hashed_key = Support.hash(key)
      FileSystem.llen( JobInfo.map(pid, hashed_key) )
    end

    def self.map_values(pid, key)
      hashed_key = Support.hash(key)
      FileSystem.lrange( JobInfo.map(pid, hashed_key), 0, -1 )
    end
    
    def self.reduce_values(pid, key)
      hashed_key = Support.hash(key)
      FileSystem.lrange( JobInfo.reduce(pid, hashed_key), 0, -1 )
    end

    # Emissions, when we get map/reduce results back we emit these 
    # to be stored in our file system (redis)
    #
    # pid_or_job - The process or job id
    # key_value  - The key, value
    #
    # Examples
    #   emit_intermediate(pid, [key, value])
    #   # =>
    #
    # Returns the true on success.
    def self.emit_intermediate(pid_or_job, key_value)
      job = Job.open(pid_or_job)
      return unless job

      if( not job.ordered )
        key, value = key_value
        FileSystem.sadd( JobInfo.keys(job.pid), key )
        hashed_key = Support.hash(key)
        FileSystem.rpush( JobInfo.map(job.pid, hashed_key), value )
      else
        # if there's an order for the job then we should use a zset above
        # ordered job's map emits [rank, key, value]
        #
        rank, key, value = key_value
        FileSystem.zadd( JobInfo.keys(job.pid), rank, key )
        hashed_key = Support.hash(key)
        FileSystem.rpush( JobInfo.map(job.pid, hashed_key), value )
      end
      raise "Key Collision: key:#{key}, #{key.class} => hashed key:#{hashed_key}" if key_collision?(job.pid, hashed_key, key)
      true
    end

    def self.emit(pid, key, reduce_val)
      hashed_key = Support.hash(key)
      FileSystem.rpush( JobInfo.reduce(pid, hashed_key), reduce_val )
    end

    def self.key_collision?(pid, hashed_key, key)
      not ( FileSystem.setnx( JobInfo.hash_to_key(pid, hashed_key), key ) ||
            FileSystem.get( JobInfo.hash_to_key(pid, hashed_key) ) == key.to_s )
    end
    
    # Iterates through the key, values
    # 
    # Example
    #   each_key_value(pid)
    # 
    # Returns nothing.
    def self.each_key_value(pid)
      map_keys(pid).each do |key|
        reduce_values(pid, key).each do |value|
          yield key, value
        end
      end
    end

    def self.each_key_original_value(pid)
      map_keys(pid).each do |key|
        map_values(pid, key).each do |value|
          yield key, value
        end
      end
    end
  end
end
