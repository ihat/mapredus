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
  #     result_timeout : lenght of time a result is saved ## 3600 * 24
  #     keyname : the location to the save the result of the process (cache location)
  #     state : the current state of the process (shouldn't be set by the process and starts off as nil)
  #   }
  #
  # The user has the ability in subclassing this class to create extra features if needed
  # 
  class Process
    # Public: Keep track of information that may show up as the redis json value
    #         This is so we know exactly what might show up in the json hash
    READERS = [:pid]
    ATTRS = [:mapper, :reducer, :finalizer, :outputter, :data, :ordered, :synchronous, :result_timeout, :keyname, :state]
    READERS.each { |r| attr_reader r }
    ATTRS.each { |a| attr_accessor a }

    DEFAULT_TIME = 3600 * 24
    def initialize(pid, json_info)
      @pid = pid
      @json = json_info
      @mapper = Helper.class_get(json_helper(:mapper))
      @reducer = Helper.class_get(json_helper(:reducer))
      @finalizer = Helper.class_get(json_helper(:finalizer))
      @data = json_helper(:data)
      @ordered = json_helper(:ordered)
      @synchronous = json_helper(:synchronous)
      @result_timeout = json_helper(:result_timeout) || DEFAULT_TIME
      @keyname = json_helper(:keyname)
      @state = json_helper(:state)
      @outputter = json_helper(:outputter)
      @outputter = @outputter ? Helper.class_get(@outputter) : MapRedus::Outputter
    end

    def json_helper(key)
      @json[key.to_s] || @json[key.to_sym]
    end

    def to_s; to_json; end

    def to_hash
      (ATTRS + READERS).inject({}) do |h, attr|
        h[attr] = send(attr)
        h 
      end
    end

    def to_json
      Helper.encode(to_hash)
    end

    def save
      FileSystem.sadd( ProcessInfo.processes, @pid ) 
      FileSystem.save( ProcessInfo.pid(@pid), to_json )
      self
    end

    def update(attrs = {})
      attrs.each do |attr, val|
        send("#{attr}=", val)
      end
      save
    end

    # This will not delete if the master is working
    # It can't get ahold of the files to shred while the master is working
    #
    # if safe is set to false, this will delete all the redis stores associated
    # with this process, but will not kill the process from the queue, if it is
    # on the queue.  The process operations will fail to work when its data is deleted 
    #
    # Examples
    #   delete(safe)
    #   # => true or false
    #
    # Returns true as long as the master is not working.
    def delete(safe = true)
      return false if (safe && Master.working?(@pid))
      FileSystem.keys("mapredus:process:#{@pid}*").each do |k|
        FileSystem.del(k)
      end        
      FileSystem.srem(ProcessInfo.processes, @pid)
      FileSystem.set(ProcessInfo.processes_count, 0) if( 0 == FileSystem.scard(ProcessInfo.processes) )
      true
    end

    # Iterates through the key, values
    # 
    # Example
    #   each_key_value(pid)
    # 
    # Returns nothing.
    def each_key_reduced_value
      map_keys.each do |key|
        reduce_values(key).each do |value|
          yield key, value
        end
      end
    end

    def each_key_nonreduced_value
      map_keys.each do |key|
        map_values(key).each do |value|
          yield key, value
        end
      end
    end

    def run( synchronous = false )
      update(:synchronous => synchronous)
      Master.mapreduce( self )
      true
    end

    # TODO:
    # Should also have some notion of whether the process is completed or not
    # since the master might not be working, but the process is not yet complete
    # so it is still running
    #
    def running?
      Master.working?(@pid)
    end

    # Change the process state
    #
    # Examples
    #   process.next_state(pid)
    #
    def next_state
      if((not running?) and (not @synchronous))
        new_state = STATE_MACHINE[self.state]
        update(:state => new_state)
        method = "enslave_#{new_state}".to_sym
        Master.send(method, self) if( Master.respond_to?(method) )
        new_state
      end
    end

    ### The following functions deal with keys/values produced during the
    ### running of a process
    
    # Emissions, when we get map/reduce results back we emit these 
    # to be stored in our file system (redis)
    #
    # key_value  - The key, value
    #
    # Examples
    #   emit_intermediate([key, value])
    #   # =>
    #
    # Returns the true on success.
    def emit_intermediate(key_value)
      if( not @ordered )
        key, value = key_value
        FileSystem.sadd( ProcessInfo.keys(@pid), key )
        hashed_key = Helper.hash(key)
        FileSystem.rpush( ProcessInfo.map(@pid, hashed_key), value )
      else
        # if there's an order for the process then we should use a zset above
        # ordered process's map emits [rank, key, value]
        #
        rank, key, value = key_value
        FileSystem.zadd( ProcessInfo.keys(@pid), rank, key )
        hashed_key = Helper.hash(key)
        FileSystem.rpush( ProcessInfo.map(@pid, hashed_key), value )
      end
      raise "Key Collision: key:#{key}, #{key.class} => hashed key:#{hashed_key}" if key_collision?(hashed_key, key)
      true
    end

    def emit(key, reduce_val)
      hashed_key = Helper.hash(key)
      FileSystem.rpush( ProcessInfo.reduce(@pid, hashed_key), reduce_val )
    end

    def key_collision?(hashed_key, key)
      not ( FileSystem.setnx( ProcessInfo.hash_to_key(@pid, hashed_key), key ) ||
            FileSystem.get( ProcessInfo.hash_to_key(@pid, hashed_key) ) == key.to_s )
    end

    # Saves the result to the specified keyname
    #
    # Example
    #   (mapreduce:process:result:KEYNAME)
    # OR
    #   process:pid:result
    #
    # The client must ensure the the result will not be affected when to_s is applied
    # since redis stores all values as strings
    #
    # Returns true on success.
    def save_result(result)
      res = @outputter.encode(result)
      FileSystem.save(ProcessInfo.result(@pid), res)
      FileSystem.save(ProcessInfo.result_cache(@keyname), res, @result_timeout) if @keyname
      true
    end

    def get_saved_result
      @outputter.decode(Process.get_saved_result(@keyname))
    end

    def delete_saved_result
      Process.delete_saved_result(@keyname)
    end

    # Keys that the map operation produced
    #
    # pid  - The process id
    #
    # Examples
    #   map_keys(pid)
    #   # =>
    #
    # Returns the Keys.
    def map_keys
      if( not @ordered )
        FileSystem.smembers( ProcessInfo.keys(@pid) )
      else
        FileSystem.zrange( ProcessInfo.keys(@pid), 0, -1 )
      end
    end

    def num_values(key)
      hashed_key = Helper.hash(key)
      FileSystem.llen( ProcessInfo.map(@pid, hashed_key) )
    end

    def map_values(key)
      hashed_key = Helper.hash(key)
      FileSystem.lrange( ProcessInfo.map(@pid, hashed_key), 0, -1 )
    end
    
    def reduce_values(key)
      hashed_key = Helper.hash(key)
      FileSystem.lrange( ProcessInfo.reduce(@pid, hashed_key), 0, -1 )
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
    # should be whatever extra information the process may need to have during running
    # 
    # Returns the new process id.
    def self.create( *args )
      new_pid = get_available_pid
      
      spec = specification( *args )
      return nil unless spec

      Process.new(new_pid, spec).save
    end
    
    def self.specification(*args)
      mapper_class, reducer_class, finalizer, outputter, keyname, data = args
      {
        :mapper => mapper_class,
        :reducer => reducer_class,
        :finalizer => finalizer,
        :outputter => outputter,
        :keyname => keyname,
        :data => data
      }
    end

    def self.info(pid)
      FileSystem.keys(ProcessInfo.pid(pid) + "*")
    end
    
    def self.open(pid)
      spec = Helper.decode( FileSystem.get(ProcessInfo.pid(pid)) )
      spec && Process.new( pid, spec )
    end

    # Find out what map reduce processes are out there
    # 
    # Examples
    #   FileSystem::ps
    #
    # Returns a list of the map reduce process ids
    def self.ps
      FileSystem.smembers(ProcessInfo.processes)
    end

    # Find out what map reduce processes are out there
    # 
    # Examples
    #   FileSystem::get_available_pid
    #
    # Returns an avilable pid.
    def self.get_available_pid
      FileSystem.incrby(ProcessInfo.processes_count, 1 + rand(20)) 
    end

    def self.get_saved_result(keyname)
      FileSystem.get( ProcessInfo.result_cache(keyname) )
    end

    def self.delete_saved_result(keyname)
      FileSystem.del( ProcessInfo.result_cache(keyname) )
    end
    
    # Remove redis keys associated with this process if the Master isn't working.
    #
    # potentially is very expensive.
    #
    # Example
    #   Process::kill(pid)
    #   # => true
    #
    # Returns true on success.
    def self.kill(pid)
      num_killed = Master.emancipate(pid)
      Process.open(pid).delete
      num_killed
    end

    def self.kill_all
      ps.each do |pid|
        kill(pid)
      end
      FileSystem.del(ProcessInfo.processes)
      FileSystem.del(ProcessInfo.processes_count)
    end
  end
end
