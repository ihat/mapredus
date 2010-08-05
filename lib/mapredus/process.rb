module MapRedus

  # This is what keeps track of our map reduce processes
  #  
  # We use a redis key to identify the id of map reduce process
  # the value of the redis object is a json object which contains: 
  # 
  #   {
  #     inputter : inputstreamclass,
  #     mapper : mapclass,
  #     reducer : reduceclass,
  #     finalizer : finalizerclass,
  #     outputter : outputterclass,
  #     partitioner : <not supported>,
  #     combiner : <not supported>,
  #     ordered : true_or_false   ## ensures ordering keys from the map output --> [ order, key, value ],
  #     synchronous : true_or_false   ## runs the process synchronously or not (generally used for testing)
  #     result_timeout : lenght of time a result is saved ## 3600 * 24
  #     key_args : arguments to be added to the key location of the result save (cache location)
  #     state : the current state of the process (shouldn't be set by the process and starts off as nil)
  #     type : the original process class ( currently this is needed so we can have namespaces for the result_cache keys )
  #   }
  #
  # The user has the ability in subclassing this class to create extra features if needed
  # 
  class Process
    # Public: Keep track of information that may show up as the redis json value
    #         This is so we know exactly what might show up in the json hash
    READERS = [:pid]
    ATTRS = [:inputter, :mapper, :reducer, :finalizer, :outputter, :ordered, :synchronous, :result_timeout, :key_args, :state, :type]
    READERS.each { |r| attr_reader r }
    ATTRS.each { |a| attr_accessor a }

    DEFAULT_TIME = 3600 * 24
    def initialize(pid, json_info)
      @pid = pid
      read(json_info)
    end

    def read(json_info)
      @inputter = Helper.class_get(json_helper(json_info, :inputter))
      @mapper = Helper.class_get(json_helper(json_info, :mapper))
      @reducer = Helper.class_get(json_helper(json_info, :reducer))
      @finalizer = Helper.class_get(json_helper(json_info, :finalizer))
      @ordered = json_helper(json_info, :ordered)
      @synchronous = json_helper(json_info, :synchronous)
      @result_timeout = json_helper(json_info, :result_timeout) || DEFAULT_TIME
      @key_args = json_helper(json_info, :key_args) || []
      @state = json_helper(json_info, :state) || NOT_STARTED
      @outputter = json_helper(json_info, :outputter)
      @outputter = @outputter ? Helper.class_get(@outputter) : MapRedus::Outputter
      @type = Helper.class_get(json_helper(json_info, :type) || Process)
    end

    def json_helper(json_info, key)
      json_info[key.to_s] || json_info[key.to_sym]
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

    def reload
      read(Helper.decode(FileSystem.get(ProcessInfo.pid(@pid))))
      self
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
    #   each_key_reduced_value(pid)
    # 
    # Returns nothing.
    def each_key_reduced_value
      map_keys.each do |key|
        reduce_values(key).each do |value|
          yield key, value
        end
      end
    end

    # Iterates through the key, values
    # 
    # Example
    #   each_key_nonreduced_value(pid)
    # 
    # Returns nothing.
    def each_key_nonreduced_value
      map_keys.each do |key|
        map_values(key).each do |value|
          yield key, value
        end
      end
    end

    def run( data_object, synchronous = false )
      update(:synchronous => synchronous)
      Master.mapreduce( self, data_object )
      true
    end

    # TODO:
    # Should also have some notion of whether the process is completed or not
    # since the master might not be working, but the process is not yet complete
    # so it is still running
    def running?
      Master.working?(@pid)
    end

    # Change the process state
    # if the process is not running and is not synchronous
    #
    # Examples
    #   process.next_state(pid)
    #
    # returns the state that the process switched to (or stays the same)
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
    #   emit_intermediate(key, value)
    #   # => if an ordering is required
    #   emit_intermediate(rank, key, value)
    #
    # Returns the true on success.
    def emit_intermediate(*key_value)
      if( not @ordered )
        key, value = key_value
        FileSystem.sadd( ProcessInfo.keys(@pid), key )
        hashed_key = Helper.key_hash(key)
        FileSystem.rpush( ProcessInfo.map(@pid, hashed_key), value )
      else
        # if there's an order for the process then we should use a zset above
        # ordered process's map emits [rank, key, value]
        #
        rank, key, value = key_value
        FileSystem.zadd( ProcessInfo.keys(@pid), rank, key )
        hashed_key = Helper.key_hash(key)
        FileSystem.rpush( ProcessInfo.map(@pid, hashed_key), value )
      end
      raise "Key Collision: key:#{key}, #{key.class} => hashed key:#{hashed_key}" if key_collision?(hashed_key, key)
      true
    end

    # The emission associated with a reduce.  Currently all reduced
    # values are pushed onto a redis list.  It may be the case that we
    # want to directly use a different redis type given the kind of
    # reduce we are doing.  Often a reduce only returns one value, so
    # instead of a rpush, we should do a set.
    # 
    # Examples
    #   emit(key, reduced_value)
    #
    # Returns "OK" on success.
    def emit(key, reduce_val)
      hashed_key = Helper.key_hash(key)
      FileSystem.rpush( ProcessInfo.reduce(@pid, hashed_key), reduce_val )
    end

    def key_collision?(hashed_key, key)
      not ( FileSystem.setnx( ProcessInfo.hash_to_key(@pid, hashed_key), key ) ||
            FileSystem.get( ProcessInfo.hash_to_key(@pid, hashed_key) ) == key.to_s )
    end

    # Convenience methods to get the mapredus internal key string for a key
    #
    # Examples
    #   reduce_key("document")
    #   # => mapredus:process:PID:map_key:<Helper.key_hash("document")>:reduce
    #   map_key("document")
    #   # => mapredus:process:PID:map_key:<Helper.key_hash("document")>
    #
    # Returns the internal mapreduce string key for a given key.
    [:reduce, :map].each do |internal_key|
      define_method("#{internal_key}_key") do |key|
        ProcessInfo.send(internal_key, @pid, Helper.key_hash(key))
      end
    end

    # Keys that the map operation produced
    #
    # Examples
    #   map_keys
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

    def num_keys()
      if( not @ordered )
        FileSystem.scard( ProcessInfo.keys(@pid) )
      else
        FileSystem.zcard( ProcessInfo.keys(@pid) )
      end
    end

    # values that the map operation produced, for a key
    #
    # Examples
    #   map_values(key)
    #   # =>
    #
    # Returns the values.
    def map_values(key)
      hashed_key = Helper.key_hash(key)
      FileSystem.lrange( ProcessInfo.map(@pid, hashed_key), 0, -1 )
    end

    def num_values(key)
      hashed_key = Helper.key_hash(key)
      FileSystem.llen( ProcessInfo.map(@pid, hashed_key) )
    end

    # values that the reduce operation produced, for a key
    #
    # Examples
    #   reduce_values(key)
    #   # =>
    #
    # Returns the values.
    def reduce_values(key)
      hashed_key = Helper.key_hash(key)
      FileSystem.lrange( ProcessInfo.reduce(@pid, hashed_key), 0, -1 )
    end

    # functions to manage the location of the result in the FileSystem
    #
    # Examples
    #   process.result_key(extra, arguments)
    #   Process.result_key(all, needed, arguments)
    #   # => "something:that:uses:the:extra:arguments"
    # 
    #   SomeProcessSubclass.set_result_key("something:ARG:something:VAR")
    #   # sets the result key for (CAPITAL require arguments to fill in the values)
    def result_key(*args)
      Helper.class_get(@type).result_key(*[@key_args, args].flatten)
    end

    def self.result_key(*args)
      key_maker = "#{self.to_s.gsub(/\W/,"_")}_result_cache"
      key_maker = ProcessInfo.respond_to?(key_maker) ? key_maker : "#{MapRedus::Process.to_s.gsub(/\W/,"_")}_result_cache"
      ProcessInfo.send( key_maker, *args )
    end

    def self.set_result_key(key_struct)
      MapRedus.redefine_redis_key( "#{self.to_s.gsub(/\W/,"_")}_result_cache", key_struct )
    end

    # Create sets up a process to be run with the given specification.
    # It saves the information in the FileSystem and returns an
    # instance of the process that run should be called on when
    # running is desired.
    # 
    # Example
    #   process = MapRedus::Process.create
    #   process.run
    #   
    # Returns an instance of the process
    def self.create
      new_pid = get_available_pid
      specification = ATTRS.inject({}) do |ret, attr|
        ret[attr] = send(attr)
        ret
      end
      specification[:type] = self
      self.new(new_pid, specification).save
    end

    # This defines the attributes to be associated with a MapRedus process
    # This will allow us to subclass a Process, creating a new specification
    # by specifying what say the inputter should equal
    #
    # Example
    #   class AnswerDistribution < MapRedus::Process
    #     inputter JudgmentStream
    #     mapper ResponseFrequencyMap
    #     reducer Adder
    #     finalizer AnswerCount
    #     outputter MapRedus::RedisHasher
    #   end
    class << self; attr_reader *ATTRS; end

    # Setter/Getter method definitions to set/get the attribute for
    # the class. In the getter if it is not defined (nil) then return
    # the default attribute defined in MapRedus::Process.
    #
    # Example
    #   class AnswerDistribution < MapRedus::Process
    #     inputter JudgmentStream
    #     mapper ResponseFrequency
    #   end
    #   AnswerDistribution.reducer.should == Adder
    ATTRS.each do |attr|
      (class << self; self; end).send(:define_method, attr) do |*one_arg|
        attribute = "@#{attr}"
        case one_arg.size
        when 0
          instance_variable_get(attribute) || MapRedus::Process.instance_variable_get(attribute)
        when 1
          instance_variable_set(attribute, one_arg.first)
        else
          raise ArgumentError.new("wrong number of arguments (#{one_arg.size}) when zero or one arguments were expected")
        end
      end
    end

    # Default attributes for the process class.  All other attributes
    # are nil by default.
    inputter WordStream
    mapper WordCounter
    reducer Adder
    finalizer ToRedisHash
    outputter RedisHasher
    type Process
    set_result_key DEFAULT_RESULT_KEY
    
    # This function returns all the redis keys produced associated
    # with a process's process id.
    #
    # Example
    #   Process.info(17)
    #
    # Returns an array of keys associated with the process id.
    def self.info(pid)
      FileSystem.keys(ProcessInfo.pid(pid) + "*")
    end
    
    # Returns an instance of the process class given the process id.
    # If no such process id exists returns nil.
    #
    # Example
    #   process = Process.open(17)
    def self.open(pid)
      spec = Helper.decode( FileSystem.get(ProcessInfo.pid(pid)) )
      spec && self.new( pid, spec )
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

    # Given a arguments for a result key, delete the result from the
    # filesystem.
    #
    # Examples
    #   Process.delete_saved_result(key)
    def self.delete_saved_result(*key_args)
      FileSystem.del( result_key(*key_args) )
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
      proc = Process.open(pid)
      proc.delete if proc
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
