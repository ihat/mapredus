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
end
