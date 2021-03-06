module MapRedus
  # Note: Instead of using Resque directly within the process, we implement
  # a master interface with Resque
  #
  # Does bookkeeping to keep track of how many slaves are doing work. If we have
  # no slaves doing work for a process then the process is donex. While there is work available
  # the slaves will always be doing work.
  #
  class Master < QueueProcess
    # Check whether there are still workers working on process PID's processes
    #
    # In synchronous condition, master is always working since nothing is going to
    # the queue.
    def self.working?(pid)
      0 < FileSystem.llen(ProcessInfo.slaves(pid))
    end

    #
    # Master performs the work that it needs to do: 
    #   it must free itself as a slave from Resque
    #   enslave mappers
    #
    def self.perform( pid, data_object )
      process = Process.open(pid)
      enslave_inputter(process, *data_object)
      process.update(:state => INPUT_MAP_IN_PROGRESS)
    end

    #
    # The order of operations that occur in the mapreduce process
    #
    # The inputter sets off the mapper processes
    #
    def self.mapreduce( process, *data_object )
      start_metrics(process.pid)
      if process.synchronous
        process.update(:state => INPUT_MAP_IN_PROGRESS)
        enslave_inputter(process, *data_object)
        process.update(:state => REDUCE_IN_PROGRESS)
        enslave_reducers(process)
        process.update(:state => FINALIZER_IN_PROGRESS)
        enslave_finalizer(process)
      else
        Resque.push(QueueProcess.queue, {:class => MapRedus::Master , :args => [process.pid, data_object]} )
      end
    end

    def self.enslave_inputter(process, *data_object)
      enslave( process, process.inputter, process.pid, *data_object )
    end
    
    # Enslave the reducers:
    # 
    # For each key, enslave a reducer to process the values on that
    # key. If there were no keys produced during the map operation we
    # must set off the finalizer.
    #
    # TODO: inject optimizations here for special reducers like the
    # identity reduce
    #
    # returns nothing
    def self.enslave_reducers( process )
      if( process.num_keys > 0 )
        process.map_keys.each do |key|
          enslave_reduce( process, key )
        end
      else
        process.next_state
      end
    end

    def self.enslave_finalizer( process )
      enslave( process, process.finalizer, process.pid )
    end

    # Have these to match what the Mapper/Reducer perform function expects to see as arguments
    #
    # though instead of process the perform function will receive the pid
    def self.enslave_map(process, data_chunk)
      enslave( process, process.mapper, process.pid, data_chunk )
    end

    def self.enslave_reduce(process, key)
      enslave( process, process.reducer, process.pid, key )
    end

    def self.enslave_later_reduce(process, key)
      enslave_later( process.reducer.wait, process, process.reducer, process.pid, key )
    end

    # The current default (QUEUE) that we push on to is
    #   :mapredus
    #
    def self.enslave( process, klass, *args )
      FileSystem.rpush(ProcessInfo.slaves(process.pid), 1)

      if( process.synchronous )
        klass.perform(*args)
      else
        Resque.push( klass.queue, { :class => klass.to_s, :args => args } )
      end
    end

    def self.enslave_later( delay_in_seconds, process, klass, *args)
      FileSystem.rpush(ProcessInfo.slaves(process.pid), 1)

      if( process.synchronous )
        klass.perform(*args)
      else
        #
        # TODO: I cannot get enqueue_in to work with my tests
        #       there seems to be a silent failure somewhere
        #       in the tests such that it never calls the function
        #       and the queue gets emptied
        #
        # Resque.enqueue_in(delay_in_seconds, klass, *args)
        
        ##
        ## Temporary, immediately just push process back onto the resque queue
        Resque.push( klass.queue, { :class => klass.to_s, :args => args } )
      end
    end

    def self.slaves(pid)
      FileSystem.lrange(ProcessInfo.slaves(pid), 0, -1)
    end

    def self.free_slave(pid)
      FileSystem.lpop(ProcessInfo.slaves(pid))
    end

    def self.emancipate(pid)
      process = Process.open(pid)
      return unless process
      
      # Working on resque directly seems dangerous
      #
      # Warning: this is supposed to be used as a debugging operation
      # and isn't intended for normal use.  It is potentially very expensive.
      #
      destroyed = 0
      qs = [queue, process.mapper.queue, process.reducer.queue, process.finalizer.queue].uniq
      qs.each do |q|
        q_key = "queue:#{q}"
        Resque.redis.lrange(q_key, 0, -1).each do | string |
          json   = Helper.decode(string)
          match  = json['class'] == "MapRedus::Master"
          match |= json['class'] == process.inputter.to_s
          match |= json['class'] == process.mapper.to_s
          match |= json['class'] == process.reducer.to_s
          match |= json['class'] == process.finalizer.to_s
          match &= json['args'].first.to_s == process.pid.to_s
          if match
            destroyed += Resque.redis.lrem(q_key, 0, string).to_i
          end
        end
      end

      #
      # our slave information is kept track of on file and not in Resque
      #
      FileSystem.del(ProcessInfo.slaves(pid))
      destroyed
    end

    # Time metrics for measuring how long it takes map reduce to do a process
    #
    def self.set_request_time(pid)
      FileSystem.set( ProcessInfo.requested_at(pid), Time.now.to_i )
    end

    def self.start_metrics(pid)
      started  = ProcessInfo.started_at( pid )
      FileSystem.set started, Time.now.to_i
    end

    def self.finish_metrics(pid)
      started  = ProcessInfo.started_at( pid )
      finished = ProcessInfo.finished_at( pid )
      requested = ProcessInfo.requested_at( pid )
      
      completion_time = Time.now.to_i
      
      FileSystem.set finished, completion_time
      time_to_complete = completion_time - FileSystem.get(started).to_i
      
      recent_ttcs = ProcessInfo.recent_time_to_complete
      FileSystem.lpush( recent_ttcs , time_to_complete )
      FileSystem.ltrim( recent_ttcs , 0, 30 - 1)
      
      FileSystem.expire finished, 60 * 60
      FileSystem.expire started, 60 * 60
      FileSystem.expire requested, 60 * 60
    end
  end
end
