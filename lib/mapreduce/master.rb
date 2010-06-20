module MapRedus
  # Note: Instead of using Resque directly within the process, we implement
  # a master interface with Resque
  #
  # Does bookkeeping to keep track of how many slaves are doing work. If we have
  # no slaves doing work for a process then the process is done. While there is work available
  # the slaves will always be doing work.
  #
  class Master < QueueProcess
    DEFAULT_WAIT = 10 # seconds
    
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
    def self.perform( pid )
      process = Process.open(pid)
      process.update(:state => MAP_IN_PROGRESS)
      enslave_mappers(process)
    end

    #
    # The order of operations that occur in the mapreduce process
    #
    def self.mapreduce(process)
      if process.synchronous
        process.update(:state => MAP_IN_PROGRESS)
        enslave_mappers(process)
        process.update(:state => REDUCE_IN_PROGRESS)
        enslave_reducers(process)
        process.update(:state => FINALIZER_IN_PROGRESS)
        enslave_finalizer(process)
      else
        Resque.push(QueueProcess.queue, {:class => MapRedus::Master , :args => [process.pid]} )
      end
    end

    # TODO: this is where we would define an input reader 
    #
    def self.partition_data( arr, partition_size )
      0.step(arr.size, partition_size) do |i|
        yield arr[i...(i + partition_size)]
      end
    end
    
    def self.enslave_mappers( process )
      start_metrics(process.pid)
      
      partition_data( process.data, process.mapper.partition_size ) do |data_chunk|
        unless data_chunk.empty?
          enslave_map( process, data_chunk )
        end
      end
    end

    def self.enslave_reducers( process )
      process.map_keys.each do |key|
        enslave_reduce( process, key )
      end
    end

    def self.enslave_finalizer( process )
      enslave( process, process.finalizer.queue, process.finalizer, process.pid )
    end

    # Have these to match what the Mapper/Reducer perform function expects to see as arguments
    #
    # though instead of process the perform function will receive the pid
    def self.enslave_map(process, data_chunk)
      enslave( process, process.mapper.queue, process.mapper, process.pid, data_chunk )
    end

    def self.enslave_reduce(process, key)
      enslave( process, process.reducer.queue, process.reducer, process.pid, key )
    end

    def self.enslave_later_reduce(process, key)
      enslave_later( DEFAULT_WAIT, process, process.reducer.queue, process.reducer, process.pid, key )
    end

    # The current default q (QUEUE) that we push on to is
    #   :mapredus
    #
    def self.enslave( process, q, klass, *args )
      FileSystem.rpush(ProcessInfo.slaves(process.pid), 1)
      
      if( process.synchronous )
        klass.perform(*args)
      else
        Resque.push( q, { :class => klass.to_s, :args => args } )
      end
    end

    def self.enslave_later( delay_in_seconds, process, q, klass, *args)
      FileSystem.rpush(ProcessInfo.slaves(process.pid), 1)

      if( process.synchronous )
        klass.perform(*args)
      else
        Resque.enqueue_at(Time.now + delay_in_seconds, q, klass, *args)
      end
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
        q_key = "resque:queue:#{q}"
        Resque.redis.lrange(q_key, 0, -1).each do | string |
          json   = Helper.decode(string)
          match  = json['class'] == "MapRedus::Master"
          match |= json['class'] == process.mapper.to_s
          match |= json['class'] == process.reducer.to_s
          match |= json['class'] == process.finalizer.to_s
          match &= json['args'][0] == process.pid
          if match
            destroyed += Resque.redis.lrem(q_key, 0, string).to_i
          end
        end
      end
      
      Resque.redis.del(ProcessInfo.slaves(pid))
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
