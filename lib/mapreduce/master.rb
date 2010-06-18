module MapRedus
  # Note: Instead of using Resque directly within the job, we implement
  # a master interface with Resque
  #
  # Does bookkeeping to keep track of how many slaves are doing work. If we have
  # no slaves doing work for a job then the job is done. While there is work available
  # the slaves will always be doing work.
  #
  class Master < QueueProcess
    DEFAULT_WAIT = 10 # seconds
    
    # Check whether there are still workers working on job PID's processes
    #
    # In synchronous condition, master is always working since nothing is going to
    # the queue.
    def self.working?(pid)
      0 < FileSystem.llen(JobInfo.slaves(pid))
    end

    #
    # Master performs the work that it needs to do: 
    #   it must free itself as a slave from Resque
    #   enslave mappers
    #
    def self.perform( pid )
      job = Job.open(pid)
      job.update(:state => MAP_IN_PROGRESS)
      enslave_mappers(job)
    end

    #
    # The order of operations that occur in the mapreduce process
    #
    def self.mapreduce(job)
      if job.synchronous
        job.update(:state => MAP_IN_PROGRESS)
        enslave_mappers(job)
        job.update(:state => REDUCE_IN_PROGRESS)
        enslave_reducers(job)
        job.update(:state => FINALIZER_IN_PROGRESS)
        enslave_finalizer(job)
      else
        Resque.push(QueueProcess.queue, {:class => MapRedus::Master , :args => [job.pid]} )
      end
    end

    # TODO: this is where we would define an input reader 
    #
    def self.partition_data( arr, partition_size )
      0.step(arr.size, partition_size) do |i|
        yield arr[i...(i + partition_size)]
      end
    end
    
    def self.enslave_mappers( job )
      start_metrics(job.pid)
      
      partition_data( job.data, job.mapper.partition_size ) do |data_chunk|
        unless data_chunk.empty?
          enslave_map( job, data_chunk )
        end
      end
    end

    def self.enslave_reducers( job )
      job.map_keys.each do |key|
        enslave_reduce( job, key )
      end
    end

    def self.enslave_finalizer( job )
      enslave( job, job.finalizer.queue, job.finalizer, job.pid )
    end

    # Have these to match what the Mapper/Reducer perform function expects to see as arguments
    #
    # though instead of job the perform function will receive the pid
    def self.enslave_map(job, data_chunk)
      enslave( job, job.mapper.queue, job.mapper, job.pid, data_chunk )
    end

    def self.enslave_reduce(job, key)
      enslave( job, job.reducer.queue, job.reducer, job.pid, key )
    end

    def self.enslave_later_reduce(job, key)
      enslave_later( DEFAULT_WAIT, job, job.reducer.queue, job.reducer, job.pid, key )
    end

    # The current default q (QUEUE) that we push on to is
    #   :mapreduce
    #
    def self.enslave( job, q, klass, *args )
      FileSystem.rpush(JobInfo.slaves(job.pid), 1)
      
      if( job.synchronous )
        klass.perform(*args)
      else
        Resque.push( q, { :class => klass.to_s, :args => args } )
      end
    end

    def self.enslave_later( delay_in_seconds, job, q, klass, *args)
      FileSystem.rpush(JobInfo.slaves(job.pid), 1)

      if( job.synchronous )
        klass.perform(*args)
      else
        Resque.enqueue_at(Time.now + delay_in_seconds, q, klass, *args)
      end
    end

    def self.free_slave(pid)
      FileSystem.lpop(JobInfo.slaves(pid))
    end

    def self.emancipate(pid)
      job = Job.open(pid)
      return unless job
      
      # Working on resque directly seems dangerous
      #
      # Warning: this is supposed to be used as a debugging operation
      # and isn't intended for normal use.  It is potentially very expensive.
      #
      destroyed = 0
      qs = [queue, job.mapper.queue, job.reducer.queue, job.finalizer.queue].uniq
      qs.each do |q|
        q_key = "resque:queue:#{q}"
        Resque.redis.lrange(q_key, 0, -1).each do | string |
          json   = Support.decode(string)
          match  = json['class'] == "MapRedus::Job::Master"
          match |= json['class'] == job.mapper.to_s
          match |= json['class'] == job.reducer.to_s
          match |= json['class'] == job.finalizer.to_s
          match &= json['args'][0] == job.pid
          if match
            destroyed += Resque.redis.lrem(q_key, 0, string).to_i
          end
        end
      end
      
      Resque.redis.del(JobInfo.slaves(pid))
      destroyed
    end

    # Time metrics for measuring how long it takes map reduce to do a job
    #
    def self.set_request_time(pid)
      FileSystem.set( JobInfo.requested_at(pid), Time.now.to_i )
    end

    def self.start_metrics(pid)
      started  = JobInfo.started_at( pid )
      FileSystem.set started, Time.now.to_i
    end

    def self.finish_metrics(pid)
      started  = JobInfo.started_at( pid )
      finished = JobInfo.finished_at( pid )
      requested = JobInfo.requested_at( pid )
      
      completion_time = Time.now.to_i
      
      FileSystem.set finished, completion_time
      time_to_complete = completion_time - FileSystem.get(started).to_i
      
      recent_ttcs = JobInfo.recent_time_to_complete
      FileSystem.lpush( recent_ttcs , time_to_complete )
      FileSystem.ltrim( recent_ttcs , 0, 30 - 1)
      
      FileSystem.expire finished, 60 * 60
      FileSystem.expire started, 60 * 60
      FileSystem.expire requested, 60 * 60
    end
  end
end
