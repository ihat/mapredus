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

    #
    # Master performs the work that it needs to do: 
    #   it must free itself as a slave from Resque
    #   enslave mappers
    #
    def self.perform( pid )
      free_slave(pid)
      enslave_mappers(pid)
    end
    
    # TODO: this is where we would define an input reader 
    #
    def self.partition_data( arr, partition_size )
      0.step(arr.size, partition_size) do |i|
        yield arr[i...(i + partition_size)]
      end
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
      FileSystem.map_keys(pid).each do |key|
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

    def self.enslave_later_reduce(pid, key)
      job = Job.open(pid)
      return unless job
      enslave_later( DEFAULT_WAIT, pid, job.reducer.queue, job.reducer, pid, key )
    end

    def self.enslave_finalizer( pid )
      job = Job.open(pid)
      return unless job
      enslave( pid, job.finalizer.queue, job.finalizer, pid )
    end

    # The current default q (QUEUE) that we push on to is
    #   :mapreduce
    #
    def self.enslave( pid, q, klass, *args )
      job = Job.open(pid)
      return unless job
      
      FileSystem.rpush(JobInfo.slaves(pid), 1)
      
      if( job.synchronous )
        klass.perform(*args)
      else
        Resque.push( q, { :class => klass.to_s, :args => args } )
      end
    end

    def self.enslave_later( delay_in_seconds, pid, q, klass, *args)
      job = Job.open(pid)
      return unless job

      FileSystem.rpush(JobInfo.slaves(pid), 1)

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
      # FIXME:
      # returns the number of jobs killed on the queue
      #
      destroyed = 0
      qs = [queue, job.mapper.queue, job.reducer.queue, job.finalizer.queue].uniq
      qs.each do |q|
        q_key = "resque:queue:#{q}"
        FileSystem.lrange(q_key, 0, -1).each do | string |
          json   = Support.decode(string)
          match  = json['class'] == "MapRedus::Job::Master"
          match |= json['class'] == job.mapper.to_s
          match |= json['class'] == job.reducer.to_s
          match |= json['class'] == job.finalizer.to_s
          match &= json['args'][0] == job.pid
          if match
            destroyed += FileSystem.lrem(q_key, 0, string).to_i
          end
        end
      end
      
      FileSystem.del(JobInfo.slaves(pid))
      destroyed
    end

    # Check whether there are still workers working on job PID's processes
    #
    def self.working?(pid)
      0 < FileSystem.llen(JobInfo.slaves(pid))
    end

    # Time metrics for measuring how long it takes map reduce to do a job
    #
    def self.set_request_time(pid)
      FileSystem.set( JobInfo.requested_at(pid), Time.now.to_i )
    end

    def self.start_metrics(pid)
      job = Job.open(pid)
      return unless job
      
      started  = JobInfo.started_at( pid )
      FileSystem.set started, Time.now.to_i
    end

    def self.finish_metrics(pid)
      job = Job.open(pid)
      return unless job
      
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
