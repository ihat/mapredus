module MapRedus
  class Job
    # Note: Instead of using Resque directly within the job, we implement
    # a master interface with Resque
    #
    # Does bookkeeping to keep track of how many slaves are doing work. If we have
    # no slaves doing work for a job then the job is done. While there is work available
    # the slaves will always be doing work.
    #
    class Master < Manager
      DEFAULT_WAIT = 5 # seconds
      

      # Keeps track of the current slaves (by appending "1" to a redis list)
      #
      # TODO: should append some sort of proper process id so we can explicitly keep track
      #       of processes
      #
      redis_key :slaves, "map_reduce_job:PID:master:slaves"

      # These keep track of timing information for a map reduce job of pid PID
      #
      redis_key :requested_at, "map_reduce_job:PID:request_at"
      redis_key :started_at, "map_reduce_job:PID:started_at"
      redis_key :finished_at, "map_reduce_job:PID:finished_at"
      redis_key :recent_time_to_complete, "map_reduce_job:recent_time_to_complete"

      # a partitioner to partition the data in a set size
      # TODO: this is where we would define a mapper's partition function
      #
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
        
        redis.rpush(Keys.slaves(pid), 1)
        
        if( job.synchronous )
          klass.perform(*args)
        else
          Resque.push( q, { :class => klass.to_s, :args => args } )
        end
      end

      def self.enslave_later( delay_in_seconds, pid, q, klass, *args)
        job = Job.open(pid)
        return unless job

        redis.rpush(Keys.slaves(pid), 1)

        if( job.synchronous )
          klass.perform(*args)
        else
          Resque.enqueue_at(Time.now + delay_in_seconds, q, klass, *args)
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

  end
end
