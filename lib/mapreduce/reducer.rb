module MapRedus
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
    def self.reduce(values); raise InvalidReducer; end
    
    # Doesn't handle redundant workers and fault tolerance
    #
    # TODO: Resque::AutoRetry might mess this up.
    def self.perform(pid, key)
      job = Job.open(pid)
      return unless job
      
      reduce(job.map_values(key)) do |reduce_val|
        job.emit( key, reduce_val )
      end
    rescue MapRedus::RecoverableFail
      Master.enslave_later_reduce(job, key)
    ensure
      Master.free_slave(pid)
      job.next_state
    end
  end
end
