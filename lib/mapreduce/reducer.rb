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
    
    # Ensure all mappers finished their work before reducers started doing work./
    # Skewed distributions would be dealt with a combiner, help deal with reducers doing too much work.
    # Don't have to worry about associativity.
    # 
    # All the values for a given key are process by a single reduce.
    # Fault tolerance and redundancy are not handled.
    # 
    # When a reduce is made, if it finishes then the reduce operation for that key is complete.
    #
    def self.associative?; true; end
    def self.reduce_unwrap(value)
      JSON.parse(value)["_reduced"]
    end
    def self.reduce_wrap(value)
      associative? ? value : { :_reduced => value }.to_json
    end
    def self.reduced?(value)
      r = reduce_unwrap(value)
      r.is_a?(String) && r
    end

    def self.reduce(values); raise InvalidReducer ;end
    
    # Doesn't handle redundant workers and fault tolerance
    #
    def self.perform(pid, key)
      Job::Master.free_slave(pid)
      
      begin
        job = Job.open(pid)
        return unless job
        reduction = reduce(Job::Manager.get_values(pid, key)) do |reduce_val|
          Job::Manager.emit( pid, key, reduce_val )
        end
      rescue MapRedus::RecoverableFail
        Job::Master.enslave_later_reduce(pid, key)
      end

      if ( not Job::Master.working?(pid) )
        # This means all the reduce jobs have finished
        # so now we can start the finalization process
        # TODO: wonky, should take this book keeping out of here
        #
        Job::Master.enslave_finalizer( pid )
      end
      
      reduction
    end
  end
end
