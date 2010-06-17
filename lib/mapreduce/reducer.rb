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
    def self.perform(pid, key)
      Master.free_slave(pid)
      
      begin
        job = Job.open(pid)
        return unless job
        reduction = reduce(Job.map_values(pid, key)) do |reduce_val|
          Job.emit( pid, key, reduce_val )
        end
      rescue MapRedus::RecoverableFail
        Master.enslave_later_reduce(pid, key)
      end

      if ( not Master.working?(pid) )
        # This means all the reduce jobs have finished
        # so now we can start the finalization process
        # TODO: wonky, should take this book keeping out of here
        #
        Master.enslave_finalizer( pid )
      end
      
      reduction
    end
  end
end
