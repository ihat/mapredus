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
    #
    # After a recoverable fail this describes how much time we shall wait before
    # readding the reducer back on to the queue.
    #
    DEFAULT_WAIT = 10 # seconds
    def self.wait; DEFAULT_WAIT; end
    
    def self.reduce(values); raise InvalidReducer; end
    
    # Doesn't handle redundant workers and fault tolerance
    #
    # TODO: Resque::AutoRetry might mess this up.
    def self.perform(pid, key)
      process = Process.open(pid)
      return unless process
      
      reduce(process.map_values(key)) do |reduce_val|
        process.emit( key, reduce_val )
      end
    rescue MapRedus::RecoverableFail
      Master.enslave_later_reduce(process, key)
    ensure
      Master.free_slave(pid)
      process.next_state
    end
  end
end
