module MapRedus
  # Run the stuff you want to run at the end of the process.
  # Define subclass which defines self.finalize and self.serialize
  # to do what is needed when you want to get the final output
  # out of redis and into ruby.
  #
  # This is basically the message back to the user program that a
  # process is completed storing the necessary info.
  #
  class Finalizer < QueueProcess

    # The default finalizer is to notify of process completion
    #
    # Example
    #   Finalizer::finalize(pid)
    #   # => "MapRedus Process : 111 : has completed"
    #
    # Returns a message notification
    def self.finalize(pid)
      "MapRedus Process : #{pid} : has completed"
    end

    def self.perform(pid)
      process = Process.open(pid)
      result = finalize(process)
      Master.finish_metrics(pid)
      result
    ensure
      Master.free_slave(pid)
      process.next_state
    end
  end
end
