module MapRedus
  # Run the stuff you want to run at the end of the job.
  # Define subclass which defines self.finalize and self.serialize
  # to do what is needed when you want to get the final output
  # out of redis and into ruby.
  #
  # This is basically the message back to the user program that a
  # job is completed storing the necessary info.
  #
  class Finalizer < QueueProcess

    # The default finalizer is to notify of job completion
    #
    # Example
    #   Finalizer::finalize(pid)
    #   # => "MapRedus Job : 111 : has completed"
    #
    # Returns a message notification
    def self.finalize(pid)
      "MapRedus Job : #{pid} : has completed"
    end

    def self.perform(pid)
      job = Job.open(pid)
      return unless job
      result = finalize(job)
      Master.finish_metrics(pid)
      result
    ensure
      Master.free_slave(pid)
      job.next_state
    end
  end
end
