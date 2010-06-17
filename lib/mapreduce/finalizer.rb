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
    def self.each_key_value(pid)
      Job.each_key_value(pid) do |key, value|
        yield key, value
      end
    end

    def self.each_key_original_value(pid)
      Job.each_key_original_value(pid) do |key, value|
        yield key, value
      end
    end
    
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

    def self.serialize(result); result; end
    def self.deserialize(serialized_result); serialized_result; end

    def self.perform(pid)
      Master.free_slave(pid)
      
      job = Job.open(pid)
      return unless job
      result = finalize(pid)
      Master.finish_metrics(pid)
      
      result
    end
  end
end
