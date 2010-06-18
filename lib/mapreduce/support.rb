module MapRedus
  module Support
    class MapRedusRunnerError < StandardError; end
    class DuplicateProcessDefinitionError < MapRedusRunnerError ; end

    module Runner; end
    
    module ClassMethods
      def mapreduce_process( process_name, mapper, reducer, finalizer, ordered, result_store )
        raise DuplicateProcessDefintionError if Support::Runner.methods.include? process_name.to_s

        runner_self = (class << Support::Runner; self; end)

        runner_self.send( :define_method, process_name.to_s ) do |data|
          process = MapRedus::Process.create(mapper, reducer, finalizer, data, ordered, result_store)
          process.run
        end

        runner_self.send( :define_method, "#{process_name.to_s}_result" ) do
            MapRedus::Process.get_saved_result(result_store)
        end
      end
    end
    
    def self.included(model)
      model.extend ClassMethods
    end
  end
end
