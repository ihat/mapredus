module MapRedus
  module Support
    class MapRedusRunnerError < StandardError; end
    class DuplicateProcessDefinitionError < MapRedusRunnerError ; end

    module Runner; end
    def mapreduce; Runner; end
    
    module ClassMethods
      def mapreduce_process( process_name, mapper, reducer, finalizer, outputter, result_store, opts = {})
        runner_self = (class << Support::Runner; self; end)
        raise DuplicateProcessDefintionError if runner_self.methods.include? process_name.to_s

        runner_self = (class << Support::Runner; self; end)

        runner_self.send( :define_method, process_name.to_s ) do |data|
          process = MapRedus::Process.create(mapper, reducer, finalizer, outputter, result_store, data)
          process.run
        end

        runner_self.send( :define_method, "#{process_name.to_s}_result" ) do
          outputter.decode(MapRedus::Process.get_saved_result(result_store))
        end
      end
    end
    
    def self.included(model)
      model.extend ClassMethods
    end
  end
end
