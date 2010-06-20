module MapRedus
  module Support
    class MapRedusRunnerError < StandardError; end
    class DuplicateProcessDefinitionError < MapRedusRunnerError ; end

    class Runner
      def initialize(class_name)
        @class = class_name
      end
      
      def method_missing(method, *args, &block)
        mr_process = "#{@class}_#{method.to_s}"
        if self.class.respond_to?(mr_process)
          self.class.send(mr_process, *args, &block)
        else
          super(method, *args, &block)
        end
      end
    end

    def mapreduce
      @@mapreduce_runner ||= Runner.new(self.class.to_s.gsub(/\W/,"_"))
    end
    
    module ClassMethods
      def mapreduce_process( process_name, mapper, reducer, finalizer, outputter, result_store, opts = {})
        runner_self = (class << Runner; self; end)
        class_name = self.to_s.gsub(/\W/,"_")
        
        global_process_name = "#{class_name}_#{process_name.to_s}"
        
        if runner_self.methods.include?(global_process_name)
          raise DuplicateProcessDefintionError
        end

        runner_self.send( :define_method, global_process_name ) do |data|
          process = MapRedus::Process.create(mapper, reducer, finalizer, outputter, result_store, data)
          process.run
        end

        runner_self.send( :define_method, "#{global_process_name}_result" ) do
          outputter.decode(MapRedus::Process.get_saved_result(result_store))
        end
      end
    end
    
    def self.included(model)
      model.extend ClassMethods
    end
  end
end
