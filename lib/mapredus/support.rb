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
        if self.respond_to?(mr_process)
          self.send(mr_process, *args, &block)
        else
          super(method, *args, &block)  
        end
      end
    end

    def mapreduce
      @mapreduce_runner ||= Runner.new(self.class.to_s.gsub(/\W/,"_"))
    end

    module ClassMethods
      def mapreduce_process( process_name, mapredus_process_class, result_store )
        runner_self = Runner
        class_name = self.to_s.gsub(/\W/,"_")

        global_process_name = "#{class_name}_#{process_name.to_s}"

        if runner_self.methods.include?(global_process_name)
          raise DuplicateProcessDefintionError
        end
        
        mapredus_process_class.set_result_key( result_store )

        runner_self.send( :define_method, global_process_name ) do |data, key_arguments|
          process = mapredus_process_class.create
          process.update(:key_args => key_arguments)
          process.run(data)
          process
        end

        runner_self.send( :define_method, "#{global_process_name}_result" ) do |key_arguments, *outputter_args|
          key = mapredus_process_class.result_key( *key_arguments )
          mapredus_process_class.outputter.decode( key, *outputter_args)
        end
      end
    end

    def self.included(model)
      model.extend ClassMethods
    end
  end
end
