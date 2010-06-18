require 'redis'
require 'redis_support'
require 'resque'

module MapRedus
  include RedisSupport
  
  class InvalidProcess < NotImplementedError
    def initialize; super("MapRedus QueueProcess: need to have perform method defined");end
  end

  class InvalidMapper < NotImplementedError
    def initialize; super("MapRedus Mapper: need to have map method defined");end
  end

  class InvalidReducer < NotImplementedError
    def initialize; super("MapRedus Reducer: need to have reduce method defined");end
  end

  class InvalidJob < NotImplementedError
    def initialize; super("MapRedus Job Creation Failed: Specifications were not specified");end
  end

  class RecoverableFail < StandardError
    def initialize; super("MapRedus Operation Failed: but it is recoverable") ;end
  end
  
  # All Queue Processes should have a function called perform
  # ensuring that when the class is put on the resque queue it can perform its work
  # 
  # Caution: defines redis, which is also defined in RedisSupport
  # 
  class QueueProcess
    def self.queue; :mapreduce; end
    def self.perform(*args); raise InvalidProcess; end
  end

  # TODO: When you send work to a worker using a mapper you define, 
  # the worker won't have that class name defined, unless it was started up
  # with the class loaded
  #
  def register_reducer(klass); end;
  def register_mapper(klass); end;

  class Support
    # resque helpers defines
    #   redis
    #   encode
    #   decode
    #   classify
    #   constantize
    #
    # This is extended here because we want to use the encode and decode function
    # when we interact with resque queues
    extend Resque::Helpers

    # Defines a hash by taking the absolute value of ruby's string
    # hash to rid the dashes since redis keys should not contain any.
    #
    # key - The key to be hashed.
    #
    # Examples
    #
    #   Support::hash( key )
    #   # => '8dd8hflf8dhod8doh9hef'
    #
    # Returns the hash.
    def self.hash( key )
      key.to_s.hash.abs.to_s(16)
    end

    # Returns the classname of the namespaced class.
    #
    # The full name of the class.
    #
    # Examples
    #
    #   Support::class_get( Super::Long::Namespace::ClassName )
    #   # => 'ClassName'
    #
    # Returns the class name.
    def self.class_get(string)
      constantize(string)
    end
  end 
end

require 'mapreduce/keys'
require 'mapreduce/job'
require 'mapreduce/filesystem'
require 'mapreduce/master'
require 'mapreduce/mapper'
require 'mapreduce/reducer'
require 'mapreduce/finalizer'
