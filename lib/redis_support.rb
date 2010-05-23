module RedisSupport
  require 'redis_support/class_extensions'
  require 'redis_support/locks'

  def redis
    return @redis ||= $redis
  end

  def keys
    Keys
  end

  module Keys ; end

  def self.included(model)
    model.extend ClassMethods
  end
end
