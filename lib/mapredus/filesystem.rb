module MapRedus
  # Manages the book keeping of redis keys and redis usage
  # provides the data storage for process information through redis
  # All interaction with redis should go through this class
  # 
  class FileSystem
    def self.storage
      MapRedus.redis
    end
    
    # Save/Read functions to save/read values for a redis key
    #
    # Examples
    #   FileSystem.save( key, value ) 
    def self.save(key, value, time = nil)
      storage.set(key, value)
      storage.expire(key, time) if time
    end

    def self.method_missing(method, *args, &block)
      storage.send(method, *args)
    end
    
    # Setup locks on results using RedisSupport lock functionality
    #
    # Examples
    #   FileSystem::has_lock?(key)
    #   # => true or false 
    #
    # Returns true if there's a lock
    def self.has_lock?(key)
      MapRedus.has_redis_lock?( RedisKey.result_cache(key) ) 
    end
    
    def self.acquire_lock(key)
      MapRedus.acquire_redis_lock_nonblock( RedisKey.result_cache(key), 60 * 60 )
    end
    
    def self.release_lock(key)
      MapRedus.release_redis_lock( RedisKey.result_cache(key) )
    end
  end
end
