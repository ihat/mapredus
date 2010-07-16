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
      if storage.respond_to?(method)
        storage.send(method, *args)
      else
        super
      end
    end

    # Copy the values from one key to a second key
    # 
    # NOTE TODO: currently only works for the redis list data
    # structure but will be extended for arbitrary data types.
    #
    # NOTE: this does not account for the key being changed during the
    # copy, so should not be used in situations where the first_key
    # value can change during the running of copy.
    #
    # Examples
    #   FileSystem.copy("key_one", "key_two")
    #
    # returns true on success false otherwise
    def self.copy(first_key, second_key)
      list_length = storage.llen(first_key)
      list_length.times do |index|
        storage.rpush(second_key, storage.lindex(first_key, index))
      end
      true
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
