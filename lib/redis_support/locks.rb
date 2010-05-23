module RedisSupport
  #
  # Locking support.
  #

  #
  # Lock a block of code so it can only be accessed by one thread in
  # our system at a time.
  #
  # See 'acquire_redis_lock' for details on parameters.
  #
  def redis_lock( key_to_lock, expiration = 30, interval = 1 )
    acquire_redis_lock( key_to_lock, expiration, interval )
    yield
  ensure
    release_redis_lock( key_to_lock )
  end

  # Acquire a lock on a key in our Redis database. This is a blocking
  # call. It sleeps until the lock has been successfully acquired.
  #
  # Basic usage:
  #   acquire_redis_lock( key.my_key )
  #   # do some stuff on my_key
  #   release_redis_lock( key.my_key )
  #
  # Described in detail here:
  #   http://code.google.com/p/redis/wiki/SetnxCommand
  #
  # === Parameters
  # key<~to_s>::
  #   the key to lock. the actual key for the lock in redis will
  #   be this value with 'lock.' prepended, which lets this whole
  #   acquire_lock business act like a standard ruby object or
  #   synchronize lock. Also it ensures that all locks in the database
  #   can be easily viewed using redis.keys("lock.*")
  # expiration<~to_i>::
  #   the expiration for the lock, expressed as an Integer. default is
  #   30 seconds from when the lock is acquired. Note that this is the
  #   amount of time others will wait for you, not the amount of time
  #   you will wait to acquire the lock.
  # interval<~to_i>::
  #   sleep interval for checking the lock's status.
  #
  def acquire_redis_lock( key_to_lock, expiration = 30, interval = 1 )
    key = lock_key( key_to_lock )
    until redis.setnx key, timeout_i( expiration )
      if redis.get( key ).to_i < Time.now.to_i
        old_timeout = redis.getset( key, timeout_i( expiration ) ).to_i
        if old_timeout < Time.now.to_i
          return # got it!
        end
      else
        sleep interval
      end
    end
  end

  # see docs for acquire_redis_lock above
  def release_redis_lock( locked_key )
    redis.del lock_key( locked_key )
  end

  def has_redis_lock?( locked_key )
    redis.exists lock_key(locked_key) 
  end

  private

  def lock_key( key_to_lock )
    "lock.#{key_to_lock}"
  end

  # Converts an Integer number of seconds into a future timestamp that
  # can be used with Redis.
  def timeout_i( timeout )
    timeout.seconds.from_now.to_i
  end

end
