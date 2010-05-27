module RedisSupport
  class RedisKeyError < StandardError ; end
  class DuplicateKeyDefinitionError < RedisKeyError ; end
  class InvalidKeyDefinitionError < RedisKeyError ; end

  module ClassMethods
    def redis
      return @redis ||= $redis
    end

    # Hacked together on the plane this weekend - this could use a code
    # review and then some. Basic goal is to allow a class to declare
    # something like this:
    #
    # Examples
    #
    #   key :workpools, "job:JOB_ID:workpools"
    #
    # Returns the redis key.
    def redis_key( name, keystruct )
      if Keys.methods.include? name.to_s
        return # New Relic re-invokes this method.
        raise DuplicateKeyDefinitionError
      end
      s = StringScanner.new( keystruct )
      var_pattern = /[A-Z]+([A-Z_]*[A-Z])*/
      str_pattern = /[a-z_:]+/
      unless first = s.scan( str_pattern )
        raise InvalidKeyDefinitionError.new "keys must begin with lowercase letters"
      end
      strs = [ first ]
      vars = []

      passes = 0
      until s.eos?
        if var = s.scan( var_pattern )
          strs << "\#{#{var.downcase}}"
          vars << var.downcase
        else
          strdata = s.scan( str_pattern )
          strs << strdata if strdata
        end
        
        if (passes += 1) > 100
          # this is just a safeguard against infinite loops due to
          # bugs in the key parsing logic. Dump the StringScanner for
          # debugging and raise an error
          #
          raise InvalidKeyDefinitionError.new "Internal error parsing #{keystruct} : Scanner: #{s.inspect}"
        end
      end

      #pp <<-RUBY
      RedisSupport::Keys.class_eval <<-RUBY, __FILE__, __LINE__ + 1
        def self.#{name.to_s}( #{vars.map {|x| x.to_s }.join(', ')} )
          "#{strs.join}"
        end
      RUBY
    end
  end
end
