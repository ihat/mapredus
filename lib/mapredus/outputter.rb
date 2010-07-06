module MapRedus
  #
  # Standard readers for the input and output of Files coming out
  # of the FileSystem.
  #
  class Outputter < QueueProcess
    def self.decode(result_key)
      FileSystem.get(result_key)
    end

    def self.encode(result_key, o)
      FileSystem.set(result_key, o)
    end

    #
    # type should either be "decode" or "encode"
    #
    def self.perform(type, o)
      send(type, o)
    end
  end

  class JsonOutputter < Outputter
    def self.decode(result_key)
      Helper.decode(FileSystem.get(result_key))
    end

    def self.encode(result_key, o)
      FileSystem.set(result_key, Helper.encode(o))
    end
  end

  class RedisHasher < Outputter
    def self.encode(result_key, k, v)
      FileSystem.hset(result_key, k, v)
    end

    def self.decode(result_key, k)
      FileSystem.hget(result_key, k)
    end
  end
end
