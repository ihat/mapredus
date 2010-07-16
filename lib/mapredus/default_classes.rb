module MapRedus
  class WordStream < InputStream
    def self.scan(data_object)
      #
      # The data_object should be a reference to an object that is
      # stored on your system.  The scanner is used to break up what you
      # need from the object into manageable pieces for the mapper.  In
      # this example, the data object is a reference to a redis string.
      #
      test_string = FileSystem.get(data_object)
      
      test_string.split.each_slice(10).each_with_index do |word_set, i|
        yield(i, word_set.join(" "))
      end
    end
  end

  ################################################################################

  class WordCounter < Mapper
    def self.map(map_data)
      map_data.split(/\W/).each do |word|
        next if word.empty?
        yield(word.downcase, 1)
      end
    end
  end

  ####################################REDUCERS####################################

  class Adder < Reducer
    def self.reduce(value_list)
      yield( value_list.reduce(0) { |r, v| r += v.to_i } )
    end
  end

  # Emits the identity function on the map values.
  #
  # The identity reducer should never actually have to reduce as a
  # special class in mapredus, the values should just be copied from
  # one key to a new key directly in redis.
  class Identity < Reducer
    def self.reduce_perform(process, key)
      FileSystem.copy( process.map_key(key), process.reduce_key(key) )
    end

    def self.reduce(value_list)
      value_list.each do |v|
        yield v 
      end
    end
  end

  # Emits the length of the mapped value list.
  # 
  # The counter reducer tells how many values were emitted by the
  # mapper.  In situations where an adder could used but only has to
  # sum up 1's, counter will be much faster.
  #
  # This works in MapRedus because all the values produced for one key
  # is processed (reduced) by a single worker.
  class Counter < Reducer
    def self.reduce_perform(process, key)
      process.emit(key, FileSystem.llen(process.map_key(key)))
    end

    def self.reduce(value_list)
      yield value_list.size
    end
  end

  ################################################################################

  class ToRedisHash < Finalizer
    def self.finalize(process)
      process.each_key_reduced_value do |key, value|
        process.outputter.encode(process.result_key, key, value)
      end
    end
  end

  class RedisHasher < Outputter
    def self.to_hash(result_key)
      keys(result_key).inject({}) do |hash, key|
        hash[key] = decode(result_key, key)
        hash
      end
    end

    def self.values(result_key)
      FileSystem.hvals(result_key)
    end

    def self.keys(result_key)
      FileSystem.hkeys(result_key)
    end

    def self.encode(result_key, k, v)
      FileSystem.hset(result_key, k, v)
    end

    def self.decode(result_key, k)
      FileSystem.hget(result_key, k)
    end
  end
end
