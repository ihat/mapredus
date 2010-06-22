module MapRedus
  # Map is a function that takes a data chunk
  # where each data chunk is a list of pieces of your raw data
  # and emits a list of key, value pairs.
  #
  # The output of th emap shall always be
  #   [ [key, value], [key, value], ... ]
  #
  # If the order is important change redis.sadd to use a zset.
  #
  # Note: Values must be string, integers, booleans, or floats.
  # i.e., They must be primitive types since these are the only
  # types that redis supports and since anything inputted into
  #
  # redis becomes a string.
  class Mapper < QueueProcess    
    def self.partition_size
      30
    end

    def self.map(data_chunk); raise InvalidMapper; end
    
    def self.perform(pid, data_chunk)
      process = Process.open(pid)
      return unless process
      
      map( data_chunk ) do |*key_value|
        process.emit_intermediate(*key_value)
      end
    ensure
      Master.free_slave(pid)
      process.next_state
    end
  end
end
