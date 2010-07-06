module MapRedus
  # Map is a function that takes a data chunk
  # where each data chunk is a list of pieces of your raw data
  # and emits a list of key, value pairs.
  #
  # The output of the map shall always be
  #   [ [key, value], [key, value], ... ]
  #
  # Note: Values must be string, integers, booleans, or floats.
  # i.e., They must be primitive types since these are the only
  # types that redis supports and since anything inputted into
  # redis becomes a string.
  class Mapper < QueueProcess    
    def self.map(data_chunk); raise InvalidMapper; end
    
    def self.perform(pid, data_key)
      process = Process.open(pid)
      data_chunk = FileSystem.hget(ProcessInfo.input(pid), data_key)
      map( data_chunk ) do |*key_value|
        process.emit_intermediate(*key_value)
      end
    ensure
      Master.free_slave(pid)
      process.next_state
    end
  end
end
