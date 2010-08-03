module MapRedus
  class InputStream < QueueProcess
    #
    # An InputSteam needs to implement a way to scan through the
    # data_object (the object data that is sent to the MapRedus
    # process). The scan function implements how the data object is
    # broken sizable pieces for the mappers to operate on.
    #
    # It does this by yielding a <key, map_data> pair.  The key
    # specifies the location storage in redis.  map_data is string
    # data that will be written to the redis.
    #
    # Example
    #   scan(data_object) do |key, map_data|
    #     ...
    #   end
    def self.scan(*data_object)
      raise InvalidInputStream
    end

    def self.perform(pid, data_object)
      process = Process.open(pid)
      scan(*data_object) do |key, map_data|
        FileSystem.hset(ProcessInfo.input(pid), key, map_data)
        Master.enslave_map(process, key)
      end
    ensure
      Master.free_slave(pid)
    end
  end
end
