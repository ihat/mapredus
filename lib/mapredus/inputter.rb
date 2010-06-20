module MapRedus
  #
  # Readers for working with data to setup the mapreduce process.
  #
  class InputReader < QueueProcess
    def self.decode(o); o; end
    def self.encode(o); o; end

    #
    # type should either be "decode" or "encode"
    #
    def self.perform(type, o)
      send(type, o)
    end
  end
end
