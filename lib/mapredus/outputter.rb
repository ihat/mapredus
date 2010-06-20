module MapRedus
  #
  # Standard readers for the input and output of Files coming out
  # of the FileSystem.
  #
  class Outputter < QueueProcess
    def self.decode(o); o; end
    def self.encode(o); o; end

    #
    # type should either be "decode" or "encode"
    #
    def self.perform(type, o)
      send(type, o)
    end
  end

  class JsonOutputter < Outputter
    def self.decode(o); Helper.decode(o); end
    def self.encode(o); Helper.encode(o); end
  end
end
