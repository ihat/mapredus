class GetWordCount < MapRedus::Process
  TEST = "He pointed his finger in friendly jest and went over to the parapet laughing to himself. Stephen Dedalus stepped up, followed him wearily halfway and sat down on the edge of the gunrest, watching him still as he propped his mirror on the parapet, dipped the brush in the bowl and lathered cheeks and neck."
  def self.specification
    {
      :inputter => WordStream,
      :mapper => WordCounter,
      :reducer => Adder,
      :finalizer => ToRedisHash,
      :outputter => MapRedus::RedisHasher,
      :ordered => false,
      :keyname => "test:result"
    }
  end
end

class WordStream < MapRedus::InputStream
  def self.scan(data_object)
    #
    # The data_object should be a reference to an object that is
    # stored on your system.  The scanner is used to break up what you
    # need from the object into manageable pieces for the mapper.  In
    # this example, the data object is a reference to a redis string.
    #
    test_string = MapRedus::FileSystem.get(data_object)
    
    test_string.split.each_slice(10).each_with_index do |word_set, i|
      yield(i, word_set.join(" "))
    end
  end
end

class WordCounter < MapRedus::Mapper
  def self.partition_size; 1; end
  def self.map(map_data)
    map_data.split(/\W/).each do |word|
      next if word.empty?
      yield(word.downcase, 1)
    end
  end
end

class Adder < MapRedus::Reducer
  def self.reduce(value_list)
    yield( value_list.reduce(0) { |r, v| r += v.to_i } )
  end
end

class ToRedisHash < MapRedus::Finalizer
  def self.finalize(process)
    process.each_key_reduced_value do |key, value|
      process.outputter.encode(process.keyname, key, value)
    end
  end
end
