class GetWordCount < MapRedus::Process
  def self.specification(data)
    {
      :mapper => WordCounter,
      :reducer => Adder,
      :finalizer => ToHash,
      :data => data,
      :ordered => false,
      :keyname => "test:result"
    }
  end
end

class WordCounter < MapRedus::Mapper
  def self.partition_size; 1; end
  def self.map(map_data)
    map_data.join(" ").split(/\W/).each do |word|
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

class ToHash < MapRedus::Finalizer
  def self.finalize(process)
    result = {}
    process.each_key_reduced_value do |key, value|
      result[key] = value.to_i
    end
    process.save_result(MapRedus::Helper.encode(result))
    process.delete
    result
  end
end
