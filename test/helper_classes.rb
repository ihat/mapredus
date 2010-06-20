class GetWordCount < MapRedus::Process
  def self.specification(data)
    {
      :mapper => WordCounter,
      :reducer => Adder,
      :finalizer => ToHash,
      :data => data,
      :outputter => MapRedus::JsonOutputter,
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

class CharCounter < MapRedus::Mapper
  def self.partition_size; 1; end
  def self.map(map_data)
    map_data.join("").each_char do |char|
      yield char, 1
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
    process.save_result(result)
    process.delete
    result
  end
end

class Something < MapRedus::Mapper
  def self.map(values)
  end
end

class SomethingElse < MapRedus::Mapper
end

class SomeReducer < MapRedus::Reducer
end

class SomeSuperReduce < MapRedus::Reducer
end

class Document
  include MapRedus::Support

  mapreduce_process :word_count, WordCounter, Adder, ToHash, MapRedus::JsonOutputter, "store:result"
  mapreduce_process :char_count, CharCounter, Adder, ToHash, MapRedus::JsonOutputter, "store:char:result"

  def initialize
    @words = ["He pointed his finger in friendly jest and went over to the parapet",
              "laughing to himself. Stephen Dedalus stepped up, followed him wearily",
              "halfway and sat down on the edge of the gunrest, watching him still as",
              "he propped his mirror on the parapet, dipped the brush in the bowl and", 
              "lathered cheeks and neck."]
  end

  def word_answer
    {"gunrest"=>1, "over"=>1, "still"=>1, "of"=>1, "him"=>2, "and"=>4, "bowl"=>1, "himself"=>1, "went"=>1, "friendly"=>1, "finger"=>1, "propped"=>1, "cheeks"=>1, "dipped"=>1, "down"=>1, "wearily"=>1, "up"=>1, "stepped"=>1, "dedalus"=>1, "to"=>2, "in"=>2, "sat"=>1, "the"=>6, "pointed"=>1, "as"=>1, "followed"=>1, "stephen"=>1, "laughing"=>1, "his"=>2, "he"=>2, "brush"=>1, "jest"=>1, "neck"=>1, "mirror"=>1, "edge"=>1, "on"=>2, "parapet"=>2, "lathered"=>1, "watching"=>1, "halfway"=>1}
  end

  def char_answer
    {" "=>50, "k"=>2, "v"=>1, "a"=>17, ","=>3, "l"=>12, "w"=>7, "b"=>2, "m"=>4, "c"=>3, "."=>2, "n"=>18, "y"=>3, "D"=>1, "d"=>15, "o"=>13, "e"=>34, "p"=>14, "f"=>6, "g"=>6, "r"=>13, "h"=>19, "H"=>1, "S"=>1, "s"=>12, "i"=>16, "t"=>20, "j"=>1, "u"=>5}
  end

  def run_word_count
    mapreduce.word_count(@words)
  end

  def get_word_count
    mapreduce.word_count_result
  end

  def run_char_count
    mapreduce.char_count(@words)
  end

  def get_char_count
    mapreduce.char_count_result
  end
end
