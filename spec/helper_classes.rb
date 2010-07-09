class CharStream < MapRedus::InputStream
  def self.scan(data_object)
    test_string = MapRedus::FileSystem.get(data_object)
    
    0.step(test_string.size, 30) do |index|
      char_set = test_string[index...(index+30)]
      next if char_set.empty?
      yield(index, char_set)
    end
  end
end

class CharCounter < MapRedus::Mapper
  def self.map(map_data)
    map_data.each_char do |char|
      yield(char, 1)
    end
  end
end

class GetCharCount < MapRedus::Process
  EXPECTED_ANSWER = {"k"=>2, "v"=>1, " "=>54, ","=>3, "w"=>7, "a"=>17, "l"=>12, "b"=>2, "m"=>4, "c"=>3, "."=>2, "y"=>3, "n"=>18, "D"=>1, "d"=>15, "o"=>13, "p"=>14, "e"=>34, "f"=>6, "r"=>13, "g"=>6, "S"=>1, "s"=>12, "h"=>19, "H"=>1, "t"=>20, "i"=>16, "u"=>5, "j"=>1}
  inputter CharStream
  mapper CharCounter
end

class GetWordCount < MapRedus::Process
  TEST = "He pointed his finger in friendly jest and went over to the parapet laughing to himself. Stephen Dedalus stepped up, followed him wearily halfway and sat down on the edge of the gunrest, watching him still as he propped his mirror on the parapet, dipped the brush in the bowl and lathered cheeks and neck."
  EXPECTED_ANSWER = {"gunrest"=>1, "over"=>1, "still"=>1, "of"=>1, "him"=>2, "and"=>4, "bowl"=>1, "himself"=>1, "went"=>1, "friendly"=>1, "finger"=>1, "propped"=>1, "cheeks"=>1, "dipped"=>1, "down"=>1, "wearily"=>1, "up"=>1, "stepped"=>1, "dedalus"=>1, "to"=>2, "in"=>2, "sat"=>1, "the"=>6, "pointed"=>1, "as"=>1, "followed"=>1, "stephen"=>1, "laughing"=>1, "his"=>2, "he"=>2, "brush"=>1, "jest"=>1, "neck"=>1, "mirror"=>1, "edge"=>1, "on"=>2, "parapet"=>2, "lathered"=>1, "watching"=>1, "halfway"=>1}
  set_result_key "test:result"
end

class Document
  include MapRedus::Support
  mapreduce_process :char_count, GetCharCount, "document:count:ID"

  attr_accessor :id
  def initialize(id)
    @id = id
  end

  def calculate_chars(data_reference)
    mapreduce.char_count(data_reference, id)
  end
end
