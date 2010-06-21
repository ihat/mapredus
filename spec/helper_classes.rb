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

class GetCharCount < MapRedus::Process
  def self.specification(data, store)
    {
      :mapper => CharCounter,
      :reducer => Adder,
      :finalizer => ToHash,
      :data => data,
      :outputter => MapRedus::JsonOutputter,
      :ordered => false,
      :keyname => store
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
  include MapRedus::Support
  
  def self.partition_size; 10; end
  def self.map(data)
    count = 0
    WordCounter.map(data) do |word, one|
      result_loc = "intermediate:result:store:#{word}:#{count}"
      intermediate = GetCharCount.create([word], result_loc)
      intermediate.run
      count += 1
      yield(word, "#{intermediate.outputter.to_s}|#{result_loc}")
    end
  end
end

class SomeReducer < MapRedus::Reducer
  # for testing purposes only, we want to add the reducer back onto the queue
  # instantly!
  def self.wait; 30; end

  def self.reduce(intermediate_result_keys)
    accum = Hash.new(0)
    intermediate_result_keys.each do |key|
      outputter, result_key = key.split("|")
      result = MapRedus::Process.get_saved_result(result_key)
      if(result)
        MapRedus::Helper.class_get(outputter).decode(result).each do |k, v|
          accum[k] += v.to_i
        end
      else
        raise MapRedus::RecoverableFail
      end
    end

    yield(accum.to_json)
  end
end

class SomeFinalizer < MapRedus::Finalizer
  def self.finalize(process)
    result = {}
    process.each_key_reduced_value do |key, value|
      result[key] = process.outputter.decode(value)
    end
    process.save_result(result)
    process.delete
    result
  end
end

class Job
  include MapRedus::Support
  mapreduce_process :word_count, WordCounter, Adder, ToHash, MapRedus::JsonOutputter, "job:store:result"
end


class Document
  include MapRedus::Support

  mapreduce_process :word_count, WordCounter, Adder, ToHash, MapRedus::JsonOutputter, "store:result"
  mapreduce_process :char_count, CharCounter, Adder, ToHash, MapRedus::JsonOutputter, "store:char:result"
  mapreduce_process :recoverable_test, Something, SomeReducer, SomeFinalizer, MapRedus::JsonOutputter, "store:recover:result", :synchronous => true
  
  attr_reader :words

  TEST = ["He pointed his finger in friendly jest and went over to the parapet",
          "laughing to himself. Stephen Dedalus stepped up, followed him wearily",
          "halfway and sat down on the edge of the gunrest, watching him still as",
          "he propped his mirror on the parapet, dipped the brush in the bowl and", 
          "lathered cheeks and neck."]
  
  def initialize
    @words = TEST
  end

  def word_answer
    {"gunrest"=>1, "over"=>1, "still"=>1, "of"=>1, "him"=>2, "and"=>4, "bowl"=>1, "himself"=>1, "went"=>1, "friendly"=>1, "finger"=>1, "propped"=>1, "cheeks"=>1, "dipped"=>1, "down"=>1, "wearily"=>1, "up"=>1, "stepped"=>1, "dedalus"=>1, "to"=>2, "in"=>2, "sat"=>1, "the"=>6, "pointed"=>1, "as"=>1, "followed"=>1, "stephen"=>1, "laughing"=>1, "his"=>2, "he"=>2, "brush"=>1, "jest"=>1, "neck"=>1, "mirror"=>1, "edge"=>1, "on"=>2, "parapet"=>2, "lathered"=>1, "watching"=>1, "halfway"=>1}
  end

  def char_answer
    {" "=>50, "k"=>2, "v"=>1, "a"=>17, ","=>3, "l"=>12, "w"=>7, "b"=>2, "m"=>4, "c"=>3, "."=>2, "n"=>18, "y"=>3, "D"=>1, "d"=>15, "o"=>13, "e"=>34, "p"=>14, "f"=>6, "g"=>6, "r"=>13, "h"=>19, "H"=>1, "S"=>1, "s"=>12, "i"=>16, "t"=>20, "j"=>1, "u"=>5}
  end

  def recoverable_answer
    {"over"=>{"v"=>1, "o"=>1, "e"=>1, "r"=>1}, "gunrest"=>{"n"=>1, "e"=>1, "r"=>1, "g"=>1, "s"=>1, "t"=>1, "u"=>1}, "and"=>{"a"=>4, "n"=>4, "d"=>4}, "him"=>{"m"=>2, "h"=>2, "i"=>2}, "of"=>{"o"=>1, "f"=>1}, "still"=>{"l"=>2, "s"=>1, "i"=>1, "t"=>1}, "finger"=>{"n"=>1, "e"=>1, "f"=>1, "r"=>1, "g"=>1, "i"=>1}, "friendly"=>{"l"=>1, "y"=>1, "n"=>1, "d"=>1, "e"=>1, "f"=>1, "r"=>1, "i"=>1}, "went"=>{"w"=>1, "n"=>1, "e"=>1, "t"=>1}, "himself"=>{"l"=>1, "m"=>1, "e"=>1, "f"=>1, "s"=>1, "h"=>1, "i"=>1}, "bowl"=>{"l"=>1, "w"=>1, "b"=>1, "o"=>1}, "propped"=>{"d"=>1, "o"=>1, "e"=>1, "p"=>3, "r"=>1}, "down"=>{"w"=>1, "n"=>1, "o"=>1, "d"=>1}, "dipped"=>{"d"=>2, "e"=>1, "p"=>2, "i"=>1}, "cheeks"=>{"k"=>1, "c"=>1, "e"=>2, "s"=>1, "h"=>1}, "in"=>{"n"=>2, "i"=>2}, "to"=>{"o"=>2, "t"=>2}, "dedalus"=>{"l"=>1, "a"=>1, "d"=>2, "e"=>1, "s"=>1, "u"=>1}, "stepped"=>{"d"=>1, "p"=>2, "e"=>2, "s"=>1, "t"=>1}, "up"=>{"p"=>1, "u"=>1}, "wearily"=>{"l"=>1, "a"=>1, "w"=>1, "y"=>1, "e"=>1, "r"=>1, "i"=>1}, "pointed"=>{"n"=>1, "d"=>1, "o"=>1, "e"=>1, "p"=>1, "t"=>1, "i"=>1}, "the"=>{"e"=>6, "h"=>6, "t"=>6}, "sat"=>{"a"=>1, "s"=>1, "t"=>1}, "he"=>{"e"=>2, "h"=>2}, "his"=>{"s"=>2, "h"=>2, "i"=>2}, "laughing"=>{"a"=>1, "l"=>1, "n"=>1, "g"=>2, "h"=>1, "i"=>1, "u"=>1}, "stephen"=>{"n"=>1, "p"=>1, "e"=>2, "h"=>1, "s"=>1, "t"=>1}, "followed"=>{"w"=>1, "l"=>2, "d"=>1, "o"=>2, "e"=>1, "f"=>1}, "as"=>{"a"=>1, "s"=>1}, "jest"=>{"e"=>1, "s"=>1, "t"=>1, "j"=>1}, "brush"=>{"b"=>1, "r"=>1, "h"=>1, "s"=>1, "u"=>1}, "parapet"=>{"a"=>4, "e"=>2, "p"=>4, "r"=>2, "t"=>2}, "on"=>{"n"=>2, "o"=>2}, "edge"=>{"d"=>1, "e"=>2, "g"=>1}, "mirror"=>{"m"=>1, "o"=>1, "r"=>3, "i"=>1}, "neck"=>{"k"=>1, "c"=>1, "n"=>1, "e"=>1}, "halfway"=>{"w"=>1, "l"=>1, "a"=>2, "y"=>1, "f"=>1, "h"=>1}, "watching"=>{"a"=>1, "w"=>1, "n"=>1, "c"=>1, "g"=>1, "h"=>1, "i"=>1, "t"=>1}, "lathered"=>{"a"=>1, "l"=>1, "d"=>1, "e"=>2, "r"=>1, "h"=>1, "t"=>1}}
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
