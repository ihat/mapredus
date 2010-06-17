class WordCounter < MapRedus::Mapper
  def self.map(map_data)
    map_data.split(/\W/).each do |word|
      next if word.empty?
      yield(word.downcase, 1)
    end
  end
end
