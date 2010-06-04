class Adder < MapRedus::Reducer
  def self.reduce(value_list)
    yield( value_list.inject(0) { |r, v| r += v.to_i } )
  end
end
