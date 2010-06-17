require 'rubygems'

Gem::Specification.new do |s|
  s.name = 'mapreduce'
  s.author = 'john le'
  s.email = 'john@doloreslabs.com'
  s.homepage = 'http://github.com/sandbox/mapreduce'
  s.version = '0.1.0'
  s.summary = "A MapReduce Framework using Redis"
  s.description = "Module for doing mapreduce type operations using redis as file store"
  s.files = Dir['lib/**/*.rb']
  s.require_path = "lib"
  s.add_dependency("redis", ">= 1.0.4")
  s.add_dependency("resque", ">= 1.8")
  s.add_dependency("redis_support")
end
