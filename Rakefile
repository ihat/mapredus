## -*- ruby -*-

require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "mapreduce"
    gem.summary = %Q{mapreduce initial}
    gem.description = %Q{simple mapreduce framework using redis and resque}
    gem.email = "john@doloreslabs.com"
    gem.homepage = "http://github.com/sandbox/mapreduce"
    gem.authors = ["John Le"]
    gem.files = Dir['lib/**/*.rb']
    gem.add_dependency "redis", ">= 1.0.4"
    gem.add_dependency "resque", ">= 1.8"
    gem.add_dependency "redis_support", ">= 0"
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :test => :check_dependencies

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "mapreduce #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
