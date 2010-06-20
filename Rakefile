require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "mapredus"
    gem.summary = %Q{mapredus initial}
    gem.description = %Q{simple mapreduce framework using redis and resque}
    gem.email = "john@doloreslabs.com"
    gem.homepage = "http://github.com/dolores/mapredus"
    gem.authors = ["John Le", "Brian O'Rourke"]
    gem.files = Dir['lib/**/*.rb']
    gem.add_dependency "redis", ">= 1.0.4"
    gem.add_dependency "resque", ">= 1.8"
    gem.add_dependency "redis_support", ">= 0"
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "mapredus #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :test => :check_dependencies

task :default => :test

