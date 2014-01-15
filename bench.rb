require 'rubygems'
require 'bundler/setup'

require './benchmarker'

Mongo::WriteConcern.class_variable_set('@@safe_warn', true)

[ 1_000_000, 5_000_000, 10_000_000 ].each do |seed_size|
  Benchmarker.new("redis").seed(seed_size)
  Benchmarker.new("mongo").seed(seed_size)
  Benchmarker.new("mongoid").seed(seed_size)

  [ 20, 50, 100 ].each do |workers|
    [ 1000, 10_000, 1_000_000 ].each do |size|
      puts "*" * 80
      puts "With #{size} keys updating, seed size #{seed_size}, #{workers} workers"
      Benchmarker.new("redis").benchmark(size, workers)
      Benchmarker.new("hiredis").benchmark(size, workers)
      Benchmarker.new("mongo_unsafe").benchmark(size, workers)
      Benchmarker.new("mongo_safe").benchmark(size, workers)
      Benchmarker.new("mongoid_safe").benchmark(size, workers)
      Benchmarker.new("mongoid_unsafe").benchmark(size, workers)
    end
  end
end
