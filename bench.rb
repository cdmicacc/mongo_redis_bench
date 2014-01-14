require 'rubygems'
require 'bundler/setup'

require './benchmarker'

Mongo::WriteConcern.class_variable_set('@@safe_warn', true)

#Benchmarker.new("redis").seed
#Benchmarker.new("mongo").seed
Benchmarker.new("mongoid").seed

Benchmarker.new("redis").benchmark
Benchmarker.new("mongo_unsafe").benchmark
Benchmarker.new("mongo_safe").benchmark
Benchmarker.new("mongoid_safe").benchmark
Benchmarker.new("mongoid_unsafe").benchmark


