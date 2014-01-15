require 'redis'
require 'mongoid'
require './mongo_doc'

class SeedWorker
  REDIS_DB = 10

  attr_accessor :pid

  def self.check(benchmark_name, seed_size)
    send("check_#{benchmark_name}", seed_size)
  end

  def self.clear(benchmark_name)
    send("clear_#{benchmark_name}")
  end

  def launch(benchmark_name, start, count)
    @pid = Process.fork
    if @pid.nil?
      # Child
      send("seed_#{benchmark_name}", start, count)
      exit!
    end
    self
  end

  def wait
    Process.wait(@pid)
  end

  private

  def self.check_redis(seed_size)
    redis = Redis.new(host: 'localhost', db: REDIS_DB)

    key_count = nil
    loop do
      key_count = redis.info["db#{REDIS_DB}"].split(/[,=]/)[1].to_i
      break if key_count == seed_size

      puts "Waiting for writes to complete: #{key_count}"
      sleep 1
    end
    puts "Document count: #{key_count}"
  ensure
    redis.quit
  end

  def self.check_mongo(seed_size)
    mongo =  Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    collection = mongo.collection('mongo_raw')

    document_count = nil
    loop do
      document_count = collection.count
      break if document_count == seed_size

      puts "Waiting for writes to complete: #{document_count}"
      sleep 1
    end
    puts "Document count: #{document_count}"
  ensure
    mongo.connection.close
  end

  def self.check_mongoid(seed_size)
    Mongoid.database = Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')

    document_count = nil
    loop do
      document_count = MongoDoc.all.count
      break if document_count == seed_size

      puts "Waiting for writes to complete: #{document_count}"
      sleep 1
    end
    puts "Document count: #{document_count}"
  ensure
    Mongoid.database.connection.close
  end

  def self.clear_redis
    redis = Redis.new(host: 'localhost', db: REDIS_DB)

    redis.flushdb
  ensure
    redis.quit
  end

  def self.clear_mongo
    mongo =  Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    collection = mongo.collection('mongo_raw')

    collection.drop
  ensure
    mongo.connection.close
  end

  def self.clear_mongoid
    Mongoid.database = Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    MongoDoc.collection.drop
  ensure
    Mongoid.database.connection.close
  end

  def seed_redis(start, count)
    redis = Redis.new(host: 'localhost', db: REDIS_DB)

    count.times do |i|
      key = "tmp_key_#{start + i}"
      redis.hset(key, "counter", rand(10000))
    end
  ensure
    redis.quit
  end

  def seed_mongo(start, count)
    mongo =  Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    collection = mongo.collection('mongo_raw')

    count.times do |i|
      collection.insert({ _id: start + i, counter: rand(10000) })
    end
  ensure
    mongo.connection.close
  end

  def seed_mongoid(start, count)
    Mongoid.database = Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')

    count.times do |i|
      unless MongoDoc.create(_id: start + i, counter: rand(10000))
        puts "Failed to create #{start + i}"
      end
    end
  ensure
    Mongoid.database.connection.close
  end
end
