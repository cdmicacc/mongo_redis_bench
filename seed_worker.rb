require 'redis'
require 'mongoid'
require './mongo_doc'

class SeedWorker
  REDIS_DB = 10

  attr_accessor :pid

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

  def seed_redis(start, count)
    redis = Redis.new(host: 'localhost', db: REDIS_DB)

    redis.flushdb

    count.times do |i|
      key = "tmp_key_#{start + i}"
      redis.hset(key, "counter", rand(10000))
    end
  end

  def seed_mongo(start, count)
    mongo =  Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    collection = mongo.collection('mongo_raw')

    collection.drop

    count.times do |i|
      collection.insert({ _id: start + i, counter: rand(10000) })
    end
  end

  def seed_mongoid(start, count)
    Mongoid.database = Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    MongoDoc.delete_all

    count.times do |i|
      MongoDoc.create(_id: start + i, counter: rand(10000))
    end
  end
end
