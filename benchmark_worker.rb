require 'redis'
require 'mongoid'
require './mongo_doc'
require 'json'

class BenchmarkWorker
  COUNT=50000
  REDIS_DB = 10

  attr_accessor :results, :pid

  def launch(benchmark_name, record_count)
    @result_pipe, child_pipe = IO.pipe
    @pid = Process.fork
    if @pid
      # Parent
      child_pipe.close
    else
      # Child
      @result_pipe.close
      @result_pipe = child_pipe

      send("benchmark_#{benchmark_name}", record_count)
      exit!
    end
    self
  end

  def get_results
    return results if results

    result_json = @result_pipe.readline

    Process.wait(@pid)

    self.results = JSON.parse(result_json)
  end

  private

  def benchmark_redis(record_count)
    redis = Redis.new(host: 'localhost', db: REDIS_DB )

    benchmark(record_count) do |i, rand|
      key = "tmp_key_#{rand}"
      redis.hincrby(key, "value", 1)
    end
  end

  def benchmark_mongo_safe(record_count)
    mongo =  Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    collection = mongo.collection('mongo_raw')

    benchmark(record_count) do |i, rand|
      collection.update({ _id: rand }, { '$inc' => { counter: 1 }}, { upsert: true })
    end
  end

  def benchmark_mongo_unsafe(record_count)
    mongo =  Mongo::MongoClient.new('localhost', 27017, w: 0).db('benchmark')
    collection = mongo.collection('mongo_raw')

    ids = 20.times.map { |i| BSON::ObjectId.new }
    benchmark(record_count) do |i, rand|
      collection.update({ _id: rand }, { '$inc' => { counter: 1 }}, { upsert: true })
    end
  end

  def benchmark_mongoid_safe(record_count)
    Mongoid.database = Mongo::MongoClient.new('localhost', 27017, w: 1).db('benchmark')
    benchmark(record_count) do |i, rand|
      doc = MongoDoc.safely(w: 1).find(rand)
      doc.safely(w: 1).inc(:counter, 1)
    end
  end

  def benchmark_mongoid_unsafe(record_count)
    Mongoid.database = Mongo::MongoClient.new('localhost', 27017, w: 0).db('benchmark')

    benchmark(record_count) do |i, rand|
      doc = MongoDoc.find(rand)
      doc.inc(:counter, 1)
    end
  end

  def benchmark(record_count, &block)
    benchmark_times = []

    COUNT.times do |i|
      rand = rand(record_count)

      start_time = Time.now
      yield i, rand
      elapsed = Time.now - start_time

      benchmark_times.push(elapsed * 1000.0)
    end

    @result_pipe.print("#{benchmark_times.to_json}\n")
  end
end
