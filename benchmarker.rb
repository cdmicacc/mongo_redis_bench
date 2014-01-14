require './benchmark_worker'
require './seed_worker'

class Benchmarker
  attr_reader :benchmark_name

  def initialize(benchmark_name)
    @benchmark_name = benchmark_name
  end

  def seed(seed_size = 1_000_000, worker_count = 20)
    workers = []

    puts "Seeding #{seed_size} into #{benchmark_name} with #{worker_count} workers"
    count_per_worker = seed_size / worker_count

    start = 0
    (worker_count - 1).times do |i|
      workers.push(SeedWorker.new.launch(benchmark_name, start, count_per_worker))
      start += count_per_worker
    end

    # Rounding
    count_per_worker = seed_size - count_per_worker * (worker_count - 1)
    workers.push(SeedWorker.new.launch(benchmark_name, start, count_per_worker))

    puts "Waiting for seed workers..."
    workers.each do |worker|
      worker.wait
    end

    puts "Checking seeds..."
    SeedWorker.check(benchmark_name)

    puts "Done seeding"
  rescue
    workers.each { |worker| Process.kill("TERM", worker.pid) if worker.pid rescue nil }
    raise
  end

  def benchmark(seed_size = 1_000_000, worker_count = 20)
    workers = []

    puts "Starting #{worker_count} workers for #{benchmark_name}"
    worker_count.times do
      workers.push(BenchmarkWorker.new.launch(benchmark_name, seed_size))
    end

    total_results = []
    workers.each_with_index do |worker, i|
      results = worker.get_results

      #dump_results("worker #{i}", results)
      total_results += results
    end

    dump_results("======= Total", total_results)
  rescue
    workers.each { |worker| Process.kill("TERM", worker.pid) if worker.pid rescue nil }
    raise
  end

  def dump_results(label, results)
    avg = results.inject{ |sum, el| sum + el }.to_f / results.size
    min = results.min
    max = results.max
    percentile_90 = percentile(results, 0.9)
    percentile_95 = percentile(results, 0.95)

    puts "%16s: min %0.4f ms, avg %0.4f ms, max %0.4f ms, 90th %0.4f ms, 95th %0.4f ms" % [label, min, avg, max, percentile_90, percentile_95 ]
  end

  def percentile(n, percent)
    return nil unless n
    sorted = n.sort

    k = (sorted.size - 1) * percent
    f = k.floor
    c = k.ceil

    return sorted[k.to_i] if f == c

    d0 = sorted[f.to_i] * (c - k)
    d1 = sorted[c.to_i] * (k - f)
    return d0 + d1
  end
end
