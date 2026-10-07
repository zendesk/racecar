# frozen_string_literal: true

require "rdkafka"

module Racecar
  class ThreadedRunner
    SHUTDOWN_SIGNALS = ["INT", "QUIT", "TERM"]
    METADATA_POLL_INTERVAL = 60

    def initialize(consumer_class:, config:, logger:, instrumenter:)
      @consumer_class = consumer_class
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @runners = []
      @threads = []
      @mutex = Mutex.new
      @stop_requested = false
      @thread_error = nil
    end

    def running?
      @threads.any?(&:alive?)
    end

    def run
      partition_counts = fetch_partition_counts
      @current_thread_count = partition_counts.values.max
      partition_counts.each { |topic, count| logger.info "=> Discovered #{count} partitions for topic '#{topic}'" }
      logger.info "=> Starting #{@current_thread_count} threaded consumers"

      spawn_runners(@current_thread_count)
      start_metadata_watcher if config.dynamic_partition_scaling

      install_signal_handlers

      @threads.each(&:join)

      raise @thread_error if @thread_error
    end

    def stop
      @stop_requested = true
      @mutex.synchronize { @runners.each(&:stop) }
    end

    private

    attr_reader :consumer_class, :config, :logger, :instrumenter

    def spawn_runners(count)
      count.times do
        processor = consumer_class.new
        runner = Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter)
        thread = spawn_thread(runner)
        @mutex.synchronize do
          @runners << runner
          @threads << thread
        end
      end
    end

    def spawn_thread(runner)
      index = @mutex.synchronize { @threads.size }
      Thread.new do
        Thread.current.name = "racecar-thread-#{index}"
        runner.run
      rescue Exception => e
        logger.error "Thread #{index} crashed: #{e.class}: #{e.message}"
        @thread_error ||= e
        stop
      end
    end

    def install_signal_handlers
      SHUTDOWN_SIGNALS.each { |signal| trap(signal) { stop } }
      trap("USR1") { $stderr.puts config.inspect }
    end

    def start_metadata_watcher
      @watcher_thread = Thread.new do
        Thread.current.name = "racecar-metadata-watcher"
        until @stop_requested
          sleep(METADATA_POLL_INTERVAL)
          break if @stop_requested
          check_for_new_partitions
        end
      end
    end

    def check_for_new_partitions
      new_counts = fetch_partition_counts
      new_max = new_counts.values.max
      delta = new_max - @current_thread_count
      return unless delta > 0

      logger.info "=> Partition count increased to #{new_max}, spawning #{delta} new threads"
      spawn_runners(delta)
      @current_thread_count = new_max
    rescue => e
      logger.warn "Metadata poll failed: #{e.message}"
    end

    def fetch_partition_counts
      metadata_config = {
        "bootstrap.servers" => config.brokers.join(","),
        "group.id" => config.group_id,
      }.merge(config.rdkafka_consumer)

      consumer = Rdkafka::Config.new(metadata_config).consumer
      native_kafka = consumer.instance_variable_get(:@native_kafka)

      counts = {}
      config.subscriptions.each do |subscription|
        topic_metadata = nil
        native_kafka.with_inner do |inner|
          topic_metadata = ::Rdkafka::Metadata.new(inner, subscription.topic).topics&.first
        end
        count = topic_metadata ? topic_metadata[:partition_count] : 0
        raise Racecar::Error, "Could not discover partitions for topic '#{subscription.topic}'" if count <= 0
        counts[subscription.topic] = count
      end

      consumer.close
      counts
    end
  end
end
