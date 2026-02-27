# frozen_string_literal: true

require "rdkafka"

module Racecar
  class ThreadedRunner
    SHUTDOWN_SIGNALS = ["INT", "QUIT", "TERM"]

    def initialize(consumer_class:, config:, logger:, instrumenter:)
      @consumer_class = consumer_class
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @runners = []
      @threads = []
      @thread_error = nil
    end

    def running?
      @threads.any?(&:alive?)
    end

    def run
      partition_counts = fetch_partition_counts
      thread_count = partition_counts.values.max
      partition_counts.each { |topic, count| logger.info "=> Discovered #{count} partitions for topic '#{topic}'" }
      logger.info "=> Starting #{thread_count} threaded consumers"

      @runners = thread_count.times.map do
        processor = consumer_class.new
        Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter)
      end

      @threads = @runners.each_with_index.map do |runner, i|
        Thread.new do
          Thread.current.name = "racecar-thread-#{i}"
          runner.run
        rescue Exception => e
          logger.error "Thread #{i} crashed: #{e.class}: #{e.message}"
          @thread_error ||= e
          stop
        end
      end

      install_signal_handlers

      @threads.each(&:join)

      raise @thread_error if @thread_error
    end

    def stop
      @runners.each(&:stop)
    end

    private

    attr_reader :consumer_class, :config, :logger, :instrumenter

    def install_signal_handlers
      SHUTDOWN_SIGNALS.each { |signal| trap(signal) { stop } }
      trap("USR1") { $stderr.puts config.inspect }
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
