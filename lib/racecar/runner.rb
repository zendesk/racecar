require "kafka"

module Racecar
  class Runner
    attr_reader :consumer_class, :config, :logger

    def initialize(consumer_class, config:, logger:)
      @consumer_class, @config, @logger = consumer_class, config, logger
    end

    def run
      kafka = Kafka.new(
        client_id: config.client_id,
        seed_brokers: config.brokers,
        logger: logger,
        connect_timeout: config.connect_timeout,
        socket_timeout: config.socket_timeout,
      )

      consumer = kafka.consumer(
        group_id: config.group_id,
        offset_commit_interval: config.offset_commit_interval,
        offset_commit_threshold: config.offset_commit_threshold,
        heartbeat_interval: config.heartbeat_interval,
      )

      # Stop the consumer on SIGINT and SIGQUIT.
      trap("QUIT") { consumer.stop }
      trap("INT") { consumer.stop }

      config.subscriptions.each do |subscription|
        topic = subscription.topic
        start_from_beginning = subscription.start_from_beginning

        consumer.subscribe(topic, start_from_beginning: start_from_beginning)
      end

      consumer_object = consumer_class.new

      begin
        consumer.each_message(max_wait_time: config.max_wait_time) do |message|
          consumer_object.process(message)
        end
      rescue Kafka::ProcessingError => e
        @logger.error "Error processing partition #{e.topic}/#{e.partition} at offset #{e.offset}"

        if config.pause_timeout > 0
          # Pause fetches from the partition. We'll continue processing the other partitions in the topic.
          # The partition is automatically resumed after the specified timeout, and will continue where we
          # left off.
          @logger.warn "Pausing partition #{e.topic}/#{e.partition} for #{config.pause_timeout} seconds"
          consumer.pause(e.topic, e.partition, timeout: config.pause_timeout)
        end

        config.error_handler.call(e.cause, {
          topic: e.topic,
          partition: e.partition,
          offset: e.offset,
        })

        # Restart the consumer loop.
        retry
      rescue Kafka::Error => e
        error = "#{e.class}: #{e.message}\n" + e.backtrace.join("\n")
        @logger.error "Consumer thread crashed: #{error}"

        config.error_handler.call(e)
      end

      @logger.info "Gracefully shutting down"
    end
  end
end
