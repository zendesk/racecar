require "kafka"

module Racecar
  class Runner
    attr_reader :processor, :config, :logger

    def initialize(processor, config:, logger:)
      @processor, @config, @logger = processor, config, logger
    end

    def run
      kafka = Kafka.new(
        client_id: config.client_id,
        seed_brokers: config.brokers,
        logger: logger,
        connect_timeout: config.connect_timeout,
        socket_timeout: config.socket_timeout,
        ssl_ca_cert: config.ssl_ca_cert,
        ssl_client_cert: config.ssl_client_cert,
        ssl_client_cert_key: config.ssl_client_cert_key,
      )

      consumer = kafka.consumer(
        group_id: config.group_id,
        offset_commit_interval: config.offset_commit_interval,
        offset_commit_threshold: config.offset_commit_threshold,
        heartbeat_interval: config.heartbeat_interval,
      )

      # Stop the consumer on SIGINT, SIGQUIT or SIGTERM.
      trap("QUIT") { consumer.stop }
      trap("INT") { consumer.stop }
      trap("TERM") { consumer.stop }

      # Print the consumer config to STDERR on USR1.
      trap("USR1") { $stderr.puts config.inspect }

      config.subscriptions.each do |subscription|
        consumer.subscribe(
          subscription.topic,
          start_from_beginning: subscription.start_from_beginning,
          max_bytes_per_partition: subscription.max_bytes_per_partition,
        )
      end

      begin
        if processor.respond_to?(:process)
          consumer.each_message(max_wait_time: config.max_wait_time) do |message|
            processor.process(message)
          end
        elsif processor.respond_to?(:process_batch)
          consumer.each_batch(max_wait_time: config.max_wait_time) do |batch|
            processor.process_batch(batch)
          end
        else
          raise NotImplementedError, "Consumer class must implement process or process_batch method", caller
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
