require "kafka"

module Racecar
  class Runner
    attr_reader :processor, :config, :logger, :consumer

    def initialize(processor, config:, logger:, instrumenter: NullInstrumenter)
      @processor, @config, @logger = processor, config, logger
      @instrumenter = instrumenter
    end

    def stop
      Thread.new do
        processor.teardown
        consumer.stop unless consumer.nil?
      end.join
    end

    def run
      kafka = Kafka.new(
        client_id: config.client_id,
        seed_brokers: config.brokers,
        logger: logger,
        connect_timeout: config.connect_timeout,
        socket_timeout: config.socket_timeout,
        ssl_ca_cert: config.ssl_ca_cert,
        ssl_ca_cert_file_path: config.ssl_ca_cert_file_path,
        ssl_client_cert: config.ssl_client_cert,
        ssl_client_cert_key: config.ssl_client_cert_key,
        ssl_client_cert_key_password: config.ssl_client_cert_key_password,
        sasl_plain_username: config.sasl_plain_username,
        sasl_plain_password: config.sasl_plain_password,
        sasl_scram_username: config.sasl_scram_username,
        sasl_scram_password: config.sasl_scram_password,
        sasl_scram_mechanism: config.sasl_scram_mechanism,
        sasl_over_ssl: config.sasl_over_ssl,
        ssl_ca_certs_from_system: config.ssl_ca_certs_from_system,
        ssl_verify_hostname: config.ssl_verify_hostname
      )

      @consumer = kafka.consumer(
        group_id: config.group_id,
        offset_commit_interval: config.offset_commit_interval,
        offset_commit_threshold: config.offset_commit_threshold,
        session_timeout: config.session_timeout,
        heartbeat_interval: config.heartbeat_interval,
        offset_retention_time: config.offset_retention_time,
        fetcher_max_queue_size: config.max_fetch_queue_size,
      )

      # Stop the consumer on SIGINT, SIGQUIT or SIGTERM.
      trap("QUIT") { stop }
      trap("INT") { stop }
      trap("TERM") { stop }

      # Print the consumer config to STDERR on USR1.
      trap("USR1") { $stderr.puts config.inspect }

      config.subscriptions.each do |subscription|
        consumer.subscribe(
          subscription.topic,
          start_from_beginning: subscription.start_from_beginning,
          max_bytes_per_partition: subscription.max_bytes_per_partition,
        )
      end

      # Configure the consumer with a producer so it can produce messages.
      producer = kafka.producer(
        compression_codec: config.producer_compression_codec,
      )

      processor.configure(consumer: consumer, producer: producer)

      begin
        if processor.respond_to?(:process)
          consumer.each_message(max_wait_time: config.max_wait_time, max_bytes: config.max_bytes) do |message|
            payload = {
              consumer_class: processor.class.to_s,
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
            }

            @instrumenter.instrument("process_message.racecar", payload) do
              processor.process(message)
              producer.deliver_messages
            end
          end
        elsif processor.respond_to?(:process_batch)
          consumer.each_batch(max_wait_time: config.max_wait_time, max_bytes: config.max_bytes) do |batch|
            payload = {
              consumer_class: processor.class.to_s,
              topic: batch.topic,
              partition: batch.partition,
              first_offset: batch.first_offset,
              message_count: batch.messages.count,
            }

            @instrumenter.instrument("process_batch.racecar", payload) do
              processor.process_batch(batch)
              producer.deliver_messages
            end
          end
        else
          raise NotImplementedError, "Consumer class must implement process or process_batch method"
        end
      rescue Kafka::ProcessingError => e
        @logger.error "Error processing partition #{e.topic}/#{e.partition} at offset #{e.offset}"

        if config.pause_timeout > 0
          # Pause fetches from the partition. We'll continue processing the other partitions in the topic.
          # The partition is automatically resumed after the specified timeout, and will continue where we
          # left off.
          @logger.warn "Pausing partition #{e.topic}/#{e.partition} for #{config.pause_timeout} seconds"
          consumer.pause(
            e.topic,
            e.partition,
            timeout: config.pause_timeout,
            max_timeout: config.max_pause_timeout,
            exponential_backoff: config.pause_with_exponential_backoff?,
          )
        elsif config.pause_timeout == -1
          # A pause timeout of -1 means indefinite pausing, which in ruby-kafka is done by passing nil as
          # the timeout.
          @logger.warn "Pausing partition #{e.topic}/#{e.partition} indefinitely, or until the process is restarted"
          consumer.pause(e.topic, e.partition, timeout: nil)
        end

        config.error_handler.call(e.cause, {
          topic: e.topic,
          partition: e.partition,
          offset: e.offset,
        })

        # Restart the consumer loop.
        retry
      rescue Kafka::InvalidSessionTimeout
        raise ConfigError, "`session_timeout` is set either too high or too low"
      rescue Kafka::Error => e
        error = "#{e.class}: #{e.message}\n" + e.backtrace.join("\n")
        @logger.error "Consumer thread crashed: #{error}"

        config.error_handler.call(e)

        raise
      else
        @logger.info "Gracefully shutting down"
      end
    end
  end
end
