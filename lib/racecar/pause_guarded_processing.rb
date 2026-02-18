require "rdkafka"

module Racecar
  module PauseGuardedProcessing

    def process_with_pause(message)
      instrumentation_payload = {
        consumer_class: processor.class.to_s,
        topic:          message.topic,
        partition:      message.partition,
        offset:         message.offset,
        create_time:    message.timestamp,
        key:            message.key,
        value:          message.payload,
        headers:        message.headers
      }

      @instrumenter.instrument("start_process_message", instrumentation_payload)
      with_pause(message.topic, message.partition, message.offset..message.offset) do |pause|
        begin
          @instrumenter.instrument("process_message", instrumentation_payload) do
            processor.process(Racecar::Message.new(message, retries_count: pause.pauses_count))
            processor.deliver!
            consumer.store_offset(message)
          end
        rescue => e
          instrumentation_payload[:unrecoverable_delivery_error] = reset_producer_on_unrecoverable_delivery_errors(e)
          instrumentation_payload[:retries_count] = pause.pauses_count
          config.error_handler.call(e, instrumentation_payload)
          raise e
        end
      end
    end

    def process_batch_with_pause(messages)
      first, last = messages.first, messages.last
      instrumentation_payload = {
        consumer_class: processor.class.to_s,
        topic:          first.topic,
        partition:      first.partition,
        first_offset:   first.offset,
        last_offset:    last.offset,
        last_create_time: last.timestamp,
        message_count:  messages.size
      }

      @instrumenter.instrument("start_process_batch", instrumentation_payload)
      with_pause(first.topic, first.partition, first.offset..last.offset) do |pause|
        begin
          @instrumenter.instrument("process_batch", instrumentation_payload) do
            racecar_messages = messages.map do |message|
              Racecar::Message.new(message, retries_count: pause.pauses_count)
            end
            processor.process_batch(racecar_messages)
            processor.deliver!
            consumer.store_offset(messages.last)
          end
        rescue => e
          instrumentation_payload[:unrecoverable_delivery_error] = reset_producer_on_unrecoverable_delivery_errors(e)
          instrumentation_payload[:retries_count] = pause.pauses_count
          config.error_handler.call(e, instrumentation_payload)
          raise e
        end
      end
    end

    def consumer
      @consumer ||= begin
                      ConsumerSet.new(config, logger, @instrumenter)
                    end
    end

    def producer
      @producer ||= Rdkafka::Config.new(producer_config).producer.tap do |producer|
        producer.delivery_callback = Racecar::DeliveryCallback.new(instrumenter: @instrumenter)
      end
    end

    private

    def producer_config
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      producer_config = {
        "bootstrap.servers"      => config.brokers.join(","),
        "client.id"              => config.client_id,
        "statistics.interval.ms" => config.statistics_interval_ms,
        "message.timeout.ms"     => config.message_timeout * 1000,
        "partitioner"            => config.partitioner.to_s,
      }

      producer_config["compression.codec"] = config.producer_compression_codec.to_s unless config.producer_compression_codec.nil?
      producer_config.merge!(config.rdkafka_producer)
      producer_config
    end

    def with_pause(topic, partition, offsets)
      pause = pauses[topic][partition]
      return yield pause if config.pause_timeout == 0

      begin
        yield pause
        # We've successfully processed a batch from the partition, so we can clear the pause.
        pauses[topic][partition].reset!
      rescue => e
        desc = "#{topic}/#{partition}"
        logger.error "Failed to process #{desc} at #{offsets}: #{e}"

        logger.warn "Pausing partition #{desc} for #{pause.backoff_interval} seconds"
        consumer.thread_safe_pause(topic, partition, offsets.first)
        pause.pause!
      end
    end

    # librdkafka will continue to try to deliver already queued messages, even if ruby-rdkafka
    # raised before that. This method detects any unrecoverable errors and resets the producer
    # as a last ditch effort.
    # The function returns true if there were unrecoverable errors, or false otherwise.
    def reset_producer_on_unrecoverable_delivery_errors(error)
      return false unless error.is_a?(Racecar::MessageDeliveryError)
      return false unless error.code == :msg_timed_out # -192

      logger.error error.to_s
      logger.error "Racecar will reset the producer to force a new broker connection."
      @producer.close
      @producer = nil
      processor.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
        )
      true
    end

    def resume_paused_partitions
      return if config.pause_timeout == 0

      pauses.each do |topic, partitions|
        partitions.each do |partition, pause|
          instrumentation_payload = {
            topic:      topic,
            partition:  partition,
            duration:   pause.pause_duration,
            consumer_class: processor.class.to_s,
          }
          @instrumenter.instrument("pause_status", instrumentation_payload)

          if pause.paused? && pause.expired?
            logger.info "Automatically resuming partition #{topic}/#{partition}, pause timeout expired"
            consumer.thread_safe_resume(topic, partition)
            pause.resume!
            # TODO: # During re-balancing we might have lost the paused partition. Check if partition is still in group before seek. ?
          end
        end
      end
    end
  end
end