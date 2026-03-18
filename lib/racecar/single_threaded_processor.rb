# frozen_string_literal: true

require 'racecar/producer_methods'

module Racecar
  class SingleThreadedProcessor
    include Processing
    include ProducerMethods

    attr_reader :consumer_class_instance, :config, :logger, :instrumenter, :consumer, :pauses

    def initialize(config:, logger:, instrumenter:, consumer_class_instance:, consumer:, pauses:)
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @consumer_class_instance = consumer_class_instance
      @consumer = consumer
      @pauses = pauses
    end

    def shutdown_and_wait
      # no-op for single-threaded processor
    end

    def set_to_rebalance(topic, partition)
      # no-op for single-threaded processor
    end

    def process(message)
      payload = instrumentation_payload(message)

      with_pause(message.topic, message.partition, message.offset..message.offset) do |pause|
        begin
          instrument_process_message(payload) do
            consumer_class_instance.process(Racecar::Message.new(message, retries_count: pause.pauses_count))
            consumer_class_instance.deliver!
            consumer.store_offset(message)
          end
        rescue => e
          handle_processing_error(e, payload, pause: pause)
          raise e
        end
      end
    end

    def process_batch(messages)
      payload = instrumentation_payload_for_batch(messages)
      first, last = messages.first, messages.last

      with_pause(first.topic, first.partition, first.offset..last.offset) do |pause|
        begin
          instrument_process_batch(payload) do
            racecar_messages = messages.map do |message|
              Racecar::Message.new(message, retries_count: pause.pauses_count)
            end
            consumer_class_instance.process_batch(racecar_messages)
            consumer_class_instance.deliver!
            consumer.store_offset(messages.last)
          end
        rescue => e
          handle_processing_error(e, payload, pause: pause)
          raise e
        end
      end
    end

    private

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
        consumer.pause(topic, partition, offsets.first)
        pause.pause!
      end
    end
  end
end

