# frozen_string_literal: true

require 'racecar/producer_methods'
require 'racecar/processing'
require 'racecar/pause'

module Racecar
  class PartitionProcessor
    include Processing
    include ProducerMethods

    attr_reader :consumer_class_instance, :config, :logger, :instrumenter, :consumer, :pauses
    attr_accessor :rebalancing, :shutting_down

    def initialize(config:, logger:, instrumenter:, consumer_class_instance:, runner_mutex:, consumer:, pauses:)
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @consumer_class_instance = consumer_class_instance
      @pauses = pauses
      @runner_mutex = runner_mutex
      @consumer = consumer
    end

    def process(message)
      payload = instrumentation_payload(message)
      @instrumenter.instrument("start_process_message", payload)

      with_error_handling(message, payload) do |pause|
        instrument_process_message(payload) do
          consumer_class_instance.process(Racecar::Message.new(message, retries_count: pause.pauses_count))
          consumer_class_instance.deliver!
          consumer.store_offset(message)
        end
      end
    end

    def process_batch(messages)
      payload = instrumentation_payload_for_batch(messages)
      @instrumenter.instrument("start_process_batch", payload)

      with_error_handling(messages, payload) do |pause|
        instrument_process_batch(payload) do
          racecar_messages = messages.map do |message|
            Racecar::Message.new(message, retries_count: pause.pauses_count)
          end
          consumer_class_instance.process_batch(racecar_messages)
          consumer_class_instance.deliver!
          consumer.store_offset(messages.last)
        end
      end
    end

    private

    def with_error_handling(messages, payload)
      if config.multithreaded_processing_enabled
        with_multi_threaded_error_handling(messages, payload) do |pause|
          yield(pause)
        end
      else
        with_single_threaded_error_handling(messages, payload) do |pause|
          yield(pause)
        end
      end
    end

    def with_multi_threaded_error_handling(messages, payload)
      loop do
        begin
          topic = messages.is_a?(Array) ? messages.first.topic : messages.topic
          partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
          pause = pauses[topic][partition]
          yield(pause)
          pause.reset!
          break
        rescue => e
          if rebalancing
            Thread.exit
          elsif !shutting_down
            pause = pauses[topic][partition]
            handle_processing_error(e, payload, pause: pause, with_synchronization: true)
            pause.pause!
            sleep(pause.backoff_interval) unless config.pause_timeout <= 0
          else
            break
          end
        end
      end
    end

    def with_single_threaded_error_handling(messages, payload)
      topic = messages.is_a?(Array) ? messages.first.topic : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      offsets = messages.is_a?(Array) ? messages.first.offset..messages.last.offset : messages.offset..messages.offset
      with_pause(topic, partition, offsets) do |pause|
        yield(pause)
      rescue => e
        handle_processing_error(e, payload, pause: pause)
        raise e
      end

      resume_all_paused_partitions
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
        consumer.pause(topic, partition, offsets.first)
        pause.pause!
      end
    end
  end
end

