# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/delivery_callback"

module Racecar
  class PartitionProcessor
    attr_reader :consumer_class_instance, :config, :logger, :instrumenter, :consumer, :topic, :partition, :pause
    attr_accessor :rebalancing, :shutting_down

    def initialize(config:, logger:, instrumenter:, consumer_class_instance:, consumer:, topic:, partition:, pause:)
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @consumer_class_instance = consumer_class_instance
      @pause = pause
      @topic = topic
      @partition = partition
      @consumer = consumer

      consumer_class_instance.configure(
        producer:     consumer.producer,
        consumer:     @consumer,
        instrumenter: @instrumenter,
        config:       @config,
      )
    end

    def process(message)
      payload = {
        consumer_class: consumer_class_instance.class.to_s,
        topic:          message.topic,
        partition:      message.partition,
        offset:         message.offset,
        create_time:    message.timestamp,
        key:            message.key,
        value:          message.payload,
        headers:        message.headers,
      }
      @instrumenter.instrument("start_process_message", payload)

      with_error_handling(message, payload) do |pause|
        @instrumenter.instrument("process_message", payload) do
          reconfigure_consumer_class_instance! if consumer_class_instance.instance_variable_get(:@producer)&.closed?
          consumer_class_instance.process(Racecar::Message.new(message, retries_count: pause.pauses_count))
          consumer_class_instance.deliver!
          consumer.store_offset(message)
        end
      end
    end

    def process_batch(messages)
      first, last = messages.first, messages.last
      payload = {
        consumer_class:   consumer_class_instance.class.to_s,
        topic:            first.topic,
        partition:        first.partition,
        first_offset:     first.offset,
        last_offset:      last.offset,
        last_create_time: last.timestamp,
        message_count:    messages.size,
      }
      @instrumenter.instrument("start_process_batch", payload)

      with_error_handling(messages, payload) do |pause|
        @instrumenter.instrument("process_batch", payload) do
          racecar_messages = messages.map do |message|
            Racecar::Message.new(message, retries_count: pause.pauses_count)
          end
          reconfigure_consumer_class_instance! if consumer_class_instance.instance_variable_get(:@producer)&.closed?
          consumer_class_instance.process_batch(racecar_messages)
          consumer_class_instance.deliver!
          consumer.store_offset(messages.last)
        end
      end
    end

    def teardown
      consumer_class_instance.deliver! unless rebalancing
    ensure
      consumer_class_instance.teardown
    end

    def resume_paused_partition
      return if config.pause_timeout == 0

      @instrumenter.instrument("pause_status", {
        topic:          topic,
        partition:      partition,
        duration:       pause.pause_duration,
        consumer_class: consumer_class_instance.class.to_s,
      })

      if pause.paused? && pause.expired?
        logger.info "Automatically resuming partition #{topic}/#{partition}, pause timeout expired"
        consumer.resume(topic, partition)
        pause.resume!
      end
    end

    def rebalance!
      @rebalancing = true
      resume_paused_partition
    end

    def shut_down!
      @shutting_down = true
      resume_paused_partition
    end

    private

    def with_error_handling(messages, payload)
      if config.multithreaded_processing_enabled
        with_multi_threaded_error_handling(messages, payload) { |pause| yield(pause) }
      else
        with_single_threaded_error_handling(messages, payload) { |pause| yield(pause) }
      end
    end

    def with_multi_threaded_error_handling(messages, payload)
      loop do
        begin
          yield(pause)
          pause.reset!
          break
        rescue => e
          if rebalancing
            Thread.exit
          elsif !shutting_down
            handle_processing_error(e, payload, pause: pause)
            pause.pause!
            sleep(pause.backoff_interval) unless config.pause_timeout <= 0
          else
            break
          end
        end
      end
    end

    def with_single_threaded_error_handling(messages, payload)
      offsets = messages.is_a?(Array) ? messages.first.offset..messages.last.offset : messages.offset..messages.offset
      with_pause(offsets) do
        yield(pause)
      rescue => e
        handle_processing_error(e, payload, pause: pause)
        raise e
      end
    end

    def with_pause(offsets)
      return yield if config.pause_timeout == 0

      begin
        yield
        pause.reset!
      rescue => e
        desc = "#{topic}/#{partition}"
        logger.error "Failed to process #{desc} at #{offsets}: #{e}"
        logger.warn "Pausing partition #{desc} for #{pause.backoff_interval} seconds"
        consumer.pause(topic, partition, offsets.first)
        pause.pause!
      end
    end

    def handle_processing_error(error, payload, pause:)
      if error.is_a?(Racecar::MessageDeliveryError) && error.code == :msg_timed_out
        logger.error error.to_s
        logger.error "Racecar will reset the producer to force a new broker connection."
        reset_producer!
        payload[:unrecoverable_delivery_error] = true
      else
        payload[:unrecoverable_delivery_error] = false
      end
      payload[:retries_count] = pause.pauses_count
      config.error_handler.call(error, payload)
    end

    def reset_producer!
      consumer.reset_producer!
      reconfigure_consumer_class_instance!
    end

    def reconfigure_consumer_class_instance!
      consumer_class_instance.configure(
        producer:     consumer.producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
      )
    end
  end
end
