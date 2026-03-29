# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/delivery_callback"

module Racecar
  class PartitionProcessor
    attr_reader :consumer_class_instance, :config, :logger, :instrumenter, :consumer, :pauses
    attr_accessor :rebalancing, :shutting_down

    def initialize(config:, logger:, instrumenter:, consumer_class:, consumer:, pauses:)
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @consumer_class_instance = consumer_class.new
      @pauses = pauses
      @consumer = consumer

      if consumer_class.method_defined?(:statistics_callback) && Rdkafka::Config.statistics_callback.nil?
        Rdkafka::Config.statistics_callback = @consumer_class_instance.method(:statistics_callback).to_proc
      end

      @consumer_class_instance.configure(
        producer:     producer,
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
          consumer_class_instance.process_batch(racecar_messages)
          consumer_class_instance.deliver!
          consumer.store_offset(messages.last)
        end
      end
    end

    def producer
      @producer ||= Rdkafka::Config.new(producer_config).producer.tap do |p|
        p.delivery_callback = Racecar::DeliveryCallback.new(instrumenter: @instrumenter)
      end
    end

    def teardown
      consumer_class_instance.deliver! unless rebalancing
    ensure
      consumer_class_instance.teardown unless rebalancing
    end

    def close
      producer.close
    end

    private

    def resume_all_paused_partitions
      return if config.pause_timeout == 0

      pauses.each do |topic, partitions|
        partitions.each do |partition, pause|
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
      end
    end

    def producer_config
      cfg = {
        "bootstrap.servers"      => config.brokers.join(","),
        "client.id"              => config.client_id,
        "statistics.interval.ms" => config.statistics_interval_ms,
        "message.timeout.ms"     => config.message_timeout * 1000,
        "partitioner"            => config.partitioner.to_s,
      }
      cfg["compression.codec"] = config.producer_compression_codec.to_s unless config.producer_compression_codec.nil?
      cfg.merge!(config.rdkafka_producer)
      cfg
    end

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
          topic, partition = topic_and_partition(messages)
          pause = pauses[topic][partition]
          yield(pause)
          pause.reset!
          break
        rescue => e
          if rebalancing
            Thread.exit
          elsif !shutting_down
            pause = pauses[topic][partition]
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
      topic, partition = topic_and_partition(messages)
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
        pauses[topic][partition].reset!
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
      producer.close
      @producer = nil
      consumer_class_instance.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
      )
    end

    def topic_and_partition(messages)
      topic     = messages.is_a?(Array) ? messages.first.topic     : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      [topic, partition]
    end
  end
end
