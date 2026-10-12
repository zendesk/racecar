# frozen_string_literal: true

require "racecar/message_delivery_error"
require "racecar/delivery_callback"

at_exit do
  Racecar::Producer.shutdown!
end

module Racecar
  class Producer

    @@mutex = Mutex.new
    @@internal_producer = nil

    class << self
      # Close the internal rdkafka producer. Subsequent attempts to
      # produce messages after shutdown will result in errors.
      def shutdown!
        @@mutex.synchronize do
          @@internal_producer&.close
        end
      end

      # Reset internal state. Subsequent attempts to produce messages will
      # reinitialize internal resources.
      #
      # Before forking a process, it is recommended to call this method. See:
      # https://github.com/karafka/rdkafka-ruby/blob/main/README.md#forking
      def reset!
        @@mutex.synchronize do
          @@internal_producer&.close
          @@internal_producer = nil
        end
      end
    end

    def initialize(config: nil, logger: nil, instrumenter: NullInstrumenter)
      @config = config
      @logger = logger
      @delivery_handles = []
      @instrumenter = instrumenter
      @batching = false
      init_internal_producer(config)
    end

    def init_internal_producer(config)
      @@mutex.synchronize do
        @@internal_producer ||= begin
          # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
          producer_config = {
            "bootstrap.servers"      => config.brokers.join(","),
            "client.id"              => config.client_id,
            "statistics.interval.ms" => config.statistics_interval_ms,
            "message.timeout.ms"     => config.message_timeout * 1000,
            "partitioner"            => config.partitioner.to_s
          }
          producer_config["compression.codec"] = config.producer_compression_codec.to_s unless config.producer_compression_codec.nil?
          producer_config.merge!(config.rdkafka_producer)
          Rdkafka::Config.new(producer_config).producer.tap do |producer|
            producer.delivery_callback = DeliveryCallback.new(instrumenter: @instrumenter)
          end
        end
      end
    end

    # fire and forget - you won't get any guarantees or feedback from
    # Racecar on the status of the message and it won't halt execution
    # of the rest of your code.
    def produce_async(value:, topic:, **options)
      with_instrumentation(action: "produce_async", value: value, topic: topic, **options) do
        begin
          handle = internal_producer.produce(payload: value, topic: topic, **options)
          @delivery_handles << handle if @batching
        rescue Rdkafka::RdkafkaError => e
          raise MessageDeliveryError.new(e, handle)
        end
      end

      nil
    end

    # synchronous message production - will wait until the delivery handle succeeds, fails or times out.
    def produce_sync(value:, topic:, **options)
      with_instrumentation(action: "produce_sync", value: value, topic: topic, **options) do
        begin
          handle = internal_producer.produce(payload: value, topic: topic, **options)
          deliver_with_error_handling(handle)
        rescue Rdkafka::RdkafkaError => e
          raise MessageDeliveryError.new(e, handle)
        end
      end

      nil
    end

    # Blocks until all messages that have been asynchronously produced in the block have been delivered.
    # Usage:
    # messages = [
    #             {value: "message1", topic: "topic1"},
    #             {value: "message2", topic: "topic1"},
    #             {value: "message3", topic: "topic2"}
    #             ]
    # Racecar.wait_for_delivery {
    #   messages.each do |msg|
    #     Racecar.produce_async(value: msg[:value], topic: msg[:topic])
    #   end
    # }
    def wait_for_delivery
      @batching = true
      @delivery_handles.clear
      yield
      @delivery_handles.each do |handle|
        deliver_with_error_handling(handle)
      end
    ensure
      @delivery_handles.clear
      @batching = false

      nil
    end

    private

    def internal_producer
      @@internal_producer || init_internal_producer(@config)
    end

    def deliver_with_error_handling(handle)
      handle.wait
    rescue Rdkafka::AbstractHandle::WaitTimeoutError => e
      partition = MessageDeliveryError.partition_from_delivery_handle(handle)
      @logger.warn "Still trying to deliver message to (partition #{partition})... (will try up to Racecar.config.message_timeout)"
      retry
    rescue Rdkafka::RdkafkaError => e
      raise MessageDeliveryError.new(e, handle)
    end

    def with_instrumentation(action:, value:, topic:, **options)
      message_size = value.respond_to?(:bytesize) ? value.bytesize : 0
      instrumentation_payload = {
        value: value,
        topic: topic,
        message_size: message_size,
        buffer_size: @delivery_handles.size,
        key: options.fetch(:key, nil),
        partition: options.fetch(:partition, nil),
        partition_key: options.fetch(:partition_key, nil)
      }
      @instrumenter.instrument(action, instrumentation_payload) do
        yield
      end
    end
  end
end
