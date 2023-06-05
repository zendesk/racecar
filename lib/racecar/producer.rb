# frozen_string_literal: true

require "racecar/message_delivery_error"

at_exit do
  Racecar::Producer.shutdown!
end

module Racecar
  class Producer

    @@mutex = Mutex.new

    class << self
      def shutdown!
        @@mutex.synchronize do
          if !@internal_producer.nil?
            @internal_producer.close
          end
        end
      end
    end

    def initialize(config: nil, logger: nil, instrumenter: NullInstrumenter)
      @config = config
      @logger = logger
      @delivery_handles = []
      @instrumenter = instrumenter
      @batching = false
      @internal_producer = init_internal_producer(config)
    end

    def init_internal_producer(config)
      @@mutex.synchronize do
        @@init_internal_producer ||= begin
          # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
          producer_config = {
            "bootstrap.servers"      => config.brokers.join(","),
            "client.id"              => config.client_id,
            "statistics.interval.ms" => config.statistics_interval_ms,
            "message.timeout.ms"     => config.message_timeout * 1000,
          }
          producer_config["compression.codec"] = config.producer_compression_codec.to_s unless config.producer_compression_codec.nil?
          producer_config.merge!(config.rdkafka_producer)
          producer = Rdkafka::Config.new(producer_config).producer

          producer.delivery_callback = lambda { |delivery_report|
            if delivery_report.error.to_i.positive?
              instrumentation_payload = {
                topic: delivery_report.topic,
                partition: delivery_report.partition,
                exception: delivery_report.error
              }
              @instrumenter.instrument("produce_error", instrumentation_payload)
            end
          } 
          producer
        end
      end
    end


    # fire and forget - you won't get any guarantees or feedback from 
    # Racecar on the status of the message and it won't halt execution
    # of the rest of your code.
    def produce_async(value:, topic:, **options)
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
      @instrumenter.instrument("produce_async", instrumentation_payload) do
        handle = internal_producer.produce(payload: value, topic: topic, **options)
        @delivery_handles << handle if @batching
      end

      nil
    end

    # synchronous message production - will wait until the delivery handle succeeds, fails or times out.
    def produce_sync(value:, topic:, **options)
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
      @instrumenter.instrument("produce_sync", instrumentation_payload) do
        handle = internal_producer.produce(payload: value, topic: topic, **options)
        begin
          deliver_with_error_handling(handle)
        rescue MessageDeliveryError => e
          instrumentation_payload[:exception] = e
          raise e
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

    attr_reader :internal_producer

    def deliver_with_error_handling(handle)
      handle.wait
    rescue Rdkafka::AbstractHandle::WaitTimeoutError => e
      partition = MessageDeliveryError.partition_from_delivery_handle(handle)
      @logger.warn "Still trying to deliver message to (partition #{partition})... (will try up to Racecar.config.message_timeout)"
      retry
    rescue Rdkafka::RdkafkaError => e
      raise MessageDeliveryError.new(e, handle)
    end
  end
end
