# frozen_string_literal: true

require "racecar/message_delivery_error"

module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :start_from_beginning, :max_bytes_per_partition, :additional_config)

    class << self
      attr_accessor :max_wait_time
      attr_accessor :group_id
      attr_accessor :producer, :consumer, :parallel_workers, :fetch_messages

      def subscriptions
        @subscriptions ||= []
      end

      # Adds one or more topic subscriptions.
      #
      # Can be called multiple times in order to subscribe to more topics.
      #
      # @param topics [String] one or more topics to subscribe to.
      # @param start_from_beginning [Boolean] whether to start from the beginning or the end
      #   of each partition.
      # @param max_bytes_per_partition [Integer] the maximum number of bytes to fetch from
      #   each partition at a time.
      # @param additional_config [Hash] Configuration properties for consumer.
      #   See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      # @return [nil]
      def subscribes_to(
        *topics,
        start_from_beginning: true,
        max_bytes_per_partition: 1048576,
        additional_config: {}
      )
        topics.each do |topic|
          subscriptions << Subscription.new(topic, start_from_beginning, max_bytes_per_partition, additional_config)
        end
      end

      # Rebalance hooks for subclasses to override
      def on_partitions_assigned(rebalance_event); end
      def on_partitions_revoked(rebalance_event); end
    end

    def configure(producer:, consumer:, instrumenter: NullInstrumenter, config: Racecar.config)
      @producer = producer
      @delivery_handles = []

      @consumer = consumer

      @instrumenter = instrumenter
      @config = config
    end

    def teardown; end

    # Blocks until all messages produced so far have been successfully published. If
    # message delivery finally fails, a Racecar::MessageDeliveryError is raised. The
    # delivery failed for the reason in the exception. The error can be broker side
    # (e.g. downtime, configuration issue) or specific to the message being sent. The
    # caller must handle the latter cases or run into head of line blocking.
    def deliver!
      @delivery_handles ||= []
      if @delivery_handles.any?
        instrumentation_payload = { delivered_message_count: @delivery_handles.size }

        @instrumenter.instrument('deliver_messages', instrumentation_payload) do
          @delivery_handles.each do |handle|
            begin
              # rdkafka-ruby checks with exponential backoff starting at 0 seconds wait
              # if the message was successfully delivered, up to max_wait_timeout seconds
              # before raising Rdkafka::AbstractHandle::WaitTimeoutError. librdkafka will 
              # (re)try to deliver all messages in the background, until "config.message_timeout"
              # (message.timeout.ms) is exceeded. Phrased differently, rdkafka-ruby's
              # WaitTimeoutError is just informative.
              # The raising can be avoided if max_wait_timeout below is greater than
              # config.message_timeout, but config is not available here (without
              # changing the interface).
              handle.wait(max_wait_timeout: 60)
            rescue Rdkafka::AbstractHandle::WaitTimeoutError => e
              partition = MessageDeliveryError.partition_from_delivery_handle(handle)
              # ideally we could use the logger passed to the Runner, but it is not
              # available here. The runner sets it for Rdkafka, though, so we can use
              # that instead.
              @config.logger.debug "Still trying to deliver message to (partition #{partition})... (will try up to Racecar.config.message_timeout)"
              retry
            rescue Rdkafka::RdkafkaError => e
              raise MessageDeliveryError.new(e, handle)
            end
          end
        end
      end
      @delivery_handles.clear
    end

    protected

    # https://github.com/appsignal/rdkafka-ruby#producing-messages
    def produce(payload, topic:, key: nil, partition: nil, partition_key: nil, headers: nil, create_time: nil)
      @delivery_handles ||= []
      message_size = payload.respond_to?(:bytesize) ? payload.bytesize : 0
      instrumentation_payload = {
        value: payload,
        headers: headers,
        key: key,
        partition: partition,
        partition_key: partition_key,
        topic: topic,
        message_size: message_size,
        create_time: Time.now,
        buffer_size: @delivery_handles.size,
      }

      @instrumenter.instrument("produce_message", instrumentation_payload) do
        @delivery_handles << @producer.produce(
          topic: topic,
          payload: payload,
          key: key,
          partition: partition,
          partition_key: partition_key,
          timestamp: create_time,
          headers: headers,
        )
      end
    end

    def heartbeat
      warn "DEPRECATION WARNING: Manual heartbeats are not supported and not needed with librdkafka."
    end
  end
end
