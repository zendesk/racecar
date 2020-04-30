module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :start_from_beginning, :max_bytes_per_partition, :additional_config)

    class << self
      attr_accessor :max_wait_time
      attr_accessor :group_id
      attr_accessor :producer, :consumer

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
      def subscribes_to(*topics, start_from_beginning: true, max_bytes_per_partition: 1048576, additional_config: {})
        topics.each do |topic|
          subscriptions << Subscription.new(topic, start_from_beginning, max_bytes_per_partition, additional_config)
        end
      end
    end

    def configure(producer:, consumer:, instrumenter: NullInstrumenter)
      @producer = producer
      @consumer = consumer
      @instrumenter = instrumenter
    end

    def teardown; end

    # Delivers messages that got produced.
    def deliver!
      @delivery_handles ||= []
      if @delivery_handles.any?
        instrumentation_payload = { delivered_message_count: @delivery_handles.size }

        @instrumenter.instrument('deliver_messages', instrumentation_payload) do
          @delivery_handles.each(&:wait)
        end
      end
      @delivery_handles.clear
    end

    protected

    # https://github.com/appsignal/rdkafka-ruby#producing-messages
    def produce(payload, topic:, key:, headers: nil, create_time: nil)
      @delivery_handles ||= []
      message_size = payload.respond_to?(:bytesize) ? payload.bytesize : 0
      instrumentation_payload = {
        value: payload,
        headers: headers,
        key: key,
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
