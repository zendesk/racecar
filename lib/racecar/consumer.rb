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

    def configure(producer:, consumer:, instrumenter:)
      @producer = producer
      @consumer = consumer
      @instrumenter = instrumenter
    end

    def teardown; end

    # Delivers messages that got produced.
    def deliver!
      @delivery_handles ||= []
      @delivery_handles.each(&:wait)
      @delivery_handles.clear
    end

    protected

    # https://github.com/appsignal/rdkafka-ruby#producing-messages
    def produce(topic:, payload:, key:, headers: nil)
      @delivery_handles ||= []

      extra_info = {
        value:       payload,
        key:         key,
        topic:       topic,
        create_time: Time.now,
      }
      @instrumenter.instrument("produce_message.racecar", extra_info) do
        @delivery_handles << @producer.produce(topic: topic, payload: payload, key: key, headers: headers)
      end
    end

    def heartbeat
      warn "DEPRECATION WARNING: Manual heartbeats are not supported and not needed with librdkafka."
    end
  end
end
