module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :start_from_beginning, :max_bytes_per_partition)

    class << self
      attr_accessor :group_id

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
      # @return [nil]
      def subscribes_to(*topics, start_from_beginning: true, max_bytes_per_partition: 1048576)
        topics.each do |topic|
          subscriptions << Subscription.new(topic, start_from_beginning, max_bytes_per_partition)
        end
      end
    end

    def configure(producer:)
      @_producer = producer
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
    def produce(topic:, payload:, key:)
      @delivery_handles ||= []
      @delivery_handles << @_producer.produce(topic: topic, payload: payload, key: key)
    end
  end
end
