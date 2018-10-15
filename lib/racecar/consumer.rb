module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :config)

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
      # @param config [Hash] Configuration properties for consumer.
      #   See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      # @return [nil]
      def subscribes_to(*topics, config: {})
        topics.each do |topic|
          subscriptions << Subscription.new(topic, config)
        end
      end
    end

    attr_accessor :producer, :consumer

    def configure(producer:, consumer:)
      @producer = producer
      @consumer = consumer
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
      @delivery_handles << @producer.produce(topic: topic, payload: payload, key: key)
    end
  end
end
