module Racecar
  class RebalanceListener
    def initialize(consumer_class, instrumenter)
      @consumer_class = consumer_class
      @instrumenter = instrumenter
      @rdkafka_consumer = nil
    end

    attr_writer :rdkafka_consumer

    attr_reader :consumer_class, :instrumenter, :rdkafka_consumer
    private     :consumer_class, :instrumenter, :rdkafka_consumer

    def on_partitions_assigned(rdkafka_topic_partition_list)
      event = Event.new(rdkafka_consumer: rdkafka_consumer, rdkafka_topic_partition_list: rdkafka_topic_partition_list)

      instrument("partitions_assigned", partitions: event.topic_partition_numbers.values.flatten) do
        consumer_class.on_partitions_assigned(event)
      end
    end

    def on_partitions_revoked(rdkafka_topic_partition_list)
      event = Event.new(rdkafka_consumer: rdkafka_consumer, rdkafka_topic_partition_list: rdkafka_topic_partition_list)

      instrument("partitions_revoked", partitions: event.topic_partition_numbers.values.flatten) do
        consumer_class.on_partitions_revoked(event)
      end
    end

    private

    def instrument(event, payload, &block)
      instrumenter.instrument(event, payload, &block)
    end

    class Event
      def initialize(rdkafka_topic_partition_list:, rdkafka_consumer:)
        @__rdkafka_topic_partition_list = rdkafka_topic_partition_list
        @__rdkafka_consumer = rdkafka_consumer
      end

      def topic_name
        ensure_single_topic!("topic_name")
        __rdkafka_topic_partition_list.to_h.keys.first
      end

      def partition_numbers
        ensure_single_topic!("partition_numbers")
        __rdkafka_topic_partition_list.to_h.values.flatten.map(&:partition)
      end

      # {topic name => [partition numbers]} for every topic in the event.
      def topic_partition_numbers
        __rdkafka_topic_partition_list.to_h.transform_values { |partitions| partitions.map(&:partition) }
      end

      def empty?
        __rdkafka_topic_partition_list.empty?
      end

      # API private and not guaranteed stable
      attr_reader :__rdkafka_topic_partition_list, :__rdkafka_consumer

      private

      def ensure_single_topic!(method_name)
        return if __rdkafka_topic_partition_list.to_h.size <= 1

        raise Racecar::Error, "##{method_name} is ambiguous for a rebalance event spanning multiple topics; use #topic_partition_numbers"
      end
    end
  end
end
