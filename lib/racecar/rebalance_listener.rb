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
      partitions_by_topic = rdkafka_topic_partition_list.to_h

      instrument("partitions_assigned", partitions: partitions_by_topic ) do
        consumer_class.on_partitions_assigned(
          partitions_by_topic: partitions_by_topic,
          rdkafka_consumer: rdkafka_consumer
        )
      end
    end

    def on_partitions_revoked(rdkafka_topic_partition_list)
      partitions_by_topic = rdkafka_topic_partition_list.to_h

      instrument("partitions_revoked", partitions: partitions_by_topic ) do
        consumer_class.on_partitions_revoked(
          partitions_by_topic: partitions_by_topic,
          rdkafka_consumer: rdkafka_consumer
        )
      end
    end

    private

    def instrument(event, payload, &block)
      instrumenter.instrument(event, payload, &block)
    end
  end
end
