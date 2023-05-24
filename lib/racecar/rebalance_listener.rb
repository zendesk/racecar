module Racecar
  class RebalanceListener
    def initialize(config)
      @config = config
      @consumer_class = config.consumer_class
    end

    attr_reader :config, :consumer_class

    def on_partitions_assigned(_consumer, topic_partition_list)
      consumer_class.respond_to?(:on_partitions_assigned) &&
        consumer_class.on_partitions_assigned(topic_partition_list.to_h)
    rescue
    end

    def on_partitions_revoked(_consumer, topic_partition_list)
      consumer_class.respond_to?(:on_partitions_revoked) &&
        consumer_class.on_partitions_revoked(topic_partition_list.to_h)
    rescue
    end
  end
end
