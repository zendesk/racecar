# `rd_kafka_offsets_store()` (et.al) returns an error for any
# partition that is not currently assigned (through `rd_kafka_*assign()`).
# This prevents a race condition where an application would store offsets
# after the assigned partitions had been revoked (which resets the stored
# offset), that could cause these old stored offsets to be committed later
# when the same partitions were assigned to this consumer again - effectively
# overwriting any committed offsets by any consumers that were assigned the
# same partitions previously. This would typically result in the offsets
# rewinding and messages to be reprocessed.
# As an extra effort to avoid this situation the stored offset is now
# also reset when partitions are assigned (through `rd_kafka_*assign()`).
module Racecar
  class ErroneousStateError < StandardError
    def initialize(rdkafka_error)
      raise rdkafka_error unless rdkafka_error.is_a?(Rdkafka::RdkafkaError)

      @rdkafka_error = rdkafka_error
    end

    attr_reader :rdkafka_error

    def code
      @rdkafka_error.code
    end

    def to_s
      <<~EOM
        Partition is no longer assigned to this consumer and the offset could not be stored for commit.
        #{@rdkafka_error.to_s}
      EOM
    end

  end
end
