# frozen_string_literal: true

module Racecar
  # MessageDeliveryError wraps an Rdkafka error and tries to give
  # specific hints on how to debug or resolve the error within the
  # Racecar context.
  class MessageDeliveryError < StandardError
    # partition_from_delivery_handle takes an rdkafka delivery handle
    # and returns a human readable version of the partition. It handles
    # the case where the partition is unknown.
    def self.partition_from_delivery_handle(delivery_handle)
      partition = delivery_handle&.create_result&.partition
      # -1 is rdkafka-ruby's default value, which gets eventually set by librdkafka
      return "no yet known" if partition.nil? || partition == -1
      partition.to_s
    end

    def initialize(rdkafka_error, delivery_handle)
      raise rdkafka_error unless rdkafka_error.is_a?(Rdkafka::RdkafkaError)

      @rdkafka_error = rdkafka_error
      @delivery_handle = delivery_handle
    end

    attr_reader :rdkafka_error

    def code
      @rdkafka_error.code
    end

    def to_s
      msg = <<~EOM
        Message delivery finally failed:
        #{@rdkafka_error.to_s}

        #{explain}
      EOM
    end

    private

    def explain
      case @rdkafka_error.code
      when :msg_timed_out # -192
        <<~EOM
          Could not deliver message within Racecar.config.message_timeout.

          This can happen for various reasons, but most commonly because the connection to the broker is interrupted or there is no leader available. Check the broker's logs or the network for more insight.

          Upstream documentation:
          https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#error-local-time-out
        EOM

      when :msg_size_too_large # 10
        <<~EOM
          Could not deliver message, since it is bigger than either the broker's or Racecar's maximum message size.

          The broker's config option on the topic is called "max.message.bytes" and the broker wide default is "message.max.bytes". The client's is "message.max.bytes". Take extra care to distinguish this from similarly named properties for receiving/consuming messages (i.e. Racecar.config.max_bytes is NOT related).

          Racecar's limit is currently not configurable and uses librdkafka's default of 1 MB (10³ bytes). As of writing, librdkafka will send at least one message regardless of this limit. It is therefore very likely you're hitting the broker's limit and not Racecar's/librdkafka's.

          Upstream documentation:
          broker per topic: https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#topicconfigs_max.message.bytes
          broker default:   https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_message.max.bytes
          client:           https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        EOM

      when :unknown_topic_or_part # 3
        partition = self.class.partition_from_delivery_handle(@delivery_handle)

        <<~EOM
          Could not deliver message, since the targeted topic or partition (#{partition}) does not exist.

          Check that there are no typos, or that the broker's "auto.create.topics.enable" is enabled. For freshly created topics with auto create enabled, this may appear in the beginning (race condition on creation and publishing).

          Upstream documentation:
          broker setting: https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_auto.create.topics.enable
          client:         https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#topic-metadata-propagation-for-newly-created-topics
                          https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#topic-auto-creation
        EOM

      when :record_list_too_large # 18
        <<~EOM
          Tried to deliver more messages in a batch than the broker's segment size.

          Either increase the broker's "log.segment.bytes", or decrease any of the client's related settings "batch.num.messages", "batch.size" or "message.max.bytes". None of these are configurable through Racecar yet, as the defaults should be sufficient and sane.

          Upstream documentation:
          broker: https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_log.segment.bytes
          client: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        EOM

      when :topic_authorization_failed # 29
        <<~EOM
          Failed to deliver message because of insufficient authorization to write into the topic.

          Double check that it is not a race condition on topic creation. If it isn't, verify the ACLs are correct.

          Upstream documentation:
          https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#unknown-or-unauthorized-topics
        EOM

      else
        <<~EOM
          No specific information is available for this error. Consider adding it to Racecar. You can find generally helpful information in the upstream documentation:
          https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md
          https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        EOM
      end
    end
  end
end
