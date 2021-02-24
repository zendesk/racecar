# frozen_string_literal: true

require "forwardable"

module Racecar
  # MessageDeliveryHandle is a wrapper around Rdkafka::Producer::DeliveryHandle, mostly
  # to keep context around.
  class MessageDeliveryHandle
    extend Forwardable

    attr_reader :rdkafka_handle, :topic, :key, :partition_key, :timestamp, :headers

    def initialize(rdkafka_handle, topic:, key:, partition_key:, timestamp:, headers:)
      if !rdkafka_handle.is_a?(Rdkafka::Producer::DeliveryHandle)
        raise TypeError, "expected a Rdkafka::Producer::DeliveryHandle, got #{rdkafka_handle.class}"
      end

      @rdkafka_handle = rdkafka_handle
      @topic = topic
      @key = key
      @partition_key = partition_key
      @timestamp = timestamp
      @headers = headers
    end

    def_delegators :@rdkafka_handle, :wait, :pending?

    # offset returns the offset of the delivered message. If the offset is not yet
    # known it will be set to -1.
    def offset
      @rdkafka_handle.create_result.offset || -1
    end

    # partition returns the assigned partition of the message. If the partition is not
    # yet known it will be set to -1.
    def partition
      @rdkafka_handle.create_result.partition || -1
    end

    # partition text returns a string describing the partition of the message. If the
    # partition is not yet known, it will return a readable message saying so.
    def partition_text
      part = partition
      return "no yet known" if part == -1
      part.to_s
    end
  end
end
