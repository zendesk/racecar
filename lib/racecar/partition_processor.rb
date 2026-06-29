# frozen_string_literal: true

require "racecar/message"

module Racecar
  class PartitionProcessor
    def initialize(processor:, consumer:, instrumenter:)
      @processor = processor
      @consumer = consumer
      @instrumenter = instrumenter
    end

    def message_payload(message)
      {
        consumer_class: @processor.class.to_s,
        topic:          message.topic,
        partition:      message.partition,
        offset:         message.offset,
        create_time:    message.timestamp,
        key:            message.key,
        value:          message.payload,
        headers:        message.headers,
      }
    end

    def batch_payload(messages)
      first, last = messages.first, messages.last
      {
        consumer_class:   @processor.class.to_s,
        topic:            first.topic,
        partition:        first.partition,
        first_offset:     first.offset,
        last_offset:      last.offset,
        last_create_time: last.timestamp,
        message_count:    messages.size,
      }
    end

    def process(message, retries_count, payload)
      @instrumenter.instrument("process_message", payload) do
        @processor.process(Racecar::Message.new(message, retries_count: retries_count))
        @processor.deliver!
        @consumer.store_offset(message)
      end
    end

    def process_batch(messages, retries_count, payload)
      @instrumenter.instrument("process_batch", payload) do
        racecar_messages = messages.map do |message|
          Racecar::Message.new(message, retries_count: retries_count)
        end
        @processor.process_batch(racecar_messages)
        @processor.deliver!
        @consumer.store_offset(messages.last)
      end
    end
  end
end
