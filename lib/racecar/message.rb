# frozen_string_literal: true

require "forwardable"

module Racecar
  class Message
    extend Forwardable

    attr_reader :retries_count

    def initialize(rdkafka_message, retries_count: nil)
      @rdkafka_message = rdkafka_message
      @retries_count   = retries_count
    end

    def_delegators :@rdkafka_message, :topic, :partition, :offset, :key, :headers

    def value
      @rdkafka_message.payload
    end

    def create_time
      @rdkafka_message.timestamp
    end

    def ==(other)
      @rdkafka_message == other.instance_variable_get(:@rdkafka_message)
    end
  end
end
