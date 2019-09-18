require "forwardable"

module Racecar
  class Message
    extend Forwardable

    def initialize(rdkafka_message)
      @rdkafka_message = rdkafka_message
    end

    def_delegators :@rdkafka_message, :topic, :partition, :offset, :key, :value, :headers

    def create_time
      @rdkafka_message.timestamp
    end

    def ==(other)
      @rdkafka_message == other.instance_variable_get(:@rdkafka_message)
    end
  end
end
