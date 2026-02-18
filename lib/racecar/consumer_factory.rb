# frozen_string_literal: true

module Racecar
  class ConsumerFactory
    def initialize(consumer_class, config: Racecar.config, instrumenter: Racecar.config.instrumenter)
      @consumer_class = consumer_class
      @config = config
      @instrumenter = instrumenter
    end

    def create_consumer_instance
      @consumer_class.new
    end

    def configure_consumer_instance(consumer_instance, producer:, kafka_consumer:)
      consumer_instance.configure(
        producer: producer,
        consumer: kafka_consumer,
        instrumenter: @instrumenter,
        config: @config
      )
      consumer_instance
    end

    attr_reader :consumer_class, :config, :instrumenter
  end
end