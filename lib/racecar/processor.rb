# frozen_string_literal: true

require 'racecar/producer_methods'
require 'racecar/processing'
require 'racecar/single_threaded_error_handling'
require 'racecar/multi_threaded_error_handling'
require 'racecar/pause'

module Racecar
  class Processor
    include Processing
    include ProducerMethods

    attr_reader :consumer_class_instance, :config, :logger, :instrumenter, :consumer, :pauses
    attr_accessor :rebalancing, :shutting_down

    def initialize(config:, logger:, instrumenter:, consumer_class_instance:, runner_mutex:, consumer:, pauses:)
      @config = config
      @logger = logger
      @instrumenter = instrumenter
      @consumer_class_instance = consumer_class_instance
      @pauses = pauses
      @runner_mutex = runner_mutex
      @consumer = consumer

      error_handling_mod = config.multithreaded_processing_enabled ? MultiThreadedErrorHandling : SingleThreadedErrorHandling
      extend(error_handling_mod)
    end

    def consumer=(consumer)
      @consumer = consumer
    end

    def process(message)
      payload = instrumentation_payload(message)
      @instrumenter.instrument("start_process_message", payload)

      with_error_handling(message, payload) do |pause|
        instrument_process_message(payload) do
          consumer_class_instance.process(Racecar::Message.new(message, retries_count: pause.pauses_count))
          consumer_class_instance.deliver!
          consumer.store_offset(message)
        end
      end
    end

    def process_batch(messages)
      payload = instrumentation_payload_for_batch(messages)
      @instrumenter.instrument("start_process_batch", payload)

      with_error_handling(messages, payload) do |pause|
        instrument_process_batch(payload) do
          racecar_messages = messages.map do |message|
            Racecar::Message.new(message, retries_count: pause.pauses_count)
          end
          consumer_class_instance.process_batch(racecar_messages)
          consumer_class_instance.deliver!
          consumer.store_offset(messages.last)
        end
      end
    end
  end
end

