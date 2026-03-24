# frozen_string_literal: true

require "racecar/processing"
require "racecar/thread_manager"
require "racecar/producer_methods"

module Racecar
  class MultiThreadedProcessor
    include Processing
    include ProducerMethods

    def synchronize_per_process(&block)
      @finalize_mutex.synchronize(&block)
    end

    def self.thread_key(topic, partition)
      "#{topic}/#{partition}"
    end

    def initialize(config:, pauses:, consumer_class_instance:, instrumenter:, logger:)
      @logger          = logger
      @thread_managers = {}
      @config          = config
      @pauses          = pauses
      @consumer_class_instance = consumer_class_instance
      @instrumenter    = instrumenter
      @finalize_mutex = Mutex.new
    end

    def consumer=(consumer)
      @consumer = consumer
    end

    def threads
      @thread_managers.transform_values(&:thread)
    end

    def thread_queues
      @thread_managers.transform_values(&:queue)
    end

    def process(message)
      wrapped_message = Racecar::Message.new(message, retries_count: pauses[message.topic][message.partition].pauses_count)
      push_messages(wrapped_message)
    end

    def process_batch(messages)
      wrapped_messages = messages.map do |message|
        Racecar::Message.new(message, retries_count: pauses[message.topic][message.partition].pauses_count)
      end
      push_messages(wrapped_messages)
    end

    def set_to_rebalance(topic, partition)
      thread_key = self.class.thread_key(topic, partition)
      @thread_managers[thread_key]&.set_rebalancing
    end

    def shutdown_and_wait
      @thread_managers.each_value(&:set_shutting_down)
      @thread_managers.each_value(&:join)
    end

    private

    attr_reader :config, :pauses, :consumer_class_instance, :consumer, :logger

    def push_messages(messages)
      topic     = messages.is_a?(Array) ? messages.first.topic     : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      thread_key = self.class.thread_key(topic, partition)

      manager = @thread_managers[thread_key]
      unless manager&.alive?
        manager = ThreadManager.new(thread_key: thread_key, logger: logger)
        @thread_managers[thread_key] = manager
        manager.spawn do |msgs|
          process_with_error_handling_and_retrying(manager, msgs) do
            process_messages(manager, msgs)
          end
        end
        logger.debug "Spawned thread for topic: #{topic}, partition: #{partition}"
      end

      manager.push(messages)
      maybe_apply_backpressure(manager, topic, partition, messages)
    end

    def process_messages(manager, msgs)
      topic     = msgs.first.topic
      partition = msgs.first.partition
      maybe_resume_the_partition(manager, topic, partition)
      exit_on_rebalance(manager)

      if consumer_class_instance.respond_to?(:process_batch)
        payload = instrumentation_payload_for_batch(original_messages(msgs))
        @instrumenter.instrument("start_process_batch", payload)
        instrument_process_batch(payload) do
          consumer_class_instance.process_batch(msgs)
          finalize_messages_processing(msgs.last)
        end
      else
        msg = msgs.first
        payload = instrumentation_payload(msg.original_message)
        @instrumenter.instrument("start_process_message", payload)
        instrument_process_message(payload) do
          consumer_class_instance.process(msg)
          finalize_messages_processing(msg)
        end
      end
    end

    def maybe_resume_the_partition(manager, topic, partition)
      if manager.queue_size < 0.5 * config.multithreaded_processing_max_queue_size
        synchronize_per_process do
          if consumer.respond_to?(:paused?)
            return unless consumer.paused?(topic, partition)
          end
          consumer.resume(topic, partition)
        end
      end
    end

    def exit_on_rebalance(manager)
      if manager.metadata[:rebalancing]
        Thread.exit
      end
    end

    def process_with_error_handling_and_retrying(manager, msgs)
      loop do
        begin
          yield
          pauses[msgs.first.topic][msgs.first.partition].reset!
          break
        rescue => e
          metadata = manager.metadata
          if metadata[:rebalancing]
            Thread.exit
          elsif !metadata[:shutting_down]
            pause = pauses[msgs.first.topic][msgs.first.partition]
            original_msgs = original_messages(msgs)
            payload = consumer_class_instance.respond_to?(:process_batch) ?
              instrumentation_payload_for_batch(original_msgs) :
              instrumentation_payload(original_msgs.first)
            handle_processing_error(e, payload, pause: pause, with_synchronization: true)
            pause.pause!
            sleep(pause.backoff_interval)
          else
            break
          end
        end
      end
    end

    def maybe_apply_backpressure(manager, topic, partition, messages)
      if manager.queue_size >= config.multithreaded_processing_max_queue_size
        synchronize_per_process do
          consumer.pause(topic, partition, Array(messages).last.offset + 1)
        end
        logger.debug "Paused partition #{topic}/#{partition}: queue reached capacity (#{manager.queue_size}/#{config.multithreaded_processing_max_queue_size})"
      end
    end

    def finalize_messages_processing(msg)
      consumer_class_instance.deliver!
      synchronize_per_process do
        consumer.store_offset(msg)
      end
    end

    def original_messages(messages)
      Array(messages).map(&:original_message)
    end
  end
end