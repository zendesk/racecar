# frozen_string_literal: true

require "racecar/processing"

module Racecar
  class MultiThreadedProcessor
    include Processing

    attr_reader :thread_queues, :threads

    def self.thread_key(topic, partition)
      "#{topic}-#{partition}"
    end

    def initialize(config:, pauses:, consumer_class_instance:, instrumenter:, logger:, consumer:)
      @logger = logger
      @threads = {}
      @thread_queues = {}
      @thread_mutexes = {}
      @thread_metadata = {}
      @finalize_mutex = Mutex.new
      @config = config
      @pauses = pauses
      @consumer_class_instance = consumer_class_instance
      @instrumenter = instrumenter
      @consumer = consumer
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
      return unless @thread_mutexes[thread_key]
      @thread_mutexes[thread_key].synchronize do
        @thread_metadata[thread_key][:rebalancing] = true
      end
      wakeup(thread_key)
    end

    def shutdown_and_wait
      @threads.keys.each do |thread_key|
        @thread_mutexes[thread_key].synchronize do
          @thread_metadata[thread_key][:shutting_down] = true
        end
        wakeup(thread_key)
      end
      @threads.values.each do |thread|
        thread.join
      end
    end

    private

    attr_reader :config, :pauses, :consumer_class_instance, :consumer, :logger

    def push_messages(messages)
      topic = messages.is_a?(Array) ? messages.first.topic : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      thread_key = self.class.thread_key(topic, partition)
      unless @thread_queues[thread_key] && @threads[thread_key]&.alive?
        spawn_thread(thread_key) do |current_thread_key, processed_messages|
          process_messages(current_thread_key, processed_messages)
        end
        logger.debug "Spawned thread for topic: #{topic}, partition: #{partition}"
      end
      Array(messages).each { |m| @thread_queues[thread_key] << m }
      wakeup(thread_key)

      maybe_apply_backpressure(thread_key, topic, partition, messages)
    end

    def spawn_thread(thread_key)
      prepare_initial_values(thread_key)
      @threads[thread_key] = Thread.new do
        Thread.current.name = "Racecar thread for #{thread_key}"
        loop do
          maybe_stop_or_exit(thread_key)
          msgs = acquire_messages_from_queue(thread_key)
          process_with_error_handling_and_retrying(thread_key, msgs) do
            yield thread_key, msgs
          end
        end
      end
    end

    def process_messages(thread_key, msgs)
      topic = msgs.first.topic
      partition = msgs.first.partition
      maybe_resume_the_partition(thread_key, topic, partition)
      exit_on_rebalance(thread_key)

      if consumer_class_instance.respond_to?(:process_batch)
        instrument_process_batch(instrumentation_payload_for_batch(original_messages(msgs))) do
          consumer_class_instance.process_batch(msgs)
          finalize_messages_processing(msgs.last)
        end
      else
        msg = msgs.first
        instrument_process_message(instrumentation_payload(msg.original_message)) do
          consumer_class_instance.process(msg)
          finalize_messages_processing(msg)
        end
      end
    end

    def maybe_stop_or_exit(thread_key)
      while @thread_queues[thread_key].empty?
        metadata = thread_metadata(thread_key)
        if metadata[:rebalancing] || metadata[:shutting_down]
          logger.debug "Thread for #{thread_key} exiting"
          Thread.exit
        else
          Thread.stop
        end
      end
    end

    def maybe_resume_the_partition(thread_key, topic, partition)
      if @thread_queues[thread_key].size < 0.5 * config.multithreaded_processing_max_queue_size
        consumer.resume(topic, partition)
      end
    end

    def thread_metadata(thread_key)
      @thread_mutexes[thread_key].synchronize do
        @thread_metadata[thread_key]
      end
    end

    def exit_on_rebalance(thread_key)
      metadata = thread_metadata(thread_key)
      if metadata[:rebalancing]
        logger.debug "Thread for #{thread_key} exiting"
        Thread.exit
      end
    end

    def prepare_initial_values(thread_key)
      @thread_queues[thread_key] ||= Queue.new
      @thread_mutexes[thread_key] ||= Mutex.new
      @thread_metadata[thread_key] = { rebalancing: false, shutting_down: false }
    end

    def process_with_error_handling_and_retrying(thread_key, msgs)
      loop do
        begin
          yield
          break
        rescue => e
          metadata = thread_metadata(thread_key)
          if metadata[:rebalancing]
            logger.debug "Thread for #{thread_key} exiting"
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

    def wakeup(thread_key)
      @threads[thread_key]&.wakeup
    rescue ThreadError
      # thread died between the alive check and wakeup, safe to ignore
    end

    def maybe_apply_backpressure(thread_key, topic, partition, messages)
      if @thread_queues[thread_key].size >= config.multithreaded_processing_max_queue_size
        consumer.pause(topic, partition, Array(messages).last.offset + 1)
        logger.debug "Paused partition #{topic}/#{partition}: queue reached capacity (#{@thread_queues[thread_key].size}/#{config.multithreaded_processing_max_queue_size})"
      end
    end

    def finalize_messages_processing(msg)
      @finalize_mutex.synchronize do
        consumer_class_instance.deliver!
        consumer.store_offset(msg)
      end
    end

    def acquire_messages_from_queue(thread_key)
      if config.multithreaded_processing_fetch_full_batch && consumer_class_instance.respond_to?(:process_batch)
        msgs = []
        while !@thread_queues[thread_key].empty? && msgs.size < config.fetch_messages
          msgs << @thread_queues[thread_key].pop
        end
      else
        msgs = [@thread_queues[thread_key].pop]
      end
      msgs
    end

    def original_messages(messages)
      Array(messages).map(&:original_message)
    end
  end
end
