# frozen_string_literal: true

require 'racecar/pause'

module Racecar
  class MultiThreadedProcessor
    attr_reader :thread, :queue, :config, :processor, :logger, :consumer, :consumer_class_instance, :instrumenter

    THREAD_KEY = 'thread_key'.freeze

    def self.thread_key(topic, partition)
      "#{topic}/#{partition}"
    end

    def initialize(topic:, partition:, logger:, config:, runner_mutex:, consumer:, consumer_class_instance:, instrumenter:)
      @topic      = topic
      @partition  = partition
      @logger     = logger
      @config     = config
      @runner_mutex = runner_mutex
      @consumer   = consumer
      @consumer_class_instance = consumer_class_instance
      @instrumenter = instrumenter
      @pauses = Pause.instantiate_pauses(config)

      setup_multi_threaded_processing
    end

    def process(message)
      push(message)
    end

    def process_batch(messages)
      push(messages)
    end

    def rebalancing=(value)
      processor.rebalancing = value
      @mutex.synchronize do
        @cv.signal
      end
    end

    def shutting_down=(value)
      processor.shutting_down = value
      @mutex.synchronize do
        @cv.signal
      end
    end

    private

    def setup_multi_threaded_processing
      @processor  = Processor.new(
        config: config,
        logger: logger,
        instrumenter: instrumenter,
        consumer_class_instance: consumer_class_instance,
        runner_mutex: @runner_mutex,
        consumer: consumer,
        pauses: @pauses
      )
      @queue      = Queue.new
      @mutex      = Mutex.new
      @cv         = ConditionVariable.new
      @thread     = nil

      spawn_thread do |msgs, use_process_batch|
        if use_process_batch
          processor.process_batch(msgs)
        else
          msgs.each do |msg|
            processor.process(msg)
          end
        end
      end
    end

    def spawn_thread(&block)
      use_process_batch = consumer_class_instance.respond_to?(:process_batch)
      @thread = Thread.new do
        Thread.current.name = "Racecar thread for #{thread_key}"
        Thread.current[MultiThreadedProcessor::THREAD_KEY] = thread_key
        loop do
          wait_for_messages_or_exit
          maybe_resume_the_partition
          msgs = @queue.pop
          block.call(msgs, use_process_batch)
          maybe_apply_backpressure(msgs)
        rescue => e
          logger.error "Error in processing thread for #{thread_key}: #{e.class} - #{e.message}"
        end
      end
    end

    def push(messages)
      @mutex.synchronize do
        @queue << Array(messages)
        @cv.signal
      end
    end

    def queue_size
      @queue.size
    end

    def wait_for_messages_or_exit
      @mutex.synchronize do
        while @queue.empty?
          if processor.shutting_down || processor.rebalancing
            @logger.debug "Thread for #{thread_key} exiting"
            Thread.exit
          end
          @cv.wait(@mutex)
        end
        if processor.rebalancing
          @logger.debug "Thread for #{thread_key} exiting"
          Thread.exit
        end
      end
    end

    def maybe_apply_backpressure(messages)
      if queue_size >= config.multithreaded_processing_max_queue_size
        @runner_mutex.synchronize do
          consumer.pause(@topic, @partition, Array(messages).last.offset + 1)
        end
        logger.debug "Paused partition #{@topic}/#{@partition}: queue reached capacity (#{queue_size}/#{config.multithreaded_processing_max_queue_size})"
      end
    end

    def maybe_resume_the_partition
      if queue_size < config.multithreaded_processing_resume_threshold * config.multithreaded_processing_max_queue_size
        @runner_mutex.synchronize do
          if consumer.respond_to?(:paused?)
            return unless consumer.paused?(@topic, @partition)
          end
          consumer.resume(@topic, @partition)
        end
      end
    end

    def thread_key
      self.class.thread_key(@topic, @partition)
    end
  end
end