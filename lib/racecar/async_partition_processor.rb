# frozen_string_literal: true

require 'racecar/pause'

module Racecar
  class AsyncPartitionProcessor
    attr_reader :thread, :queue, :config, :processor, :logger, :consumer, :consumer_class, :instrumenter

    THREAD_KEY_IDENTIFIER = 'racecar_topic_partition_identifier'.freeze

    def self.thread_key(topic, partition)
      "#{topic}/#{partition}"
    end

    def initialize(topic:, partition:, logger:, config:, consumer:, consumer_class:, instrumenter:)
      @topic      = topic
      @partition  = partition
      @logger     = logger
      @config     = config
      @consumer   = consumer
      @consumer_class = consumer_class
      @instrumenter = instrumenter
      @pauses = Pause.instantiate_pauses(config)

      setup_async_processing
    end

    def process(message)
      push(message)
    end

    def process_batch(messages)
      push(messages)
    end

    def rebalancing=(value)
      processor.rebalancing = value
      @queue << nil
    end

    def shutting_down=(value)
      processor.shutting_down = value
      @queue << nil
    end

    private

    def setup_async_processing
      @processor = PartitionProcessor.new(
        config: config,
        logger: logger,
        instrumenter: instrumenter,
        consumer_class: consumer_class,
        consumer: consumer,
        pauses: @pauses
      )
      @queue  = Queue.new
      @thread = nil

      use_process_batch = consumer_class.method_defined?(:process_batch)

      if use_process_batch
        spawn_thread do |msgs|
          processor.process_batch(msgs)
        end
      else
        spawn_thread do |msgs|
          msgs.each do |msg|
            processor.process(msg)
          end
        end
      end
    end

    def spawn_thread(&block)
      @thread = Thread.new do
        Thread.current.name = "Racecar thread for #{thread_key}"
        Thread.current[AsyncPartitionProcessor::THREAD_KEY_IDENTIFIER] = thread_key
        main_processing_loop(block)
      end
    end

    def push(messages)
      @queue << Array(messages)
      maybe_apply_backpressure
    end

    def maybe_apply_backpressure
      if @queue.size >= config.multithreaded_processing_max_queue_size
        consumer.pause(@topic, @partition)
        logger.debug "Paused partition #{@topic}/#{@partition}: queue reached capacity (#{@queue.size}/#{config.multithreaded_processing_max_queue_size})"
      end
    end

    def maybe_resume_the_partition
      if @queue.size < config.multithreaded_processing_resume_threshold * config.multithreaded_processing_max_queue_size
        return unless consumer.paused?(@topic, @partition)
        consumer.resume(@topic, @partition)
      end
    end

    def thread_key
      self.class.thread_key(@topic, @partition)
    end

    def main_processing_loop(block)
      loop do
        msgs = @queue.pop
        break if msgs.nil?

        maybe_resume_the_partition
        block.call(msgs)
      rescue => e
        logger.error "Error in processing thread for #{thread_key}: #{e.class} - #{e.message}"
      end
    ensure
      begin
        @processor.teardown
      ensure
        @processor.close
      end
    end
  end
end
