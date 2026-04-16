# frozen_string_literal: true

require 'racecar/pause'
require 'concurrent-ruby'

module Racecar
  class AsyncPartitionProcessor
    attr_reader :thread

    THREAD_KEY_IDENTIFIER = 'racecar_topic_partition_identifier'.freeze

    def self.thread_key(topic, partition)
      "#{topic}/#{partition}"
    end

    def initialize(topic:, partition:, logger:, config:, consumer:, consumer_class:, instrumenter:, rdkafka_consumer:)
      @topic      = topic
      @partition  = partition
      @logger     = logger
      @config     = config
      @consumer   = consumer
      @consumer_class = consumer_class
      @instrumenter = instrumenter
      @rdkafka_consumer = rdkafka_consumer
      @backpressure_paused = Concurrent::AtomicBoolean.new
      @tpl = build_tpl(topic, partition)
      setup_async_processing
    end

    def process(message)
      push(message)
    end

    def process_batch(messages)
      push(messages)
    end

    def rebalance!
      processor.rebalance!
      @queue << nil
    end

    def shut_down!
      processor.shut_down!
      @queue << nil
    end

    def rebalancing_or_shutting_down?
      processor.rebalancing_or_shutting_down?
    end

    def resume_paused_partition
      processor.resume_paused_partition
    end

    private

    attr_reader :backpressure_paused, :instrumenter, :consumer_class, :consumer, :queue, :config, :processor, :logger

    def setup_async_processing
      @processor = PartitionProcessor.new(
        config: config,
        logger: logger,
        instrumenter: instrumenter,
        consumer_class_instance: consumer_class.new,
        consumer: consumer,
        topic: @topic,
        partition: @partition,
        pause: Pause.new_from_config(config),
        rdkafka_consumer: @rdkafka_consumer,
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
      if @backpressure_paused.false? && @queue.size >= config.multithreaded_processing_max_queue_size
        @backpressure_paused.make_true
        @rdkafka_consumer.pause(@tpl)
        logger.debug "Paused partition #{@topic}/#{@partition}: queue reached capacity (#{@queue.size}/#{config.multithreaded_processing_max_queue_size})"
      end
    end

    def maybe_resume_the_partition
      if @backpressure_paused.true? && @queue.size < config.multithreaded_processing_resume_threshold * config.multithreaded_processing_max_queue_size
        @backpressure_paused.make_false
        @rdkafka_consumer.resume(@tpl)
      end
    end

    def build_tpl(topic, partition)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |tpl|
        tpl.add_topic_and_partitions_with_offsets(topic, partition => -1001)
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
        logger.error "Error in processing thread for #{thread_key}: #{e.class} - #{e.full_message}. backtrace: #{e.backtrace&.first(10)&.join("\n")}"
      end
    ensure
      @processor.teardown
    end
  end
end
