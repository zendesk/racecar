# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/message"
require "racecar/message_delivery_error"
require "racecar/erroneous_state_error"
require "racecar/delivery_callback"
require "racecar/processing"
require "racecar/processor"
require "racecar/multi_threaded_processor"
require 'racecar/producer_methods'

module Racecar
  class Runner
    include Processing
    include ProducerMethods

    attr_reader :consumer_class_instance, :config, :logger, :partition_processors

    def initialize(consumer_class_instance, config:, logger:, instrumenter: NullInstrumenter)
      @consumer_class_instance, @config, @logger = consumer_class_instance, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      @partition_processors = {}
      @mutex = Mutex.new
      Rdkafka::Config.logger = logger

      if consumer_class_instance.respond_to?(:statistics_callback)
        Rdkafka::Config.statistics_callback = consumer_class_instance.method(:statistics_callback).to_proc
      end
    end

    def self.topic_partition_key(topic, partition)
      "#{topic}/#{partition}"
    end

    def run
      install_signal_handlers
      @stop_requested = false

      # Configure the consumer with a producer so it can produce messages and
      # with a consumer so that it can support advanced use-cases.
      consumer_class_instance.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
        runner_mutex: @mutex,
      )

      loop_payload = {
        consumer_class: consumer_class_instance.class.to_s,
        consumer_set: consumer
      }

      unless config.multithreaded_processing_enabled
        Thread.current[MultiThreadedProcessor::THREAD_KEY] = "main"
      end

      # Main loop
      loop do
        break if @stop_requested

        @instrumenter.instrument("start_main_loop", loop_payload)
        @instrumenter.instrument("main_loop", loop_payload) do
          case process_method
          when :batch then
            msg_per_part = consumer.batch_poll(config.max_wait_time_ms).group_by(&:partition)
            msg_per_part.each_value do |messages_per_partition|
              messages_per_partition.group_by(&:topic).each_value do |messages_per_topic_and_partition|
                processor = assign_and_get_processor(messages_per_topic_and_partition)
                processor.process_batch(messages_per_topic_and_partition)
              end
            end
          when :single then
            message = consumer.poll(config.max_wait_time_ms)
            if message
              processor = assign_and_get_processor(message)
              processor.process(message)
            end
          end
        end
      end

      logger.info "Gracefully shutting down"
      begin
        shutdown_processors_and_wait
        consumer_class_instance.deliver!
        consumer_class_instance.teardown
        consumer.commit
      ensure
        @instrumenter.instrument('leave_group') do
          consumer.close
        end
      end
    ensure
      producer.close
      Racecar::Datadog.close if config.datadog_enabled
      @instrumenter.instrument("shut_down", loop_payload || {})
    end

    def stop
      @stop_requested = true
    end

    def consumer
      @consumer ||= begin
        ConsumerSet.new(config, logger, @partition_processors, @mutex, @instrumenter)
      end
    end

    private

    def process_method
      @process_method ||= begin
        case
        when consumer_class_instance.respond_to?(:process_batch)
          if consumer_class_instance.method(:process_batch).arity != 1
            raise Racecar::Error, "Invalid method signature for `process_batch`. The method must take exactly 1 argument."
          end

          :batch
        when consumer_class_instance.respond_to?(:process)
          if consumer_class_instance.method(:process).arity != 1
            raise Racecar::Error, "Invalid method signature for `process`. The method must take exactly 1 argument."
          end

          :single
        else
          raise NotImplementedError, "Consumer class `#{consumer_class_instance.class}` must implement a `process` or `process_batch` method"
        end
      end
    end

    def install_signal_handlers
      # Stop the consumer on SIGINT, SIGQUIT or SIGTERM.
      trap("QUIT") { stop }
      trap("INT")  { stop }
      trap("TERM") { stop }

      # Print the consumer config to STDERR on USR1.
      trap("USR1") { $stderr.puts config.inspect }
    end

    def assign_and_get_processor(messages)
      @mutex.synchronize do
        topic, partition = topic_and_partition_for_messages(messages)
        key = Runner.topic_partition_key(topic, partition)
        return partition_processors[key] if partition_processors[key]

        processor_args = {
          config: config,
          logger: logger,
          instrumenter: @instrumenter,
          consumer_class_instance: consumer_class_instance,
          runner_mutex: @mutex,
          consumer: consumer,
        }
        processor = if config.multithreaded_processing_enabled
                      MultiThreadedProcessor.new(**processor_args, topic: topic, partition: partition)
                    else
                      @single_threaded_pauses ||= Pause.instantiate_pauses(config)
                      Processor.new(**processor_args, pauses: @single_threaded_pauses)
                    end

        partition_processors[key] = processor
      end
    end

    def shutdown_processors_and_wait
      processors_snapshot = @mutex.synchronize { partition_processors.values }
      processors_snapshot.each { |processor| processor.shutting_down = true if processor }
      processors_snapshot.each do |processor|
        if processor.respond_to?(:thread)
          processor.thread.join(config.multithreaded_processing_shutdown_timeout)
        end
      end
    end
  end
end
