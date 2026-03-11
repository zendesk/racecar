# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/message"
require "racecar/message_delivery_error"
require "racecar/erroneous_state_error"
require "racecar/delivery_callback"
require "racecar/partition_processor"
require "racecar/async_partition_processor"

module Racecar
  class Runner
    attr_reader :consumer_class, :config, :logger, :partition_processors

    # Kept for backward compatibility — external code calls `processor`.
    def processor
      @consumer_class_instance
    end

    def initialize(consumer_class, config:, logger:, instrumenter: NullInstrumenter)
      @consumer_class, @config, @logger = consumer_class, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      @partition_processors = Concurrent::Hash.new
      @consumer_class_instance = consumer_class.new
      if @consumer_class_instance.respond_to?(:statistics_callback) && Rdkafka::Config.statistics_callback.nil?
        Rdkafka::Config.statistics_callback = @consumer_class_instance.method(:statistics_callback).to_proc
      end
      Rdkafka::Config.logger = logger
    end

    def self.topic_partition_key(topic, partition)
      "#{topic}/#{partition}"
    end

    def run
      install_signal_handlers
      @stop_requested = false

      unless config.multithreaded_processing_enabled
        @consumer_class_instance.configure(
          producer:     consumer.producer,
          consumer:     consumer,
          instrumenter: @instrumenter,
          config:       config,
        )
      end

      loop_payload = {
        consumer_class: consumer_class.to_s,
        consumer_set: consumer
      }
      # Main loop
      begin
        loop do
          break if @stop_requested

          @instrumenter.instrument("start_main_loop", loop_payload)
          @instrumenter.instrument("main_loop", loop_payload) do
            resume_all_paused_partitions unless config.multithreaded_processing_enabled

            case process_method
            when :batch then
              msg_per_part = consumer.batch_poll(config.max_wait_time_ms).group_by(&:partition)
              msg_per_part.each_value do |messages_per_partition|
                processor = assign_and_get_processor(messages_per_partition)
                processor&.process_batch(messages_per_partition) unless processor&.rebalancing_or_shutting_down?
              end
            when :single then
              message = consumer.poll(config.max_wait_time_ms)
              if message
                processor = assign_and_get_processor(message)
                processor&.process(message) unless processor&.rebalancing_or_shutting_down?
              end
            end
          end
        end
      ensure
        logger.info "Gracefully shutting down"
        shutdown_processors_and_wait
        consumer.commit
      end
    ensure
      begin
        @instrumenter.instrument('leave_group') do
          consumer.close
        end
      ensure
        Racecar::Datadog.close if config.datadog_enabled
        @instrumenter.instrument("shut_down", loop_payload || {})
      end
    end

    def stop
      @stop_requested = true
    end

    def consumer
      @consumer ||= begin
        ConsumerSet.new(config, logger, @partition_processors, @instrumenter)
      end
    end

    private

    def process_method
      @process_method ||= begin
        case
        when consumer_class.method_defined?(:process_batch)
          if consumer_class.instance_method(:process_batch).arity != 1
            raise Racecar::Error, "Invalid method signature for `process_batch`. The method must take exactly 1 argument."
          end

          :batch
        when consumer_class.method_defined?(:process)
          if consumer_class.instance_method(:process).arity != 1
            raise Racecar::Error, "Invalid method signature for `process`. The method must take exactly 1 argument."
          end

          :single
        else
          raise NotImplementedError, "Consumer class `#{consumer_class}` must implement a `process` or `process_batch` method"
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
      topic     = messages.is_a?(Array) ? messages.first.topic     : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      key = Runner.topic_partition_key(topic, partition)
      return partition_processors[key] if partition_processors[key]

      processor = if config.multithreaded_processing_enabled
        AsyncPartitionProcessor.new(
          **common_processor_params,
          consumer_class: consumer_class,
          topic: topic,
          partition: partition,
          rdkafka_consumer: consumer.current,
        )
      else
        PartitionProcessor.new(
          **common_processor_params,
          consumer_class_instance: @consumer_class_instance,
          topic: topic,
          partition: partition,
          pause: Pause.new_from_config(config),
        )
      end
      partition_processors[key] = processor
    end

    def shutdown_processors_and_wait
      if config.multithreaded_processing_enabled
        processors_snapshot = partition_processors.values
        processors_snapshot.each { |processor| processor.shut_down! if processor }
        processors_snapshot.each do |processor|
          if processor.respond_to?(:thread)
            begin
              processor.thread.join(config.multithreaded_processing_shutdown_timeout)
            rescue => e
              logger.error "Error while waiting for processor thread to finish: #{e}"
            end
          end
        end
      else
        begin
          @consumer_class_instance.deliver!
        ensure
          @consumer_class_instance.teardown
        end
      end
    end

    def resume_all_paused_partitions
      partition_processors.values.reject(&:rebalancing_or_shutting_down?).each(&:resume_paused_partition)
    end

    def common_processor_params
      {
        config: config,
        logger: logger,
        instrumenter: @instrumenter,
        consumer: consumer,
      }
    end
  end
end
