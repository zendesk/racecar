# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/message"
require "racecar/message_delivery_error"
require "racecar/erroneous_state_error"
require "racecar/delivery_callback"
require "racecar/processing"
require "racecar/single_threaded_processor"
require "racecar/multi_threaded_processor"
require 'racecar/producer_methods'

module Racecar
  class Runner
    include Processing
    include ProducerMethods

    attr_reader :consumer_class_instance, :config, :logger, :processor

    def initialize(consumer_class_instance, config:, logger:, instrumenter: NullInstrumenter)
      @consumer_class_instance, @config, @logger = consumer_class_instance, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      Rdkafka::Config.logger = logger

      if consumer_class_instance.respond_to?(:statistics_callback)
        Rdkafka::Config.statistics_callback = consumer_class_instance.method(:statistics_callback).to_proc
      end

      setup_pauses

      processor_args = {
        config: config,
        logger: logger,
        instrumenter: @instrumenter,
        consumer_class_instance: consumer_class_instance,
        pauses: pauses,
      }

      @processor = if config.multithreaded_processing_enabled
                     MultiThreadedProcessor.new(**processor_args)
                   else
                     SingleThreadedProcessor.new(**processor_args)
                   end
      @processor.consumer = consumer
    end

    def setup_pauses
      timeout = if config.pause_timeout == -1
        nil
      elsif config.pause_timeout == 0
        # no op, handled elsewhere
      elsif config.pause_timeout > 0
        config.pause_timeout
      else
        raise ArgumentError, "Invalid value for pause_timeout: must be integer greater or equal -1"
      end

      @pauses = Hash.new {|h, k|
        h[k] = Hash.new {|h2, k2|
          h2[k2] = ::Racecar::Pause.new(
            timeout:             timeout,
            max_timeout:         config.max_pause_timeout,
            exponential_backoff: config.pause_with_exponential_backoff
          )
        }
      }
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
      )

      loop_payload = {
        consumer_class: consumer_class_instance.class.to_s,
        consumer_set: consumer
      }

      # Main loop
      loop do
        break if @stop_requested

        @instrumenter.instrument("start_main_loop", loop_payload)
        @instrumenter.instrument("main_loop", loop_payload) do
          case process_method
          when :batch then
            msg_per_part = consumer.batch_poll(config.max_wait_time_ms).group_by(&:partition)
            msg_per_part.each_value do |messages|
              processor.process_batch(messages)
            end
          when :single then
            message = consumer.poll(config.max_wait_time_ms)
            processor.process(message) if message
          end
        end
      end

      logger.info "Gracefully shutting down"
      begin
        processor.shutdown_and_wait
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
        ConsumerSet.new(config, logger, processor, @instrumenter)
      end
    end

    private

    attr_reader :pauses

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
  end
end
