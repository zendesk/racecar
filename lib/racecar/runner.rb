# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/message"
require "racecar/message_delivery_error"
require "racecar/erroneous_state_error"
require "racecar/delivery_callback"
require "racecar/consumer_factory"
require "racecar/pause_guarded_processing"
require "racecar/partition_worker"

module Racecar
  class Runner
    attr_reader :processor, :config, :logger

    include PauseGuardedProcessing

    def initialize(processor, config:, logger:, instrumenter: NullInstrumenter)
      @processor, @config, @logger = processor, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      @workers = {}
      @consumer_factory = ConsumerFactory.new(processor.class, config: config, instrumenter: instrumenter)
      Rdkafka::Config.logger = logger

      if processor.respond_to?(:statistics_callback)
        Rdkafka::Config.statistics_callback = processor.method(:statistics_callback).to_proc
      end

      setup_pauses
    end

    def setup_multithreading
      if config.partition_threading_enabled
        consumer.subscribe_all
        partitions = processor.all_partition_ids
        if config.max_parallel_threads
          @max_threads = config.max_parallel_threads
        else
          @max_threads = partitions.size
        end
        workers_array_cycle = @max_threads.times.map { create_worker }.cycle
        partitions.each do |partition_id|
          @workers[partition_id] = workers_array_cycle.next
        end
      end
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
      processor.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
      )

      instrumentation_payload = {
        consumer_class: processor.class.to_s,
        consumer_set: consumer
      }

      setup_multithreading

      # Main loop
      loop do
        break if @stop_requested
        resume_paused_partitions

        @instrumenter.instrument("start_main_loop", instrumentation_payload)
        @instrumenter.instrument("main_loop", instrumentation_payload) do
          case process_method
          when :batch then
            msg_per_part = consumer.batch_poll(config.max_wait_time_ms).group_by(&:partition)
            msg_per_part.each_value do |messages|
              if config.partition_threading_enabled
                first = messages.first
                partition = first.partition
                worker = @workers[partition]
                if worker
                  worker << { action: 'process_batch', messages: messages }
                else
                  logger.warn "No worker found for partition #{partition}, processing batch in main thread"
                  process_batch_with_pause(messages)
                end
              else
                process_batch_with_pause(messages)
              end

            end
          when :single then
            message = consumer.poll(config.max_wait_time_ms)
            if config.partition_threading_enabled && message
              partition = message.partition
              worker = @workers[partition]
              if worker
                worker << { action: 'process', message: message }
              else
                logger.warn "No worker found for partition #{partition}, processing message in main thread"
                process_with_pause(message) if message
              end
            else
              process_with_pause(message) if message
            end
          end
        end
      end

      logger.info "Gracefully shutting down"
      begin
        processor.deliver!
        processor.teardown
        consumer.thread_safe_commit
        if config.partition_threading_enabled
          shutdown_workers
        end
      ensure
        @instrumenter.instrument('leave_group') do
          consumer.close
        end
      end
    ensure
      producer.close
      Racecar::Datadog.close if config.datadog_enabled
      @instrumenter.instrument("shut_down", instrumentation_payload || {})
    end

    def stop
      @stop_requested = true
    end

    def shutdown_workers
      if config.partition_threading_enabled
        @workers.each do |partition_id, worker|
          logger.info "Shutting down worker for partition #{partition_id}"
          worker.ask!(:terminate!)
        end
      end
    end

    private

    attr_reader :pauses

    def process_method
      @process_method ||= begin
        case
        when processor.respond_to?(:process_batch)
          if processor.method(:process_batch).arity != 1
            raise Racecar::Error, "Invalid method signature for `process_batch`. The method must take exactly 1 argument."
          end

          :batch
        when processor.respond_to?(:process)
          if processor.method(:process).arity != 1
            raise Racecar::Error, "Invalid method signature for `process`. The method must take exactly 1 argument."
          end

          :single
        else
          raise NotImplementedError, "Consumer class `#{processor.class}` must implement a `process` or `process_batch` method"
        end
      end
    end

    def create_worker
      processor_instance = @consumer_factory.create_consumer_instance
      processor_instance.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
        )
      PartitionWorker.spawn(args: {
        processor: processor_instance,
        pauses: pauses,
        logger: logger,
        config: config,
        instrumenter: @instrumenter,
      })
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
