# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/message"
require "racecar/message_delivery_error"
require "racecar/erroneous_state_error"
require "racecar/delivery_callback"
require "racecar/threads_orchestrator"
require "racecar/processing"

module Racecar
  class Runner
    include Processing

    attr_reader :processor, :config, :logger, :threads_orchestrator

    def initialize(processor, config:, logger:, instrumenter: NullInstrumenter)
      @processor, @config, @logger = processor, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      Rdkafka::Config.logger = logger

      if processor.respond_to?(:statistics_callback)
        Rdkafka::Config.statistics_callback = processor.method(:statistics_callback).to_proc
      end

      setup_pauses

      @threads_orchestrator = if config.multithreaded_processing_enabled
                                orchestrator = ThreadsOrchestrator.new(
                                  config:         config,
                                  pauses:         pauses,
                                  processor:      processor,
                                  instrumenter:   @instrumenter,
                                  logger:         logger,
                                  reset_producer: method(:reset_producer!),
                                  )

                                orchestrator.consumer = consumer
                                orchestrator
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

      loop_payload = {
        consumer_class: processor.class.to_s,
        consumer_set: consumer
      }

      # Main loop
      loop do
        break if @stop_requested
        resume_paused_partitions

        @instrumenter.instrument("start_main_loop", loop_payload)
        @instrumenter.instrument("main_loop", loop_payload) do
          case process_method
          when :batch then
            msg_per_part = consumer.batch_poll(config.max_wait_time_ms).group_by(&:partition)
            msg_per_part.each_value do |messages|
              process_batch(messages)
            end
          when :single then
            message = consumer.poll(config.max_wait_time_ms)
            process(message) if message
          end
        end
      end

      logger.info "Gracefully shutting down"
      begin
        if config.multithreaded_processing_enabled
          threads_orchestrator.shutdown_and_wait
        end
        processor.deliver!
        processor.teardown
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
        ConsumerSet.new(config, logger, @instrumenter, threads_orchestrator)
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

    def producer
      @producer ||= Rdkafka::Config.new(producer_config).producer.tap do |producer|
        producer.delivery_callback = Racecar::DeliveryCallback.new(instrumenter: @instrumenter)
      end
    end

    def producer_config
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      producer_config = {
        "bootstrap.servers"      => config.brokers.join(","),
        "client.id"              => config.client_id,
        "statistics.interval.ms" => config.statistics_interval_ms,
        "message.timeout.ms"     => config.message_timeout * 1000,
        "partitioner"            => config.partitioner.to_s,
      }

      producer_config["compression.codec"] = config.producer_compression_codec.to_s unless config.producer_compression_codec.nil?
      producer_config.merge!(config.rdkafka_producer)
      producer_config
    end

    def install_signal_handlers
      # Stop the consumer on SIGINT, SIGQUIT or SIGTERM.
      trap("QUIT") { stop }
      trap("INT")  { stop }
      trap("TERM") { stop }

      # Print the consumer config to STDERR on USR1.
      trap("USR1") { $stderr.puts config.inspect }
    end

    def process(message)
      if config.multithreaded_processing_enabled
        wrapped_message = Racecar::Message.new(message, retries_count: pauses[message.topic][message.partition].pauses_count)
        threads_orchestrator.push_messages(wrapped_message)
        return
      end

      payload = instrumentation_payload(message)

      with_pause(message.topic, message.partition, message.offset..message.offset) do |pause|
        begin
          instrument_process_message(payload) do
            processor.process(Racecar::Message.new(message, retries_count: pause.pauses_count))
            processor.deliver!
            consumer.store_offset(message)
          end
        rescue => e
          handle_processing_error(e, payload, pause: pause)
          raise e
        end
      end
    end

    def process_batch(messages)
      if config.multithreaded_processing_enabled
        wrapped_messages = messages.map do |message|
          Racecar::Message.new(message, retries_count: pauses[message.topic][message.partition].pauses_count)
        end
        threads_orchestrator.push_messages(wrapped_messages)
        return
      end

      payload = instrumentation_payload_for_batch(messages)
      first, last = messages.first, messages.last

      with_pause(first.topic, first.partition, first.offset..last.offset) do |pause|
        begin
          instrument_process_batch(payload) do
            racecar_messages = messages.map do |message|
              Racecar::Message.new(message, retries_count: pause.pauses_count)
            end
            processor.process_batch(racecar_messages)
            processor.deliver!
            consumer.store_offset(messages.last)
          end
        rescue => e
          handle_processing_error(e, payload, pause: pause)
          raise e
        end
      end
    end

    def reset_producer!
      @producer.close
      @producer = nil
      processor.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
      )
    end

    def with_pause(topic, partition, offsets)
      pause = pauses[topic][partition]
      return yield pause if config.pause_timeout == 0

      begin
        yield pause
        # We've successfully processed a batch from the partition, so we can clear the pause.
        pauses[topic][partition].reset!
      rescue => e
        desc = "#{topic}/#{partition}"
        logger.error "Failed to process #{desc} at #{offsets}: #{e}"

        logger.warn "Pausing partition #{desc} for #{pause.backoff_interval} seconds"
        consumer.pause(topic, partition, offsets.first)
        pause.pause!
      end
    end

    def resume_paused_partitions
      return if config.pause_timeout == 0

      pauses.each do |topic, partitions|
        partitions.each do |partition, pause|
          payload = {
            topic:          topic,
            partition:      partition,
            duration:       pause.pause_duration,
            consumer_class: processor.class.to_s,
          }
          @instrumenter.instrument("pause_status", payload)

          if pause.paused? && pause.expired?
            logger.info "Automatically resuming partition #{topic}/#{partition}, pause timeout expired"
            consumer.resume(topic, partition)
            pause.resume!
            # TODO: # During re-balancing we might have lost the paused partition. Check if partition is still in group before seek. ?
          end
        end
      end
    end
  end
end
