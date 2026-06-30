# frozen_string_literal: true

require "rdkafka"
require "racecar/pause"
require "racecar/message"
require "racecar/message_delivery_error"
require "racecar/erroneous_state_error"
require "racecar/delivery_callback"
require "racecar/partition_processor"

module Racecar
  class Runner
    attr_reader :processor, :config, :logger

    def initialize(processor, config:, logger:, instrumenter: NullInstrumenter)
      @processor, @config, @logger = processor, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      @partition_processors = Hash.new
      Rdkafka::Config.logger = logger

      if processor.respond_to?(:statistics_callback)
        Rdkafka::Config.statistics_callback = processor.method(:statistics_callback).to_proc
      end

      setup_pauses
    end

    def self.topic_partition_key(topic, partition)
      "#{topic}/#{partition}"
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
      @instrumenter.instrument("shut_down", instrumentation_payload || {})
    end

    def stop
      @stop_requested = true
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

    def consumer
      @consumer ||= begin
        ConsumerSet.new(config, logger, @instrumenter)
      end
    end

    def assign_and_get_processor(messages)
      topic     = messages.is_a?(Array) ? messages.first.topic     : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      key = Runner.topic_partition_key(topic, partition)
      return @partition_processors[key] if @partition_processors[key]

      @partition_processors[key] = PartitionProcessor.new(processor: processor, consumer: consumer, instrumenter: @instrumenter)
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
      processor = assign_and_get_processor(message)
      instrumentation_payload = processor.message_payload(message)

      @instrumenter.instrument("start_process_message", instrumentation_payload)
      with_pause(message.topic, message.partition, message.offset..message.offset) do |pause|
        begin
          processor.process(message, pause.pauses_count, instrumentation_payload)
        rescue => e
          instrumentation_payload[:unrecoverable_delivery_error] = reset_producer_on_unrecoverable_delivery_errors(e)
          instrumentation_payload[:retries_count] = pause.pauses_count
          config.error_handler.call(e, instrumentation_payload)
          raise e
        end
      end
    end

    def process_batch(messages)
      processor = assign_and_get_processor(messages)
      instrumentation_payload = processor.batch_payload(messages)

      @instrumenter.instrument("start_process_batch", instrumentation_payload)
      first = messages.first
      with_pause(first.topic, first.partition, first.offset..messages.last.offset) do |pause|
        begin
          processor.process_batch(messages, pause.pauses_count, instrumentation_payload)
        rescue => e
          instrumentation_payload[:unrecoverable_delivery_error] = reset_producer_on_unrecoverable_delivery_errors(e)
          instrumentation_payload[:retries_count] = pause.pauses_count
          config.error_handler.call(e, instrumentation_payload)
          raise e
        end
      end
    end

    # librdkafka will continue to try to deliver already queued messages, even if ruby-rdkafka
    # raised before that. This method detects any unrecoverable errors and resets the producer
    # as a last ditch effort.
    # The function returns true if there were unrecoverable errors, or false otherwise.
    def reset_producer_on_unrecoverable_delivery_errors(error)
      return false unless error.is_a?(Racecar::MessageDeliveryError)
      return false unless error.code == :msg_timed_out # -192

      logger.error error.to_s
      logger.error "Racecar will reset the producer to force a new broker connection."
      @producer.close
      @producer = nil
      processor.configure(
        producer:     producer,
        consumer:     consumer,
        instrumenter: @instrumenter,
        config:       @config,
      )

      true
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
          instrumentation_payload = {
            topic:      topic,
            partition:  partition,
            duration:   pause.pause_duration,
            consumer_class: processor.class.to_s,
          }
          @instrumenter.instrument("pause_status", instrumentation_payload)

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
