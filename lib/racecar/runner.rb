require "rdkafka"
require "racecar/pause"

module Racecar
  class Runner
    attr_reader :processor, :config, :logger

    def initialize(processor, config:, logger:, instrumenter: NullInstrumenter)
      @processor, @config, @logger = processor, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      Rdkafka::Config.logger = logger

      if processor.respond_to?(:statistics_callback)
        Rdkafka::Config.statistics_callback = processor.method(:statistics_callback).to_proc
      end

      setup_pauses
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
      )

      instrument_payload = { consumer_class: processor.class.to_s, consumer_set: consumer }

      # Main loop
      loop do
        break if @stop_requested
        resume_paused_partitions
        @instrumenter.instrument("main_loop.racecar", instrument_payload) do
          case process_method
          when :batch then
            msg_per_part = consumer.batch_poll(config.max_wait_time).group_by(&:partition)
            msg_per_part.each_value do |messages|
              process_batch(messages)
            end
          when :single then
            message = consumer.poll(config.max_wait_time)
            process(message) if message
          end
        end
      end

      logger.info "Gracefully shutting down"
      processor.deliver!
      processor.teardown
      consumer.commit
      consumer.close
    end

    def stop
      @stop_requested = true
    end

    private

    attr_reader :pauses

    def process_method
      @process_method ||= begin
        case
        when processor.respond_to?(:process_batch) then :batch
        when processor.respond_to?(:process) then :single
        else
          raise NotImplementedError, "Consumer class must implement process or process_batch method"
        end
      end
    end

    def consumer
      @consumer ||= begin
        # Manually store offset after messages have been processed successfully
        # to avoid marking failed messages as committed. The call just updates
        # a value within librdkafka and is asynchronously written to proper
        # storage through auto commits.
        config.consumer << "enable.auto.offset.store=false"
        ConsumerSet.new(config, logger)
      end
    end

    def producer
      @producer ||= Rdkafka::Config.new(producer_config).producer.tap do |producer|
        producer.delivery_callback = delivery_callback
      end
    end

    def producer_config
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      producer_config = {
        "bootstrap.servers"      => config.brokers.join(","),
        "client.id"              => config.client_id,
        "statistics.interval.ms" => 1000,
      }
      producer_config["compression.codec"] = config.producer_compression_codec.to_s unless config.producer_compression_codec.nil?
      producer_config.merge!(config.rdkafka_producer)
      producer_config
    end

    def delivery_callback
      ->(delivery_report) do
        data = {offset: delivery_report.offset, partition: delivery_report.partition}
        @instrumenter.instrument("acknowledged_message.racecar", data)
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

    def process(message)
      payload = {
        consumer_class: processor.class.to_s,
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      }

      @instrumenter.instrument("process_message.racecar", payload) do
        with_pause(message.topic, message.partition, message.offset..message.offset) do
          processor.process(message)
          processor.deliver!
          consumer.store_offset(message)
        end
      end
    end

    def process_batch(messages)
      payload = {
        consumer_class: processor.class.to_s,
        topic: messages.first.topic,
        partition: messages.first.partition,
        first_offset: messages.first.offset,
        message_count: messages.size,
      }

      @instrumenter.instrument("process_batch.racecar", payload) do
        first, last = messages.first, messages.last
        with_pause(first.topic, first.partition, first.offset..last.offset) do
          processor.process_batch(messages)
          processor.deliver!
          consumer.store_offset(messages.last)
        end
      end
    end

    def with_pause(topic, partition, offsets)
      return yield if config.pause_timeout == 0

      begin
        yield
        # We've successfully processed a batch from the partition, so we can clear the pause.
        pauses[topic][partition].reset!
      rescue => e
        desc = "#{topic}/#{partition}"
        logger.error "Failed to process #{desc} at #{offsets}: #{e}"

        pause = pauses[topic][partition]
        logger.warn "Pausing partition #{desc} for #{pause.backoff_interval} seconds"
        consumer.pause(topic, partition, offsets.first)
        pause.pause!
      end
    end

    def resume_paused_partitions
      return if config.pause_timeout == 0

      pauses.each do |topic, partitions|
        partitions.each do |partition, pause|
          @instrumenter.instrument("pause_status.racecar", {
            topic: topic,
            partition: partition,
            duration: pause.pause_duration,
          })

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
