require "rdkafka"

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
    end

    def run
      install_signal_handlers

      # Manually store offset after messages have been processed successfully
      # to avoid marking failed messages as committed. The call just updates
      # a value within librdkafka and is asynchronously written to proper
      # storage through auto commits.
      config.consumer << "enable.auto.offset.store=false"

      @consumer = ConsumerSet.new(config, logger)

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

    attr_reader :consumer

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
        processor.process(message)
        processor.deliver!
        consumer.store_offset(message)
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
        processor.process_batch(messages)
        processor.deliver!
        consumer.store_offset(messages.last)
      end
    end
  end
end
