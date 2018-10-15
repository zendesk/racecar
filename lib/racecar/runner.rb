require "rdkafka"

module Racecar
  class Runner
    attr_reader :processor, :config, :logger

    def initialize(processor, config:, logger:, instrumenter: NullInstrumenter)
      @processor, @config, @logger = processor, config, logger
      @instrumenter = instrumenter
      @stop_requested = false
      Rdkafka::Config.logger = logger
    end

    def run
      install_signal_handlers

      consumer = ConsumerSet.new(config, logger)
      consumer.subscribe

      # Configure the consumer with a producer so it can produce messages and
      # with a consumer so that it can support advanced use-cases.
      processor.configure(
        producer: producer,
        consumer: consumer
      )

      # Main loop
      loop do
        break if @stop_requested
        if message = consumer.poll(250)
          # TODO: process_batch
          process(message)
        end
      # TODO: pause handling
      end

      logger.info "Gracefully shutting down"
      processor.deliver!
      processor.teardown
      consumer.commit
      consumer.close
    end

    private

    def producer
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      @producer ||= Rdkafka::Config.new({
        "bootstrap.servers": config.brokers.join(","),
        "client.id":         config.client_id,
      }.merge(config.rdkafka_producer)).producer
    end

    def install_signal_handlers
      # Stop the consumer on SIGINT, SIGQUIT or SIGTERM.
      trap("QUIT") { @stop_requested = true }
      trap("INT")  { @stop_requested = true }
      trap("TERM") { @stop_requested = true }

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
      end
    end
  end
end
