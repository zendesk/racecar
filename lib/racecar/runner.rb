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

      # TODO: start from beginning
      # TODO: subscribing to multiple topics
      config.subscriptions.each do |subscription|
        consumer.subscribe(subscription.topic)
      end

      # Configure the consumer with a producer so it can produce messages.
      processor.configure(producer: producer)

      # Main loop
      loop do
        break if @stop_requested
        if message = consumer.poll(250)
          # TODO: process_batch
          process(message)
        end
      # TODO: pause handling
      rescue Rdkafka::RdkafkaError => e
        raise if e.message != "Broker: No more messages (partition_eof)"
        logger.debug "No more messages on this partition."
      end

      logger.info "Gracefully shutting down"
      processor.deliver!
      processor.teardown
      consumer_commit
      consumer.close
    end

    private

    def consumer
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      @consumer ||= Rdkafka::Config.new({
        "bootstrap.servers": config.brokers.join(","),
        "group.id":          config.group_id,
        "client.id":         config.client_id,
      }.merge(config.rdkafka_consumer)).consumer
    end

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

    def consumer_commit
      consumer.commit
    rescue Rdkafka::RdkafkaError => e
      raise e if e.message != "Local: No offset stored (no_offset)"
      logger.debug "Nothing to commit."
    end
  end
end
