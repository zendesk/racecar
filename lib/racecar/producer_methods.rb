# frozen_string_literal: true

module Racecar
  module ProducerMethods
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
  end
end

