require "optparse"
require "racecar/config_loader"

module Racecar
  class Ctl
    ProduceMessage = Struct.new(:value, :key, :topic)

    def self.main(args)
      command = args.shift or raise Racecar::Error, "no command specified"

      ctl = new

      if ctl.respond_to?(command)
        ctl.send(command, args)
      else
        raise Racecar::Error, "invalid command: #{command}"
      end
    end

    def produce(args)
      message = ProduceMessage.new

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: racecarctl produce [options]"

        opts.on("-v", "--value VALUE", "Set the message value") do |value|
          message.value = value
        end

        opts.on("-k", "--key KEY", "Set the message key") do |key|
          message.key = key
        end

        opts.on("-t", "--topic TOPIC", "Set the message topic") do |topic|
          message.topic = topic
        end
      end

      parser.parse!(args)

      ConfigLoader.load!

      Racecar.config.validate!

      kafka = Kafka.new(
        client_id: Racecar.config.client_id,
        seed_brokers: Racecar.config.brokers,
        logger: Racecar.logger,
        connect_timeout: Racecar.config.connect_timeout,
        socket_timeout: Racecar.config.socket_timeout,
      )

      kafka.deliver_message(message.value, key: message.key, topic: message.topic)

      puts "=> Delivered message to Kafka cluster"
    end
  end
end
