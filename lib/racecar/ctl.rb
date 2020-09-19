# frozen_string_literal: true

require "optparse"
require "racecar/rails_config_file_loader"
require "racecar/daemon"

module Racecar
  class Ctl
    ProduceMessage = Struct.new(:value, :key, :topic)

    def self.main(args)
      command = args.shift

      if command.nil?
        puts "No command specified. Commands:"
        puts " - status"
        puts " - stop"
        puts " - produce"
      else
        ctl = new(command)

        if ctl.respond_to?(command)
          ctl.send(command, args)
        else
          raise Racecar::Error, "invalid command: #{command}"
        end
      end
    end

    def initialize(command)
      @command = command
    end

    def status(args)
      parse_options!(args)

      pidfile = Racecar.config.pidfile
      daemon = Daemon.new(pidfile)

      if daemon.running?
        puts "running (PID = #{daemon.pid})"
      else
        puts daemon.pid_status
      end
    end

    def stop(args)
      parse_options!(args)

      pidfile = Racecar.config.pidfile
      daemon = Daemon.new(pidfile)

      if daemon.running?
        daemon.stop!
        while daemon.running?
          puts "Waiting for Racecar process to stop..."
          sleep 5
        end
        puts "Racecar stopped"
      else
        puts "Racecar is not currently running"
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

      if message.topic.nil?
        raise Racecar::Error, "no topic specified"
      end

      if message.value.nil?
        raise Racecar::Error, "no message value specified"
      end

      RailsConfigFileLoader.load!

      Racecar.config.validate!

      producer = Rdkafka::Config.new({
        "bootstrap.servers": Racecar.config.brokers.join(","),
        "client.id":         Racecar.config.client_id,
      }.merge(Racecar.config.rdkafka_producer)).producer

      producer.produce(payload: message.value, key: message.key, topic: message.topic).wait

      $stderr.puts "=> Delivered message to Kafka cluster"
    end

    private

    def parse_options!(args)
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: racecarctl #{@command} [options]"

        opts.on("--pidfile PATH", "Use the PID stored in the specified file") do |path|
          Racecar.config.pidfile = File.expand_path(path)
        end
      end

      parser.parse!(args)
    end
  end
end
