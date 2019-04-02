require "optparse"
require "logger"
require "fileutils"
require "racecar/rails_config_file_loader"
require "racecar/daemon"

module Racecar
  class Cli
    def self.main(args)
      new(args).run
    end

    def initialize(args)
      @parser = build_parser
      @parser.parse!(args)
      @consumer_name = args.first or raise Racecar::Error, "no consumer specified"
    end

    def config
      Racecar.config
    end

    def run
      $stderr.puts "=> Starting Racecar consumer #{consumer_name}..."

      RailsConfigFileLoader.load!

      if File.exist?("config/racecar.rb")
        require "config/racecar"
      end

      # Find the consumer class by name.
      consumer_class = Kernel.const_get(consumer_name)

      # Load config defined by the consumer class itself.
      config.load_consumer_class(consumer_class)

      config.validate!

      if config.logfile
        $stderr.puts "=> Logging to #{config.logfile}"
        Racecar.logger = Logger.new(config.logfile)
      end

      if config.log_level
        Racecar.logger.level = Object.const_get("Logger::#{config.log_level.upcase}")
      end

      if config.datadog_enabled
        configure_datadog
      end

      $stderr.puts "=> Wrooooom!"

      if config.daemonize
        daemonize!
      else
        $stderr.puts "=> Ctrl-C to shutdown consumer"
      end

      processor = consumer_class.new

      Racecar.run(processor)
    rescue => e
      $stderr.puts "=> Crashed: #{e.class}: #{e}\n#{e.backtrace.join("\n")}"

      config.error_handler.call(e)

      raise
    end

    private

    attr_reader :consumer_name

    def daemonize!
      daemon = Daemon.new(File.expand_path(config.pidfile))

      daemon.check_pid

      $stderr.puts "=> Starting background process"
      $stderr.puts "=> Writing PID to #{daemon.pidfile}"

      daemon.suppress_input

      if config.logfile.nil?
        daemon.suppress_output
      else
        daemon.redirect_output(config.logfile)
      end

      daemon.daemonize!
      daemon.write_pid
    end

    def build_parser
      load_path_modified = false

      OptionParser.new do |opts|
        opts.banner = "Usage: racecar MyConsumer [options]"

        opts.on("-r", "--require STRING", "Require a library before starting the consumer") do |lib|
          $LOAD_PATH.unshift(Dir.pwd) unless load_path_modified
          load_path_modified = true
          require lib
        end

        opts.on("-l", "--log STRING", "Log to the specified file") do |logfile|
          config.logfile = logfile
        end

        Racecar::Config.variables.each do |variable|
          opt_name = "--" << variable.name.to_s.gsub("_", "-")
          opt_name << " #{variable.type.upcase}" unless variable.boolean?

          desc = variable.description || "N/A"

          if variable.default
            desc << " (default: #{variable.default.inspect})"
          end

          opts.on(opt_name, desc) do |value|
            if variable.boolean?
              # Boolean switches are automatically mapped to true/false.
              config.set(variable.name, value)
            else
              # Other CLI params need to be decoded into values of the correct type.
              config.decode(variable.name, value)
            end
          end
        end

        opts.on_tail("--version", "Show Racecar version") do
          require "racecar/version"
          $stderr.puts "Racecar #{Racecar::VERSION}"
          exit
        end

        opts.on_tail("-h", "--help", "Show this message") do
          puts opts
          exit
        end
      end
    end

    def configure_datadog
      require "kafka/datadog"

      datadog = Kafka::Datadog
      datadog.host = config.datadog_host unless config.datadog_host.nil?
      datadog.port = config.datadog_port unless config.datadog_port.nil?
      datadog.namespace = config.datadog_namespace unless config.datadog_namespace.nil?
      datadog.tags = config.datadog_tags unless config.datadog_tags.nil?
    end
  end
end
