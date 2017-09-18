require "optparse"
require "logger"
require "fileutils"
require "racecar/rails_config_file_loader"
require "racecar/daemon"

module Racecar
  module Cli
    def self.main(args)
      config = Racecar.config

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: racecar MyConsumer [options]"

        opts.on("-r", "--require LIBRARY", "Require the LIBRARY before starting the consumer") do |lib|
          require lib
        end

        opts.on("-l", "--log LOGFILE", "Log to the specified file") do |logfile|
          config.logfile = logfile
        end

        Racecar::Config.variables.each do |variable|
          opt_name = "--" << variable.name.to_s.gsub("_", "-")
          opt_name << " VALUE" unless variable.boolean?

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

      parser.parse!(args)

      consumer_name = args.first or raise Racecar::Error, "no consumer specified"

      $stderr.puts "=> Starting Racecar consumer #{consumer_name}..."

      RailsConfigFileLoader.load!

      # Find the consumer class by name.
      consumer_class = Kernel.const_get(consumer_name)

      # Load config defined by the consumer class itself.
      config.load_consumer_class(consumer_class)

      config.validate!

      if config.logfile
        $stderr.puts "=> Logging to #{config.logfile}"
        Racecar.logger = Logger.new(config.logfile)
      end

      $stderr.puts "=> Wrooooom!"

      if config.daemonize
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
      else
        $stderr.puts "=> Ctrl-C to shutdown consumer"
      end

      processor = consumer_class.new

      Racecar.run(processor)
    rescue => e
      $stderr.puts "=> Crashed: #{e}"

      raise
    end
  end
end
