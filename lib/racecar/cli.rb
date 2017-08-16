require "optparse"
require "logger"
require "racecar/rails_config_file_loader"

module Racecar
  module Cli
    def self.main(args)
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: racecar MyConsumer [options]"

        opts.on("-r", "--require LIBRARY", "Require the LIBRARY before starting the consumer") do |lib|
          require lib
        end

        opts.on("-l", "--log LOGFILE", "Log to the specified file") do |logfile|
          Racecar.config.logfile = logfile
        end

        opts.on_tail("--version", "Show Racecar version") do
          require "racecar/version"
          puts "Racecar #{Racecar::VERSION}"
          exit
        end
      end

      parser.parse!(args)

      consumer_name = args.first or raise Racecar::Error, "no consumer specified"

      puts "=> Starting Racecar consumer #{consumer_name}..."

      RailsConfigFileLoader.load!

      # Find the consumer class by name.
      consumer_class = Kernel.const_get(consumer_name)

      # Load config defined by the consumer class itself.
      Racecar.config.load_consumer_class(consumer_class)

      Racecar.config.validate!

      if Racecar.config.logfile
        puts "=> Logging to #{Racecar.config.logfile}"
        Racecar.logger = Logger.new(Racecar.config.logfile)
      end

      puts "=> Wrooooom!"
      puts "=> Ctrl-C to shutdown consumer"

      processor = consumer_class.new

      Racecar.run(processor)

      puts "=> Shut down"
    end
  end
end
