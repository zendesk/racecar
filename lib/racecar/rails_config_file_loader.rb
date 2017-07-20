module Racecar
  module RailsConfigFileLoader
    def self.load!
      config_file = "config/racecar.yml"

      begin
        require "rails"

        puts "=> Detected Rails, booting application..."

        require "./config/environment"

        Racecar.config.load_file(config_file, Rails.env)

        if Racecar.config.log_to_stdout
          # Write to STDOUT as well as to the log file.
          console = ActiveSupport::Logger.new($stdout)
          console.formatter = Rails.logger.formatter
          console.level = Rails.logger.level
          Rails.logger.extend(ActiveSupport::Logger.broadcast(console))
        end

        Racecar.logger = Rails.logger
      rescue LoadError
        # Not a Rails application.
      end
    end
  end
end
