# frozen_string_literal: true

module Racecar
  module RailsConfigFileLoader
    def self.load!
      config_file = "config/racecar.yml"

      begin
        require "rails"
      rescue LoadError
        # Not a Rails application.
      end

      if defined?(Rails)
        $stderr.puts "=> Detected Rails, booting application..."

        require "./config/environment"

        if (Rails.root + config_file).readable?
          Racecar.config.load_file(config_file, Rails.env)
        end

        # In development, write Rails logs to STDOUT. This mirrors what e.g.
        # Unicorn does.
        if Rails.env.development? && defined?(ActiveSupport::Logger)
          console = ActiveSupport::Logger.new($stdout)
          console.formatter = Rails.logger.formatter
          console.level = Rails.logger.level
          if ::Rails::VERSION::STRING < "7.1"
            Rails.logger.extend(ActiveSupport::Logger.broadcast(console))
          else
            Rails.logger = ActiveSupport::BroadcastLogger.new(Rails.logger, console)
          end
        end
      end
    end
  end
end
