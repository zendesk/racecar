# frozen_string_literal: true

module Racecar
  module ConfigLoader
    def self.load!
      # This loads config from ENV vars and cli args
      config = Racecar.config

      # Load generic config file
      if File.exist?("config/racecar.rb")
        require "./config/racecar"
      end

      # Load rails config unless explictly disabled already via env var or cli args
      RailsConfigFileLoader.load! unless config.without_rails?

      if config.datadog_enabled
        require_relative './datadog'

        Datadog.configure do |datadog|
          datadog.host      = config.datadog_host unless config.datadog_host.nil?
          datadog.port      = config.datadog_port unless config.datadog_port.nil?
          datadog.namespace = config.datadog_namespace unless config.datadog_namespace.nil?
          datadog.tags      = config.datadog_tags unless config.datadog_tags.nil?
        end
      end
    end
  end
end
