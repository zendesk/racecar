require "logger"

require "racecar/instrumenter"
require "racecar/null_instrumenter"
require "racecar/consumer"
require "racecar/consumer_set"
require "racecar/runner"
require "racecar/config"
require "ensure_hash_compact"

module Racecar
  class Error < StandardError
  end

  class ConfigError < Error
  end

  def self.config
    @config ||= Config.new
  end

  def self.config=(config)
    @config = config
  end

  def self.configure
    yield config
  end

  def self.logger
    config.logger
  end

  def self.logger=(logger)
    config.logger = logger
  end

  def self.instrumenter
    @instrumenter ||= begin
      default_payload = { client_id: config.client_id, group_id: config.group_id }

      Instrumenter.new(default_payload).tap do |instrumenter|
        if instrumenter.backend == NullInstrumenter
          logger.warn "ActiveSupport::Notifications not available, instrumentation is disabled"
        end
      end
    end
  end

  def self.mutex
    @mutex ||= Mutex.new
  end

  def self.install_at_exit
    return if @installed

    @installed = true
    at_exit { stop }
  end

  def self.runners
    @runners ||= []
  end

  def self.run(processor)
    unless config.standalone
      # Ensure thread-safe at_exit installation
      mutex.synchronize { install_at_exit }
  
      # Load the config specific to this processor.
      configuration = config.dup
      configuration.load_consumer_class(processor.class)
      configuration.validate!
    end

    configuration ||= config

    runners << Runner.new(
      processor,
      config: configuration,
      logger: logger,
      instrumenter: instrumenter,
    ).tap(&:run)
  end

  def self.stop
    runners.each(&:stop)
  end
end
