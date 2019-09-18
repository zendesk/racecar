require "logger"

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
    require "active_support/notifications"

    ActiveSupport::Notifications
  rescue LoadError
    logger.warn "ActiveSupport::Notifications not available, instrumentation is disabled"

    NullInstrumenter
  end

  def self.run(processor)
    Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter).run
  end
end
