require "logger"

require "racecar/consumer"
require "racecar/runner"
require "racecar/config"

module Racecar
  # Ignores all instrumentation events.
  class NullInstrumenter
    def self.instrument(*)
      yield if block_given?
    end
  end

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
    @logger ||= Logger.new(STDOUT)
  end

  def self.logger=(logger)
    @logger = logger
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
