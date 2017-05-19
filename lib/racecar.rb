require "logger"

require "racecar/consumer"
require "racecar/runner"
require "racecar/config"

module Racecar
  def self.config
    @config ||= Config.new
  end

  def self.logger
    @logger ||= Logger.new(STDOUT)
  end

  def self.logger=(logger)
    @logger = logger
  end

  def self.run(processor)
    Runner.new(processor, config: config, logger: logger).run
  end
end
