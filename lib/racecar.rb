# frozen_string_literal: true

require "logger"

require "racecar/instrumenter"
require "racecar/null_instrumenter"
require "racecar/consumer"
require "racecar/consumer_set"
require "racecar/runner"
require "racecar/parallel_runner"
require "racecar/config"
require "racecar/version"
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

  def self.run(processor)
    runner = Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter)

    if config.parallel_workers && config.parallel_workers > 1
      ParallelRunner.new(runner: runner, config: config, logger: logger).run
    else
      runner.run
    end
  end
end
