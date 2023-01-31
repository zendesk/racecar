# frozen_string_literal: true

require "logger"

require "racecar/instrumenter"
require "racecar/null_instrumenter"
require "racecar/consumer"
require "racecar/consumer_set"
require "racecar/runner"
require "racecar/parallel_runner"
require "racecar/threadpool_runner_proxy"
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
    runner(processor).run
  end

  def self.runner(processor)
    if config.threads && config.threads > 1
      runner = multithreaded_runner(processor)
    else
      runner = standard_runner(processor)
    end

    if config.forks && config.forks > 1
      runner = forking_runner(processor, runner)
    end

    runner
  end

  private_class_method def self.forking_runner(processor, base_runner)
    ParallelRunner.new(runner: base_runner, config: config, logger: logger)
  end

  private_class_method def self.multithreaded_runner(processor)
    standard_runner_factory = method(:standard_runner)
    ThreadPoolRunnerProxy.new(processor, config: config, runner_factory: standard_runner_factory)
  end

  private_class_method def self.standard_runner(processor)
    Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter)
  end
end
