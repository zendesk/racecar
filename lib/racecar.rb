# frozen_string_literal: true

require "logger"

require "racecar/instrumenter"
require "racecar/null_instrumenter"
require "racecar/consumer"
require "racecar/consumer_set"
require "racecar/runner"
require "racecar/parallel_runner"
require "racecar/producer"
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

  def self.produce_async(value:, topic:, **options)
    producer.produce_async(value: value, topic: topic, **options)
  end

  def self.produce_sync(value:, topic:, **options)
    producer.produce_sync(value: value, topic: topic, **options)
  end  
  
  def self.wait_for_delivery(&block)
    producer.wait_for_delivery(&block)
  end

  def self.producer
    Thread.current[:racecar_producer] ||= begin
      if config.datadog_enabled
        require "racecar/datadog"
      end
      Racecar::Producer.new(config: config, logger: logger, instrumenter: instrumenter)
    end
  end

  def self.instrumenter
    config.instrumenter
  end

  def self.run(processor)
    runner(processor).run
  end

  def self.runner(processor)
    runner = Runner.new(processor, config: config, logger: logger, instrumenter: config.instrumenter)

    if config.parallel_workers && config.parallel_workers > 1
      ParallelRunner.new(runner: runner, config: config, logger: logger)
    else
      runner
    end
  end
end
