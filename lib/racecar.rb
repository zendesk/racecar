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

  def self.install_signal_handlers
    return if @installed

    # Stop the consumers on SIGINT, SIGQUIT or SIGTERM.
    trap("QUIT") { stop }
    trap("INT")  { stop }
    trap("TERM") { stop }

    # Print the consumer config to STDERR on USR1.
    trap("USR1") { $stderr.puts config.inspect }
    @installed = true
  end

  def self.runners
    @runners ||= []
  end

  def self.threads
    @threads ||= []
  end

  def self.run(processor)
    if config.threaded
      run_threaded(processor)
    else
      @runners << Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter).tap(&:run)
    end
  end

  def self.run_threaded(processor)
    # Ensure signal-handlers installation.
    install_signal_handlers

    # Load the config specific to this processor.
    configuration = mutex.synchronize { config.dup }
    configuration.load_consumer_class(processor.class)
    configuration.validate!

    thr = Thread.new do
      runner = Runner.new(
        processor,
        config: configuration,
        logger: logger,
        instrumenter: instrumenter,
      )
      mutex.synchronize { runners << runner }
      runner.run
    end
    mutex.synchronize { threads << thr }
  end

  def self.stop
    runners.each(&:stop)
  end
end
