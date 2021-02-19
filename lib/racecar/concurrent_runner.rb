# frozen_string_literal: true

require "concurrent-ruby"
require "racecar/runner"
require "racecar/signals"

module Racecar
  class ConcurrentRunner
    def initialize(consumer_class:, config:, logger:, instrumenter:)
      @consumer_class = consumer_class
      @config = config
      @logger = logger
      @instrumenter = instrumenter
    end

    def run
      puts "=> Running with max concurrency of #{config.max_concurrency}"

      config.max_concurrency.times do
        runner = Racecar::Runner.new(
          consumer_class.new,
          config: config,
          logger: logger,
          instrumenter: instrumenter,
          disable_signal_handlers: true
        )
        consumers << runner

        schedule(runner)
      end

      trap("USR1") { $stderr.puts config.inspect }

      Signals.setup_shutdown_handlers
      self.signals_ready = true
      Signals.wait_for_shutdown { stop }
    end

    def stop
      consumers.each(&:stop)
      pool.shutdown
      pool.wait_for_termination
      logger.info "All consumers shut down"
      raise exception if exception
    end

    private

    attr_accessor :signals_ready, :exception
    attr_reader :consumer_class, :config, :logger, :instrumenter

    def schedule(runner)
      pool.post do
        until signals_ready; end
        begin
          runner.run
        rescue Exception => e
          # Store exception to be reraised after graceful shutdown
          self.exception = e
          # Ensure that any uncaught exceptions cause a crash
          Process.kill("INT", 0)
        end
      end
    end

    def consumers
      @consumers ||= []
    end

    def pool
      @pool ||= Concurrent::FixedThreadPool.new(config.max_concurrency)
    end
  end
end
