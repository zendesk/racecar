require "concurrent-ruby"
require "racecar/pause_guarded_processing"

module Racecar
  class PartitionWorker < Concurrent::Actor::Context
    attr_reader :processor, :producer, :consumer, :pauses, :logger, :config

    include PauseGuardedProcessing

    def initialize(args)
      @processor = args[:processor]
      @producer = @processor.producer
      @consumer = @processor.consumer
      @pauses = args[:pauses]
      @logger = args[:logger]
      @config = args[:config]
      @instrumenter = args[:instrumenter]
    end

    def on_message(message)
      case message
      when Hash
        case message[:action]
        when 'process'
          process_with_pause(message[:message])
        when 'process_batch'
          process_batch_with_pause(message[:messages])
        else
          Racecar.logger.warn "Unknown action '#{message[:action]}' received by a partition worker"
        end
      when :terminate!
        handle_shutdown
        :terminate!
      else
        Racecar.logger.warn "Unknown message received by a partition worker: #{message}"
      end
    end

    private

    def handle_shutdown
      logger.info "Shutting down a partition worker..."
      processor.deliver!
      processor.teardown
      consumer.thread_safe_commit
    end
  end
end