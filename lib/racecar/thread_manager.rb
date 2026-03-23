# frozen_string_literal: true

module Racecar
  class ThreadManager
    @finalize_mutex = Mutex.new

    def self.synchronize(&block)
      @finalize_mutex.synchronize(&block)
    end

    attr_reader :thread, :queue

    def initialize(thread_key:, logger:)
      @thread_key = thread_key
      @logger     = logger
      @queue      = Queue.new
      @mutex      = Mutex.new
      @metadata   = { rebalancing: false, shutting_down: false }
      @thread     = nil
    end

    def spawn(&block)
      @thread = Thread.new do
        Thread.current.name = "Racecar thread for #{@thread_key}"
        loop do
          wait_for_messages_or_exit
          msgs = @queue.pop
          block.call(msgs)
        end
      end
    end

    def push(messages)
      @queue << Array(messages)
    end

    def queue_size
      @queue.size
    end

    def alive?
      @thread&.alive?
    end

    def join
      @thread&.join
    end

    def wakeup
      @thread&.wakeup
    rescue ThreadError
      # thread died between the alive check and wakeup, safe to ignore
    end

    def set_rebalancing
      @mutex.synchronize { @metadata[:rebalancing] = true }
      wakeup
    end

    def set_shutting_down
      @mutex.synchronize { @metadata[:shutting_down] = true }
      wakeup
    end

    def metadata
      @mutex.synchronize { @metadata.dup }
    end

    private

    def wait_for_messages_or_exit
      while @queue.empty?
        m = metadata
        if m[:rebalancing] || m[:shutting_down]
          @logger.debug "Thread for #{@thread_key} exiting"
          Thread.exit
        else
          Thread.stop
        end
      end
    end
  end
end