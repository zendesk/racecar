# frozen_string_literal: true

module Racecar
  class ThreadManager
    attr_reader :thread, :queue

    THREAD_KEY = 'thread_key'.freeze

    def initialize(thread_key:, logger:)
      @thread_key = thread_key
      @logger     = logger
      @queue      = Queue.new
      @mutex      = Mutex.new
      @cv         = ConditionVariable.new
      @metadata   = { rebalancing: false, shutting_down: false }
      @thread     = nil
    end

    def spawn(&block)
      @thread = Thread.new do
        Thread.current.name = "Racecar thread for #{@thread_key}"
        Thread.current[ThreadManager::THREAD_KEY] = @thread_key
        loop do
          wait_for_messages_or_exit
          msgs = @queue.pop
          block.call(msgs)
        end
      end
    end

    def push(messages)
      @mutex.synchronize do
        @queue << Array(messages)
        @cv.signal
      end
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

    def set_rebalancing
      @mutex.synchronize { @metadata[:rebalancing] = true; @cv.signal }
    end

    def set_shutting_down
      @mutex.synchronize { @metadata[:shutting_down] = true; @cv.signal }
    end

    def metadata
      @mutex.synchronize { @metadata.dup }
    end

    private

    def wait_for_messages_or_exit
      @mutex.synchronize do
        while @queue.empty?
          if @metadata[:rebalancing] || @metadata[:shutting_down]
            @logger.debug "Thread for #{@thread_key} exiting"
            Thread.exit
          end
          @cv.wait(@mutex)
        end
      end
    end
  end
end