# frozen_string_literal: true

module Racecar
  class Pause
    attr_reader :pauses_count

    def initialize(timeout: nil, max_timeout: nil, exponential_backoff: false)
      @started_at = nil
      @pauses_count = 0
      @timeout = timeout
      @max_timeout = max_timeout
      @exponential_backoff = exponential_backoff
    end

    def pause!
      @started_at = Time.now
      @ends_at = @started_at + backoff_interval unless @timeout.nil?
      @pauses_count += 1
    end

    def resume!
      @started_at = nil
      @ends_at = nil
    end

    def paused?
      !@started_at.nil?
    end

    def pause_duration
      if paused?
        Time.now - @started_at
      else
        0
      end
    end

    def expired?
      return false if @timeout.nil?
      return true unless @ends_at
      Time.now >= @ends_at
    end

    def reset!
      @pauses_count = 0
    end

    def backoff_interval
      return Float::INFINITY if @timeout.nil?

      backoff_factor = @exponential_backoff ? 2**@pauses_count : 1
      timeout = backoff_factor * @timeout

      timeout = @max_timeout if @max_timeout && timeout > @max_timeout

      timeout
    end
  end
end
