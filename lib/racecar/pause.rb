module Racecar
  class Pause
    def initialize(timeout: nil, max_timeout: nil, exponential_backoff: false)
      @started_at = nil
      @pauses = 0
      @timeout = timeout
      @max_timeout = max_timeout
      @exponential_backoff = exponential_backoff
    end

    def pause!
      @started_at = Time.now
      @ends_at = @started_at + backoff_interval unless @timeout.nil?
      @pauses += 1
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
      @pauses = 0
    end

    def backoff_interval
      return Float::INFINITY if @timeout.nil?

      backoff_factor = @exponential_backoff ? 2**@pauses : 1
      timeout = backoff_factor * @timeout

      timeout = @max_timeout if @max_timeout && timeout > @max_timeout

      timeout
    end
  end
end
