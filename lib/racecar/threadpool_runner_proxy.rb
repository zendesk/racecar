require "concurrent-ruby"

module Racecar
  class ThreadPoolRunnerProxy
    def initialize(processor, config:, runner_factory:)
      @processor = processor
      @config = config
      @runner_factory = runner_factory

      @parent_thread = Thread.current
      @runners = []
    end

    attr_reader :runner_factory, :processor, :config
    private     :runner_factory, :processor, :config

    attr_reader :parent_thread, :runners
    private     :parent_thread, :runners

    def stop
      runners.each(&:stop)
    end

    def running?
      @runners.any?(&:running?)
    end

    def run
      Thread.current.name = "main"

      logger.debug("Spawning #{thread_count} " + "🧵" * thread_count)
      logger.info("ThreadPoolRunnerProxy starting #{thread_count} worker threads")

      thread_count.times do |id|
        thread_pool.post do
          work_in_thread(id)
        end
      end

      wait_for_runners_to_start
      logger.debug("Threaded runners running 🧵🏃")
      wait_for_normal_stop
    ensure
      ensure_thread_pool_terminates
      nil
    end

    private

    attr_reader :runners

    def work_in_thread(id)
      Thread.current.name = "Racecar worker thread #{Process.pid}-#{id}"
      Thread.current.abort_on_exception = true

      runner = runner_factory.call(processor)
      runners << runner
      logger.debug("ThreadPoolRunnerProxy starting runner #{runner} with consumer #{processor} 🏃🏁")

      runner.run

      logger.debug("ThreadPoolRunnerProxy runner stopped #{runner} 🏃🛑")
    rescue Exception => e
      logger.error("Error in threadpool, raising in parent thread. #{e.full_message}")
      stop
      parent_thread.raise(e)
    end

    def wait_for_runners_to_start
      started_waiting_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      until runners.length == thread_count && runners.all?(&:running?)
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_waiting_at

        summary = runners.map { |r| "#{r}: running=#{r.running?}" }.join(", ")
        logger.debug("Waiting for runners to start #{summary} 🥱🏃🏃🏃🏁")

        break if elapsed > thread_pool_timeout
        sleep(thread_check_interval)
      end
    end

    def wait_for_normal_stop
      until @runners.none?(&:running?)
        sleep(thread_check_interval)
      end
      logger.debug("ThreadPoolRunnerProxy all runners have stopped 🏃🏃🏃 🛑🛑🛑")
    end

    def ensure_thread_pool_terminates
      logger.debug("ThreadPoolRunnerProxy waiting for thread pool to terminate 🎱💀💀💀")
      thread_pool.wait_for_termination(thread_pool_timeout)
    end

    def thread_pool
      @thread_pool ||= Concurrent::FixedThreadPool.new(thread_count)
    end

    def thread_count
      @threads ||= (config.threads || 1)
    end

    def thread_pool_timeout
      config.thread_pool_timeout
    end

    def thread_check_interval
      1
    end

    def logger
      config.logger
    end
  end
end
