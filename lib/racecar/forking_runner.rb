# frozen_string_literal: true

module Racecar
  class ForkingRunner
    def initialize(runner:, config:, logger:, parent_monitor: ParentProcessMonitor.new)
      @runner = runner
      @config = config
      @logger = logger
      @pids = []
      @parent_monitor = parent_monitor
      @running = false
    end

    attr_reader :config, :runner, :logger, :pids, :parent_monitor
    private     :config, :runner, :logger, :pids, :parent_monitor

    def run
      config.prefork.call
      install_signal_handlers

      @running = true

      @pids = config.forks.times.map do |n|
        pid = fork do
          parent_monitor.child_post_fork
          config.postfork.call

          parent_monitor.on_parent_exit do
            logger.warn("Supervisor dead, exiting.")
            runner.stop
          end

          runner.run
        end
        logger.debug("Racecar forked consumer process #{pid}.")

        pid
      end

      parent_monitor.parent_post_fork

      wait_for_child_processes
    end

    def stop
      @running = false
      logger.debug("Racecar::ForkingRunner runner stopping #{Process.pid}.")
      terminate_workers
    end

    def running?
      !!@running
    end

    private

    def terminate_workers
      pids.each do |pid|
        begin
          Process.kill("TERM", pid)
        rescue Errno::ESRCH
          logger.debug("Racecar::ForkingRunner Process not found #{Process.pid}.")
        end
      end
    end

    def check_workers
      pids.each do |pid|
        unless worker_running?(pid)
          logger.debug("A forked worker has exited unepxectedly. Shutting everything down.")
          stop
          return
        end
      end
    end

    def worker_running?(pid)
      _, status = Process.waitpid2(pid, Process::WNOHANG)
      status.nil?
    rescue Errno::ECHILD
      false
    end

    def wait_for_child_processes
      pids.each do |pid|
        begin
          Process.wait(pid)
        rescue Errno::ECHILD
        end
      end
    end

    def install_signal_handlers
      Signal.trap("CHLD") do |sid|
        logger.warn("Received SIGCHLD")
        check_workers if running?
      end
      Signal.trap("TERM") do |sid|
        stop
      end
      Signal.trap("INT") do |sid|
        stop
      end
    end

    class ParentProcessMonitor
      def initialize(pipe_ends = IO.pipe)
        @read_end, @write_end = pipe_ends
        @monitor_thread = nil
      end

      attr_reader :read_end, :write_end, :monitor_thread
      private :read_end, :write_end, :monitor_thread

      def on_parent_exit(&block)
        child_post_fork
        monitor_thread = Thread.new do
          IO.select([read_end])
          block.call
        end
      end

      def parent_post_fork
        read_end.close
      end

      def child_post_fork
        write_end.close
      end
    end
  end
end
