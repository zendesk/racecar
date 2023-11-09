# frozen_string_literal: true

module Racecar
  class ForkingRunner
    def initialize(runner:, config:, logger:, liveness_monitor: LivenessMonitor.new)
      @runner = runner
      @config = config
      @logger = logger
      @pids = []
      @liveness_monitor = liveness_monitor
      @running = false
    end

    attr_reader :config, :runner, :logger, :pids, :liveness_monitor
    private     :config, :runner, :logger, :pids, :liveness_monitor

    def run
      config.prefork.call
      @running = true

      @pids = config.forks.times.map do |n|
        pid = fork do
          liveness_monitor.child_post_fork
          config.postfork.call

          liveness_monitor.on_exit do
            logger.warn "Supervisor dead, exiting."
            runner.stop
          end

          runner.run
        end
        logger.info "Racecar forked consumer process #{pid}"

        pid
      end

      liveness_monitor.parent_post_fork

      install_signal_handlers

      wait_for_child_processes
    end

    def stop
      @running = false
      $stdout.puts "Racecar::ForkingRunner runner stopping #{Process.pid}"
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
        end
      end
    end

    def check_workers
      pids.each do |pid|
        unless worker_running?(pid)
          $stdout.puts("A forker worker has exited. Shuttin' it down ...")
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
        # Received when child process terminates
        # $stderr.puts "👼👼👼👼👼👼👼 SIGCHLD"
        check_workers if running?
      end
      Signal.trap("TERM") do |sid|
        stop
      end
      Signal.trap("INT") do |sid|
        stop
      end
    end

    class LivenessMonitor
      def initialize(pipe_ends = IO.pipe)
        @read_end, @write_end = pipe_ends
        @monitor_thread = nil
      end

      attr_reader :read_end, :write_end, :monitor_thread
      private :read_end, :write_end, :monitor_thread

      def parent_post_fork
        read_end.close
      end

      def child_post_fork
        write_end.close
      end

      def on_exit(&block)
        monitor_thread = Thread.new do
          IO.select([read_end])
          block.call
        end
      end
    end
  end
end
