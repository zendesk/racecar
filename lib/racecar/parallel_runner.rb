# frozen_string_literal: true

module Racecar
  class ParallelRunner
    Worker = Struct.new(:pid, :parent_reader)

    SHUTDOWN_SIGNALS = ["INT", "QUIT", "TERM"]

    def initialize(runner:, config:, logger:)
      @runner = runner
      @config = config
      @logger = logger
    end

    def worker_pids
      workers.map(&:pid)
    end

    def running?
      @running
    end

    def run
      logger.info "=> Running with #{config.parallel_workers} parallel workers"

      self.workers = config.parallel_workers.times.map do
        run_worker.tap { |w| logger.info "=> Forked new Racecar consumer with process id #{w.pid}" }
      end

      # Print the consumer config to STDERR on USR1.
      trap("USR1") { $stderr.puts config.inspect }

      SHUTDOWN_SIGNALS.each { |signal| trap(signal) { terminate_workers } }

      @running = true

      wait_for_exit
    end

    private

    attr_accessor :workers
    attr_reader :runner, :config, :logger

    def run_worker
      parent_reader, child_writer = IO.pipe

      pid = fork do
        begin
          parent_reader.close

          runner.run
        rescue Exception => e
          # Allow the parent process to re-raise the exception after shutdown
          child_writer.binmode
          child_writer.write(Marshal.dump(e))
        ensure
          child_writer.close
        end
      end

      child_writer.close

      Worker.new(pid, parent_reader)
    end

    def terminate_workers
      return if @terminating

      @terminating = true
      $stderr.puts "=> Terminating workers"

      Process.kill("TERM", *workers.map(&:pid))
    end

    def wait_for_exit
      # The call to IO.select blocks until one or more of our readers are ready for reading,
      # which could be for one of two reasons:
      #
      # - An exception is raised in the child process, in which case we should initiate
      #   a shutdown;
      #
      # - A graceful shutdown was already initiated, and the pipe writer has been closed, in
      #   which case there is nothing more to do.
      #
      # - One of the child processes was killed somehow. If this turns out to be too strict
      #   (i.e. closing down all the workers, we could revisit and look at restarting dead
      #   workers.
      #
      ready_readers = IO.select(workers.map(&:parent_reader)).first

      first_read = ready_readers.first.read

      terminate_workers

      workers.map(&:pid).each do |pid|
        logger.debug "=> Waiting for worker with pid #{pid} to exit"
        Process.waitpid(pid)
        logger.debug "=> Worker with pid #{pid} shutdown"
      end

      exception_found = !first_read.empty?
      raise Marshal.load(first_read) if exception_found
    end
  end
end
