# frozen_string_literal: true

module Racecar
  class Daemon
    attr_reader :pidfile

    def initialize(pidfile)
      raise Racecar::Error, "No pidfile specified" if pidfile.nil?

      @pidfile = pidfile
    end

    def pidfile?
      !pidfile.nil?
    end

    def daemonize!
      exit if fork
      Process.setsid
      exit if fork
      Dir.chdir "/"
    end

    def redirect_output(logfile)
      FileUtils.mkdir_p(File.dirname(logfile), mode: 0755)
      FileUtils.touch(logfile)

      File.chmod(0644, logfile)

      $stderr.reopen(logfile, 'a')
      $stdout.reopen($stderr)
      $stdout.sync = $stderr.sync = true
    end

    def suppress_input
      $stdin.reopen('/dev/null')
    end

    def suppress_output
      $stderr.reopen('/dev/null', 'a')
      $stdout.reopen($stderr)
    end

    def check_pid
      if pidfile?
        case pid_status
        when :running, :not_owned
          $stderr.puts "=> Racecar is already running with that PID file (#{pidfile})"
          exit(1)
        when :dead
          File.delete(pidfile)
        end
      end
    end

    def pid
      if File.exists?(pidfile)
        File.read(pidfile).to_i
      else
        nil
      end
    end

    def running?
      pid_status == :running || pid_status == :not_owned
    end

    def stop!
      Process.kill("TERM", pid)
    end

    def pid_status
      return :exited if pid.nil?
      return :dead if pid == 0

      # This will raise Errno::ESRCH if the process doesn't exist.
      Process.kill(0, pid)

      :running
    rescue Errno::ESRCH
      :dead
    rescue Errno::EPERM
      :not_owned
    end

    def write_pid
      File.open(pidfile, ::File::CREAT | ::File::EXCL | ::File::WRONLY) do |f|
        f.write(Process.pid.to_s)
      end

      at_exit do
        File.delete(pidfile) if File.exists?(pidfile)
      end
    rescue Errno::EEXIST
      check_pid
      retry
    end
  end
end
