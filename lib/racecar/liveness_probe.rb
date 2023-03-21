require "fileutils"

module Racecar
  class LivenessProbe
    def initialize(message_bus, file_path, max_interval)
      @message_bus = message_bus
      @file_path = file_path
      @max_interval = max_interval
      @subscribers = []
    end

    attr_reader :message_bus, :file_path, :max_interval, :subscribers
    private     :message_bus, :file_path, :max_interval, :subscribers

    def check_liveness_within_interval!
      unless liveness_event_within_interval?
        $stderr.puts "Racecar healthcheck failed: No liveness within interval #{max_interval}s. Last liveness at #{last_liveness_event_at}, #{elapsed_since_liveness_event} seconds ago."
        Process.exit(1)
      end
    end

    def liveness_event_within_interval?
      elapsed_since_liveness_event < max_interval
    rescue Errno::ENOENT
      $stderr.puts "Racecar healthcheck failed: Liveness file not found `#{file_path}`"
      Process.exit(1)
    end

    def install
      unless file_path && file_writeable?
        raise(
          "Liveness probe configuration error: `liveness_probe_file_path` must be set to a writable file path.\n" \
            "  Set `RACECAR_LIVENESS_PROBE_FILE_PATH` and `RACECAR_LIVENESS_MAX_INTERVAL` environment variables."
        )
      end

      subscribers << message_bus.subscribe("start_main_loop.racecar") do
        touch_liveness_file
      end

      subscribers = message_bus.subscribe("shut_down.racecar") do
        delete_liveness_file
      end

      nil
    end

    def uninstall
      subscribers.each { |s| message_bus.unsubscribe(s) }
    end

    private

    def elapsed_since_liveness_event
      Time.now - last_liveness_event_at
    end

    def last_liveness_event_at
      File.mtime(file_path)
    end

    def touch_liveness_file
      FileUtils.touch(file_path)
    end

    def delete_liveness_file
      FileUtils.rm_rf(file_path)
    end

    def file_writeable?
      File.write(file_path, "")
      File.unlink(file_path)
      true
    rescue
      false
    end
  end
end
