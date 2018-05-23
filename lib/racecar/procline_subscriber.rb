require "active_support/notifications"

module Racecar
  # Subscribes to ruby-kafka instrumentation events and updates the process' title
  # to display the consumer status.
  class ProclineSubscriber
    PATTERN = /\w+\.consumer\.kafka/

    def self.setup!
      subscriber = new
      ActiveSupport::Notifications.subscribe(PATTERN, subscriber)
    end

    # ActiveSupport 4+ calls this on event start.
    def start(name, _id, payload)
      delegate(:start, name, payload)
    end

    # ActiveSupport 4+ calls this on event finish.
    def finish(name, _id, payload)
      delegate(:finish, name, payload)
    end

    def start_join_group(payload)
      procline! "joining group"
    end

    def finish_join_group(payload)
      exception = payload[:exception]

      if exception.nil?
        procline! "joined group"
      else
        procline! "failed to join group"
      end
    end

    def start_sync_group(payload)
      procline! "syncing group"
    end

    def finish_sync_group(payload)
      exception = payload[:exception]

      if exception.nil?
        procline! "synced group"
      else
        procline! "failed to sync group"
      end
    end

    def start_leave_group(payload)
      procline! "leaving group"
    end

    def finish_leave_group(payload)
      exception = payload[:exception]

      if exception.nil?
        procline! "left group"
      else
        procline! "error when leaving group"
      end
    end

    def start_loop(payload)
      procline! "processing"
    end

    def finish_loop(payload)
      procline! "stopped processing"
    end

    private

    def delegate(hook, name, payload)
      event_name = name.split(".").first
      method_name = "#{hook}_#{event_name}"
      send(method_name, payload) if respond_to?(method_name)
    end

    def procline!(status)
      Process.setproctitle("racecar (#{status})")
    end
  end
end
