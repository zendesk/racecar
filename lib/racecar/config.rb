require "erb"
require "yaml"

module Racecar
  class Config
    ALLOWED_KEYS = %w(
      brokers
      client_id
      offset_commit_interval
      offset_commit_threshold
      heartbeat_interval
      pause_timeout
      connect_timeout
      socket_timeout
      group_id_prefix
      error_handler
      default_max_wait_time
      log_to_stdout
    )

    DEFAULT_CONFIG = {
      client_id: "racecar",
      group_id_prefix: nil,

      # Default is to commit offsets every 10 seconds.
      offset_commit_interval: 10,

      # Default is no buffer threshold trigger.
      offset_commit_threshold: 0,

      # Default is to send a heartbeat every 10 seconds.
      heartbeat_interval: 10,

      # Default is to not pause partitions on processing errors.
      pause_timeout: 0,

      connect_timeout: nil,
      socket_timeout: nil,
      error_handler: proc {},
      default_max_wait_time: 5,
      log_to_stdout: false,
    }

    attr_reader(*ALLOWED_KEYS)

    def initialize
      load(DEFAULT_CONFIG)
      load_env!
    end

    def load_file(path, environment)
      # First, load the ERB template from disk.
      template = ERB.new(File.new(path).read)

      # The last argument to `safe_load` allows us to use aliasing to share
      # configuration between environments.
      processed = YAML.safe_load(template.result(binding), [], [], true)

      data = processed.fetch(environment)

      load(data)
    end

    def load(data)
      data.each do |key, value|
        unless ALLOWED_KEYS.include?(key.to_s)
          raise "unknown configuration key `#{key}`"
        end

        instance_variable_set("@#{key}", value)
      end
    end

    def on_error(&handler)
      @error_handler = handler
    end

    private

    def load_env!
      if ENV.key?("RACECAR_BROKERS")
        @brokers = ENV["RACECAR_BROKERS"].split(",")
      end
    end
  end
end
