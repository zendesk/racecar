require "erb"
require "yaml"
require "racecar/env_loader"

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
      group_id
      subscriptions
      error_handler
      max_wait_time
      log_to_stdout
    )

    REQUIRED_KEYS = %w(
      brokers
      client_id
    )

    DEFAULT_CONFIG = {
      brokers: ["localhost:9092"],
      client_id: "racecar",
      group_id_prefix: nil,

      subscriptions: [],

      # Default is to commit offsets every 10 seconds.
      offset_commit_interval: 10,

      # Default is no buffer threshold trigger.
      offset_commit_threshold: 0,

      # Default is to send a heartbeat every 10 seconds.
      heartbeat_interval: 10,

      # Default is to not pause partitions on processing errors.
      pause_timeout: 0,

      # Default is to allow at most 10 seconds when connecting to a broker.
      connect_timeout: 10,

      # Default is to allow at most 30 seconds when reading or writing to
      # a broker socket.
      socket_timeout: 30,

      # Default is to allow the brokers up to 5 seconds before returning
      # messages.
      max_wait_time: 5,

      # Default is to do nothing on exceptions.
      error_handler: proc {},

      # Default is to only log to the logger.
      log_to_stdout: false,
    }

    attr_accessor(*ALLOWED_KEYS)

    def initialize
      load(DEFAULT_CONFIG)
      load_env!
    end

    def validate!
      REQUIRED_KEYS.each do |key|
        if send(key).nil?
          raise ConfigError, "required configuration key `#{key}` not defined"
        end
      end

      if socket_timeout <= max_wait_time
        raise ConfigError, "`socket_timeout` must be longer than `max_wait_time`"
      end

      if connect_timeout <= max_wait_time
        raise ConfigError, "`connect_timeout` must be longer than `max_wait_time`"
      end
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

    def set(key, value)
      unless ALLOWED_KEYS.include?(key.to_s)
        raise ConfigError, "unknown configuration key `#{key}`"
      end

      instance_variable_set("@#{key}", value)
    end

    def load(data)
      data.each do |key, value|
        set(key, value)
      end
    end

    def load_consumer_class(consumer_class)
      @group_id = consumer_class.group_id || @group_id

      @group_id ||= [
        # Configurable and optional prefix:
        group_id_prefix,

        # MyFunnyConsumer => my-funny-consumer
        consumer_class.name.gsub(/[a-z][A-Z]/) {|str| str[0] << "-" << str[1] }.downcase,
      ].compact.join("")

      @subscriptions = consumer_class.subscriptions
      @max_wait_time = consumer_class.max_wait_time || @max_wait_time
    end

    def on_error(&handler)
      @error_handler = handler
    end

    private

    def load_env!
      loader = EnvLoader.new(ENV, self)

      loader.string_list(:brokers)
      loader.string(:client_id)
      loader.string(:group_id_prefix)
      loader.string(:group_id)
      loader.integer(:offset_commit_interval)
      loader.integer(:offset_commit_threshold)
      loader.integer(:heartbeat_interval)
      loader.integer(:pause_timeout)
      loader.integer(:connect_timeout)
      loader.integer(:socket_timeout)
      loader.integer(:max_wait_time)
    end
  end
end
