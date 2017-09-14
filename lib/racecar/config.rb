require "king_konf"

module Racecar
  class Config < KingKonf::Config
    prefix :racecar

    list :brokers, default: ["localhost:9092"]
    string :client_id, default: "racecar"

    list :subscriptions, default: []

    # Default is to commit offsets every 10 seconds.
    integer :offset_commit_interval, default: 10

    # Default is no buffer threshold trigger.
    integer :offset_commit_threshold, default: 0

    # Default is to send a heartbeat every 10 seconds.
    integer :heartbeat_interval, default: 10

    # Default is to pause partitions for 10 seconds on processing errors.
    integer :pause_timeout, default: 10

    # Default is to kick consumers out of a group after 30 seconds without activity.
    integer :session_timeout, default: 30

    # Default is to allow at most 10 seconds when connecting to a broker.
    integer :connect_timeout, default: 10

    # Default is to allow at most 30 seconds when reading or writing to
    # a broker socket.
    integer :socket_timeout, default: 30

    # Default is to allow the brokers up to 5 seconds before returning
    # messages.
    integer :max_wait_time, default: 5

    string :group_id_prefix
    string :group_id

    string :logfile

    string :ssl_ca_cert
    string :ssl_ca_cert_file_path
    string :ssl_client_cert
    string :ssl_client_cert_key

    string :sasl_gssapi_principal
    string :sasl_gssapi_keytab
    string :sasl_plain_authzid
    string :sasl_plain_username

    # The error handler must be set directly on the object.
    attr_reader :error_handler

    def initialize(env: ENV)
      super(env: env)
      @error_handler = proc {}
    end

    def inspect
      self.class.variables
        .map(&:name)
        .map {|key| [key, get(key).inspect].join(" = ") }
        .join("\n")
    end

    def validate!
      if brokers.empty?
        raise ConfigError, "`brokers` must not be empty"
      end

      if socket_timeout <= max_wait_time
        raise ConfigError, "`socket_timeout` must be longer than `max_wait_time`"
      end

      if connect_timeout <= max_wait_time
        raise ConfigError, "`connect_timeout` must be longer than `max_wait_time`"
      end
    end

    def load_consumer_class(consumer_class)
      self.group_id = consumer_class.group_id || self.group_id

      self.group_id ||= [
        # Configurable and optional prefix:
        group_id_prefix,

        # MyFunnyConsumer => my-funny-consumer
        consumer_class.name.gsub(/[a-z][A-Z]/) {|str| str[0] << "-" << str[1] }.downcase,
      ].compact.join("")

      self.subscriptions = consumer_class.subscriptions
      self.max_wait_time = consumer_class.max_wait_time || self.max_wait_time
    end

    def on_error(&handler)
      @error_handler = handler
    end
  end
end
