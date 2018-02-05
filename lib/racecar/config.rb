require "king_konf"

module Racecar
  class Config < KingKonf::Config
    env_prefix :racecar

    desc "A list of Kafka brokers in the cluster that you're consuming from"
    list :brokers, default: ["localhost:9092"]

    desc "A string used to identify the client in logs and metrics"
    string :client_id, default: "racecar"

    desc "How frequently to commit offset positions"
    float :offset_commit_interval, default: 10

    desc "How many messages to process before forcing a checkpoint"
    integer :offset_commit_threshold, default: 0

    desc "How often to send a heartbeat message to Kafka"
    float :heartbeat_interval, default: 10

    desc "How long to pause a partition for if the consumer raises an exception while processing a message"
    float :pause_timeout, default: 10

    desc "The idle timeout after which a consumer is kicked out of the group"
    float :session_timeout, default: 30

    desc "How long to wait when trying to connect to a Kafka broker"
    float :connect_timeout, default: 10

    desc "How long to wait when trying to communicate with a Kafka broker"
    float :socket_timeout, default: 30

    desc "How long to allow the Kafka brokers to wait before returning messages"
    float :max_wait_time, default: 1

    desc "A prefix used when generating consumer group names"
    string :group_id_prefix

    desc "The group id to use for a given group of consumers"
    string :group_id

    desc "A filename that log messages should be written to"
    string :logfile

    desc "The log level for the Racecar logs"
    string :log_level

    desc "A valid SSL certificate authority"
    string :ssl_ca_cert

    desc "The path to a valid SSL certificate authority file"
    string :ssl_ca_cert_file_path

    desc "A valid SSL client certificate"
    string :ssl_client_cert

    desc "A valid SSL client certificate key"
    string :ssl_client_cert_key

    desc "The GSSAPI principal"
    string :sasl_gssapi_principal

    desc "Optional GSSAPI keytab"
    string :sasl_gssapi_keytab

    desc "The authorization identity to use"
    string :sasl_plain_authzid

    desc "The username used to authenticate"
    string :sasl_plain_username

    desc "The password used to authenticate"
    string :sasl_plain_password

    desc "The file in which to store the Racecar process' PID when daemonized"
    string :pidfile

    desc "Run the Racecar process in the background as a daemon"
    boolean :daemonize, default: false

    desc "Enable Datadog metrics"
    boolean :datadog_enabled, default: false

    desc "The host running the Datadog agent"
    string :datadog_host

    desc "The port of the Datadog agent"
    integer :datadog_port

    desc "The namespace to use for Datadog metrics"
    string :datadog_namespace

    desc "Tags that should always be set on Datadog metrics"
    list :datadog_tags

    # The error handler must be set directly on the object.
    attr_reader :error_handler

    attr_accessor :subscriptions

    def initialize(env: ENV)
      super(env: env)
      @error_handler = proc {}
      @subscriptions = []
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
      self.pidfile ||= "#{group_id}.pid"
    end

    def on_error(&handler)
      @error_handler = handler
    end
  end
end
