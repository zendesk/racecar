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

    desc "How often to send a heartbeat message to Kafka"
    float :heartbeat_interval, default: 10

    desc "The minimum number of messages in the local consumer queue"
    integer :min_message_queue_size, default: 2000

    desc "Kafka consumer configuration options, separated with '=' -- https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md"
    list :consumer, default: []

    desc "Kafka producer configuration options, separated with '=' -- https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md"
    list :producer, default: []

    desc "The maxium number of messages that get consumed within one batch"
    integer :fetch_messages, default: 1000

    desc "Automatically store offset of last message provided to application"
    boolean :synchronous_commits, default: false

    desc "How long to pause a partition for if the consumer raises an exception while processing a message -- set to -1 to pause indefinitely"
    float :pause_timeout, default: 10

    desc "When `pause_timeout` and `pause_with_exponential_backoff` are configured, this sets an upper limit on the pause duration"
    float :max_pause_timeout, default: nil

    desc "Whether to exponentially increase the pause timeout on successive errors -- the timeout is doubled each time"
    boolean :pause_with_exponential_backoff, default: false

    desc "The idle timeout after which a consumer is kicked out of the group"
    float :session_timeout, default: 30

    desc "The maximum time between two message fetches before the consumer is kicked out of the group (in seconds)"
    integer :max_poll_interval, default: 5*60

    desc "How long to wait when trying to communicate with a Kafka broker"
    float :socket_timeout, default: 30

    desc "How long to allow the Kafka brokers to wait before returning messages"
    float :max_wait_time, default: 1

    desc "Maximum amount of data the broker shall return for a Fetch request"
    integer :max_bytes, default: 10485760

    desc "A prefix used when generating consumer group names"
    string :group_id_prefix

    desc "The group id to use for a given group of consumers"
    string :group_id

    desc "A filename that log messages should be written to"
    string :logfile

    desc "The log level for the Racecar logs"
    string :log_level, default: "info"

    desc "Protocol used to communicate with brokers"
    symbol :security_protocol, allowed_values: %i{plaintext ssl sasl_plaintext sasl_ssl}

    desc "File or directory path to CA certificate(s) for verifying the broker's key"
    string :ssl_ca_location

    desc "Path to CRL for verifying broker's certificate validity"
    string :ssl_crl_location

    desc "Path to client's keystore (PKCS#12) used for authentication"
    string :ssl_keystore_location

    desc "Client's keystore (PKCS#12) password"
    string :ssl_keystore_password

    desc "SASL mechanism to use for authentication"
    string :sasl_mechanism, allowed_values: %w{GSSAPI PLAIN SCRAM-SHA-256 SCRAM-SHA-512}

    desc "Kerberos principal name that Kafka runs as, not including /hostname@REALM"
    string :sasl_kerberos_service_name

    desc "This client's Kerberos principal name"
    string :sasl_kerberos_principal

    desc "Full kerberos kinit command string, %{config.prop.name} is replaced by corresponding config object value, %{broker.name} returns the broker's hostname"
    string :sasl_kerberos_kinit_cmd

    desc "Path to Kerberos keytab file. Uses system default if not set"
    string :sasl_kerberos_keytab

    desc "Minimum time in milliseconds between key refresh attempts"
    integer :sasl_kerberos_min_time_before_relogin

    desc "SASL username for use with the PLAIN and SASL-SCRAM-.. mechanism"
    string :sasl_username

    desc "SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism"
    string :sasl_password

    desc "Whether to use SASL over SSL."
    boolean :sasl_over_ssl, default: true

    desc "The file in which to store the Racecar process' PID when daemonized"
    string :pidfile

    desc "Run the Racecar process in the background as a daemon"
    boolean :daemonize, default: false

    desc "The codec used to compress messages with"
    symbol :producer_compression_codec, allowed_values: %i{none lz4 snappy gzip}

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

    attr_accessor :subscriptions, :logger

    def initialize(env: ENV)
      super(env: env)
      @error_handler = proc {}
      @subscriptions = []
      @logger = Logger.new(STDOUT)
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

      if max_pause_timeout && !pause_with_exponential_backoff?
        raise ConfigError, "`max_pause_timeout` only makes sense when `pause_with_exponential_backoff` is enabled"
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

    def rdkafka_consumer
      consumer_config = consumer.map do |param|
        param.split("=", 2).map(&:strip)
      end.to_h
      consumer_config.merge!(rdkafka_security_config)
      consumer_config
    end

    def rdkafka_producer
      producer_config = producer.map do |param|
        param.split("=", 2).map(&:strip)
      end.to_h
      producer_config.merge!(rdkafka_security_config)
      producer_config
    end

    private

    def rdkafka_security_config
      {
        "security.protocol" => security_protocol,
        "ssl.ca.location" => ssl_ca_location,
        "ssl.crl.location" => ssl_crl_location,
        "ssl.keystore.location" => ssl_keystore_location,
        "ssl.keystore.password" => ssl_keystore_password,
        "sasl.mechanism" => sasl_mechanism,
        "sasl.kerberos.service.name" => sasl_kerberos_service_name,
        "sasl.kerberos.principal" => sasl_kerberos_principal,
        "sasl.kerberos.kinit.cmd" => sasl_kerberos_kinit_cmd,
        "sasl.kerberos.keytab" => sasl_kerberos_keytab,
        "sasl.kerberos.min.time.before.relogin" => sasl_kerberos_min_time_before_relogin,
        "sasl.username" => sasl_username,
        "sasl.password" => sasl_password,
      }.compact
    end
  end
end
