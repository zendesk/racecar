require "ostruct"
require "racecar/config"

describe Racecar::Config do
  let(:config) { Racecar::Config.new }

  it "uses the default config if no explicit value has been set" do
    expect(config.offset_commit_interval).to eq 10
  end

  it "requires `sasl_mechanism` to be a valid mechanism" do
    expect {
      config.sasl_mechanism = "GSSAPI"
      config.sasl_mechanism = "PLAIN"
      config.sasl_mechanism = "SCRAM-SHA-256"
      config.sasl_mechanism = "SCRAM-SHA-512"
    }.not_to raise_exception

    expect {
      config.sasl_mechanism = "banana"
    }.to raise_exception(KingKonf::ConfigError)
  end

  it "requires `producer_compression_codec` to be a valid" do
    expect {
      config.producer_compression_codec = :none
      config.producer_compression_codec = :gzip
      config.producer_compression_codec = :snappy
      config.producer_compression_codec = :lz4
      config.producer_compression_codec = "none"
      config.producer_compression_codec = "gzip"
      config.producer_compression_codec = "snappy"
      config.producer_compression_codec = "lz4"
    }.not_to raise_exception

    expect {
      config.producer_compression_codec = "apple"
    }.to raise_exception(KingKonf::ConfigError)
  end

  it "requires `security_protocol` to be a valid" do
    expect {
      config.security_protocol = :plaintext
      config.security_protocol = :ssl
      config.security_protocol = :sasl_plaintext
      config.security_protocol = :sasl_ssl
      config.security_protocol = "plaintext"
      config.security_protocol = "ssl"
      config.security_protocol = "sasl_plaintext"
      config.security_protocol = "sasl_ssl"
    }.not_to raise_exception

    expect {
      config.security_protocol = "peach"
    }.to raise_exception(KingKonf::ConfigError)
  end

  describe "#load_env" do
    it "sets the brokers from RACECAR_BROKERS" do
      ENV["RACECAR_BROKERS"] = "hansel,gretel"

      expect(config.brokers).to eq ["hansel", "gretel"]
    end

    it "sets the client id from RACECAR_CLIENT_ID" do
      ENV["RACECAR_CLIENT_ID"] = "witch"

      expect(config.client_id).to eq "witch"
    end

    it "sets the offset commit interval from RACECAR_OFFSET_COMMIT_INTERVAL" do
      ENV["RACECAR_OFFSET_COMMIT_INTERVAL"] = "45"

      expect(config.offset_commit_interval).to eq 45
    end

    it "sets the heartbeat interval from RACECAR_HEARTBEAT_INTERVAL" do
      ENV["RACECAR_HEARTBEAT_INTERVAL"] = "45"

      expect(config.heartbeat_interval).to eq 45
    end
  end

  describe "#load_consumer_class" do
    let(:consumer_class) {
      OpenStruct.new(group_id: nil, name: "DoStuffConsumer", subscriptions: [])
    }

    it "sets the group id if one has been explicitly defined" do
      consumer_class.group_id = "fiddle"

      config.load_consumer_class(consumer_class)

      expect(config.group_id).to eq "fiddle"
    end

    it "defaults the group id to a dasherized version of the class name with the prefix" do
      config.group_id_prefix = "my-app."

      config.load_consumer_class(consumer_class)

      expect(config.group_id).to eq "my-app.do-stuff-consumer"
    end

    it "sets the subscriptions to the ones defined on the consumer class" do
      consumer_class.subscriptions = ["one", "two"]

      config.load_consumer_class(consumer_class)

      expect(config.subscriptions).to eq ["one", "two"]
    end

    it "doesn't override existing values if the consumer hasn't specified anything" do
      consumer_class.max_wait_time = nil
      consumer_class.group_id = nil

      config.max_wait_time = 10
      config.group_id = "cats"
      config.load_consumer_class(consumer_class)

      expect(config.max_wait_time).to eq 10
      expect(config.group_id).to eq "cats"
    end
  end

  describe "#validate!" do
    before do
      config.brokers = ["a"]
      config.client_id = "x"
    end

    it "raises an exception if no brokers have been configured" do
      expect { config.validate! }.not_to raise_exception

      config.brokers = []

      expect {
        config.validate!
      }.to raise_exception(Racecar::ConfigError, "`brokers` must not be empty")
    end

    it "raises an exception if max_wait_time is greater than socket_timeout" do
      config.socket_timeout = 10
      config.max_wait_time = 11

      expect {
        config.validate!
      }.to raise_exception(Racecar::ConfigError, "`socket_timeout` must be longer than `max_wait_time`")
    end

    it "raises an exception if max_pause_timeout is set but pause_with_exponential_backoff is disabled" do
      config.max_pause_timeout = 30
      config.pause_with_exponential_backoff = false

      expect {
        config.validate!
      }.to raise_exception(Racecar::ConfigError, "`max_pause_timeout` only makes sense when `pause_with_exponential_backoff` is enabled")
    end
  end

  describe "#inspect" do
    it "returns an easy-to-read list of the configuration keys and values" do
      config.client_id = "elvis"

      expect(config.inspect.split("\n")).to include %(client_id = "elvis")
    end
  end
end
