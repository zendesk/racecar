# frozen_string_literal: true

require "ostruct"
require "racecar/config"

RSpec.describe Racecar::Config do
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

  it "requires `partitioner` to be a valid value" do
    expect {
      config.partitioner = :consistent
      config.partitioner = :consistent_random
      config.partitioner = :murmur2
      config.partitioner = :murmur2_random
      config.partitioner = :fnv1a
      config.partitioner = :fnv1a_random
      config.partitioner = "consistent"
      config.partitioner = "consistent_random"
      config.partitioner = "murmur2"
      config.partitioner = "murmur2_random"
      config.partitioner = "fnv1a"
      config.partitioner = "fnv1a_random"
    }.not_to raise_exception

    expect {
      config.partitioner = "abc"
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
      with_env("RACECAR_BROKERS", "hansel,gretel") do
        expect(config.brokers).to eq ["hansel", "gretel"]
      end
    end

    it "sets the client id from RACECAR_CLIENT_ID" do
      with_env('RACECAR_CLIENT_ID', 'witch') do
        expect(config.client_id).to eq "witch"
      end
    end

    it "sets the offset commit interval from RACECAR_OFFSET_COMMIT_INTERVAL" do
      with_env("RACECAR_OFFSET_COMMIT_INTERVAL", '45') do
        expect(config.offset_commit_interval).to eq 45
      end
    end

    it "sets the heartbeat interval from RACECAR_HEARTBEAT_INTERVAL" do
      with_env("RACECAR_HEARTBEAT_INTERVAL", "45") do
        expect(config.heartbeat_interval).to eq 45
      end
    end
  end

  describe "#load_consumer_class" do
    let(:consumer_class) {
      OpenStruct.new(group_id: nil, fetch_messages: nil, name: "DoStuffConsumer", subscriptions: [])
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

    it "sets the fetch_messages to the one defined on the consumer class" do
      consumer_class.fetch_messages = 10

      config.load_consumer_class(consumer_class)

      expect(config.fetch_messages).to eq 10
    end

    it "doesn't override existing values if the consumer hasn't specified anything" do
      consumer_class.max_wait_time = nil
      consumer_class.group_id = nil
      consumer_class.fetch_messages = nil

      config.max_wait_time = 10
      config.group_id = "cats"
      config.fetch_messages = 100
      config.load_consumer_class(consumer_class)

      expect(config.max_wait_time).to eq 10
      expect(config.group_id).to eq "cats"
      expect(config.fetch_messages).to eq 100
    end
  end

  describe '#max_wait_time_ms' do
    before { config.max_wait_time = 12 }

    it 'returns max_wait_time in milliseconds' do
      expect(config.max_wait_time_ms).to eq(12_000)
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

  describe "#statistics_interval_ms" do
    before do
      # If we set this for real, it can't then be unset (at least not without delving into
      # the guts of Rdkafka)
      allow(Rdkafka::Config).to receive(:statistics_callback).and_return(stats_callback)
    end

    context "when there is no statistics_callback defined in the rdkafka config" do
      let(:stats_callback) { nil }

      context "when statistics_interval is not set" do
        it "returns 0" do
          expect(config.statistics_interval_ms).to eq(0)
        end
      end

      context "when statistics_interval is set" do
        before { config.statistics_interval = 5 }

        it "returns 0" do
          expect(config.statistics_interval_ms).to eq(0)
        end
      end
    end

    context "when there is a statistics_callback defined in the rdkafka config" do
      let(:stats_callback) do
        ->(stats) { puts "Nice, a stats callback" }
      end

      context "when statistics_interval is not set" do
        it "returns 1000 by default" do
          expect(config.statistics_interval_ms).to eq(1000)
        end
      end

      context "when statistics_interval is set" do
        before { config.statistics_interval = 5 }

        it "returns the interval in ms" do
          expect(config.statistics_interval_ms).to eq(5000)
        end
      end
    end
  end

  describe "#instrumenter" do
    context "when ActiveSupport::Notifications is available" do
      before { require "active_support/notifications" }

      it "returns the 'real' instrumenter" do
        expect(config.instrumenter).to be_a(Racecar::Instrumenter)
      end
    end

    context "when ActiveSupport::Notifications is not available" do
      before { hide_const("ActiveSupport::Notifications") }

      it "returns the compatible null instrumenter singleton" do
        expect(config.instrumenter).to be(Racecar::NullInstrumenter)
      end

      it "warns that instrumentation is disabled" do
        expect(config.logger).to receive(:warn).with(/instrumentation is disabled/)

        config.instrumenter
      end
    end
  end

  describe "#install_liveness_probe" do
    it "delegates to LivenessProbe#install" do
      expect(config.liveness_probe).to receive(:install)

      config.install_liveness_probe
    end
  end

  describe "#liveness_probe" do
    it "returns the liveness probe" do
      expect(config.liveness_probe).to be_a(Racecar::LivenessProbe)
    end

    it "returns the same LivenessProbe instance" do
      expect(config.liveness_probe).to be(config.liveness_probe)
    end

    it "loads ActiveSupport::Notifications" do
      expect(config).to receive(:require).with("active_support/notifications").and_call_original

      config.liveness_probe
    end

    it "loads ActiveSupport::Notifications" do
      expect(config).to receive(:require).with("active_support/notifications").and_call_original

      config.liveness_probe
    end
  end
end
