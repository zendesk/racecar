require "racecar/producer"
require "racecar/config"
require "racecar/message_delivery_error"
require "rdkafka"
require "rspec/mocks"

RSpec.describe Racecar::Producer do
  let(:config) { Racecar::Config.new() }
  let(:producer) { Racecar::Producer.new(config: config) }
  let(:topic) { "test_topic" }
  let(:value) { "test_message" }
  let(:delivery_handle) { instance_double("Rdkafka::Producer::DeliveryHandle") }

  let(:rdkafka_config) { double("Rdkafka::Config", producer: rdkafka_producer) }
  let(:rdkafka_producer) do
    double(
      "Rdkafka::Producer",
      :produce => delivery_handle,
      :delivery_callback= => nil,
      :close => nil
    )
  end

  before { allow(Rdkafka::Config).to receive(:new).and_return(rdkafka_config) }
  after { Racecar::Producer.reset! }

  describe "#initialize" do
    it "creates one shared rdkafka producer" do
      expect(Rdkafka::Config).to receive(:new).with(
        hash_including(
          "bootstrap.servers" => "localhost:9092",
          "client.id" => "racecar",
          "statistics.interval.ms" => 0,
          "message.timeout.ms" => 300000.0,
          "partitioner" => "consistent_random"
        )
      ).and_return(rdkafka_config).once
      expect(rdkafka_config).to receive(:producer).once
      3.times { Racecar::Producer.new(config: config) }
    end
  end

  describe ".shutdown!" do
    it "closes the rdkafka producer if it has been initialized" do
      producer
      expect(rdkafka_producer).to receive(:close)
      Racecar::Producer.shutdown!
    end

    it "does not raise an error if no producer was initialized" do
      expect(rdkafka_producer).to_not receive(:close)
      expect { Racecar::Producer.shutdown! }.to_not raise_error
    end
  end

  describe ".reset!" do
    it "closes the rdkafka producer" do
      producer
      expect(rdkafka_producer).to receive(:close)
      Racecar::Producer.reset!
    end

    it "recreates a new rdkafka producer after reset" do
      Racecar::Producer.new(config: config)
      expect(Rdkafka::Config).to receive(:new).and_return(rdkafka_config).once
      expect(rdkafka_config).to receive(:producer).once
      Racecar::Producer.reset!
      Racecar::Producer.new(config: config)
    end
  end

  describe "#produce_async" do
    it "sends the message without waiting for feedback or guarantees" do
      expect(producer.produce_async(value: value, topic: topic)).to be_nil
    end
  end

  describe "#produce_sync" do
    it "sends the message and waits for a delivery handle" do
      expect(delivery_handle).to receive(:wait)
      expect(producer.produce_sync(value: value, topic: topic)).to eq(nil)
    end
  end

  describe "#wait_for_delivery" do
    it "sends the message and waits for a delivery handle" do
      expect(delivery_handle).to receive(:wait).exactly(5).times

      producer.wait_for_delivery do
        5.times do |message|
          producer.produce_async(value: message.to_s, topic: topic)
        end
      end
    end
  end

  context "when producing message fails" do
    it "raises a message delivery error" do
      allow(delivery_handle).to receive(:wait).and_raise(Rdkafka::RdkafkaError.new(-18))
      expect {producer.produce_sync(value: value, topic: topic) }.to raise_error(Racecar::MessageDeliveryError)
    end
  end

  context "for producer config" do
    let(:config) do
      test_config = Racecar::Config.new()
      test_config.partitioner = "murmur2_random"
      test_config
    end

    it "sets the partitioner correctly on the internal producer" do
      expect(Rdkafka::Config).to receive(:new).with(
        hash_including("partitioner" => "murmur2_random")
      ).and_return(rdkafka_config).once
      expect(rdkafka_config).to receive(:producer).and_return(rdkafka_producer).once
      expect(producer.send(:internal_producer)).to eq(rdkafka_producer)
    end
  end
end
