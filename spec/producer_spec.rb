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

  before do
    allow(producer).to receive(:internal_producer).and_return(double("Rdkafka::Producer", :produce => delivery_handle))
  end

  after do
    Racecar::Producer.shutdown!
    Racecar::Producer.class_variable_set(:@@init_internal_producer, nil)
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

    it "sets the partitioner corretly" do
      internal_producer = producer.instance_variable_get("@internal_producer")
      expect(internal_producer.instance_variable_get("@partitioner_name")).to eq("murmur2_random")
    end
  end
end
