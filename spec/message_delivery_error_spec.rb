# frozen_string_literal: true

RSpec.describe Racecar::MessageDeliveryError do
  let(:rdkafka_msg_timed_out) { Rdkafka::RdkafkaError.new(-192) }
  let(:rdkafka_unknown_topic_or_part) { Rdkafka::RdkafkaError.new(3) }
  let(:rdkafka_delivery_handle) do
    Rdkafka::Producer::DeliveryHandle.new.tap do |dh|
      dh[:partition] = 37
      dh[:offset] = 42
      dh[:topic_name] = FFI::MemoryPointer.from_string("produce_test_topic")
    end
  end

  it "passes through error code" do
    error = described_class.new(rdkafka_msg_timed_out, rdkafka_delivery_handle)
    expect(error.code).to eq rdkafka_msg_timed_out.code
  end

  it "includes partition of delivery handle" do
    error = described_class.new(rdkafka_unknown_topic_or_part, rdkafka_delivery_handle)
    expect(error.to_s).to include "(37)"
  end

  it "handles delivery handle being nil" do
    described_class.new(rdkafka_unknown_topic_or_part, nil).to_s
  end
end
