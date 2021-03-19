# frozen_string_literal: true

require "racecar/message_delivery_handle"

RSpec.describe Racecar::MessageDeliveryError do
  let(:rdkafka_msg_timed_out) { Rdkafka::RdkafkaError.new(-192) }
  let(:rdkafka_unknown_topic_or_part) { Rdkafka::RdkafkaError.new(3) }
  let(:rdkafka_delivery_handle) do
    Rdkafka::Producer::DeliveryHandle.new.tap do |dh|
      dh[:partition] = 37
      dh[:offset] = 42
    end
  end
  let(:racecar_delivery_handle) do
    Racecar::MessageDeliveryHandle.new(
      rdkafka_delivery_handle,
      topic: "a_topic",
      key: "a_key",
      partition_key: "a_partition_key",
      timestamp: "a_timestamp",
      headers: "some_headers"
    )
  end

  it "passes through error code" do
    error = described_class.new(rdkafka_msg_timed_out, racecar_delivery_handle)
    expect(error.code).to eq rdkafka_msg_timed_out.code
  end

  it "includes partition of delivery handle" do
    error = described_class.new(rdkafka_unknown_topic_or_part, racecar_delivery_handle)
    expect(error.to_s).to include "(37)"
  end

  it "includes topic of delivery handle" do
    error = described_class.new(rdkafka_unknown_topic_or_part, racecar_delivery_handle)
    expect(error.to_s).to include "a_topic"
  end
end
