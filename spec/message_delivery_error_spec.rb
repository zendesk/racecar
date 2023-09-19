# frozen_string_literal: true

RSpec.describe Racecar::MessageDeliveryError do
  let(:rdkafka_msg_timed_out) { Rdkafka::RdkafkaError.new(-192) }
  let(:rdkafka_unknown_topic_or_part) { Rdkafka::RdkafkaError.new(3) }
  let(:rdkafka_delivery_handle) do
    instance_double(Rdkafka::Producer::DeliveryHandle).tap { |mock|
      allow(mock).to receive_message_chain(:create_result, :partition).and_return(37)
    }
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
