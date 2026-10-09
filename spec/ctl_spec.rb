# frozen_string_literal: true

require "racecar/ctl"

RSpec.describe Racecar::Ctl do
  describe "#produce" do
    let(:handle) { double("handle", wait: nil) }
    let(:rdkafka_producer) { double("rdkafka_producer", produce: handle) }

    before do
      allow(Racecar::RailsConfigFileLoader).to receive(:load!)
      allow($stderr).to receive(:puts)
      Racecar.config.message_timeout = 1
    end

    it "creates the producer through OAuthBearer" do
      provider = -> { {} }
      Racecar.config.oauth_token_provider = provider
      expect(Racecar::OAuthBearer).to receive(:create_client)
        .with(an_instance_of(Rdkafka::Config), :producer, provider)
        .and_return(rdkafka_producer)

      described_class.new("produce").produce(["--topic", "greetings", "--value", "hello"])

      expect(rdkafka_producer).to have_received(:produce).with(payload: "hello", key: nil, topic: "greetings")
    end
  end
end
