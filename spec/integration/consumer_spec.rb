require "securerandom"
require "racecar/cli"
require "racecar/ctl"

class EchoConsumer < Racecar::Consumer
  subscribes_to "input"

  self.group_id = "test-consumer-#{SecureRandom.hex(8)}"

  def process(message)
    produce message.value, key: message.key, topic: "output"
  end
end

module IntegrationSupport
  INCOMING_MESSAGES = []

  CONSUMER = Thread.new do
    consumer = Rdkafka::Config.new({
      "bootstrap.servers": Racecar.config.brokers.join(","),
      "client.id":         Racecar.config.client_id,
      "group.id":          "racecar-tests",
    }.merge(Racecar.config.rdkafka_consumer)).consumer

    consumer.subscribe("output")

    consumer.each do |message|
      puts "Received message #{message}"
      INCOMING_MESSAGES << message
    end
  end

  def self.incoming_messages
    INCOMING_MESSAGES
  end
end

RSpec.describe "running a Racecar consumer" do
  it "works" do
    worker = Thread.new do
      cli = Racecar::Cli.new(["EchoConsumer"])
      cli.run
    end

    ctl = Racecar::Ctl.main %w(
      produce -t input -v hello -k greetings
    )

    message = nil
    attempt = 1

    while message.nil? && attempt <= 10
      puts "Waiting for message..."
      sleep 2 ** attempt
      message = IntegrationSupport.incoming_messages.last
      attempt += 1
    end

    expect(message).not_to be_nil
    expect(message.topic).to eq "output"
    expect(message.payload).to eq "hello"
    expect(message.key).to eq "greetings"
  end
end
