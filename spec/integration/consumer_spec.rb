# frozen_string_literal: true

require "securerandom"
require "racecar/cli"
require "racecar/ctl"

def generate_token
  SecureRandom.hex(8)
end

INPUT_TOPIC = "input-#{generate_token}"
OUTPUT_TOPIC = "output-#{generate_token}"

class EchoConsumer < Racecar::Consumer
  subscribes_to INPUT_TOPIC

  self.group_id = "echo-consumer-#{generate_token}"

  def process(message)
    produce message.value, key: message.key, topic: "output"
  end
end

class ResultsConsumer < Racecar::Consumer
  subscribes_to OUTPUT_TOPIC

  self.group_id = "results-consumer-#{generate_token}"

  MESSAGES = []

  def process(message)
    puts "Received token #{message.value} in #{message.inspect}"
    MESSAGES << message
  end
end

RSpec.context "Integrating with a real Kafka cluster" do
  before :all do
    Thread.new do
      Racecar::Cli.main(["ResultsConsumer"])
    end
  end

  after do
    ResultsConsumer::MESSAGES.clear
  end

  describe "Single-consumer groups" do
    it "can consume and produce messages" do
      token = generate_token

      worker = Thread.new do
        Racecar::Cli.main(["EchoConsumer"])
      end

      produce_token(token)

      message = wait_for_token(token)

      expect(message).not_to be_nil
      expect(message.value).to eq token
      expect(message.key).to eq "greetings"
    end
  end

  describe "Multi-consumer groups" do
    it "can consume and produce messages" do
      token = generate_token

      worker1 = Thread.new do
        Racecar::Cli.main(["EchoConsumer"])
      end

      worker2 = Thread.new do
        Racecar::Cli.main(["EchoConsumer"])
      end

      worker3 = Thread.new do
        Racecar::Cli.main(["EchoConsumer"])
      end

      Racecar::Ctl.main %W(
        produce -t #{INPUT_TOPIC} -v #{token} -k greetings
      )

      message = wait_for_token(token)

      expect(message).not_to be_nil
      expect(message.value).to eq token
      expect(message.key).to eq "greetings"
    end
  end

  def wait_for_token(token)
    message = nil
    attempt = 1

    while message.nil? && attempt <= 3
      puts "Waiting for token #{token}..."
      sleep 2 ** attempt
      message = ResultsConsumer::MESSAGES.find {|m| m.value == token }
      attempt += 1
    end

    message
  end

  def produce_token(token)
    puts "Writing token #{token} to #{INPUT_TOPIC}"

    Racecar::Ctl.main %W(
      produce -t #{INPUT_TOPIC} -v #{token} -k greetings
    )
  end
end
