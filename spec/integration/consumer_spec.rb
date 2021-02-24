# frozen_string_literal: true

require "securerandom"
require "racecar/cli"
require "racecar/ctl"

class NoSubsConsumer < Racecar::Consumer
  def process(message); end
end

class NoProcessConsumer < Racecar::Consumer
  subscribes_to "some-topic"
end

RSpec.describe "running a Racecar consumer", type: :integration do
  context "when an error occurs trying to start the runner" do
    context "when there are no subscriptions" do
      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there is no process method" do
      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoProcessConsumer"]).run
        end.to raise_error(NotImplementedError)
      end
    end
  end

  context "when the runner starts successfully" do
    let(:input_topic) { generate_input_topic_name }
    let(:output_topic) { generate_output_topic_name }
    let(:mock_echo_consumer_class) do
      Class.new(Racecar::Consumer) do
        class << self
          attr_accessor :output_topic
        end

        def process(message)
          produce message.value, key: message.key, topic: self.class.output_topic
          deliver!
        end
      end
    end

    let(:run_in_background!) do
      Thread.new do
        @runner_pid = Process.pid
        Thread.current.abort_on_exception = true
        Racecar::Cli.new([consumer_class.name.to_s]).run
      end
    end

    before do
      create_topic(topic: input_topic, partitions: topic_partitions)

      consumer_class.subscribes_to(input_topic)
      consumer_class.output_topic = output_topic

      rdkafka_consumer.subscribe(output_topic)

      publish_messages!(input_topic, input_messages)

      run_in_background!
    end

    after { Process.kill("INT", @runner_pid) }

    after(:all) { delete_all_test_topics }

    context "for a single threaded consumer" do
      let(:consumer_class) do
        class EchoConsumer1 < mock_echo_consumer_class
          self.group_id = "echo-consumer-1"
        end
        EchoConsumer1
      end
      let(:input_messages) { [{ payload: "hello", key: "greetings", partition: nil }] }
      let(:topic_partitions) { 1 }
      let(:concurrency) { 1 }

      it "can consume and publish a message" do
        wait_for_messages(topic: input_topic, expected_message_count: 1)

        message = incoming_messages.first

        expect(message).not_to be_nil
        expect(message.topic).to eq output_topic
        expect(message.payload).to eq "hello"
        expect(message.key).to eq "greetings"
      end
    end
  end
end
