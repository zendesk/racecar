# frozen_string_literal: true

require "racecar/cli"
require "active_support/notifications"

class NoSubsConsumer < Racecar::Consumer
  def process(message); end
end

class NoProcessConsumer < Racecar::Consumer
  subscribes_to "some-topic"
end

RSpec.describe "running a Racecar consumer", type: :integration do
  context "when an error occurs trying to start the runner" do
    context "when there are no subscriptions, and no parallelism" do
      before { NoSubsConsumer.parallel_workers = nil }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there are no subscriptions, and parallelism" do
      before { NoSubsConsumer.parallel_workers = 3 }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there is no process method, and no parallelism" do
      before { NoProcessConsumer.parallel_workers = nil }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoProcessConsumer"]).run
        end.to raise_error(NotImplementedError)
      end
    end

    context "when there is no process method, and parallelism" do
      before { NoSubsConsumer.parallel_workers = 3 }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoProcessConsumer"]).run
        end.to raise_error(NotImplementedError)
      end
    end
  end

  context "when the runner starts successfully" do
    let!(:racecar_cli) { Racecar::Cli.new([consumer_class.name.to_s]) }
    let(:input_topic) { generate_input_topic_name }
    let(:output_topic) { generate_output_topic_name }
    let(:group_id) { generate_group_id }
    let(:consumer_class) { IntegrationTestConsumer = echo_consumer_class }

    before do
      Racecar.config.max_wait_time = 0.1
      create_topic(topic: input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic, partitions: topic_partitions)

      consumer_class.subscribes_to(input_topic)
      consumer_class.output_topic = output_topic
      consumer_class.parallel_workers = parallelism
    end

    context "for a single threaded consumer" do
      let(:input_messages) { [{ payload: "hello", key: "greetings", partition: nil }] }
      let(:topic_partitions) { 1 }
      let(:parallelism) { nil }

      it "can consume and publish a message" do
        start_racecar

        publish_messages
        wait_for_messages

        message = incoming_messages.first

        expect(message).not_to be_nil
        expect(message.topic).to eq output_topic
        expect(message.payload).to eq "hello"
        expect(message.key).to eq "greetings"
      end
    end

    context "when running parallel workers" do
      let(:input_messages) do
        6.times.map { |n|
          { payload: "message-#{n}", partition: n % topic_partitions }
        }
      end

      context "when partitions exceed parallelism" do
        let(:topic_partitions) { 6 }
        let(:parallelism) { 3 }

        it "assigns partitions to all parallel workers" do
          start_racecar

          wait_for_assignments(parallelism)
          publish_messages
          wait_for_messages

          message_count_by_worker = incoming_messages.group_by { |m| m.headers.fetch(:processed_by_pid) }.transform_values(&:count)

          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
          expect(message_count_by_worker.values).to eq([2,2,2])
        end
      end

      context "when the parallelism exceeds the number of partitions" do
        let(:topic_partitions) { 3 }
        let(:parallelism) { 5 }

        it "assigns all the consumers that it can, up to the total number of partitions" do
          start_racecar

          wait_for_assignments(parallelism)
          publish_messages
          wait_for_messages

          message_count_by_worker = incoming_messages.group_by { |m| m.headers.fetch(:processed_by_pid) }.transform_values(&:count)

          expect(incoming_messages.count).to eq(6)
          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
          expect(message_count_by_worker.values).to eq([2,2,2])
        end
      end
    end
  end

  after do
    Object.send(:remove_const, :IntegrationTestConsumer) if defined?(IntegrationTestConsumer)
  end

  def echo_consumer_class
    test_instance = self

    Class.new(Racecar::Consumer) do
      class << self
        attr_accessor :output_topic
        attr_accessor :pipe_to_test
      end
      self.group_id = test_instance.group_id
      self.pipe_to_test = test_instance.consumer_message_pipe.write_end

      def self.on_partitions_assigned(topic_partition_list)
        Racecar.logger.info("on_partitions_assigned #{topic_partition_list.to_h}")

        pipe_to_test.puts(JSON.dump({group_id: self.group_id, pid: Process.pid}))
      end

      def process(message)
        produce(message.value, key: message.key, topic: self.class.output_topic, headers: headers)
        deliver!
      end

      private

      def headers
        {
          processed_by_pid: Process.pid,
        }
      end
    end
  end
end
