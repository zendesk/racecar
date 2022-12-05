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
  let(:threads_per_process) { nil }
  let(:forks) { nil }

  context "when an error occurs trying to start the runner" do
    context "when there are no subscriptions, and no forks" do
      before { NoSubsConsumer.forks = nil }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there are no subscriptions, and forks" do
      before { NoSubsConsumer.forks = 3 }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there is no process method, and no forks" do
      before { NoProcessConsumer.forks = nil }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoProcessConsumer"]).run
        end.to raise_error(NotImplementedError)
      end
    end

    context "when there is no process method, and forks" do
      before { NoSubsConsumer.forks = 3 }

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

    before do
      create_topic(topic: input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic, partitions: 1)

      consumer_class.subscribes_to(input_topic)
      consumer_class.output_topic = output_topic
      consumer_class.forks = forks
      consumer_class.threads = threads_per_process

      publish_messages!(input_topic, input_messages)
    end

    after(:all) { delete_all_test_topics }

    context "for a single process consumer" do
      let(:consumer_class) do
        class EchoConsumer1 < mock_echo_consumer_class
          self.group_id = "echo-consumer-1"
        end
        EchoConsumer1
      end

      let(:input_messages) { [{ payload: "hello", key: "greetings", partition: nil }] }
      let(:topic_partitions) { 1 }

      it "can consume and publish a message" do
        in_background(cleanup_callback: -> { Process.kill("INT", Process.pid) }) do
          wait_for_messages(topic: output_topic, expected_message_count: 1)
        end

        Racecar::Cli.new([consumer_class.name.to_s]).run

        message = incoming_messages.first

        expect(message).not_to be_nil
        expect(message.topic).to eq output_topic
        expect(message.payload).to eq "hello"
        expect(message.key).to eq "greetings"
      end

      context "when running multithreaded" do
        class MultiThreadedEchoConsumer < Racecar::Consumer
          class << self
            attr_accessor :output_topic
          end
          SeriousError = Class.new(Exception)
          self.group_id = "multithreaded-echo-consumer"

          def process(message)
            raise SeriousError.new("Error in worker thread") if message.value == "horrible news"
            new_value = JSON.dump({ "handling_thread" => Thread.current.name, "incoming_value" => message.value })
            produce new_value, key: message.key, topic: self.class.output_topic
            deliver!
          end
        end

        let(:consumer_class) { MultiThreadedEchoConsumer }

        let(:input_messages) { message_count.times.map { |i|
          { payload: "message payload #{i}", partition: (i%topic_partitions) }
        } }

        let(:message_count) { 20 }
        let(:topic_partitions) { 4 }
        let(:messages_per_partition) { 5 }
        let(:threads_per_process) { 4 }

        it "can consume multiple partitions concurrently" do
          in_background(cleanup_callback: -> { Process.kill("INT", Process.pid) }) do
            wait_for_assignments(
              group_id: consumer_class.group_id,
              topic: input_topic,
              expected_members_count: threads_per_process,
            )
            wait_for_messages(topic: output_topic, expected_message_count: message_count)
          end

          Racecar::Cli.new([consumer_class.name.to_s]).run

          messages_by_handling_thread = incoming_messages
            .group_by { |m| JSON.parse(m.payload).fetch("handling_thread") }

          aggregate_failures do
            expect(messages_by_handling_thread.keys.size).to eq(threads_per_process)
            expect(messages_by_handling_thread.values.map(&:size).uniq).to eq([messages_per_partition])
          end
        end

        context "when there is an uncaught exception in a worker thread" do
          let(:input_messages) { [] }
          let(:message_count) { 0 }

          it "stops the other threads and exits" do
            in_background(cleanup_callback: ->{ cause_problems }) do
              wait_for_assignments(
                group_id: consumer_class.group_id,
                topic: input_topic,
                expected_members_count: threads_per_process,
              )
            end

            expect {
              Racecar::Cli.new([consumer_class.name.to_s]).run
            }.to raise_error(MultiThreadedEchoConsumer::SeriousError)
          end

          def cause_problems
            publish_messages!(input_topic, [{ payload: "horrible news", partition: 0 }])
          end
        end

        context "when combining threading and forking" do
          let(:message_count) { 20 }
          let(:topic_partitions) { 4 }
          let(:messages_per_partition) { 5 }
          let(:forks) { 2 }
          let(:threads_per_process) { 2 }
          let(:total_concurrency) { forks * threads_per_process}

          it "can consume multiple partitions concurrently" do
            in_background(cleanup_callback: -> { Process.kill("INT", Process.pid) }) do
              wait_for_assignments(
                group_id: consumer_class.group_id,
                topic: input_topic,
                expected_members_count: total_concurrency,
              )

              wait_for_messages(topic: output_topic, expected_message_count: message_count)
            end

            Racecar::Cli.new([consumer_class.name.to_s]).run

            messages_by_handling_process_and_thread = incoming_messages
              .group_by { |m| JSON.parse(m.payload).fetch("handling_thread") }

            aggregate_failures do
              expect(messages_by_handling_process_and_thread.keys.size).to eq(total_concurrency)
              expect(messages_by_handling_process_and_thread.values.map(&:size).uniq).to eq([messages_per_partition])
            end
          end
        end
      end
    end

    context "when running forked workers" do
      let(:input_messages) do
        [
          { payload: "message-0", partition: 0, key: "a" },
          { payload: "message-1", partition: 1, key: "a" },
          { payload: "message-2", partition: 2, key: "a" },
          { payload: "message-3", partition: 3, key: "a" },
          { payload: "message-4", partition: 4, key: "a" },
          { payload: "message-5", partition: 5, key: "a" }
        ]
      end

      context "when partitions exceed single threaded forked workers" do
        let(:topic_partitions) { 6 }
        let(:forks) { 3 }
        let(:consumer_class) do
          class EchoConsumer2 < mock_echo_consumer_class
            self.group_id = "echo-consumer-2"
          end
          EchoConsumer2
        end

        it "assigns partitions to all forks workers" do
          in_background(cleanup_callback: -> { Process.kill("INT", Process.pid) }) do
            wait_for_assignments(
              group_id: "echo-consumer-2",
              topic: input_topic,
              expected_members_count: forks
            )
            wait_for_messages(topic: output_topic, expected_message_count: input_messages.count)
          end

          Racecar::Cli.new([consumer_class.name.to_s]).run

          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
        end
      end

      context "when the (single threaded) forked workers exceeds the number of partitions" do
        let(:consumer_class) do
          class EchoConsumer3 < mock_echo_consumer_class
            self.group_id = "echo-consumer-3"
          end
          EchoConsumer3
        end

        let(:topic_partitions) { 3 }
        let(:forks) { 5 }
        let(:input_messages) do
          [
            { payload: "message-0", partition: 0, key: "a" },
            { payload: "message-1", partition: 0, key: "a" },
            { payload: "message-2", partition: 1, key: "a" },
            { payload: "message-3", partition: 1, key: "a" },
            { payload: "message-4", partition: 2, key: "a" },
            { payload: "message-5", partition: 2, key: "a" }
          ]
        end

        it "assigns all the consumers that it can, up to the total number of partitions" do
          in_background(cleanup_callback: -> { Process.kill("INT", Process.pid) }) do
            wait_for_assignments(
              group_id: "echo-consumer-3",
              topic: input_topic,
              expected_members_count: topic_partitions
            )
            wait_for_messages(topic: output_topic, expected_message_count: input_messages.count)
          end

          Racecar::Cli.new([consumer_class.name.to_s]).run

          expect(incoming_messages.count).to eq(6)
          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
        end
      end
    end
  end
end
