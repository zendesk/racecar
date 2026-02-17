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
      consumer_class.group_id = group_id
      consumer_class.pipe_to_test = consumer_message_pipe
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

          message_count_by_worker = incoming_messages.group_by { |m| m.headers.fetch("processed_by") }.transform_values(&:count)

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

          message_count_by_worker = incoming_messages.group_by { |m| m.headers.fetch("processed_by") }.transform_values(&:count)

          expect(incoming_messages.count).to eq(6)
          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
          expect(message_count_by_worker.values).to eq([2,2,2])
        end
      end
    end

    context "when multithreading batch processing is enabled" do
      let(:input_messages) do
        6.times.map { |n|
          { payload: "message-#{n}", partition: n % topic_partitions }
        }
      end
      let(:topic_partitions) { 6 }
      let(:parallelism) { nil }
      let(:parallel_batches_executors) { 3 }

      before do
        consumer_class.parallel_batches_executors = parallel_batches_executors
      end

      it "ensures processing is not disrupted" do
        start_racecar

        wait_for_assignments(1)
        publish_messages
        wait_for_messages

        batch_count_by_executor = incoming_messages.group_by { |m| m.headers.fetch("processed_by") }.transform_values(&:count)

        expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
        expect(incoming_messages.map(&:payload))
          .to match_array(input_messages.map { |m| m[:payload] })
        expect(batch_count_by_executor.values).to eq([2, 2, 2])
      end

      context "when consumer is using a thread_safe block" do
        let(:consumer_class) { IntegrationTestConsumer = echo_thread_safe_consumer_class }

        context "with global synchronization" do
          let(:input_messages) do
            5.times.map { |n|
              { payload: "test_global_sync", partition: n % topic_partitions, key: "msg_#{n}" }
            }
          end

          it "ensures thread-safe access to shared resources" do
            start_racecar

            wait_for_assignments(1)
            publish_messages
            wait_for_messages

            expect(incoming_messages.count).to eq(5)

            expect(consumer_class.shared_counter).to eq(5)

            results = incoming_messages.map(&:payload).sort
            expected_results = (1..5).map { |i| "result_#{i}" }
            expect(results).to match_array(expected_results)
          end
        end

        context "with key-based synchronization" do
          let(:input_messages) do
            messages = []
            ["user1", "user2"].each do |user|
              3.times do |n|
                messages << { payload: "test_key_sync", partition: n % topic_partitions, key: user }
              end
            end
            messages
          end

          it "provides separate synchronization for different keys" do
            start_racecar

            wait_for_assignments(1)
            publish_messages
            wait_for_messages

            expect(incoming_messages.count).to eq(6)

            results_by_user = incoming_messages.group_by(&:key)

            expect(results_by_user["user1"].count).to eq(3)
            expect(results_by_user["user2"].count).to eq(3)

            expect(consumer_class.key_counters["user1"]).to eq(3)
            expect(consumer_class.key_counters["user2"]).to eq(3)

            user1_results = results_by_user["user1"].map(&:payload).sort
            user2_results = results_by_user["user2"].map(&:payload).sort

            expect(user1_results).to match_array(%w[key_user1_count_1 key_user1_count_2 key_user1_count_3])
            expect(user2_results).to match_array(%w[key_user2_count_1 key_user2_count_2 key_user2_count_3])
          end
        end
      end
    end
  end

  after do
    Object.send(:remove_const, :IntegrationTestConsumer) if defined?(IntegrationTestConsumer)
  end
end
