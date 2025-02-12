# frozen_string_literal: true

require "racecar/cli"
require 'benchmark'

RSpec.describe "cooperative-sticky assignment", type: :integration do
  before do
    create_topic(topic: input_topic, partitions: topic_partitions)
    create_topic(topic: output_topic, partitions: topic_partitions)

    set_config

    consumer_class.group_id = group_id
    consumer_class.output_topic = output_topic
    consumer_class.pipe_to_test = consumer_message_pipe
    consumer_class.subscribes_to(input_topic)
  end

  let(:input_topic) { generate_input_topic_name }
  let(:output_topic) { generate_output_topic_name }
  let(:group_id) { generate_group_id }
  let(:topic_partitions) { 2 }
  let(:consumer_class) { CoopStickyConsumer ||= echo_consumer_class }
  let(:input_messages) do
    message_count.times.map { |n|
      { payload: "message-#{n}", partition: n % topic_partitions }
    }
  end
  let(:message_count) { 200 }

  context "during a rebalance" do
    let!(:consumers) { [] }
    let(:consumer_index_by_id) { {} }

    after { terminate_all_consumers }

    it "allows healthy consumers to keep processing their paritions" do
      start_consumer
      start_consumer

      wait_for_assignments(2)
      time = Benchmark.measure do
        publish_messages
      end

      wait_time = Benchmark.measure do wait_for_a_few_messages end

      terminate_consumer1

      wait_for_all_messages

      puts "Time to publish messages: #{time.real}"
      puts "Time to wait for messages: #{wait_time.real}"

      aggregate_failures do
        expect_consumer0_did_not_have_partitions_revoked_but_consumer1_did
        expect_consumer0_took_over_processing_from_consumer1
      end
    end

    def expect_consumer0_took_over_processing_from_consumer1
      consumer0_partitions = messages_by_consumer[0].map(&:partition).uniq
      consumer1_partitions = messages_by_consumer[1].map(&:partition).uniq

      raise "consumer1 got assigned more than 1 parition" if consumer1_partitions.count > 1
      consumer1_partition = consumer1_partitions.fetch(0)

      expect(consumer0_partitions).to include(consumer1_partition)
    end

    def expect_consumer0_did_not_have_partitions_revoked_but_consumer1_did
      revocations_by_consumer_thread_id = revocation_events.group_by { |e| e.fetch("consumer_id") }

      revocations_by_consumer_index = revocations_by_consumer_thread_id
        .transform_keys { |consumer_id| consumer_index_by_id.fetch(consumer_id) }

      expect(revocations_by_consumer_index.keys).to eq([1])
    end

    def messages_by_consumer
      incoming_messages.group_by { |m| m.headers["processed_by"] }
        .transform_keys { |consumer_id| consumer_index_by_id.fetch(consumer_id) }
    end

    def start_consumer
      runner = Racecar.runner(consumer_class.new)

      thread = Thread.new do
        Thread.current.name = "Racecar runner #{consumers.size}"
        runner.run
      end

      consumers << runner
      consumer_index_by_id["#{Process.pid}-#{thread.object_id}"] = consumers.index(runner)
    end

    def terminate_consumer1
      consumers[1].stop
    end

    def terminate_all_consumers
      consumers.each(&:stop)
    end

    def wait_for_a_few_messages
      wait_for_messages(expected_message_count: 5)
    end

    def wait_for_all_messages
      wait_for_messages(expected_message_count: message_count)
    end
  end

  def set_config
    Racecar.config.fetch_messages = 1
    Racecar.config.max_wait_time = 0.1
    Racecar.config.session_timeout = 6 # minimum allowed by default broker config
    Racecar.config.heartbeat_interval = 1.5
    Racecar.config.partition_assignment_strategy = "cooperative-sticky"
    Racecar.config.load_consumer_class(consumer_class)
  end

  after do |test|
    Object.send(:remove_const, :CoopStickyConsumer) if defined?(CoopStickyConsumer)
  end
end
