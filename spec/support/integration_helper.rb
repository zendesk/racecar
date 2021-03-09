# frozen_string_literal: true

require "open3"

module IntegrationHelper
  def self.included(klass)
    klass.instance_eval do
      let!(:polling_thread) do
        Thread.new do
          Thread.current.abort_on_exception = true
          rdkafka_consumer.each do |message|
            puts "Received message #{message}"
            incoming_messages << message
          end
        end
      end

      let(:rdkafka_consumer) { rdkafka_config.consumer }
      let(:rdkafka_producer) { rdkafka_config.producer }

      let(:rdkafka_config) do
        Rdkafka::Config.new({
          "bootstrap.servers": kafka_brokers,
          "client.id":         Racecar.config.client_id,
          "group.id":          "racecar-tests"
        }.merge(Racecar.config.rdkafka_consumer))
      end

      after do
        polling_thread.kill
        incoming_messages.clear
        rdkafka_consumer.unsubscribe
        rdkafka_producer.close
      end
    end
  end

  def publish_messages!(topic, messages)
    messages.map do |m|
      rdkafka_producer.produce(
        topic: topic,
        key: m.fetch(:key),
        payload: m.fetch(:payload),
        partition: m.fetch(:partition)
      )
    end.each(&:wait)

    puts "Published messages to topic: #{topic}; messages: #{messages}"
  end

  def create_topic(topic:, partitions: 1)
    puts "Creating topic #{topic}"
    msg, process = run_kafka_command("kafka-topics --bootstrap-server #{kafka_brokers} --create "\
                                     "--topic #{topic} --partitions #{partitions} --replication-factor 1")
    unless process.exitstatus.zero?
      raise "Kafka topic creation exited with status #{process.exitstatus}, message: #{msg}"
    end
  end

  def wait_for_assignments(group_id:, topic:, expected_members_count:)
    rebalance_attempt = 1

    until members_count(group_id, topic) == expected_members_count
      puts "Waiting for rebalance..."
      sleep 2 * rebalance_attempt
      raise "Did not rebalance in time" if rebalance_attempt > 5
      rebalance_attempt += 1
    end
  end

  def wait_for_messages(topic:, expected_message_count:)
    attempt = 1

    until incoming_messages.count == expected_message_count
      puts "Waiting for message..."
      sleep 2 * attempt
      raise "Did not receive messages in time" if attempt > 5
      attempt += 1
    end
  end

  def generate_input_topic_name
    "#{input_topic_prefix}-#{SecureRandom.hex(8)}"
  end

  def generate_output_topic_name
    "#{output_topic_prefix}-#{SecureRandom.hex(8)}"
  end

  def delete_all_test_topics
    message, process = run_kafka_command(
      "kafka-topics --bootstrap-server localhost:9092 --delete --topic '#{input_topic_prefix}-.*'"
    )
    puts "Input topics deletion exited with status #{process.exitstatus}, message: #{message}"

    message, process = run_kafka_command(
      "kafka-topics --bootstrap-server localhost:9092 --delete --topic '#{output_topic_prefix}-.*'"
    )
    puts "Output topics deletion exited with status #{process.exitstatus}, message: #{message}"
  end

  def incoming_messages
    @incoming_messages ||= []
  end

  private

  def run_kafka_command(command)
    Open3.capture2e("docker-compose exec -T broker #{command}")
  end

  def kafka_brokers
    Racecar.config.brokers.join(",")
  end

  def input_topic_prefix
    "input-test-topic"
  end

  def output_topic_prefix
    "output-test-topic"
  end

  def members_count(group_id, topic)
    consumer_group_partitions_and_member_ids(group_id, topic).uniq { |data| data.fetch(:member_id) }.count
  end

  def consumer_group_partitions_and_member_ids(group_id, topic)
    message, process = run_kafka_command(
      "kafka-consumer-groups --bootstrap-server #{kafka_brokers} --describe --group #{group_id}"
    )
    unless process.exitstatus.zero?
      raise "Kafka consumer group inspection exited with status #{process.exitstatus}, message: #{message}"
    end

    message.scan(/^#{topic}\ +(?<partition>\d+)\ +\S+\ +\S+\ +\S+\ +(?<member_id>\S+)\ /)
           .map { |partition, member_id| { partition: partition, member_id: member_id } }
  end
end
