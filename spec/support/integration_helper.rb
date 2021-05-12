# frozen_string_literal: true

require "open3"

module IntegrationHelper
  def self.included(klass)
    klass.instance_eval do
      after { incoming_messages.clear }
    end
  end

  def rdkafka_config
    @rdkafka_config ||= Rdkafka::Config.new({
      "bootstrap.servers": kafka_brokers,
      "client.id":         Racecar.config.client_id,
      "group.id":          "racecar-tests",
      "auto.offset.reset": "beginning"
    }.merge(Racecar.config.rdkafka_consumer))
  end

  def publish_messages!(topic, messages)
    rdkafka_producer = rdkafka_config.producer
    messages.map do |m|
      rdkafka_producer.produce(
        topic: topic,
        key: m.fetch(:key),
        payload: m.fetch(:payload),
        partition: m.fetch(:partition)
      )
    end.each(&:wait)
    rdkafka_producer.close

    $stderr.puts "Published messages to topic: #{topic}; messages: #{messages}"
  end

  def create_topic(topic:, partitions: 1)
    $stderr.puts "Creating topic #{topic}"
    msg, process = run_kafka_command("kafka-topics --bootstrap-server #{kafka_brokers} --create "\
                                     "--topic #{topic} --partitions #{partitions} --replication-factor 1")
    return if process.exitstatus.zero?

    raise "Kafka topic creation exited with status #{process.exitstatus}, message: #{msg}"
  end

  def wait_for_messages(topic:, expected_message_count:)
    rdkafka_consumer = rdkafka_config.consumer
    rdkafka_consumer.subscribe(topic)

    attempts = 0

    while incoming_messages.count < expected_message_count && attempts < 20
      attempts += 1
      $stderr.puts "Waiting for messages..."

      while (message = rdkafka_consumer.poll(1000))
        $stderr.puts "Received message #{message}"
        incoming_messages << message
      end
    end
  end

  def wait_for_assignments(group_id:, topic:, expected_members_count:)
    rebalance_attempt = 1

    until members_count(group_id, topic) == expected_members_count
      $stderr.puts "Waiting for rebalance..."
      sleep 2 * rebalance_attempt
      raise "Did not rebalance in time" if rebalance_attempt > 5
      rebalance_attempt += 1
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
      "kafka-topics --bootstrap-server #{kafka_brokers} --delete --topic '#{input_topic_prefix}-.*'"
    )
    $stderr.puts "Input topics deletion exited with status #{process.exitstatus}, message: #{message}"

    message, process = run_kafka_command(
      "kafka-topics --bootstrap-server #{kafka_brokers} --delete --topic '#{output_topic_prefix}-.*'"
    )
    $stderr.puts "Output topics deletion exited with status #{process.exitstatus}, message: #{message}"
  end

  def incoming_messages
    @incoming_messages ||= []
  end

  private

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

  def in_background(cleanup_callback:, &block)
    Thread.new do
      begin
        block.call
      ensure
        cleanup_callback.call
      end
    end
  end

  def consumer_group_partitions_and_member_ids(group_id, topic)
    message, process = run_kafka_command(
      "kafka-consumer-groups --bootstrap-server #{kafka_brokers} --describe --group #{group_id}"
    )
    unless process.exitstatus.zero?
      raise "Kafka consumer group inspection exited with status #{process.exitstatus}, message: #{message}"
    end

    return [] if message.match?(/consumer.+(is rebalancing|does not exist|has no active members)/i)

    message
      .scan(/^#{group_id}\ +#{topic}\ +(?<partition>\d+)\ +\S+\ +\S+\ +\S+\ +(?<member_id>\S+)\ /)
      .map { |partition, member_id| { partition: partition, member_id: member_id } }
      .tap do |partitions|
        raise("Unexpected command output, please review regexp matcher:\n\n #{message}") if partitions.empty?
      end
  end

  def run_kafka_command(command)
    maybe_sudo = "sudo " if ENV["DOCKER_SUDO"] == "true"

    Open3.capture2e(
      "#{maybe_sudo}docker exec -t $(#{maybe_sudo}docker ps | grep broker | awk '{print $1}') #{command}"
    )
  end
end
