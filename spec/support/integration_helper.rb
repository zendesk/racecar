# frozen_string_literal: true

require "securerandom"
require "active_support/isolated_execution_state"

module IntegrationHelper
  def self.included(klass)
    klass.instance_eval do
      before(:all) do
        @test_topic_names ||= []
      end

      before do
        listen_for_consumer_events
        set_config_for_speed
      end

      after do
        stop_listening_for_consumer_events
        incoming_messages.clear
        @rdkafka_consumer&.close
        @rdkafka_producer&.close
        @rdkafka_admin&.close
        stop_racecar
        wait_for_child_processes
        reset_signal_handlers
        reset_config
      end

      after(:all) do
        delete_test_topics
      end
    end
  end

  def start_racecar
    @cli_run_thread = Thread.new do
      Thread.current.name = "Racecar CLI"
      racecar_cli.run
    end
  end

  def stop_racecar
    return unless @cli_run_thread && @cli_run_thread.alive?

    racecar_cli.stop
    $stderr.puts "Stopping racecar"

    @cli_run_thread.wakeup
    @cli_run_thread.join(2)
    @cli_run_thread.terminate
  end

  def publish_messages(topic: input_topic, messages: input_messages)
    messages.map do |m|
      rdkafka_producer.produce(
        topic: topic,
        key: m.fetch(:key, nil),
        payload: m.fetch(:payload),
        partition: m.fetch(:partition, nil),
      )
    end.each(&:wait)

    # $stderr.puts "Published messages to topic: #{topic}; messages: #{messages}"
  end

  def wait_for_messages(topic: output_topic, expected_message_count: input_messages.count)
    rdkafka_consumer.subscribe(topic)

    attempts = 0
    max_attempts = 30

    $stderr.print "\nWaiting for messages "
    while incoming_messages.count < expected_message_count && attempts < max_attempts
      $stderr.print "."
      attempts += 1

      while (message = rdkafka_consumer.poll(250))
        incoming_messages << message
        if incoming_messages.count == expected_message_count
          break
        end
      end
    end
    $stderr.print("\n")

    if incoming_messages.count < expected_message_count
      raise "Timed out waiting for messages, expected: #{expected_message_count}, got: #{incoming_messages.count}"
    end
  end

  def wait_for_assignments(n)
    $stderr.print "\nWaiting for assignments (#{n} consumers) "
    Timeout.timeout(10*n) do
      loop do
        consumer_count = assignment_events.uniq { |e| e["consumer_id"] }.length
        break if consumer_count == n
        sleep 0.5
        print "."
      end
    end
  rescue Timeout::Error => e
    raise Timeout::Error.new("Timeout waiting for assignments, expected #{n} got #{assignment_events.length}")
  end

  def assignment_events
    received_consumer_events.select { |e| e["event"] == "partitions_assigned" }
  end

  def revocation_events
    received_consumer_events.select { |e| e["event"] == "partitions_revoked" }
  end

  def create_topic(topic:, partitions: 1, replication_factor: 1)
    $stderr.puts "Creating topic #{topic}"
    handle = rdkafka_admin.create_topic(topic, partitions, replication_factor)
    handle.wait
    @test_topic_names.push(topic)
    nil
  end

  def delete_test_topics
    @test_topic_names.map { |topic_name|
      $stdout.puts "Deleting topic #{topic_name.inspect}"
      rdkafka_admin.delete_topic(topic_name)
    }.each(&:wait)
    rdkafka_admin.close
  end

  def listen_for_consumer_events
    @received_consumer_events ||= []
    @listening_for_consumer_events = true

    Thread.new do
      Thread.current.name = "Test consumer event listener"
      while @listening_for_consumer_events
        event = consumer_message_pipe.read
        if event
          @received_consumer_events << event
        end
      end
    end
  end
  attr_reader :received_consumer_events

  def stop_listening_for_consumer_events
    @listening_for_consumer_events = false
  end

  def consumer_message_pipe
    @consumer_message_pipe ||= JSONPipe.new
  end

  def generate_input_topic_name
    "#{input_topic_prefix}-#{SecureRandom.hex(8)}"
  end

  def generate_output_topic_name
    "#{output_topic_prefix}-#{SecureRandom.hex(8)}"
  end

  def generate_group_id
    "racecar_test_consumers-#{SecureRandom.hex(8)}"
  end

  def rdkafka_consumer
    @rdkafka_consumer ||= Rdkafka::Config.new(
      "bootstrap.servers" => kafka_brokers,
      "client.id" =>         Racecar.config.client_id,
      "group.id" =>          "racecar-tests",
      "auto.offset.reset" => "beginning"
    ).consumer
  end

  def rdkafka_admin
    @rdkafka_admin ||= Rdkafka::Config.new({
      "bootstrap.servers" => kafka_brokers,
    }).admin
  end

  def rdkafka_producer
    @rdkafka_producer ||= Rdkafka::Config.new({
      "bootstrap.servers" => kafka_brokers,
    }).producer
  end

  def incoming_messages
    @incoming_messages ||= []
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

  def reset_signal_handlers
    ["INT", "TERM", "QUIT"].each do |signal|
      Signal.trap(signal, "DEFAULT")
    end
  end

  def set_config_for_speed
    Racecar.config.fetch_messages = 1
    Racecar.config.max_wait_time = 0.1
    Racecar.config.session_timeout = 6 # minimum allowed by default broker config
    Racecar.config.heartbeat_interval = 0.5
  end

  def reset_config
    Racecar.config = Racecar::Config.new
  end

  def wait_for_child_processes
    Timeout.timeout(5) do
      Process.waitall
    end
  rescue Timeout::Error
    warn "Timed out waiting for child processes to exit, may have left zombie processes."
  end

  def echo_consumer_class
    Class.new(Racecar::Consumer) do
      class << self
        attr_accessor :output_topic, :pipe_to_test
      end

      def self.on_partitions_assigned(event)
        message = { event: "partitions_assigned", partitions: event.partition_numbers, consumer_id: consumer_id}
        pipe_to_test.write(message)
      end

      def self.on_partitions_revoked(event)
        message = { event: "partitions_revoked", partitions: event.partition_numbers, consumer_id: consumer_id}
        pipe_to_test.write(message)
      end

      def self.consumer_id
        "#{Process.pid}-#{Thread.current.object_id}"
      end

      def process(message)
        produce(message.value, topic: self.class.output_topic, partition: message.partition, key: message.key, headers: headers(message))
        deliver!
      end

      private

      def headers(message)
        {
          processed_by: self.class.consumer_id,
          pid: Process.pid,
          processed_at: Process.clock_gettime(Process::CLOCK_MONOTONIC),
          partition: message.partition,
        }
      end
    end
  end

  class JSONPipe
    def initialize(actual_pipe = IO.pipe)
      @read_end = actual_pipe[0]
      @write_end = actual_pipe[1]
    end
    attr_reader :read_end, :write_end

    def write(data)
      write_end.write(JSON.dump(data) + "\n")
      write_end.flush
    end

    def read
      data = read_end.readline
      data && JSON.parse(data)
    end
  end
end
