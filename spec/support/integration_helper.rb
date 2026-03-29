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

    $stderr.puts "Published messages to topic: #{topic}; messages: #{messages}"
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
    $stderr.print "Waiting for assignments: #{n}"
    Timeout.timeout(5*n) do
      until assigned_consumer_ids.size >= n
        sleep 0.5
      end
    end
  rescue Timeout::Error
    raise Timeout::Error.new("Timeout waiting for assignments, expected #{n} unique consumers, got #{assigned_consumer_ids.size}")
  end

  def assignment_events
    received_consumer_events.select { |e| e["event"] == "partitions_assigned" }
  end

  def assigned_consumer_ids
    assignment_events
      .select { |e| e["partitions"]&.any? }
      .map { |e| e["consumer_id"] }
      .uniq
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

  def wait_for_child_processes
    Timeout.timeout(5) do
      Process.waitall
    end
  rescue Timeout::Error
    warn "Timed out waiting for child processes to exit, may have left zombie processes."
  end

  def start_consumer
    runner = Racecar.runner(consumer_class)
    Thread.new do
      Thread.current.name = "Racecar MT runner #{consumers.size}"
      runner.run
    end
    consumers << runner
  end

  def configure_consumer_class(klass, partitions:)
    create_topic(topic: input_topic,  partitions: partitions)
    create_topic(topic: output_topic, partitions: partitions)
    klass.subscribes_to(input_topic)
    klass.output_topic  = output_topic
    klass.group_id      = group_id
    klass.pipe_to_test  = consumer_message_pipe
  end

  def expect_all_messages_processed(expected: input_messages)
    expect(incoming_messages.count).to eq(expected.count)
    expect(incoming_messages.map(&:payload))
      .to match_array(expected.map { |m| m[:payload] })
  end

  def expect_one_thread_per_partition(count)
    thread_ids_by_partition = incoming_messages
      .group_by(&:partition)
      .transform_values { |msgs| msgs.map { |m| m.headers.fetch("processed_by") }.uniq }
    expect(thread_ids_by_partition.values.map(&:size)).to all(eq(1))
    expect(thread_ids_by_partition.values.flatten.uniq.size).to eq(count)
  end

  def wait_until(timeout: 10, &block)
    Timeout.timeout(timeout) { sleep 0.05 until block.call }
  end

  def start_consumer_for(topic, g_id)
    config = Racecar::Config.new
    config.multithreaded_processing_enabled        = true
    config.multithreaded_processing_max_queue_size = 100
    config.max_wait_time    = 0.1
    config.group_id         = g_id
    config.consumer_class   = consumer_class
    config.subscriptions    = [Racecar::Consumer::Subscription.new(topic, true, 1048576, {})]
    runner = Racecar::Runner.new(
      consumer_class,
      config:       config,
      logger:       config.logger,
      instrumenter: config.instrumenter
    )
    Thread.new do
      Thread.current.name = "Racecar MT runner for #{topic}"
      runner.run
    end
    consumers << runner
  end

  def slow_consumer_class
    Class.new(Racecar::Consumer) do
      class << self
        attr_accessor :output_topic, :pipe_to_test, :processing_delay

        def on_partitions_assigned(event)
          pipe_to_test.write({ event: "partitions_assigned", partitions: event.partition_numbers, consumer_id: consumer_id })
        end

        def consumer_id
          "#{Process.pid}-#{Thread.current.object_id}"
        end
      end

      def process(message)
        sleep self.class.processing_delay
        produce(message.value, topic: self.class.output_topic, partition: message.partition)
        deliver!
      end
    end
  end

  def retry_on_error_consumer_class
    Class.new(Racecar::Consumer) do
      class << self
        attr_accessor :output_topic, :pipe_to_test

        def on_partitions_assigned(event)
          pipe_to_test.write({ event: "partitions_assigned", consumer_id: consumer_id })
        end

        def consumer_id
          "#{Process.pid}-#{Thread.current.object_id}"
        end
      end

      def process(message)
        @attempt_count = (@attempt_count || 0) + 1
        raise "Simulated processing error" if @attempt_count == 1

        produce(message.value, topic: self.class.output_topic, partition: message.partition)
        deliver!
      end
    end
  end

  def echo_batch_consumer_class
    Class.new(Racecar::Consumer) do
      class << self
        attr_accessor :output_topic, :pipe_to_test
      end

      def self.on_partitions_assigned(event)
        pipe_to_test.write({ event: "partitions_assigned", partitions: event.partition_numbers, consumer_id: consumer_id })
      end

      def self.consumer_id
        "#{Process.pid}-#{Thread.current.object_id}"
      end

      def process_batch(messages)
        messages.each do |message|
          produce(message.value, topic: self.class.output_topic, partition: message.partition, key: message.key, headers: { processed_by: self.class.consumer_id })
        end
        deliver!
      end
    end
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
    end

    def read
      data = read_end.readline
      data && JSON.parse(data)
    end
  end
end
