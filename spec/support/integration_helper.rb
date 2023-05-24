# frozen_string_literal: true

require "securerandom"

module IntegrationHelper
  def self.included(klass)
    klass.instance_eval do
      before(:all) do
        @test_topic_names ||= []
      end

      after do
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

    $stderr.print "\nWaiting for messages "
    while incoming_messages.count < expected_message_count && attempts < 30
      $stderr.print "."
      attempts += 1

      while (message = rdkafka_consumer.poll(250))
        $stderr.puts "\nReceived message #{message} #{message.headers}"
        incoming_messages << message
      end
    end
    $stderr.print("\n")

    if incoming_messages.count < expected_message_count
      raise "Timed out waiting for messages, expected: #{expected_message_count}, got: #{incoming_messages.count}"
    end
  end

  def wait_for_assignments(n)
    Timeout.timeout(5*n) do
      consumer_messages = []

      until consumer_messages.length == n
        message = get_consumer_message
        if message && message["group_id"] == group_id
          consumer_messages << message
        end
      end
    end
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
  end

  def get_consumer_message
    rs, _ws, _es = IO.select([consumer_message_pipe.read_end], [], [], _timeout = 0.1)
    if rs && rs.any?
      consumer_message_pipe.read
    end
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

  class JSONPipe
    def initialize(actual_pipe = IO.pipe)
      @read_end = actual_pipe[0]
      @write_end = actual_pipe[1]
    end
    attr_reader :read_end, :write_end

    def write(data)
      write_end.puts(JSON.dump(data))
    end

    def read
      data = read_end.readline
      data && JSON.parse(data)
    end
  end
end
