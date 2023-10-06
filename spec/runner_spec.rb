# frozen_string_literal: true

require "stringio"

class TestConsumer < Racecar::Consumer
  subscribes_to "greetings"

  attr_reader :messages

  def initialize
    @messages = []
    @processor_queue = []
    @torn_down = false
  end

  def on_message(&block)
    @processor_queue << block
    self
  end

  def process(message)
    raise message.value if message.value.is_a?(StandardError)
    @messages << message

    processor = @processor_queue.shift || proc {}
    processor.call(message)
  end

  def teardown
    @torn_down = true
  end

  def torn_down?
    @torn_down
  end
end

class TestBatchConsumer < Racecar::Consumer
  subscribes_to "greetings"

  attr_reader :messages

  def initialize
    @messages = []
    @processor_queue = []
  end

  def on_message(&block)
    @processor_queue << block
    self
  end

  def process_batch(messages)
    @messages += messages

    messages.each do |message|
      processor = @processor_queue.shift || proc {}
      raise message.value if message.value.is_a?(StandardError)
      processor.call(message)
    end
  end
end

class TestMultiConsumer < TestBatchConsumer
  subscribes_to "greetings"
  subscribes_to "second"

  attr_reader :messages
end

class TestProducingConsumer < Racecar::Consumer
  subscribes_to "numbers"

  def process(message)
    value = Integer(message.value) * 2

    produce value, topic: "doubled", key: value, create_time: 123
  end
end

class TestProducingBatchConsumer < Racecar::Consumer
  subscribes_to "numbers"

  def process_batch(messages)
    messages.each do |message|
      value = Integer(message.value) * 2

      produce value, topic: "doubled", key: value, create_time: 123
    end
  end
end

class TestNilConsumer < Racecar::Consumer
  subscribes_to "greetings"
end

class FakeConsumer
  def initialize(kafka, runner)
    @kafka = kafka
    @runner = runner

    @topic = nil
    @committed_offset = "not set yet"
    @internal_offset = 0
    @_paused = false
    @previous_messages = []

    @poll_count = 0
  end

  attr_reader :topic, :committed_offset, :_paused

  def subscribe(topic, **)
    @topic ||= topic
    raise "cannot handle more than one topic per consumer" if @topic != topic
  end

  def assignment
    Rdkafka::Consumer::TopicPartitionList.new.tap do |tpl|
      tpl.add_topic(topic, 1)
    end
  end

  def poll(timeout)
    # just stop after a big amount of messages read, which is easier to figure out when to
    # exactly stop when reading from multiple topics
    @runner.stop if (@poll_count += 1) >= 10

    msg = @kafka.received_messages[@topic][@internal_offset]
    @internal_offset += 1
    msg
  end

  def commit(partitions, async)
  end

  def close
  end

  def store_offset(message)
    raise "storing offset on wrong consumer. own topic: #{@topic} vs #{message.topic}" if @topic != message.topic
    # + 1 as per: https://github.com/edenhill/librdkafka/wiki/Consumer-offset-management#terminology
    @internal_offset = @committed_offset = message.offset + 1
  end

  def pause(tpl)
    raise "not a TopicPartitionList" unless tpl.is_a?(Rdkafka::Consumer::TopicPartitionList)
    @_paused = true
  end

  def resume(tpl)
    raise "not a TopicPartitionList" unless tpl.is_a?(Rdkafka::Consumer::TopicPartitionList)
    @_paused = false
  end

  def seek(message)
    raise "seeking on wrong consumer. own topic: #{@topic} vs #{message.topic}" if @topic != message.topic
    @internal_offset = message.offset
  end
end

class FakeProducer
  def initialize(kafka, runner)
    @kafka = kafka
    @runner = runner
    @buffer = []
    @position = -1
    @delivery_callback = nil
  end

  def produce(payload:, topic:, key:, partition: nil, partition_key: nil, timestamp: nil, headers: nil)
    @buffer << FakeRdkafka::FakeMessage.new(payload, key, topic, 0, 0, timestamp, headers)
    FakeDeliveryHandle.new(@kafka, @buffer.last, @delivery_callback)
  end

  def delivery_callback=(handler)
    @delivery_callback = handler
  end

  def close
  end
end

FakeDeliveryReport = Struct.new(:partition, :offset, :error, :topic_name)

class FakeDeliveryHandle
  def initialize(kafka, msg, delivery_callback)
    @kafka = kafka
    @msg = msg
    @delivery_callback = delivery_callback
  end

  def [](key)
    @msg.public_send(key)
  end

  def report
    FakeDeliveryReport.new(0, 0, 0, "test")
  end

  def wait(max_wait_timeout: 60, wait_timeout: 0.1)
    @kafka.produced_messages << @msg
    @delivery_callback.call(report) if @delivery_callback
  end

  def create_result
    Rdkafka::Producer::DeliveryReport.new(partition, offset)
  end

  def offset
    0
  end

  def partition
    0
  end
end

class FakeRdkafka
  FakeMessage = Struct.new(:payload, :key, :topic, :partition, :offset, :timestamp, :headers)

  attr_accessor :received_messages
  attr_reader :produced_messages, :consumers

  def initialize(runner:)
    @runner = runner
    @received_messages = Hash.new { |h, k| h[k] = [] }
    @produced_messages = []
    @consumers = []
  end

  def deliver_message(payload, topic:, partition: 0)
    raise "topic may not be nil" if topic.nil?
    @received_messages[topic] << FakeMessage.new(payload, nil, topic, partition, received_messages[topic].size)
  end

  def messages_in(topic)
    produced_messages.select {|message| message.topic == topic }
  end

  def consumer(*options)
    consumers << FakeConsumer.new(self, @runner)
    consumers.last
  end

  def producer(*)
    FakeProducer.new(self, @runner)
  end

  def consumer_rebalance_listener=(_listenr)
  end
end

class FakeInstrumenter < Racecar::Instrumenter
  def initialize(*)
    super(backend: Racecar::NullInstrumenter)
    @errored_events = []
  end

  def instrument(event_name, payload = {}, &block)
    super
  rescue => e
    @errored_events << event_name unless @errored_events.include?(event_name)
    raise
  end

  def event_raised_errors?(event_name)
    @errored_events.include?(event_name)
  end
end

RSpec.shared_examples "offset handling" do |topic|
  let(:consumers) do
    runner.send(:consumer).instance_variable_get(:@consumers)
  end

  it "stores offset after processing" do
    kafka.deliver_message("2", topic: topic) # offset 0
    kafka.deliver_message("2", topic: topic) # offset 1
    kafka.deliver_message("2", topic: topic) # offset 2

    runner.run

    expect(consumers.first.committed_offset).to eq 3
  end

  it "doesn't store offset on error" do
    kafka.deliver_message(StandardError.new("2"), topic: topic)

    runner.run rescue nil

    expect(consumers.first.committed_offset).to eq "not set yet"
  end
end

RSpec.shared_examples "delivery error handling" do
  let(:msg_timed_out) { Rdkafka::RdkafkaError.new(-192) }

  it "handles unrecoverable delivery errors" do
    allow_any_instance_of(FakeDeliveryHandle).to receive(:wait).and_raise(msg_timed_out)

    expect(config.error_handler).to receive(:call)
      .at_least(:once)
      .with(
        instance_of(Racecar::MessageDeliveryError),
        hash_including({unrecoverable_delivery_error: true})
      )
    expect(processor).to receive(:configure)
      .at_least(:twice)
      .and_call_original

    kafka.deliver_message("3", topic: "numbers")

    runner.run
  end
end

RSpec.shared_examples "pause handling" do
  after do
    Timecop.return
  end

  it "pauses on failing messages" do
    kafka.deliver_message(StandardError.new("surprise"), topic: "greetings")

    runner.run

    expect(kafka.consumers.first._paused).to eq true
    runner.run
  end

  it "resumes paused partitions" do
    now = Time.local(2019, 6, 18, 14, 0, 0)
    later = Time.local(2019, 6, 18, 14, 0, 30)

    Timecop.freeze(now)
    kafka.deliver_message(StandardError.new("surprise"), topic: "greetings")
    runner.run

    # expect no op
    runner.send(:resume_paused_partitions)
    expect(kafka.consumers.first._paused).to eq true

    # expect to resume
    Timecop.freeze(later)
    runner.send(:resume_paused_partitions)
    expect(kafka.consumers.first._paused).to eq false
  end

  it "seeks to given message and returns it on resume" do
    now = Time.local(2019, 6, 18, 14, 0, 0)
    later = Time.local(2019, 6, 18, 14, 0, 30)

    kafka.deliver_message(StandardError.new("surprise"), topic: "greetings")
    kafka.deliver_message("never get here", topic: "greetings")

    Timecop.freeze(now)
    runner.run
    expect(kafka.consumers.first._paused).to eq true

    Timecop.freeze(later)
    runner.send(:resume_paused_partitions)
    expect(kafka.consumers.first._paused).to eq false

    runner.run
    expect(kafka.consumers.first._paused).to eq true
  end

  context 'with instrumentation enabled' do
    let(:pause_instrumentation) do
      {
        topic:        'greetings',
        partition:    0,
        duration:     kind_of(Float)
      }
    end
    before do
      allow(instrumenter).to receive(:instrument).and_call_original
      kafka.deliver_message(StandardError.new("surprise"), topic: "greetings")
    end

    it 'instruments pause status' do
      expect(instrumenter).
        to receive(:instrument).
        with("pause_status", hash_including(pause_instrumentation))

      runner.run
    end
  end
end

RSpec.describe Racecar::Runner do
  let(:config) { Racecar::Config.new }
  let(:logger) { Logger.new(StringIO.new) }
  let(:kafka)  { FakeRdkafka.new(runner: runner) }
  let(:instrumenter) { FakeInstrumenter.new }

  let(:runner) do
    Racecar::Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter)
  end

  before do
    allow(Rdkafka::Config).to receive(:new) { kafka }

    config.load_consumer_class(processor.class)
  end

  context "with a consumer class with a #process method" do
    let(:processor) { TestConsumer.new }

    include_examples "offset handling", "greetings"
    include_examples "pause handling"

    it "builds producer with all config options" do
      config.producer = ["hello=world", "hi=all"]

      runner.run

      expect(Rdkafka::Config).to have_received(:new).with(hash_including("hello" => "world", "hi" => "all"))
    end

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["hello world"]
    end

    it "calls error handler when an exception is raised" do
      error = StandardError.new("surprise")
      info = {
        consumer_class: 'TestConsumer',
        topic: 'greetings',
        partition: 0,
        offset: 0,
        create_time: nil,
        key: nil,
        value: error,
        headers: nil,
        retries_count: anything,
        unrecoverable_delivery_error: false,
      }

      expect(config.error_handler).to receive(:call).at_least(:once).with(error, info)

      kafka.deliver_message(error, topic: "greetings")

      runner.run
    end

    it "keeps track of the number of retries when a message causes an exception" do
      error = StandardError.new("surprise")

      [0, 1, 2].each do |retries_count|
        expect(config.error_handler).to receive(:call)
          .once.with(error, hash_including(retries_count: retries_count)).ordered
        expect(Racecar::Message).to receive(:new)
          .once.with(anything, retries_count: retries_count).and_call_original
      end

      expect(config.error_handler).to receive(:call)
        .at_least(:once).with(error, hash_including(retries_count: anything))
      expect(Racecar::Message).to receive(:new)
        .at_least(:once).with(anything, retries_count: anything).and_call_original

      kafka.deliver_message(error, topic: "greetings")

      runner.run
    end

    context 'with instrumentation enabled' do
      let(:message_instrumentation) do
        {
          partition: 0,
          offset: 0,
          create_time: nil,
          headers: nil,
          key: nil,
          value: 'hello world',
          consumer_class: "TestConsumer",
          topic: "greetings"
        }
      end

      let(:loop_instrumentation) do
        { consumer_class: "TestConsumer" }
      end

      before do
        allow(instrumenter).to receive(:instrument).and_call_original
        kafka.deliver_message("hello world", topic: "greetings")
      end

      it 'instruments main_loop' do
        expect(instrumenter).
          to receive(:instrument).
          with("main_loop", hash_including(loop_instrumentation))

        runner.run
      end

      it 'instruments the start of processing a message' do
        expect(instrumenter).
          to receive(:instrument).
          with("start_process_message", hash_including(message_instrumentation))

        runner.run
      end

      it 'instruments a message being processed' do
        expect(instrumenter).
          to receive(:instrument).
          with("process_message", hash_including(message_instrumentation))

        runner.run
      end

      specify 'message processing errors are propagated to the instrumenter' do
        kafka.deliver_message(StandardError.new("surprise"), topic: "greetings")
        expect { runner.run }.to change { instrumenter.event_raised_errors?("process_message") }.to(true)
      end
    end
  end

  context "with a consumer class with multiple subscriptions" do
    let(:processor) { TestMultiConsumer.new }

    include_examples "offset handling", "greetings"
    include_examples "pause handling"

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("to_greet", topic: "greetings")
      kafka.deliver_message("to_second", topic: "second")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["to_greet", "to_second"]
    end

    it "stores offset on correct consumer" do
      # Note: offset is generated from a global counter
      kafka.deliver_message("2", topic: "greetings") # offset 0
      kafka.deliver_message("2", topic: "greetings") # offset 1
      kafka.deliver_message("2", topic: "second")    # offset 2

      runner.run

      consumers = runner.send(:consumer).instance_variable_get(:@consumers)
      offsets = consumers.map { |c| [c.topic, c.committed_offset] }.to_h
      expect(offsets).to eq({"greetings" => 2, "second" => 1})
    end
  end

  context "with a consumer class with a #process_batch method" do
    let(:processor) { TestBatchConsumer.new }

    include_examples "offset handling", "greetings"
    include_examples "pause handling"

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["hello world"]
    end

    it "calls error handler when an exception is raised" do
      error = StandardError.new("surprise")
      info = {
        consumer_class: 'TestBatchConsumer',
        topic: 'greetings',
        partition: 0,
        first_offset: 0,
        last_offset: 0,
        last_create_time: nil,
        message_count: 1,
        retries_count: anything,
        unrecoverable_delivery_error: false,
      }

      expect(config.error_handler).to receive(:call).at_least(:once).with(error, info)

      kafka.deliver_message(error, topic: "greetings")

      runner.run
    end

    it "keeps track of the number of retries when a message causes an exception" do
      error = StandardError.new("surprise")

      [0, 1, 2].each do |retries_count|
        expect(config.error_handler).to receive(:call)
          .once.with(error, hash_including(retries_count: retries_count)).ordered
        expect(Racecar::Message).to receive(:new)
          .once.with(anything, retries_count: retries_count).and_call_original
      end

      expect(config.error_handler).to receive(:call)
        .at_least(:once).with(error, hash_including(retries_count: anything))
      expect(Racecar::Message).to receive(:new)
        .at_least(:once).with(anything, retries_count: anything).and_call_original

      kafka.deliver_message(error, topic: "greetings")

      runner.run
    end

    context 'with instrumentation enabled' do
      let(:batch_instrumentation) do
        {
          consumer_class: 'TestBatchConsumer',
          topic:          'greetings',
          partition:      0,
          first_offset:   0,
          last_offset:    0,
          last_create_time: nil,
          message_count:  1
        }
      end
      before do
        allow(instrumenter).to receive(:instrument).and_call_original
        kafka.deliver_message("hello world", topic: "greetings")
      end

      it 'instruments the main loop' do
        expect(instrumenter).
          to receive(:instrument).
          with("main_loop", hash_including(consumer_class: "TestBatchConsumer"))

        runner.run
      end

      it 'instruments the start of a batch processing' do
        expect(instrumenter).
          to receive(:instrument).
          with("start_process_batch", hash_including(batch_instrumentation))

        runner.run
      end

      it 'instruments the batch processing' do
        expect(instrumenter).
          to receive(:instrument).
          with("process_batch", hash_including(batch_instrumentation))

        runner.run
      end

      specify 'batch processing errors are propagated to the instrumenter' do
        kafka.deliver_message(StandardError.new("surprise"), topic: "greetings")
        expect { runner.run }.to change { instrumenter.event_raised_errors?("process_batch") }.to(true)
      end
    end
  end

  context "with a consumer class with neither a #process or a #process_batch method" do
    let(:processor) { TestNilConsumer.new }

    it "raises NotImplementedError" do
      kafka.deliver_message("hello world", topic: "greetings")

      expect { runner.run }.to raise_error(NotImplementedError)
    end
  end

  context "with a consumer class with an invalid #process_batch method signature" do
    class TestInvalidConsumer < Racecar::Consumer
      subscribes_to "greetings"

      def process_batch(batch, hello); end
    end

    let(:processor) { TestInvalidConsumer.new }

    it "raises NotImplementedError" do
      kafka.deliver_message("hello world", topic: "greetings")

      expect { runner.run }.to raise_error(Racecar::Error, "Invalid method signature for `process_batch`. The method must take exactly 1 argument.")
    end
  end

  context "with a consumer that produces messages" do
    let(:processor) { TestProducingConsumer.new }

    include_examples "offset handling", "numbers"
    include_examples "delivery error handling"

    it "delivers the messages to Kafka", focus: true do
      kafka.deliver_message("2", topic: "numbers")

      runner.run

      messages = kafka.messages_in("doubled")

      expect(messages.map(&:payload)).to eq [4]
      expect(messages.map(&:timestamp)).to eq [123]
    end

    it "instruments produced messages" do
      allow(instrumenter).to receive(:instrument).and_call_original
      kafka.deliver_message("2", topic: "numbers")

      payload_start = a_hash_including(:create_time, topic: "doubled", key: 4, value: 4)
      payload_finish = a_hash_including(message_count: 1)

      runner.run

      expect(instrumenter).to have_received(:instrument).with("produce_message", payload_start)
    end

    it "instruments delivery notifications" do
      allow_any_instance_of(FakeDeliveryReport).to receive(:error).and_return(0)
      allow(instrumenter).to receive(:instrument).and_call_original
      kafka.deliver_message("2", topic: "numbers")


      runner.run

      expect(instrumenter).to have_received(:instrument)
        .with("acknowledged_message", {partition: 0, offset: 0})
    end
  end

  context "with a batch consumer that produces messages" do
    let(:processor) { TestProducingBatchConsumer.new }
    include_examples "delivery error handling"
  end

  context "#stop" do
    let(:processor) { TestConsumer.new }
    let(:datadog) { double("Racecar::Datadog", close: nil) }

    it "allows the processor to tear down resources" do
      runner.run

      expect(processor.torn_down?).to eq true
    end

    context "when DataDog metrics are disabled" do
      before do
        allow(Object).to receive(:const_defined?).with("Racecar::Datadog").and_return(false)
      end

      it "does not close Datadog::Statsd instance" do
        expect(datadog).not_to receive(:close)
        runner.run
      end
    end

    context "when DataDog metrics are enabled" do
      before do
        stub_const("Racecar::Datadog", datadog)
        allow(Object).to receive(:const_defined?).with("Racecar::Datadog").and_return(true)
      end

      it "closes Datadog::Statsd instance" do
        expect(datadog).to receive(:close)
        runner.run
      end
    end
  end
end
