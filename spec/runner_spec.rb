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

    produce topic: "doubled", key: value, payload: value
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
    @last_offset = "not set yet"
  end

  attr_reader :topic, :last_offset

  def subscribe(topic, **)
    @topic ||= topic
    raise "cannot handle more than one topic per consumer" if @topic != topic
  end

  def poll(timeout)
    @runner.stop if @kafka.received_messages.empty?
    return nil if @kafka.received_messages.first && @kafka.received_messages.first.topic != @topic
    @kafka.received_messages.shift
  end

  def commit(partitions, async)
  end

  def close
  end

  def store_offset(message)
    raise "storing offset on wrong consumer. own topic: #{@topic} vs #{message.topic}" if @topic != message.topic
    @last_offset = message.offset
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

  def produce(topic:, payload:, key:, headers: nil)
    @buffer << FakeRdkafka::FakeMessage.new(payload, key, topic, 0, 0)
    FakeDeliveryHandle.new(@kafka, @buffer.last, @delivery_callback)
  end

  def delivery_callback=(handler)
    @delivery_callback = handler
  end
end

class FakeDeliveryHandle
  def initialize(kafka, msg, delivery_callback)
    @kafka = kafka
    @msg = msg
    @delivery_callback = delivery_callback
  end

  def [](key)
    @msg.public_send(key)
  end

  def wait
    @kafka.produced_messages << @msg
    @delivery_callback.call(self) if @delivery_callback
  end

  def offset
    0
  end

  def partition
    0
  end
end

class FakeRdkafka
  FakeMessage = Struct.new(:value, :key, :topic, :partition, :offset)

  attr_reader :received_messages, :produced_messages

  def initialize(runner:)
    @runner = runner
    @received_messages = []
    @produced_messages = []
  end

  def deliver_message(value, topic:, partition: 0)
    raise "topic may not be nil" if topic.nil?
    @received_messages << FakeMessage.new(value, nil, topic, partition, received_messages.size)
  end

  def messages_in(topic)
    produced_messages.select {|message| message.topic == topic }
  end

  def consumer(*options)
    FakeConsumer.new(self, @runner)
  end

  def producer(*)
    FakeProducer.new(self, @runner)
  end
end

FakeInstrumenter = Class.new(Racecar::NullInstrumenter)

RSpec.shared_examples "offset handling" do |topic|
  let(:consumers) do
    runner.send(:consumer).instance_variable_get(:@consumers)
  end

  it "stores offset after processing" do
    kafka.deliver_message("2", topic: topic)
    kafka.deliver_message("2", topic: topic)
    kafka.deliver_message("2", topic: topic)

    runner.run

    expect(consumers.first.last_offset).to eq 2
  end

  it "doesn't store offset on error" do
    kafka.deliver_message("2", topic: topic)
    mock_method = processor.respond_to?(:process_batch) ? :process_batch : :process
    allow(processor).to receive(mock_method).and_raise(StandardError)

    runner.run rescue nil

    expect(consumers.first.last_offset).to eq "not set yet"
  end
end

RSpec.describe Racecar::Runner do
  let(:config) { Racecar::Config.new }
  let(:logger) { Logger.new(StringIO.new) }
  let(:kafka)  { FakeRdkafka.new(runner: runner) }
  let(:instrumenter) { FakeInstrumenter }

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

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["hello world"]
    end

    it "sends instrumentation signals" do
      kafka.deliver_message("hello world", topic: "greetings")

      payload = a_hash_including(
        :partition,
        :offset,
        consumer_class: "TestConsumer",
        topic: "greetings"
      )

      expect(instrumenter).to receive(:instrument).with("main_loop.racecar", {consumer_class: "TestConsumer"}).and_call_original.twice
      expect(instrumenter).to receive(:instrument).with("process_message.racecar", payload)

      runner.run
    end
  end

  context "with a consumer class with multiple subscriptions" do
    let(:processor) { TestMultiConsumer.new }

    include_examples "offset handling", "greetings"

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
      offsets = consumers.map { |c| [c.topic, c.last_offset] }.to_h
      expect(offsets).to eq({"greetings" => 1, "second" => 2})
    end
  end

  context "with a consumer class with a #process_batch method" do
    let(:processor) { TestBatchConsumer.new }

    include_examples "offset handling", "greetings"

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["hello world"]
    end

    it "sends instrumentation signals" do
      kafka.deliver_message("hello world", topic: "greetings")

      payload = a_hash_including(
        :partition,
        :first_offset,
        consumer_class: "TestBatchConsumer",
        topic: "greetings"
      )

      expect(instrumenter).to receive(:instrument).with("main_loop.racecar", {consumer_class: "TestBatchConsumer"}).and_call_original
      expect(instrumenter).to receive(:instrument).with("process_batch.racecar", payload)

      runner.run
    end

    it "batches per partition" do
      kafka.deliver_message("hello", topic: "greetings", partition: 0)
      kafka.deliver_message("world", topic: "greetings", partition: 1)
      kafka.deliver_message("!", topic: "greetings", partition: 1)

      payload = a_hash_including(
        :partition,
        :first_offset,
        consumer_class: "TestBatchConsumer",
        topic: "greetings"
      )

      expect(processor).to receive(:process_batch).with([kafka.received_messages[0]])
      expect(processor).to receive(:process_batch).with(kafka.received_messages[1, 2])

      runner.run
    end
  end

  context "with a consumer class with neither a #process or a #process_batch method" do
    let(:processor) { TestNilConsumer.new }

    it "raises NotImplementedError" do
      kafka.deliver_message("hello world", topic: "greetings")

      expect { runner.run }.to raise_error(NotImplementedError)
    end
  end

  context "with a consumer that produces messages" do
    let(:processor) { TestProducingConsumer.new }

    include_examples "offset handling", "numbers"

    it "delivers the messages to Kafka" do
      kafka.deliver_message("2", topic: "numbers")

      runner.run

      expect(kafka.messages_in("doubled").map(&:value)).to eq [4]
    end

    it "instruments produced messages" do
      allow(instrumenter).to receive(:instrument).and_call_original
      kafka.deliver_message("2", topic: "numbers")

      payload_start = a_hash_including(:create_time, topic: "doubled", key: 4, value: 4)
      payload_finish = a_hash_including(message_count: 1)

      runner.run

      expect(instrumenter).to have_received(:instrument).with("produce_message.racecar", payload_start)
    end

    it "instruments delivery notifications" do
      allow(instrumenter).to receive(:instrument).and_call_original
      kafka.deliver_message("2", topic: "numbers")


      runner.run

      expect(instrumenter).to have_received(:instrument)
        .with("acknowledged_message.racecar", {partition: 0, offset: 0})
    end
  end

  context "#stop" do
    let(:processor) { TestConsumer.new }

    it "allows the processor to tear down resources" do
      runner.run

      expect(processor.torn_down?).to eq true
    end
  end
end
