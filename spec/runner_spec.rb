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
    @topics = []
  end

  def subscribe(topic, **)
    @topics << topic
  end

  def poll(timeout)
    @runner.stop if @kafka.messages.empty?
    @kafka.messages.shift
  end
end

class FakeProducer
  def initialize(kafka, runner)
    @kafka = kafka
    @runner = runner
    @buffer = []
  end

  def produce(topic:, payload:, key:)
    @buffer << FakeRdkafka::FakeMessage.new(payload, key, topic, 0, 0)
    @runner.stop
    self
  end

  def wait
    @buffer.each do |message|
      @kafka.messages << message
    end
  end
end

class FakeRdkafka
  FakeMessage = Struct.new(:value, :key, :topic, :partition, :offset)

  attr_reader :messages

  def initialize(runner:)
    @runner = runner
    @messages = []
  end

  def deliver_message(value, topic:)
    @messages << FakeMessage.new(value, nil, topic, 0, 0)
  end

  def messages_in(topic)
    messages.select {|message| message.topic == topic }
  end

  def consumer(*options)
    FakeConsumer.new(self, @runner)
  end

  def producer(*)
    FakeProducer.new(self, @runner)
  end
end

FakeInstrumenter = Class.new(Racecar::NullInstrumenter)

describe Racecar::Runner do
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

      expect(instrumenter).to receive(:instrument).with("process_message.racecar", payload)

      runner.run
    end
  end

  context "with a consumer class with a #process_batch method" do
    let(:processor) { TestBatchConsumer.new }

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

      expect(instrumenter).to receive(:instrument).with("process_batch.racecar", payload)

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

    it "delivers the messages to Kafka" do
      kafka.deliver_message("2", topic: "numbers")

      runner.run

      expect(kafka.messages_in("doubled").map(&:value)).to eq [4]
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
