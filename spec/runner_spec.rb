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

  def process_batch(batch)
    @messages += batch.messages

    batch.messages.each do |message|
      processor = @processor_queue.shift || proc {}
      processor.call(message)
    end
  end
end

class TestProducingConsumer < Racecar::Consumer
  subscribes_to "numbers"

  def process(message)
    value = Integer(message.value) * 2

    produce value, topic: "doubled"
  end
end

class TestNilConsumer < Racecar::Consumer
  subscribes_to "greetings"
end

class FakeConsumer
  def initialize(kafka)
    @kafka = kafka
    @topics = []
  end

  def subscribe(topic, **)
    @topics << topic
  end

  def each_message(*options, &block)
    @kafka.messages.each do |message|
      next unless @topics.include?(message.topic)

      begin
        block.call(message)
      rescue StandardError => e
        raise Kafka::ProcessingError.new(message.topic, message.partition, message.offset)
      end
    end
  end

  def each_batch(*options, &block)
    begin
      batch = Kafka::FetchedBatch.new(
        topic: @kafka.messages.first.topic,
        partition: @kafka.messages.first.partition,
        messages: @kafka.messages,
        highwater_mark_offset: @kafka.messages.first.offset,
      )

      block.call(batch)
    rescue StandardError => e
      raise Kafka::ProcessingError.new(batch.topic, batch.partition, batch.highwater_mark_offset)
    end
  end

  def pause(topic, partition, timeout:, max_timeout: nil, exponential_backoff: false)
    @kafka.paused_partitions[topic] ||= {}
    @kafka.paused_partitions[topic][partition] = true
  end

  def stop; end
end

class FakeProducer
  def initialize(kafka)
    @kafka = kafka
    @buffer = []
  end

  def produce(value, **options)
    @buffer << [value, options]
  end

  def deliver_messages
    @buffer.each do |value, **options|
      @kafka.deliver_message(value.to_s, **options)
    end
  end
end

class FakeKafka
  FakeMessage = Struct.new(:value, :key, :topic, :partition, :offset)

  attr_reader :messages, :paused_partitions

  def initialize(*options)
    @messages = []
    @paused_partitions = {}
  end

  def paused?(topic, partition)
    @paused_partitions[topic] ||= {}
    !!@paused_partitions[topic][partition]
  end

  def deliver_message(value, topic:)
    @messages << FakeMessage.new(value, nil, topic, 0, 0)
  end

  def messages_in(topic)
    messages.select {|message| message.topic == topic }
  end

  def consumer(*options)
    FakeConsumer.new(self)
  end

  def producer(*)
    FakeProducer.new(self)
  end
end

FakeInstrumenter = Class.new(Racecar::NullInstrumenter)

describe Racecar::Runner do
  let(:config) { Racecar::Config.new }
  let(:logger) { Logger.new(StringIO.new) }
  let(:kafka) { FakeKafka.new }
  let(:instrumenter) { FakeInstrumenter }

  let(:runner) do
    Racecar::Runner.new(processor, config: config, logger: logger, instrumenter: instrumenter)
  end

  before do
    allow(Kafka).to receive(:new) { kafka }

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

      expect(instrumenter).to receive(:instrument).with("start_process_message.racecar", payload)
      expect(instrumenter).to receive(:instrument).with("process_message.racecar", payload)

      runner.run
    end

    context "when the processing code raises an exception" do
      it "pauses the partition if `pause_timeout` is > 0" do
        config.pause_timeout = 10

        processor
          .on_message {|message| raise "OMG COOKIES!" }
          .on_message {}

        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(kafka.paused?("greetings", 0)).to eq true
      end

      it "does not pause the partition if `pause_timeout` is 0" do
        config.pause_timeout = 0

        processor
          .on_message {|message| raise "OMG COOKIES!" }
          .on_message {}

        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(kafka.paused?("greetings", 0)).to eq false
      end
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

      expect(instrumenter).to receive(:instrument).with("start_process_batch.racecar", payload)
      expect(instrumenter).to receive(:instrument).with("process_batch.racecar", payload)

      runner.run
    end

    context "when the processing code raises an exception" do
      it "pauses the partition if `pause_timeout` is > 0" do
        config.pause_timeout = 10

        processor
          .on_message {|message| raise "OMG COOKIES!" }
          .on_message {}

        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(kafka.paused?("greetings", 0)).to eq true
      end

      it "does not pause the partition if `pause_timeout` is 0" do
        config.pause_timeout = 0

        processor
          .on_message {|message| raise "OMG COOKIES!" }
          .on_message {}

        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(kafka.paused?("greetings", 0)).to eq false
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

  context "with a consumer that produces messages" do
    let(:processor) { TestProducingConsumer.new }

    it "delivers the messages to Kafka" do
      kafka.deliver_message("2", topic: "numbers")

      runner.run

      expect(kafka.messages_in("doubled").map(&:value)).to eq ["4"]
    end
  end

  context "#stop" do
    let(:processor) { TestConsumer.new }

    it "allows the processor to tear down resources" do
      runner.stop

      expect(processor.torn_down?).to eq true
    end
  end
end
