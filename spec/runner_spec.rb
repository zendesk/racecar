require "stringio"

class TestConsumer < Racecar::Consumer
  attr_reader :messages

  def initialize
    @messages = []
    @processor_queue = []
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
end

class TestBatchConsumer < Racecar::Consumer
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

class TestNilConsumer < Racecar::Consumer
end

class FakeConsumer
  def initialize(kafka)
    @kafka = kafka
  end

  def each_message(*options, &block)
    @kafka.messages.each do |message|
      begin
        block.call(message)
      rescue StandardError => e
        raise Kafka::ProcessingError.new(message.topic, message.partition, message.offset)
      end
    end
  end

  def each_batch(*options, &block)
    begin
      block.call(@kafka.messages)
    rescue StandardError => e
      raise Kafka::ProcessingError.new(@kafka.messages.first.topic, @kafka.messages.first.partition, @kafka.messages.first.offset)
    end
  end

  def pause(topic, partition, timeout:)
    @kafka.paused_partitions[topic] ||= {}
    @kafka.paused_partitions[topic][partition] = true
  end
end

class FakeKafka
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
    @messages << Kafka::FetchedMessage.new(
      value: value,
      topic: topic,
      key: nil,
      partition: 0,
      offset: 0,
    )
  end

  def consumer(*options)
    FakeConsumer.new(self)
  end
end

describe Racecar::Runner do
  let(:config) { Racecar::Config.new }
  let(:logger) { Logger.new(StringIO.new) }
  let(:processor) { TestConsumer.new }
  let(:batch_processor) { TestBatchConsumer.new }
  let(:nil_processor) { TestNilConsumer.new }
  let(:kafka) { FakeKafka.new }
  let(:runner) { Racecar::Runner.new(processor, config: config, logger: logger) }
  let(:batch_runner) { Racecar::Runner.new(batch_processor, config: config, logger: logger) }
  let(:nil_runner) { Racecar::Runner.new(nil_processor, config: config, logger: logger) }

  before do
    allow(Kafka).to receive(:new) { kafka }
  end

  context "single message processor" do
    it "calls `process` method" do
      expect(processor).to receive(:process)

      kafka.deliver_message("hello world", topic: "greetings")
      runner.run
    end

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["hello world"]
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

  context "batch message processor" do
    it "calls `process_batch` method" do
      expect(batch_processor).to receive(:process_batch)

      kafka.deliver_message("hello world", topic: "greetings")
      batch_runner.run
    end

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      batch_runner.run

      expect(batch_processor.messages.map(&:value)).to eq ["hello world"]
    end

    context "when the processing code raises an exception" do
      it "pauses the partition if `pause_timeout` is > 0" do
        config.pause_timeout = 10

        batch_processor
          .on_message {|message| raise "OMG COOKIES!" }
          .on_message {}

        kafka.deliver_message("hello world", topic: "greetings")

        batch_runner.run

        expect(kafka.paused?("greetings", 0)).to eq true
      end

      it "does not pause the partition if `pause_timeout` is 0" do
        config.pause_timeout = 0

        batch_processor
          .on_message {|message| raise "OMG COOKIES!" }
          .on_message {}

        kafka.deliver_message("hello world", topic: "greetings")

        batch_runner.run

        expect(kafka.paused?("greetings", 0)).to eq false
      end
    end
  end

  context "invalid message processor" do
    it "raises NotImplementedError" do
      kafka.deliver_message("hello world", topic: "greetings")

      expect { nil_runner.run }.to raise_error(NotImplementedError)
    end
  end
end
