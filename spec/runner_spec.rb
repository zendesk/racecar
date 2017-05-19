require "stringio"

class TestConsumer
  extend Racecar::Consumer

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
  let(:kafka) { FakeKafka.new }
  let(:runner) { Racecar::Runner.new(processor, config: config, logger: logger) }

  before do
    allow(Kafka).to receive(:new) { kafka }
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
