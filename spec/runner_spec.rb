require "stringio"

class TestConsumer < Racecar::Consumer
  def self.messages
    @@messages ||= []
  end

  def process(message)
    self.class.messages << message
  end
end

class FakeConsumer
  def initialize(kafka)
    @kafka = kafka
  end

  def each_message(*options, &block)
    @kafka.messages.each(&block)
  end
end

class FakeKafka
  attr_reader :messages

  def initialize(*options)
    @messages = []
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
  let(:consumer_class) { TestConsumer }
  let(:kafka) { FakeKafka.new }

  before do
    allow(Kafka).to receive(:new) { kafka }
  end

  it "processes messages with the specified consumer class" do
    runner = Racecar::Runner.new(consumer_class, config: config, logger: logger)

    kafka.deliver_message("hello world", topic: "greetings")

    runner.run

    expect(consumer_class.messages.map(&:value)).to eq ["hello world"]
  end
end
