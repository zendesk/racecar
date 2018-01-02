require "stringio"

class TestConsumer < Racecar::Consumer
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

    'message_processing_result'
  end

  def teardown
    @torn_down = true
  end

  def torn_down?
    @torn_down
  end
end

class TestConsumerWithResponder < TestConsumer
  subscribes_to 'greetings', responds_with: 'aloha', response_partition_key: 'response_partition'
end

class TestConsumerWithResponderPartition < TestConsumer
  subscribes_to 'greetings', responds_with: 'aloha', response_partition_key: :response_partition

  def response_partition(message)
    "partition_#{message.value}"
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

  def process_batch(batch)
    @messages += batch.messages

    batch.messages.each do |message|
      processor = @processor_queue.shift || proc {}
      processor.call(message)
    end

    'batch_processing_result'
  end
end

class TestBatchConsumerWithResponder < TestBatchConsumer
  subscribes_to 'greetings', responds_with: 'aloha', response_partition_key: 'response_partition'
end

class TestBatchConsumerWithResponderPartition < TestBatchConsumer
  subscribes_to 'greetings', responds_with: 'aloha', response_partition_key: :response_partition

  def response_partition(batch)
    "partition_#{batch.messages.first.value}"
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

  def pause(topic, partition, timeout:)
    @kafka.paused_partitions[topic] ||= {}
    @kafka.paused_partitions[topic][partition] = true
  end

  def stop; end

  def subscribe(topic, options); end
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
    config.load_consumer_class(processor.class)
    allow(Kafka).to receive(:new) { kafka }
    allow(DeliveryBoy).to receive(:deliver)
  end

  context "with a consumer class with a #process method" do
    let(:processor) { TestConsumer.new }

    it "processes messages with the specified consumer class" do
      kafka.deliver_message("hello world", topic: "greetings")

      runner.run

      expect(processor.messages.map(&:value)).to eq ["hello world"]
      expect(DeliveryBoy).not_to have_received(:deliver)
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

    context "with a responder configured for the topic" do
      let(:processor) { TestConsumerWithResponder.new }

      it "processes messages with the specified consumer class" do
        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(processor.messages.map(&:value)).to eq ["hello world"]
        expect(DeliveryBoy).to have_received(:deliver).with(
          'message_processing_result',
          topic: 'aloha',
          partition_key: 'response_partition'
        )
      end
    end

    context "with a responder configured for the topic and a partition key generation method" do
      let(:processor) { TestConsumerWithResponderPartition.new }

      it "processes messages with the specified consumer class" do
        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(processor.messages.map(&:value)).to eq ["hello world"]
        expect(DeliveryBoy).to have_received(:deliver).with(
          'message_processing_result',
          topic: 'aloha',
          partition_key: 'partition_hello world'
        )
      end
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
      expect(DeliveryBoy).not_to have_received(:deliver)
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

    context "with a responder configured for the topic" do
      let(:processor) { TestBatchConsumerWithResponder.new }

      it "processes messages with the specified consumer class" do
        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(processor.messages.map(&:value)).to eq ["hello world"]
        expect(DeliveryBoy).to have_received(:deliver).with(
          'batch_processing_result',
          topic: 'aloha',
          partition_key: 'response_partition'
        )
      end
    end

    context "with a responder configured for the topic and a partition key generation method" do
      let(:processor) { TestBatchConsumerWithResponderPartition.new }

      it "processes messages with the specified consumer class" do
        kafka.deliver_message("hello world", topic: "greetings")

        runner.run

        expect(processor.messages.map(&:value)).to eq ["hello world"]
        expect(DeliveryBoy).to have_received(:deliver).with(
          'batch_processing_result',
          topic: 'aloha',
          partition_key: 'partition_hello world'
        )
      end
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

  context "#stop" do
    let(:processor) { TestConsumer.new }

    it "allows the processor to tear down resources" do
      runner.stop

      expect(processor.torn_down?).to eq true
    end
  end
end
