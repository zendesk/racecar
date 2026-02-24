# frozen_string_literal: true

require "racecar/datadog"

RSpec.describe Racecar::Datadog do
  describe '.statsd' do
    before do
      Racecar::Datadog.socket_path = nil
      Racecar::Datadog.host = nil
      Racecar::Datadog.port = nil
    end

    it 'configures with host/port by default' do
      statsd = Racecar::Datadog.statsd
      expect(statsd.host).to eq('127.0.0.1')
      expect(statsd.port).to eq(8125)
      expect(statsd.socket_path).to be_nil
    end

    it 'configures with host/port explicitly' do
      Racecar::Datadog.host = '10.0.0.1'
      Racecar::Datadog.port = 8555
      statsd = Racecar::Datadog.statsd
      expect(statsd.host).to eq('10.0.0.1')
      expect(statsd.port).to eq(8555)
      expect(statsd.socket_path).to be_nil
    end

    it 'configures with socket_path explicitly' do
      Racecar::Datadog.socket_path = '/var/run/datadog/dsd.socket'
      statsd = Racecar::Datadog.statsd
      expect(statsd.socket_path).to eq('/var/run/datadog/dsd.socket')
      expect(statsd.host).to be_nil
      expect(statsd.port).to be_nil
    end
  end
end

RSpec.describe Racecar::Datadog::StatsdSubscriber do
  describe '#emit' do
    let(:subscriber)  { Racecar::Datadog::StatsdSubscriber.new }
    let(:tags)        { { tag_1: 'race', tag_2: 'car'} }

    it 'publishes stats with tags' do
      expect(Racecar::Datadog.statsd).
        to receive(:increment).
        with('metric', tags: ['tag_1:race', 'tag_2:car'])

      subscriber.send(:emit, :increment, 'metric', tags: tags)
    end
  end
end

def create_event(name, payload = {})
  Timecop.freeze do
    start = Time.now - 2
    ending = Time.now - 1

    transaction_id = nil

    args = [name, start, ending, transaction_id, payload]

    ActiveSupport::Notifications::Event.new(*args)
  end
end

RSpec.describe Racecar::Datadog::ConsumerSubscriber do
  before do
    %w[increment histogram count timing gauge].each do |type|
      allow(statsd).to receive(type)
    end
  end
  let(:subscriber)  { Racecar::Datadog::ConsumerSubscriber.new }
  let(:statsd)      { Racecar::Datadog.statsd }

  describe '#process_message' do
    let(:event) do
      create_event(
        'process_message',
        client_id:      'racecar',
        group_id:       'test_group',
        consumer_class: 'TestConsumer',
        topic:          'test_topic',
        partition:      1,
        offset:         2,
        create_time:    Time.now - 1,
        key:            'key',
        value:          'nothing new',
        headers:        {}
      )
    end
    let(:duration) { 1000.0 }
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
          topic:test_topic
          partition:1
        ]
    end

    it 'publishes latency' do
      expect(statsd).
        to receive(:timing).
        with('consumer.process_message.latency', duration, tags: metric_tags)

      subscriber.process_message(event)
    end

    it 'gauges offset' do
      expect(statsd).
        to receive(:gauge).
        with('consumer.offset', 2, tags: metric_tags)

      subscriber.process_message(event)
    end

    it 'gauges time lag' do
      expect(statsd).
        to receive(:gauge).
        with('consumer.time_lag', 1000, tags: metric_tags)

      subscriber.process_message(event)
    end

    it 'publishes errors' do
      event.payload[:exception] = StandardError.new("surprise")
      expect(statsd).
        to receive(:increment).
        with("consumer.process_message.errors", tags: metric_tags)

      subscriber.process_message(event)
    end
  end

  describe '#process_batch' do
    let(:event) do
      create_event(
        'process_batch',
        client_id:      'racecar',
        group_id:       'test_group',
        consumer_class: 'TestConsumer',
        topic:          'test_topic',
        partition:      1,
        first_offset:   3,
        last_offset:    10,
        last_create_time: Time.now,
        message_count:  20,
      )
    end
    let(:duration) { 1000.0 }
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
          topic:test_topic
          partition:1
        ]
    end

    it 'publishes latency' do
      expect(statsd).
        to receive(:timing).
        with('consumer.process_batch.latency', duration, tags: metric_tags)

      subscriber.process_batch(event)
    end

    it 'publishes batch count' do
      expect(statsd).
        to receive(:count).
        with('consumer.messages', 20, tags: metric_tags)

      subscriber.process_batch(event)
    end

    it 'gauges offset' do
      expect(statsd).
        to receive(:gauge).
        with('consumer.offset', 10, tags: metric_tags)

      subscriber.process_batch(event)
    end

    it 'publishes errors' do
      event.payload[:exception] = StandardError.new("surprise")
      expect(statsd).
        to receive(:increment).
        with("consumer.process_batch.errors", tags: metric_tags)

      subscriber.process_batch(event)
    end
  end

  describe '#join_group' do
    let(:event) do
      create_event(
        'join_group',
        client_id:      'racecar',
        group_id:       'test_group',
      )
    end
    let(:duration) { 1000.0 }
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
        ]
    end

    it 'publishes latency' do
      expect(statsd).
        to receive(:timing).
        with('consumer.join_group', duration, tags: metric_tags)

      subscriber.join_group(event)
    end
  end

  describe '#leave_group' do
    let(:event) do
      create_event(
        'leave_group',
        client_id:      'racecar',
        group_id:       'test_group',
      )
    end
    let(:duration) { 1000.0 }
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
        ]
    end

    it 'publishes latency' do
      expect(statsd).
        to receive(:timing).
        with('consumer.leave_group', duration, tags: metric_tags)

      subscriber.leave_group(event)
    end
  end

  describe '#poll_retry' do
    let(:event_with_known_error_code) do
      create_event(
        'poll_retry',
        client_id:      'racecar',
        group_id:       'test_group',
        exception:      Rdkafka::RdkafkaError.new(10),
      )
    end
    let(:event_with_unknown_error_code) do
      create_event(
        'poll_retry',
        client_id:      'racecar',
        group_id:       'test_group',
        exception:      Rdkafka::RdkafkaError.new(10243534),
      )
    end
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
        ]
    end

    it 'increments using error name for known errors' do
      expect(statsd).
        to receive(:increment).
        with('consumer.poll.rdkafka_error.msg_size_too_large', tags: metric_tags)

      subscriber.poll_retry(event_with_known_error_code)
    end

    it 'increments using error code for unknown errors' do
      expect(statsd).
        to receive(:increment).
        with('consumer.poll.rdkafka_error.err_10243534', tags: metric_tags)

      subscriber.poll_retry(event_with_unknown_error_code)
    end
  end

  describe '#main_loop' do
    let(:event) do
      create_event(
        'main_loop',
        client_id:      'racecar',
        group_id:       'test_group',
      )
    end
    let(:duration) { 1000.0 }
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
        ]
    end

    it 'publishes loop duration' do
      expect(statsd).
        to receive(:histogram).
        with('consumer.loop.duration', duration, tags: metric_tags)

      subscriber.main_loop(event)
    end
  end

  describe '#pause_status' do
    let(:event) do
      create_event(
        'main_loop',
        client_id: 'racecar',
        group_id:  'test_group',
        topic:     'test_topic',
        partition: 1,
        duration:  10,
      )
    end
    let(:metric_tags) do
      %w[
          client:racecar
          group_id:test_group
          topic:test_topic
          partition:1
        ]
    end

    it 'gauges pause duration' do
      expect(statsd).
        to receive(:gauge).
        with('consumer.pause.duration', 10, tags: metric_tags)

      subscriber.pause_status(event)
    end
  end
end

RSpec.describe Racecar::Datadog::ProducerSubscriber do
  before do
    %w[increment histogram count timing gauge].each do |type|
      allow(statsd).to receive(type)
    end
  end
  let(:subscriber)  { Racecar::Datadog::ProducerSubscriber.new }
  let(:statsd)      { Racecar::Datadog.statsd }

  describe '#produce_message' do
    let(:event) do
      create_event(
        'produce_message',
        client_id:      'racecar',
        group_id:       'test_group',
        topic:          'test_topic',
        message_size:   12,
        buffer_size:    10
      )
    end
    let(:metric_tags) do
      %w[
          client:racecar
          topic:test_topic
        ]
    end

    it 'increments number of produced messages' do
      expect(statsd).
        to receive(:increment).
        with('producer.produce.messages', tags: metric_tags)

      subscriber.produce_message(event)
    end

    it 'publishes message size' do
      expect(statsd).
        to receive(:histogram).
        with('producer.produce.message_size', 12, tags: metric_tags)

      subscriber.produce_message(event)
    end

    it 'aggregates message size' do
      expect(statsd).
        to receive(:count).
        with('producer.produce.message_size.sum', 12, tags: metric_tags)

      subscriber.produce_message(event)
    end

    it 'publishes buffer size' do
      expect(statsd).
        to receive(:histogram).
        with('producer.buffer.size', 10, tags: metric_tags)

      subscriber.produce_message(event)
    end
  end

  describe '#deliver_messages' do
    let(:event) do
      create_event(
        'deliver_messages',
        client_id:      'racecar',
        delivered_message_count: 10
      )
    end
    let(:duration) { 1000.0 }
    let(:metric_tags) do
      %w[
          client:racecar
        ]
    end

    it 'publishes delivery latency' do
      expect(statsd).
        to receive(:timing).
        with('producer.deliver.latency', duration, tags: metric_tags)

      subscriber.deliver_messages(event)
    end

    it 'publishes message size' do
      expect(statsd).
        to receive(:count).
        with('producer.deliver.messages', 10, tags: metric_tags)

      subscriber.deliver_messages(event)
    end
  end

  describe '#acknowledged_message' do
    let(:event) do
      create_event(
        'acknowledged_message',
        client_id: 'racecar',
        offset: 123,
        partition: 1,
        topic: 'test_topic'
      )
    end
    let(:metric_tags) do
      %w[
          client:racecar
          topic:test_topic
          partition:1
        ]
    end

    it 'publishes number of acknowledged messages' do
      expect(statsd).
        to receive(:increment).
        with('producer.ack.messages', tags: metric_tags)

      subscriber.acknowledged_message(event)
    end
  end
end
