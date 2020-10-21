# frozen_string_literal: true

begin
  require "datadog/statsd"
rescue LoadError
  $stderr.puts "In order to report Kafka client metrics to Datadog you need to install the `dogstatsd-ruby` gem."
  raise
end

require "active_support/subscriber"

module Racecar
  module Datadog
    STATSD_NAMESPACE = "racecar"

    class << self
      def configure
        yield self
      end

      def statsd
        @statsd ||= ::Datadog::Statsd.new(host, port, namespace: namespace, tags: tags)
      end

      def statsd=(statsd)
        clear
        @statsd = statsd
      end

      def host
        @host
      end

      def host=(host)
        @host = host
        clear
      end

      def port
        @port
      end

      def port=(port)
        @port = port
        clear
      end

      def namespace
        @namespace ||= STATSD_NAMESPACE
      end

      def namespace=(namespace)
        @namespace = namespace
        clear
      end

      def tags
        @tags ||= []
      end

      def tags=(tags)
        @tags = tags
        clear
      end

      private

      def clear
        @statsd && @statsd.close
        @statsd = nil
      end
    end

    class StatsdSubscriber < ActiveSupport::Subscriber
      private

      %w[increment histogram count timing gauge].each do |type|
        define_method(type) do |*args|
          emit(type, *args)
        end
      end

      def emit(type, *args, tags: {})
        tags = tags.map {|k, v| "#{k}:#{v}" }.to_a

        Racecar::Datadog.statsd.send(type, *args, tags: tags)
      end
    end

    class ConsumerSubscriber < StatsdSubscriber
      def process_message(event)
        offset = event.payload.fetch(:offset)
        create_time = event.payload.fetch(:create_time)
        time_lag = create_time && ((Time.now - create_time) * 1000).to_i
        tags = default_tags(event)

        if event.payload.key?(:exception)
          increment("consumer.process_message.errors", tags: tags)
        else
          timing("consumer.process_message.latency", event.duration, tags: tags)
          increment("consumer.messages", tags: tags)
        end

        gauge("consumer.offset", offset, tags: tags)

        # Not all messages have timestamps.
        if time_lag
          gauge("consumer.time_lag", time_lag, tags: tags)
        end
      end

      def process_batch(event)
        offset = event.payload.fetch(:last_offset)
        messages = event.payload.fetch(:message_count)
        last_create_time = event.payload.fetch(:last_create_time)
        time_lag = last_create_time && ((Time.now - last_create_time) * 1000).to_i
        tags = default_tags(event)

        if event.payload.key?(:exception)
          increment("consumer.process_batch.errors", tags: tags)
        else
          timing("consumer.process_batch.latency", event.duration, tags: tags)
          count("consumer.messages", messages, tags: tags)
        end

        histogram("consumer.batch_size", messages, tags: tags)
        gauge("consumer.offset", offset, tags: tags)

        if time_lag
          gauge("consumer.time_lag", time_lag, tags: tags)
        end
      end

      def join_group(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        timing("consumer.join_group", event.duration, tags: tags)

        if event.payload.key?(:exception)
          increment("consumer.join_group.errors", tags: tags)
        end
      end

      def leave_group(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        timing("consumer.leave_group", event.duration, tags: tags)

        if event.payload.key?(:exception)
          increment("consumer.leave_group.errors", tags: tags)
        end
      end

      def main_loop(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        histogram("consumer.loop.duration", event.duration, tags: tags)
      end

      def pause_status(event)
        duration = event.payload.fetch(:duration)

        gauge("consumer.pause.duration", duration, tags: default_tags(event))
      end

      private

      def default_tags(event)
        {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }
      end

      attach_to "racecar"
    end

    class ProducerSubscriber < StatsdSubscriber
      def produce_message(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)
        message_size = event.payload.fetch(:message_size)
        buffer_size = event.payload.fetch(:buffer_size)

        tags = {
          client: client,
          topic: topic,
        }

        # This gets us the write rate.
        increment("producer.produce.messages", tags: tags.merge(topic: topic))

        # Information about typical/average/95p message size.
        histogram("producer.produce.message_size", message_size, tags: tags.merge(topic: topic))

        # Aggregate message size.
        count("producer.produce.message_size.sum", message_size, tags: tags.merge(topic: topic))

        # This gets us the avg/max buffer size per producer.
        histogram("producer.buffer.size", buffer_size, tags: tags)
      end

      def deliver_messages(event)
        client = event.payload.fetch(:client_id)
        message_count = event.payload.fetch(:delivered_message_count)

        tags = {
          client: client,
        }

        timing("producer.deliver.latency", event.duration, tags: tags)

        # Messages delivered to Kafka:
        count("producer.deliver.messages", message_count, tags: tags)
      end

      def acknowledged_message(event)
        tags = { client: event.payload.fetch(:client_id) }

        # Number of messages ACK'd for the topic.
        increment("producer.ack.messages", tags: tags)
      end

      attach_to "racecar"
    end
  end
end
