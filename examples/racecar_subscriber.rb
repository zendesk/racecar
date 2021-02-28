# frozen_string_literal: true

class RacecarSubscriber < ActiveSupport::Subscriber
  attach_to :racecar

  def process_batch(event)
    counter_by(:processed_message_total).increment(by: event.payload[:message_count], labels: { topic: event.payload[:topic] })
  end

  def process_message(event)
    counter_by(:processed_message_total).increment(by: 1, labels: { topic: event.payload[:topic] })
  end

  def produce_message(event)
    counter_by(:produced_message_total).increment(by: 1, labels: { topic: event.payload[:topic] })
  end

  private

  def counter_by(name)
    counter = use_metric(name) { registry.counter(name, docstring: '', labels: [:topic]) }
  end

  def use_metric(name)
    registry.exist?(name) ? registry.get(name) : yield
  end

  def prometheus_registry
    @prometheus_registry ||= Prometheus::Client.registry
  end
end
