# frozen_string_literal: true

module Racecar
  module Processing
    def instrumentation_payload(message)
      {
        consumer_class: consumer_class_instance.class.to_s,
        topic:          message.topic,
        partition:      message.partition,
        offset:         message.offset,
        create_time:    message.timestamp,
        key:            message.key,
        value:          message.payload,
        headers:        message.headers
      }
    end

    def instrumentation_payload_for_batch(messages)
      first, last = messages.first, messages.last
      {
        consumer_class:   consumer_class_instance.class.to_s,
        topic:            first.topic,
        partition:        first.partition,
        first_offset:     first.offset,
        last_offset:      last.offset,
        last_create_time: last.timestamp,
        message_count:    messages.size
      }
    end

    # librdkafka will continue to try to deliver already queued messages, even if ruby-rdkafka
    # raised before that. This method detects any unrecoverable errors and resets the producer
    # as a last ditch effort. Returns true if there were unrecoverable errors, false otherwise.
    def reset_producer_on_unrecoverable_delivery_errors(error, with_synchronization: false)
      return false unless error.is_a?(Racecar::MessageDeliveryError)
      return false unless error.code == :msg_timed_out # -192

      logger.error error.to_s
      logger.error "Racecar will reset the producer to force a new broker connection."
      reset_producer!(with_synchronization:)
      true
    end

    def handle_processing_error(error, payload, pause:, with_synchronization: false)
      payload[:unrecoverable_delivery_error] = reset_producer_on_unrecoverable_delivery_errors(error, with_synchronization:)
      payload[:retries_count] = pause.pauses_count
      config.error_handler.call(error, payload)
    end

    def instrument_process_message(payload, &block)
      @instrumenter.instrument("start_process_message", payload) do
        @instrumenter.instrument("process_message", payload, &block)
      end
    end

    def instrument_process_batch(payload, &block)
      @instrumenter.instrument("start_process_batch", payload) do
        @instrumenter.instrument("process_batch", payload, &block)
      end
    end

    def reset_producer!(with_synchronization: false)
      resetting_proc = Proc.new do
        producer.close
        @producer = nil
        consumer_class_instance.configure(
          producer:     producer,
          consumer:     consumer,
          instrumenter: @instrumenter,
          config:       @config,
          )
      end

      if with_synchronization
        @finalize_mutex.synchronize do
          resetting_proc.call
        end
      else
        resetting_proc.call
      end
    end
  end
end
