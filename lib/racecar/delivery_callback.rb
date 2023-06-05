module Racecar
  class DeliveryCallback
    attr_reader :instrumenter

    def initialize(instrumenter:)
      @instrumenter = instrumenter
    end

    def call(delivery_report)
      if delivery_report.error.to_i.positive?
        instrumentation_payload = {
          topic: delivery_report.topic_name,
          partition: delivery_report.partition,
          exception: delivery_report.error
        }
        @instrumenter.instrument("produce_error", instrumentation_payload)
      else
        payload = {
          offset: delivery_report.offset,
          partition: delivery_report.partition
        }
        @instrumenter.instrument("acknowledged_message", payload)
      end
    end
  end
end
