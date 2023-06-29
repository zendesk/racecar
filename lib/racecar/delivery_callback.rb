# frozen_string_literal: true

module Racecar
  class DeliveryCallback
    attr_reader :instrumenter

    def initialize(instrumenter:)
      @instrumenter = instrumenter
    end

    def call(delivery_report)
      if delivery_report.error.to_i.zero?
        payload = {
          offset: delivery_report.offset,
          partition: delivery_report.partition
        }
        instrumenter.instrument("acknowledged_message", payload)
      else
        payload = {
          partition: delivery_report.partition,
          exception: delivery_report.error
        }
        instrumenter.instrument("produce_delivery_error", payload)
      end
    end
  end
end
