# frozen_string_literal: true

module Racecar
  ##
  # Common API for instrumentation to standardize
  # namespace and default payload
  #
  class Instrumenter
    NAMESPACE = "racecar"
    attr_reader :backend

    def initialize(backend:, default_payload: {})
      @backend = backend
      @default_payload = default_payload
    end

    def instrument(event_name, payload = {}, &block)
      @backend.instrument("#{event_name}.#{NAMESPACE}", @default_payload.merge(payload), &block)
    end
  end
end
