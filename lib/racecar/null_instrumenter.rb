module Racecar
  # Ignores all instrumentation events.
  class NullInstrumenter
    def self.instrument(*)
      yield({}) if block_given?
    end
  end
end
