# frozen_string_literal: true

module Racecar
  module Signals
    module_function

    SHUTDOWN_SIGNALS = ["INT", "QUIT", "TERM"]

    def setup_shutdown_handlers
      # We cannot run the graceful shutdown from a trap context, given that each
      # consumer is running in a thread. Instead, we're setting up a pipe so
      # that the shutdown will be initiated outside of that trap context.
      @shutdown_reader, writer = IO.pipe

      SHUTDOWN_SIGNALS.each do |signal|
        trap(signal) { writer.puts signal }
      end
    end

    def wait_for_shutdown(&callback)
      # The below will block until we receive one of the signals we are listening for.
      # Any subsequent repeats of those signals are written, but ignored, since shutdown
      # is already initiated. If we execute `reader.gets.chomp`, this would give us access
      # to the signal received, in case we ever need to respond based off the signal
      #
      # This could also be extended if we want multiple signals to trigger an ungraceful
      # shutdown
      IO.select([@shutdown_reader])

      callback.call
    end
  end
end
