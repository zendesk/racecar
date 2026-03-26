module Racecar
  module MultiThreadedErrorHandling
    include Processing

    def with_error_handling(messages, payload)
      loop do
        begin
          topic = messages.is_a?(Array) ? messages.first.topic : messages.topic
          partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
          pause = pauses[topic][partition]
          yield(pause)
          pause.reset!
          break
        rescue => e
          if rebalancing
            Thread.exit
          elsif !shutting_down
            pause = pauses[topic][partition]
            handle_processing_error(e, payload, pause: pause, with_synchronization: true)
            pause.pause!
            sleep(pause.backoff_interval) unless config.pause_timeout == 0
          else
            break
          end
        end
      end
    end
  end
end