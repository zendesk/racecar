module Racecar
  module SingleThreadedErrorHandling
    include Processing

    def with_error_handling(messages, payload)
      topic = messages.is_a?(Array) ? messages.first.topic : messages.topic
      partition = messages.is_a?(Array) ? messages.first.partition : messages.partition
      offsets = messages.is_a?(Array) ? messages.first.offset..messages.last.offset : messages.offset..messages.offset
      with_pause(topic, partition, offsets) do |pause|
        yield(pause)
      rescue => e
        handle_processing_error(e, payload, pause: pause)
        raise e
      end

      resume_all_paused_partitions
    end

    private

    def with_pause(topic, partition, offsets)
      pause = pauses[topic][partition]
      return yield pause if config.pause_timeout == 0

      begin
        yield pause
        # We've successfully processed a batch from the partition, so we can clear the pause.
        pauses[topic][partition].reset!
      rescue => e
        desc = "#{topic}/#{partition}"
        logger.error "Failed to process #{desc} at #{offsets}: #{e}"

        logger.warn "Pausing partition #{desc} for #{pause.backoff_interval} seconds"
        consumer.pause(topic, partition, offsets.first)
        pause.pause!
      end
    end
  end
end