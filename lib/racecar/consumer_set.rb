module Racecar
  class ConsumerSet
    MIN_POLL_TRIES = 2
    MAX_POLL_TRIES = 10

    def initialize(config, logger, instrumenter = NullInstrumenter)
      @config, @logger = config, logger
      @instrumenter = instrumenter
      raise ArgumentError, "Subscriptions must not be empty when subscribing" if @config.subscriptions.empty?

      @consumers = []
      @consumer_id_iterator = (0...@config.subscriptions.size).cycle

      @last_poll_read_nil_message = false
    end

    def poll(max_wait_time_ms = @config.max_wait_time)
      batch_poll(max_wait_time_ms, 1).first
    end

    # batch_poll collects messages until any of the following occurs:
    # - max_wait_time_ms time has passed
    # - max_messages have been collected
    # - a nil message was polled (end of topic, Kafka stalled, etc.)
    #
    # The messages are from a single topic, but potentially from more than one partition.
    #
    # Any errors during polling are retried in an exponential backoff fashion, roughly
    # honoring the given time limit. It will try at least MIN_POLL_TRIES times,
    # regardless of the given time limit.
    def batch_poll(max_wait_time_ms = @config.max_wait_time, max_messages = @config.fetch_messages)
      started_at = Time.now
      remain_ms = max_wait_time_ms
      maybe_select_next_consumer
      messages = []

      while remain_ms > 0 && messages.size < max_messages
        msg = poll_with_retries(remain_ms)
        break if msg.nil?
        messages << msg
        remain_ms = remaining_time_ms(max_wait_time_ms, started_at)
      end

      messages
    end

    def store_offset(message)
      current.store_offset(message)
    end

    def commit
      each_subscribed do |consumer|
        commit_rescue_no_offset(consumer)
      end
    end

    def close
      each_subscribed(&:close)
    end

    def current
      @consumers[@consumer_id_iterator.peek] ||= begin
        consumer = Rdkafka::Config.new(rdkafka_config(current_subscription)).consumer
        @instrumenter.instrument('join_group') do
          consumer.subscribe current_subscription.topic
        end
        consumer
      end
    end

    def each_subscribed
      if block_given?
        @consumers.each { |c| yield c }
      else
        @consumers.each
      end
    end

    def pause(topic, partition, offset)
      consumer, filtered_tpl = find_consumer_by(topic, partition)
      if !consumer
        @logger.info "Attempted to pause #{topic}/#{partition}, but we're not subscribed to it"
        return
      end

      consumer.pause(filtered_tpl)
      fake_msg = OpenStruct.new(topic: topic, partition: partition, offset: offset)
      consumer.seek(fake_msg)
    end

    def resume(topic, partition)
      consumer, filtered_tpl = find_consumer_by(topic, partition)
      if !consumer
        @logger.info "Attempted to resume #{topic}/#{partition}, but we're not subscribed to it"
        return
      end

      consumer.resume(filtered_tpl)
    end

    alias :each :each_subscribed

    # Subscribe to all topics eagerly, even if there's still messages elsewhere. Usually
    # that's not needed and Kafka might rebalance if topics are not polled frequently
    # enough.
    def subscribe_all
      @config.subscriptions.size.times do
        current
        select_next_consumer
      end
    end

    private

    # polls a single message from the current consumer, retrying errors with exponential
    # backoff. It will always try at least MIN_POLL_TRIES times. If time remains, it
    # will attempt more retries before re-raising the exception, up to MAX_POLL_TRIES.
    #
    # Since the time limit for a single message poll is fixed to max_wait_time_ms, the
    # function can take up to 2*max_wait_time_ms in error scenarios. This happens when a
    # retry is attempted with only 1ms time left. In other words: a full retry is valued
    # higher than honoring the time limit.
    def poll_with_retries(max_wait_time_ms)
      try ||= 0
      started_at ||= Time.now

      poll_current_consumer(max_wait_time_ms)
    rescue Rdkafka::RdkafkaError => e
      remain_ms = remaining_time_ms(max_wait_time_ms, started_at)
      wait_ms = 100 * (2**try) # 100ms, 200ms, 400ms, …

      try += 1
      @logger.error "(try #{try}): Error for topic subscription #{current_subscription}: #{e}"

      raise if try >= MIN_POLL_TRIES && wait_ms >= remain_ms
      raise if try >= MAX_POLL_TRIES

      sleep wait_ms/1000.0
      retry
    end

    # polls a message for the current consumer, handling any API edge cases.
    def poll_current_consumer(max_wait_time_ms)
      msg = current.poll(max_wait_time_ms)
    rescue Rdkafka::RdkafkaError => e
      case e.code
      when :max_poll_exceeded, :transport # -147, -195
        reset_current_consumer
      end
      raise
    ensure
      @last_poll_read_nil_message = msg.nil?
    end

    def find_consumer_by(topic, partition)
      each do |consumer|
        tpl = consumer.assignment.to_h
        rdkafka_partition = tpl[topic]&.detect { |part| part.partition == partition }
        next unless rdkafka_partition
        filtered_tpl = Rdkafka::Consumer::TopicPartitionList.new({ topic => [rdkafka_partition] })
        return consumer, filtered_tpl
      end

      return nil, nil
    end

    def current_subscription
      @config.subscriptions[@consumer_id_iterator.peek]
    end

    def reset_current_consumer
      @consumers[@consumer_id_iterator.peek] = nil
    end

    def maybe_select_next_consumer
      return unless @last_poll_read_nil_message
      @last_poll_read_nil_message = false
      select_next_consumer
    end

    def select_next_consumer
      @consumer_id_iterator.next
    end

    def commit_rescue_no_offset(consumer)
      consumer.commit(nil, !@config.synchronous_commits)
    rescue Rdkafka::RdkafkaError => e
      raise e if e.code != :no_offset
      @logger.debug "Nothing to commit."
    end

    def rdkafka_config(subscription)
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      config = {
        "auto.commit.interval.ms" => @config.offset_commit_interval * 1000,
        "auto.offset.reset"       => subscription.start_from_beginning ? "earliest" : "largest",
        "bootstrap.servers"       => @config.brokers.join(","),
        "client.id"               => @config.client_id,
        "enable.partition.eof"    => false,
        "fetch.max.bytes"         => @config.max_bytes,
        "message.max.bytes"       => subscription.max_bytes_per_partition,
        "fetch.min.bytes"         => @config.fetch_min_bytes,
        "fetch.wait.max.ms"       => @config.max_wait_time * 1000,
        "group.id"                => @config.group_id,
        "heartbeat.interval.ms"   => @config.heartbeat_interval * 1000,
        "max.poll.interval.ms"    => @config.max_poll_interval * 1000,
        "queued.min.messages"     => @config.min_message_queue_size,
        "session.timeout.ms"      => @config.session_timeout * 1000,
        "socket.timeout.ms"       => @config.socket_timeout * 1000,
        "statistics.interval.ms"  => 1000, # 1s is the highest granularity offered
      }
      config.merge! @config.rdkafka_consumer
      config.merge! subscription.additional_config
      config
    end

    def remaining_time_ms(limit_ms, started_at_time)
      r = limit_ms - ((Time.now - started_at_time)*1000).round
      r <= 0 ? 0 : r
    end
  end
end
