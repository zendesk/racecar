# frozen_string_literal: true

module Racecar
  class ConsumerSet
    MAX_POLL_TRIES = 10

    def initialize(config, logger, instrumenter = NullInstrumenter)
      @config, @logger = config, logger
      @instrumenter = instrumenter
      raise ArgumentError, "Subscriptions must not be empty when subscribing" if @config.subscriptions.empty?

      @consumers = []
      @consumer_id_iterator = (0...@config.subscriptions.size).cycle

      @previous_retries = 0

      @last_poll_read_nil_message = false
    end

    def poll(max_wait_time_ms = @config.max_wait_time_ms)
      batch_poll(max_wait_time_ms, 1).first
    end

    # batch_poll collects messages until any of the following occurs:
    # - max_wait_time_ms time has passed
    # - max_messages have been collected
    # - a nil message was polled (end of topic, Kafka stalled, etc.)
    #
    # The messages are from a single topic, but potentially from more than one partition.
    #
    # Any errors during polling are retried in an exponential backoff fashion. If an error
    # occurs, but there is no time left for a backoff and retry, it will return the
    # already collected messages and only retry on the next call.
    def batch_poll(max_wait_time_ms = @config.max_wait_time_ms, max_messages = @config.fetch_messages)
      started_at = Time.now
      remain_ms = max_wait_time_ms
      maybe_select_next_consumer
      messages = []

      while remain_ms > 0 && messages.size < max_messages
        remain_ms = remaining_time_ms(max_wait_time_ms, started_at)
        msg = poll_with_retries(remain_ms)
        break if msg.nil?
        messages << msg
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
    # backoff. The sleep time is capped by max_wait_time_ms. If there's enough time budget
    # left, it will retry before returning. If there isn't, the retry will only occur on
    # the next call. It tries up to MAX_POLL_TRIES before passing on the exception.
    def poll_with_retries(max_wait_time_ms)
      try ||= @previous_retries
      @previous_retries = 0
      started_at ||= Time.now
      remain_ms = remaining_time_ms(max_wait_time_ms, started_at)

      wait_ms = try == 0 ? 0 : 50 * (2**try) # 0ms, 100ms, 200ms, 400ms, …
      if wait_ms >= max_wait_time_ms && remain_ms > 1
        @logger.debug "Capping #{wait_ms}ms to #{max_wait_time_ms-1}ms."
        sleep (max_wait_time_ms-1)/1000.0
        remain_ms = 1
      elsif try == 0 && remain_ms == 0
        @logger.debug "No time remains for polling messages. Will try on next call."
        return nil
      elsif wait_ms >= remain_ms
        @logger.error "Only #{remain_ms}ms left, but want to wait for #{wait_ms}ms before poll. Will retry on next call."
        @previous_retries = try
        return nil
      elsif wait_ms > 0
        sleep wait_ms/1000.0
        remain_ms -= wait_ms
      end

      poll_current_consumer(remain_ms)
    rescue Rdkafka::RdkafkaError => e
      try += 1
      @instrumenter.instrument("poll_retry", try: try, rdkafka_time_limit: remain_ms, exception: e)
      @logger.error "(try #{try}/#{MAX_POLL_TRIES}): Error for topic subscription #{current_subscription}: #{e}"
      raise if try >= MAX_POLL_TRIES
      retry
    end

    # polls a message for the current consumer, handling any API edge cases.
    def poll_current_consumer(max_wait_time_ms)
      msg = current.poll(max_wait_time_ms)
    rescue Rdkafka::RdkafkaError => e
      case e.code
      when :max_poll_exceeded, :transport, :not_coordinator # -147, -195, 16
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
      current_consumer_id = @consumer_id_iterator.peek
      @logger.info "Resetting consumer with id: #{current_consumer_id}"

      consumer = @consumers[current_consumer_id]
      consumer.close unless consumer.nil?
      @consumers[current_consumer_id] = nil
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
        "fetch.min.bytes"         => @config.fetch_min_bytes,
        "fetch.wait.max.ms"       => @config.max_wait_time_ms,
        "group.id"                => @config.group_id,
        "heartbeat.interval.ms"   => @config.heartbeat_interval * 1000,
        "max.poll.interval.ms"    => @config.max_poll_interval * 1000,
        "queued.min.messages"     => @config.min_message_queue_size,
        "session.timeout.ms"      => @config.session_timeout * 1000,
        "socket.timeout.ms"       => @config.socket_timeout * 1000,
        "statistics.interval.ms"  => @config.statistics_interval_ms
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
