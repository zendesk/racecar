module Racecar
  class ConsumerSet
    MAX_POLL_TRIES = 10

    def initialize(config, logger, instrumenter = NullInstrumenter)
      @config, @logger = config, logger
      @instrumenter = instrumenter
      raise ArgumentError, "Subscriptions must not be empty when subscribing" if @config.subscriptions.empty?

      @consumers = []
      @consumer_id_iterator = (0...@config.subscriptions.size).cycle

      @last_poll_read_nil_message = false
    end

    def poll(timeout_ms)
      maybe_select_next_consumer
      started_at ||= Process.clock_gettime(Process::CLOCK_MONOTONIC)
      try ||= 0
      remain ||= timeout_ms

      msg = remain <= 0 ? nil : current.poll(remain)
    rescue Rdkafka::RdkafkaError => e
      wait_before_retry_ms = 100 * (2**try) # 100ms, 200ms, 400ms, …
      try += 1
      raise if try >= MAX_POLL_TRIES || remain <= wait_before_retry_ms

      @logger.error "(try #{try}): Error for topic subscription #{current_subscription}: #{e}"

      case e.code
      when :max_poll_exceeded, :transport # -147, -195
        reset_current_consumer
      end

      remain = remaining_time_ms(timeout_ms, started_at)
      raise if remain <= wait_before_retry_ms

      sleep wait_before_retry_ms/1000.0
      retry
    ensure
      @last_poll_read_nil_message = true if msg.nil?
    end

    # XXX: messages are not guaranteed to be from the same partition
    def batch_poll(timeout_ms)
      @batch_started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @messages = []
      while collect_messages_for_batch? do
        remain = remaining_time_ms(timeout_ms, @batch_started_at)
        break if remain <= 0
        msg = poll(remain)
        break if msg.nil?
        @messages << msg
      end
      @messages
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

    def collect_messages_for_batch?
      @messages.size < @config.fetch_messages &&
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) - @batch_started_at) < @config.max_wait_time
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
      r = limit_ms - ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at_time)*1000)
      r <= 0 ? 0 : r
    end
  end
end
