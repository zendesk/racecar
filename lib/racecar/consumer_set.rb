module Racecar
  class ConsumerSet
    def initialize(config, logger)
      @config, @logger = config, logger
      raise ArgumentError, "Subscriptions must not be empty when subscribing" if @config.subscriptions.empty?

      @consumers = []
      @consumer_id_iterator = (0...@config.subscriptions.size).cycle

      @last_poll_read_partition_eof = false
      @last_poll_read_nil_message = false
    end

    def poll(timeout_ms)
      maybe_select_next_consumer
      retried ||= false
      @last_poll_read_partition_eof = false
      msg = current.poll(timeout_ms)
    rescue Rdkafka::RdkafkaError => e
      @logger.error "Error for topic subscription #{current_subscription}: #{e}"

      case e.code
      when :max_poll_exceeded, :"err_-147?" # Note: requires unreleased librdkafka version
        reset_current_consumer
        raise if retried
        retried = true
        retry
      when :partition_eof
        @last_poll_read_partition_eof = true
        msg = nil
      else
        raise
      end
    ensure
      @last_poll_read_nil_message = true if msg.nil?
    end

    def batch_poll(timeout_ms)
      @batch_started_at = Time.now
      @messages = []
      while collect_messages_for_batch? do
        msg = poll(timeout_ms)
        break if msg.nil?
        @messages << msg
      end
      @messages
    end

    def last_poll_read_partition_eof?
      @last_poll_read_partition_eof
    end

    def store_offset(message)
      current.store_offset(message)
    end

    def commit
      each do |consumer|
        commit_rescue_no_offset(consumer)
      end
    end

    def close
      each(&:close)
    end

    def current
      @consumers[@consumer_id_iterator.peek] ||= begin
        consumer = Rdkafka::Config.new(rdkafka_config(current_subscription)).consumer
        consumer.subscribe current_subscription.topic
        consumer
      end
    end

    def each
      if block_given?
        @consumers.each { |c| yield c }
      else
        @consumers.each
      end
    end

    private

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
      (Time.now - @batch_started_at) < @config.max_wait_time
    end

    def rdkafka_config(subscription)
      # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
      config = {
        "auto.commit.interval.ms" => @config.offset_commit_interval * 1000,
        "auto.offset.reset"       => subscription.start_from_beginning ? "earliest" : "largest",
        "bootstrap.servers"       => @config.brokers.join(","),
        "client.id"               => @config.client_id,
        "enable.partition.eof"    => true,
        "fetch.max.bytes"         => @config.max_bytes,
        "fetch.message.max.bytes" => subscription.max_bytes_per_partition,
        "fetch.wait.max.ms"       => @config.max_wait_time * 1000,
        "group.id"                => @config.group_id,
        "heartbeat.interval.ms"   => @config.heartbeat_interval * 1000,
        "queued.min.messages"     => @config.min_message_queue_size,
        "session.timeout.ms"      => @config.session_timeout * 1000,
        "socket.timeout.ms"       => @config.socket_timeout * 1000,
        "statistics.interval.ms"  => 1000, # 1s is the highest granularity offered
      }
      config.merge! @config.rdkafka_consumer
      config.merge! subscription.additional_config
      config
    end
  end
end
