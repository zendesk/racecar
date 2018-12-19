module Racecar
  class ConsumerSet
    def initialize(config, logger)
      @config, @logger = config, logger
      @consumers = []
      @consumer_iterator = [].cycle
    end

    def subscribe
      raise ArgumentError, "Subscriptions must not be empty when subscribing" if @config.subscriptions.empty?
      @consumers = @config.subscriptions.map do |subscription|
        consumer = Rdkafka::Config.new(rdkafka_config(subscription)).consumer
        consumer.subscribe subscription.topic
        consumer
      end
      @consumer_iterator = @consumers.cycle
      @consumers
    end

    def poll(timeout_ms)
      current.poll(timeout_ms).tap do |msg|
        @consumer_iterator.next if msg.nil?
      end
    rescue Rdkafka::RdkafkaError => e
      raise unless e.is_partition_eof?
      @logger.debug "No more messages on these partition(s)."
      @consumer_iterator.next
      nil
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

    def commit
      each do |consumer|
        commit_rescue_no_offset(consumer)
      end
    end

    def close
      each(&:close)
    end

    def current
      @consumer_iterator.peek
    end

    def each
      if block_given?
        @consumers.each { |c| yield c }
      else
        @consumers.each
      end
    end

    private

    def commit_rescue_no_offset(consumer)
      consumer.commit(nil, !@config.synchonous_commits)
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
        "fetch.max.bytes"         => @config.max_bytes,
        "fetch.message.max.bytes" => subscription.max_bytes_per_partition,
        "fetch.wait.max.ms"       => @config.max_wait_time * 1000,
        "group.id"                => @config.group_id,
        "heartbeat.interval.ms"   => @config.heartbeat_interval * 1000,
        "queued.min.messages"     => @config.min_message_queue_size,
        "session.timeout.ms"      => @config.session_timeout * 1000,
        "socket.timeout.ms"       => @config.socket_timeout * 1000,
      }
      config.merge! @config.rdkafka_consumer
      config.merge! subscription.additional_config
      config
    end
  end
end
