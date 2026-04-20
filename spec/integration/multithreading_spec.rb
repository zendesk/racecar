# frozen_string_literal: true

require "racecar/cli"

RSpec.describe "multithreaded processing", type: :integration do
  let(:input_topic)  { generate_input_topic_name }
  let(:output_topic) { generate_output_topic_name }
  let(:group_id)     { generate_group_id }

  before do
    Racecar.config.multithreaded_processing_enabled = true
    Racecar.config.multithreaded_processing_max_queue_size = 100
    Racecar.config.max_wait_time = 0.1
  end

  context "threads are spawned and finished for a multipartitioned topic" do
    let(:topic_partitions) { 3 }
    let(:consumer_class)   { MultiPartitionedConsumer = echo_consumer_class }
    let!(:racecar_cli)     { Racecar::Cli.new([consumer_class.name.to_s]) }
    let(:input_messages) do
      9.times.map { |n| { payload: "message-#{n}", partition: n % topic_partitions } }
    end

    before { configure_consumer_class(consumer_class, partitions: topic_partitions) }

    it "processes all messages and uses a dedicated thread per partition" do
      start_racecar
      wait_for_assignments(1)
      publish_messages
      wait_for_messages

      expect(incoming_messages.count).to eq(input_messages.count)
      expect_one_thread_per_partition(topic_partitions)
    end
  end

  context "threads are spawned for multiple topics each with multiple partitions" do
    let(:topic_partitions)   { 2 }
    let(:second_input_topic) { generate_input_topic_name }
    let(:second_group_id)    { generate_group_id }
    let(:consumer_class)     { MultiTopicConsumer = echo_consumer_class }
    let(:consumers)          { [] }
    let(:input_messages) do
      4.times.map { |n| { payload: "message-#{n}", partition: n % topic_partitions } }
    end

    before do
      create_topic(topic: input_topic,        partitions: topic_partitions)
      create_topic(topic: second_input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic,       partitions: topic_partitions)
      consumer_class.output_topic = output_topic
      consumer_class.pipe_to_test = consumer_message_pipe
    end

    after { consumers.each(&:stop) }

    it "processes all messages across all topic-partition combinations" do
      start_consumer_for(input_topic,        group_id)
      start_consumer_for(second_input_topic, second_group_id)

      wait_for_assignments(2)
      publish_messages(topic: input_topic,        messages: input_messages)
      publish_messages(topic: second_input_topic, messages: input_messages)
      wait_for_messages(expected_message_count: input_messages.count * 2)

      expect(incoming_messages.count).to eq(input_messages.count * 2)
      expect(incoming_messages.map(&:payload))
        .to match_array((input_messages + input_messages).map { |m| m[:payload] })

      # Each runner spawns one thread per partition: 2 topics × 2 partitions = 4 distinct threads
      unique_thread_ids = incoming_messages.map { |m| m.headers.fetch("processed_by") }.uniq
      expect(unique_thread_ids.size).to eq(topic_partitions * 2)
    end
  end

  context "when a partition is revoked due to rebalancing" do
    let(:topic_partitions) { 2 }
    let(:message_count)    { 40 }
    let(:consumer_class)   { RebalancedMTConsumer = echo_consumer_class }
    let(:consumers)        { [] }
    let(:input_messages) do
      message_count.times.map { |n| { payload: "message-#{n}", partition: n % topic_partitions } }
    end

    before do
      Racecar.config.partition_assignment_strategy = "cooperative-sticky"
      Racecar.config.session_timeout    = 6
      Racecar.config.heartbeat_interval = 1.5
      Racecar.config.fetch_messages     = 1
      configure_consumer_class(consumer_class, partitions: topic_partitions)
      Racecar.config.load_consumer_class(consumer_class)
    end

    after { consumers.each(&:stop) }

    it "gracefully exits the revoked partition thread and finishes processing all messages" do
      start_consumer
      start_consumer

      wait_for_assignments(2)
      publish_messages
      wait_for_messages(expected_message_count: 5)

      # Stopping the second consumer triggers a rebalance; the first consumer takes over
      # all partitions and the thread for the revoked partition exits cleanly.
      revoked_consumer = consumers.last
      revoked_consumer.stop

      wait_for_messages(expected_message_count: message_count)

      expect_all_messages_processed

      revoked_threads = revoked_consumer.partition_processors.values.map(&:thread)
      expect(revoked_threads).not_to be_empty
      revoked_threads.each { |t| t.join(5) }
      expect(revoked_threads).to all(satisfy("be dead") { |t| !t.alive? })
    end
  end

  context "when processing raises an error" do
    let(:topic_partitions)  { 1 }
    let(:errors_captured)   { [] }
    let(:consumer_class) { RetryOnErrorConsumer = retry_on_error_consumer_class }
    let!(:racecar_cli)   { Racecar::Cli.new([consumer_class.name.to_s]) }
    let(:input_messages) { [{ payload: "retry-me", partition: 0 }] }

    before do
      Racecar.config.on_error { |error, _payload| errors_captured << error }
      Racecar.config.pause_timeout = 0.1
      configure_consumer_class(consumer_class, partitions: topic_partitions)
    end

    it "retries the failed message and eventually processes it successfully" do
      start_racecar
      publish_messages
      wait_for_messages

      expect(incoming_messages.count).to eq(1)
      expect(incoming_messages.first.payload).to eq("retry-me")
      expect(errors_captured).not_to be_empty
      expect(errors_captured.first.message).to eq("Simulated processing error")
    end
  end

  context "parallel workers each spawn threads for their assigned partitions" do
    let(:topic_partitions) { 15 }
    let(:workers_count)    { 3 }
    let(:consumer_class)   { ParallelWorkersConsumer = echo_consumer_class }
    let(:consumers)        { [] }
    let(:input_messages) do
      topic_partitions.times.map { |n| { payload: "message-#{n}", partition: n } }
    end

    before do
      configure_consumer_class(consumer_class, partitions: topic_partitions)
      Racecar.config.load_consumer_class(consumer_class)
    end

    after { consumers.each(&:stop) }

    it "each worker spawns one thread per assigned partition" do
      workers_count.times { start_consumer }
      wait_for_assignments(workers_count)
      publish_messages
      wait_for_messages

      expect_all_messages_processed

      # Each worker is assigned 5 partitions (15 / 3) and spawns one thread per partition
      threads_per_worker = consumers.map { |c| c.partition_processors.values.size }
      expect(threads_per_worker).to all(eq(topic_partitions / workers_count))

      # Threads across all workers are distinct — no two workers share a thread
      all_thread_ids = consumers.flat_map { |c| c.partition_processors.values.map(&:object_id) }
      expect(all_thread_ids.uniq.size).to eq(topic_partitions)
    end
  end

  context "consumer threads produce messages with correct payload, key, and partition" do
    let(:topic_partitions) { 3 }
    let(:consumer_class)   { MessageProducingConsumer = echo_consumer_class }
    let!(:racecar_cli)     { Racecar::Cli.new([consumer_class.name.to_s]) }
    let(:input_messages) do
      topic_partitions.times.flat_map do |partition|
        3.times.map { |n| { payload: "msg-p#{partition}-#{n}", partition: partition, key: "key-p#{partition}-#{n}" } }
      end
    end

    before { configure_consumer_class(consumer_class, partitions: topic_partitions) }

    it "preserves payload, key, and partition in produced messages" do
      start_racecar
      wait_for_assignments(1)
      publish_messages
      wait_for_messages

      expect_all_messages_processed

      # Keys are forwarded unchanged
      expect(incoming_messages.map(&:key))
        .to match_array(input_messages.map { |m| m[:key] })

      # Each message lands on the same partition it was consumed from
      expect(incoming_messages.map(&:partition).sort)
        .to eq(input_messages.map { |m| m[:partition] }.sort)

      # Each partition has a distinct producing thread
      expect_one_thread_per_partition(topic_partitions)
    end
  end

  context "partition is paused when the processing queue is full" do
    let(:topic_partitions) { 1 }
    let(:max_queue_size)   { 10 }
    let(:message_count)    { 30 }
    let(:processing_delay) { 0.1 }
    let(:consumers)        { [] }
    let(:consumer_class)   { BackpressureConsumer = slow_consumer_class }
    let(:input_messages) do
      message_count.times.map { |n| { payload: "message-#{n}", partition: 0 } }
    end

    before do
      Racecar.config.multithreaded_processing_max_queue_size = max_queue_size
      consumer_class.processing_delay = processing_delay
      configure_consumer_class(consumer_class, partitions: topic_partitions)
      Racecar.config.load_consumer_class(consumer_class)
    end

    after { consumers.each(&:stop) }

    it "pauses the partition when queue is full and resumes when queue drops below half capacity" do
      start_consumer
      wait_for_assignments(1)
      publish_messages

      runner    = consumers.first
      topic_key = "#{input_topic}/0"

      # Wait until the processing thread is spawned (first message has arrived)
      processor = nil
      wait_until { (processor = runner.partition_processors[topic_key])&.send(:queue)&.size.to_i > 0 }

      # The partition must be paused once the queue reaches max capacity
      wait_until { processor.send(:backpressure_paused).true? }
      expect(processor.send(:backpressure_paused).true?).to be true
      expect(processor.send(:queue).size).to be >= max_queue_size / 2

      # Once the queue drains below half capacity, the partition is automatically resumed
      wait_until(timeout: 30) { !processor.send(:backpressure_paused).true? }
      expect(processor.send(:backpressure_paused).true?).to be false
      expect(processor.send(:queue).size).to be < max_queue_size / 2

      wait_for_messages
      expect_all_messages_processed
    end
  end

  context "batch processing with multithreading" do
    let(:topic_partitions) { 3 }
    let(:consumer_class)   { BatchProcessingConsumer = echo_batch_consumer_class }
    let!(:racecar_cli)     { Racecar::Cli.new([consumer_class.name.to_s]) }
    let(:input_messages) do
      9.times.map { |n| { payload: "message-#{n}", partition: n % topic_partitions } }
    end

    before { configure_consumer_class(consumer_class, partitions: topic_partitions) }

    it "processes all batches and uses a dedicated thread per partition" do
      start_racecar
      wait_for_assignments(1)
      publish_messages
      wait_for_messages

      expect_all_messages_processed
      expect_one_thread_per_partition(topic_partitions)
    end
  end

  context "graceful shutdown drains all remaining queued messages before exiting" do
    let(:topic_partitions)  { 3 }
    let(:message_count)     { 9 }
    let(:processing_delay)  { 0.3 }
    let(:consumer_class)    { GracefulShutdownConsumer = slow_consumer_class }
    let(:consumers)         { [] }
    let(:input_messages) do
      message_count.times.map { |n| { payload: "message-#{n}", partition: n % topic_partitions } }
    end

    before do
      consumer_class.processing_delay = processing_delay
      configure_consumer_class(consumer_class, partitions: topic_partitions)
      Racecar.config.load_consumer_class(consumer_class)
    end

    after { consumers.each(&:stop) }

    it "processes all queued messages before shutting down" do
      start_consumer
      wait_for_assignments(1)
      publish_messages

      # Allow messages to be queued in worker threads while processing is slow
      sleep(processing_delay)

      # Trigger graceful shutdown while messages are still queued in threads
      consumers.first.stop

      wait_for_messages
      expect_all_messages_processed
    end
  end

  after do
    Object.send(:remove_const, :MultiPartitionedConsumer)   if defined?(MultiPartitionedConsumer)
    Object.send(:remove_const, :MultiTopicConsumer)         if defined?(MultiTopicConsumer)
    Object.send(:remove_const, :RebalancedMTConsumer)       if defined?(RebalancedMTConsumer)
    Object.send(:remove_const, :RetryOnErrorConsumer)       if defined?(RetryOnErrorConsumer)
    Object.send(:remove_const, :ParallelWorkersConsumer)    if defined?(ParallelWorkersConsumer)
    Object.send(:remove_const, :MessageProducingConsumer)   if defined?(MessageProducingConsumer)
    Object.send(:remove_const, :BackpressureConsumer)       if defined?(BackpressureConsumer)
    Object.send(:remove_const, :GracefulShutdownConsumer)   if defined?(GracefulShutdownConsumer)
    Object.send(:remove_const, :BatchProcessingConsumer)    if defined?(BatchProcessingConsumer)
  end
end
