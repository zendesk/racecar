# frozen_string_literal: true

require "racecar/cli"

RSpec.describe "forking", type: :integration do
  let!(:racecar_cli) { Racecar::Cli.new(["ForkingConsumer"]) }
  let(:input_topic) { generate_input_topic_name }
  let(:output_topic) { generate_output_topic_name }
  let(:group_id) { generate_group_id }

  let(:input_messages) do
    total_messages.times.map { |n| { payload: "message-#{n}", partition: n % topic_partitions } }
  end

  let(:total_messages) { messages_per_topic * topic_partitions }
  let(:topic_partitions) { 4 }
  let(:messages_per_topic) { 10 }
  let(:forks) { 2 }
  let(:consumer_class) { ForkingConsumer ||= echo_consumer_class }

  before do
    create_topic(topic: input_topic, partitions: topic_partitions)
    create_topic(topic: output_topic, partitions: topic_partitions)

    consumer_class.subscribes_to(input_topic)
    consumer_class.output_topic = output_topic
    consumer_class.group_id = group_id
    consumer_class.pipe_to_test = consumer_message_pipe

    Racecar.config.forks = forks
  end

  after do |test|
    Object.send(:remove_const, :ForkingConsumer) if defined?(ForkingConsumer)
  end

  it "each fork consumes messages in parallel" do
    start_racecar

    wait_for_assignments(forks)
    publish_messages
    wait_for_messages

    expect_equal_distribution_of_message_processing
    expect_processing_times_to_mostly_overlap
  end

  context "when the supervisor process receives TERM" do
    let(:messages_per_topic) { 1 }

    it "terminates the worker processes" do
      start_racecar

      wait_for_assignments(forks)
      publish_messages
      wait_for_messages

      Process.kill("TERM", supervisor_pid)
      Process.wait(supervisor_pid)

      expect_processes_to_have_exited(worker_pids)
    end
  end

  context "when the supervisor process is killed (SIGKILL)" do
    let(:messages_per_topic) { 1 }

    it "terminates the worker processes" do
      start_racecar

      wait_for_assignments(forks)
      publish_messages
      wait_for_messages

      Process.kill("KILL", supervisor_pid)
      Process.waitpid(supervisor_pid)

      expect_processes_to_have_exited(worker_pids)
    end
  end

  context "when a worker process exits" do
    let(:messages_per_topic) { 1 }

    it "terminates all other processes gracefully" do
      start_racecar

      wait_for_assignments(forks)
      publish_messages
      wait_for_messages

      Process.kill("KILL", worker_pids[0])
      Process.waitpid(supervisor_pid)

      expect_processes_to_have_exited(worker_pids)
    end
  end

  context "when prefork and postfork hooks have been set" do
    before do
      setup_hooks_and_message_pipe
    end

    let(:messages) { [] }

    it "executes the prefork hook before forking and postfork after forking" do
      start_racecar

      wait_for_fork_hook_messages

      prefork_message = messages.first
      postfork_messages = messages.drop(1)

      expect(prefork_message).to match(hash_including({
        "hook" => "prefork",
        "ppid" => Process.pid,
        "pid" => supervisor_pid,
      }))

      expect(postfork_messages).to match([
        hash_including({
          "hook" => "postfork",
          "ppid" => supervisor_pid,
        })
      ] * 2)
    end

    def wait_for_fork_hook_messages
      Timeout.timeout(15) do
        sleep 0.2 until messages.length == 3
      end
    end

    def raise_if_any_child_processes
      Timeout.timeout(0.01) { Process.waitall }
    rescue Timeout::Error
      raise "Expected no child processes but `Process.waitall` blocked."
    end

    def setup_hooks_and_message_pipe
      pipe = IntegrationHelper::JSONPipe.new

      Racecar.config.prefork = ->(*_) {
        pipe.write({hook: "prefork", pid: Process.pid, ppid: Process.ppid})
        raise_if_any_child_processes
      }

      Racecar.config.postfork = ->(*_) {
        pipe.write({hook: "postfork", pid: Process.pid, ppid: Process.ppid})
      }

      @hook_message_listener_thread = Thread.new do
        loop do
          messages << pipe.read
        end
      end
    end

    after do
      @hook_message_listener_thread&.terminate
    end
  end

  def expect_processes_to_have_exited(pids)
    any_running = pids.any? { |pid| process_running?(pid) }
    expect(any_running).to be false
  end

  def worker_pids
    messages_by_fork.keys.map(&:to_i)
  end

  def process_running?(pid)
    Process.waitpid(pid, Process::WNOHANG)
    true
  rescue Errno::ECHILD
    false
  end

  attr_accessor :supervisor_pid
  def start_racecar
    consumer_message_pipe

    self.supervisor_pid = fork do
      at_exit do
        nil
      end

      racecar_cli.run
    end
  end

  def stop_racecar
    Process.kill("TERM", supervisor_pid)
  rescue Errno::ESRCH
  end

  def expect_equal_distribution_of_message_processing
    expect(message_count_by_fork.values).to eq([20, 20])
  end

  def expect_processing_times_to_mostly_overlap
    expect(processing_time_intersection.size).to be_within(total_time*0.49).of(total_time)
  end

  def total_time
    processing_windows.map(&:size).max
  end

  def processing_windows
    processed_at_times_by_fork.values.map { |times|
      ms = times.map { |s| s.to_f * 1000 }
      (ms.min..ms.max)
    }
  end

  def processing_time_intersection
    range_intersection(*processing_windows)
  end

  def processed_at_times_by_fork
    messages_by_fork.transform_values { |ms|
      ms.map { |m| m.headers.fetch("processed_at") }
    }
  end

  def message_count_by_fork
    messages_by_fork.transform_values(&:count)
  end

  def messages_by_fork
    incoming_messages.group_by { |m| m.headers.fetch("pid") }
  end

  def range_intersection(range1, range2)
    # Determine the maximum of the lower bounds and the minimum of the upper bounds
    lower_bound = [range1.begin, range2.begin].max
    upper_bound = [range1.end, range2.end].min

    # Check if the ranges actually intersect
    if lower_bound < upper_bound || (lower_bound == upper_bound && range1.include?(upper_bound) && range2.include?(upper_bound))
      lower_bound...upper_bound
    else
      nil # or return an empty range, depending on your requirements
    end
  end
end
