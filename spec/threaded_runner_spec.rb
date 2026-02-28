# frozen_string_literal: true

require "spec_helper"
require "racecar/threaded_runner"

RSpec.describe Racecar::ThreadedRunner do
  let(:config) do
    Racecar::Config.new.tap do |c|
      c.subscriptions = subscriptions
      c.group_id = "test-group"
      c.threaded = true
      c.dynamic_partition_scaling = dynamic_partition_scaling
    end
  end
  let(:subscriptions) { [Racecar::Consumer::Subscription.new("test-topic", true, 1048576, {})] }
  let(:dynamic_partition_scaling) { false }
  let(:logger) { Logger.new(StringIO.new) }
  let(:instrumenter) { Racecar::NullInstrumenter }

  let(:consumer_class) do
    Class.new(Racecar::Consumer) do
      subscribes_to "test-topic"

      def process(message)
      end
    end
  end

  let(:threaded_runner) do
    described_class.new(
      consumer_class: consumer_class,
      config: config,
      logger: logger,
      instrumenter: instrumenter
    )
  end

  def stub_runners_with_stop
    runners = []
    allow(Racecar::Runner).to receive(:new).and_wrap_original do |method, *args, **kwargs|
      method.call(*args, **kwargs).tap do |runner|
        runners << runner
        allow(runner).to receive(:run) do
          sleep 0.1 until runner.instance_variable_get(:@stop_requested)
        end
      end
    end
    runners
  end

  describe "#run" do
    context "with a single topic" do
      before do
        allow(threaded_runner).to receive(:fetch_partition_counts).and_return({ "test-topic" => 3 })
      end

      it "creates one runner per partition and joins all threads" do
        runners = []
        allow(Racecar::Runner).to receive(:new).and_wrap_original do |method, *args, **kwargs|
          method.call(*args, **kwargs).tap do |runner|
            runners << runner
            allow(runner).to receive(:run) { sleep 0.05 }
            allow(runner).to receive(:stop)
          end
        end

        threaded_runner.run

        expect(runners.size).to eq(3)
      end

      it "stops all runners when stop is called" do
        runners = stub_runners_with_stop

        Thread.new do
          sleep 0.1
          threaded_runner.stop
        end

        threaded_runner.run

        runners.each do |runner|
          expect(runner.instance_variable_get(:@stop_requested)).to eq(true)
        end
      end
    end

    context "with multiple topics" do
      let(:subscriptions) do
        [
          Racecar::Consumer::Subscription.new("topic-a", true, 1048576, {}),
          Racecar::Consumer::Subscription.new("topic-b", true, 1048576, {}),
        ]
      end

      before do
        allow(threaded_runner).to receive(:fetch_partition_counts).and_return({
          "topic-a" => 3,
          "topic-b" => 5,
        })
      end

      it "uses the max partition count across topics" do
        runners = []
        allow(Racecar::Runner).to receive(:new).and_wrap_original do |method, *args, **kwargs|
          method.call(*args, **kwargs).tap do |runner|
            runners << runner
            allow(runner).to receive(:run) { sleep 0.05 }
            allow(runner).to receive(:stop)
          end
        end

        threaded_runner.run

        expect(runners.size).to eq(5)
      end
    end

    context "when a thread raises an error" do
      before do
        allow(threaded_runner).to receive(:fetch_partition_counts).and_return({ "test-topic" => 2 })
      end

      it "propagates the error after stopping all threads" do
        call_count = 0
        allow(Racecar::Runner).to receive(:new).and_wrap_original do |method, *args, **kwargs|
          method.call(*args, **kwargs).tap do |runner|
            current_count = (call_count += 1)
            allow(runner).to receive(:run) do
              if current_count == 1
                raise RuntimeError, "Thread crashed"
              else
                sleep 0.1 until runner.instance_variable_get(:@stop_requested)
              end
            end
            allow(runner).to receive(:stop) do
              runner.instance_variable_set(:@stop_requested, true)
            end
          end
        end

        expect { threaded_runner.run }.to raise_error(RuntimeError, "Thread crashed")
      end
    end

    context "when partition count is 0" do
      before do
        mock_consumer = double("rdkafka_consumer")
        mock_native_kafka = double("native_kafka")
        mock_config = double("rdkafka_config", consumer: mock_consumer)

        allow(Rdkafka::Config).to receive(:new).and_return(mock_config)
        allow(mock_consumer).to receive(:instance_variable_get).with(:@native_kafka).and_return(mock_native_kafka)
        allow(mock_native_kafka).to receive(:with_inner).and_yield(double("inner"))
        allow(Rdkafka::Metadata).to receive(:new).and_return(double(topics: [{ partition_count: 0 }]))
        allow(mock_consumer).to receive(:close)
      end

      it "raises an error" do
        expect { threaded_runner.run }.to raise_error(Racecar::Error, /Could not discover partitions/)
      end
    end
  end

  describe "dynamic_partition_scaling" do
    let(:dynamic_partition_scaling) { true }

    before do
      stub_const("Racecar::ThreadedRunner::METADATA_POLL_INTERVAL", 0.1)
    end

    it "spawns additional threads when partition count increases" do
      call_count = 0
      allow(threaded_runner).to receive(:fetch_partition_counts) do
        call_count += 1
        if call_count <= 1
          { "test-topic" => 2 }
        else
          { "test-topic" => 4 }
        end
      end

      runners = []
      allow(Racecar::Runner).to receive(:new).and_wrap_original do |method, *args, **kwargs|
        method.call(*args, **kwargs).tap do |runner|
          runners << runner
          allow(runner).to receive(:run) do
            sleep 0.1 until runner.instance_variable_get(:@stop_requested)
          end
        end
      end

      Thread.new do
        sleep 0.5
        threaded_runner.stop
      end

      threaded_runner.run

      expect(runners.size).to eq(4)
    end

    it "does not spawn threads when partition count stays the same" do
      allow(threaded_runner).to receive(:fetch_partition_counts).and_return({ "test-topic" => 2 })

      runners = []
      allow(Racecar::Runner).to receive(:new).and_wrap_original do |method, *args, **kwargs|
        method.call(*args, **kwargs).tap do |runner|
          runners << runner
          allow(runner).to receive(:run) do
            sleep 0.1 until runner.instance_variable_get(:@stop_requested)
          end
        end
      end

      Thread.new do
        sleep 0.5
        threaded_runner.stop
      end

      threaded_runner.run

      expect(runners.size).to eq(2)
    end

    it "handles metadata poll failures gracefully" do
      call_count = 0
      allow(threaded_runner).to receive(:fetch_partition_counts) do
        call_count += 1
        raise "Connection refused" if call_count > 1
        { "test-topic" => 2 }
      end

      runners = stub_runners_with_stop

      Thread.new do
        sleep 0.5
        threaded_runner.stop
      end

      expect { threaded_runner.run }.not_to raise_error
      expect(runners.size).to eq(2)
    end
  end

  describe "#running?" do
    it "returns false when not started" do
      expect(threaded_runner.running?).to eq(false)
    end
  end
end
