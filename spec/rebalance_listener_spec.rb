require "racecar/rebalance_listener"

RSpec.describe Racecar::RebalanceListener do
  subject(:listener) { Racecar::RebalanceListener.new(config, instrumenter, partition_processors, runner_mutex) }

  let(:consumer_class) { class_double(Racecar::Consumer, on_partitions_assigned: nil, on_partitions_revoked: nil) }
  let(:instrumenter) { Racecar::NullInstrumenter }
  let(:rdkafka_consumer) { double(:rdkafka_consumer) }
  let(:rdkafka_topic_partition_list) { double(:rdkafka_topic_partition_list, to_h: partitions_by_topic_hash, empty?: false) }
  let(:partitions_by_topic_hash) {
    { "topic_name" => [double(:partition, partition: 0, offset: 0, err: 0)] }
  }
  let(:partition_processors) { { "topic_name/0" => double(:processor, :rebalancing= => true) } }
  let(:runner_mutex) { double(:runner_mutex) }
  let(:config) do
    c = Racecar::Config.new
    c.consumer_class = consumer_class
    c
  end

  before do
    listener.rdkafka_consumer = rdkafka_consumer
    allow(runner_mutex).to receive(:synchronize).and_yield
  end

  describe "#on_partitions_assigned" do
    it "calls the consumer class' callback passing the rebalance event" do
      listener.on_partitions_assigned(rdkafka_topic_partition_list)

      aggregate_failures do
        expect(consumer_class).to have_received(:on_partitions_assigned).with(an_instance_of(Racecar::RebalanceListener::Event)) do |rebalance_event|
          expect(rebalance_event.topic_name).to eq("topic_name")
          expect(rebalance_event.partition_numbers).to eq([0])
          expect(rebalance_event).not_to be_empty
        end

        expect(consumer_class).not_to have_received(:on_partitions_revoked)
      end
    end
  end

  describe "#on_partitions_revoked" do
    it "calls the consumer class' callback passing the rebalance event" do
      listener.on_partitions_revoked(rdkafka_topic_partition_list)

      aggregate_failures do
        expect(consumer_class).to have_received(:on_partitions_revoked).with(an_instance_of(Racecar::RebalanceListener::Event)) do |rebalance_event|
          expect(rebalance_event.topic_name).to eq("topic_name")
          expect(rebalance_event.partition_numbers).to eq([0])
          expect(rebalance_event).not_to be_empty
        end

        expect(consumer_class).not_to have_received(:on_partitions_assigned)
      end
    end
  end
end
