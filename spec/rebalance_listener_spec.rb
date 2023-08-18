require "racecar/rebalance_listener"

RSpec.describe Racecar::RebalanceListener do
  subject(:listener) { Racecar::RebalanceListener.new(consumer_class, instrumenter) }

  let(:instrumenter) { Racecar::NullInstrumenter }
  let(:topic_partition_list) { double(:topic_partition_list, to_h: partitions_by_topic_hash) }
  let(:partitions_by_topic_hash) {
    { "topic_name" => [double(:partition, partition: 0, offset: 0, err: 0)] }
  }

  let(:consumer_class) { class_double(Racecar::Consumer, on_partitions_assigned: nil, on_partitions_revoked: nil) }
  let(:rdkafka_consumer) { double(:rdkafka_consumer) }

  before do
    listener.rdkafka_consumer = rdkafka_consumer
  end

  describe "#on_partitions_assigned" do
    it "calls the consumer class' callback with the topic partition list" do
      listener.on_partitions_assigned(topic_partition_list)

      expect(consumer_class).to have_received(:on_partitions_assigned)
        .with(
          partitions_by_topic: partitions_by_topic_hash,
          rdkafka_consumer: rdkafka_consumer
        )
      expect(consumer_class).not_to have_received(:on_partitions_revoked)
    end
  end

  describe "#on_partitions_revoked" do
    it "calls the consumer class' callback with the topic partition list" do
      listener.on_partitions_revoked(topic_partition_list)

      expect(consumer_class).to have_received(:on_partitions_revoked)
        .with(
          partitions_by_topic: partitions_by_topic_hash,
          rdkafka_consumer: rdkafka_consumer
        )
      expect(consumer_class).not_to have_received(:on_partitions_assigned)
    end
  end
end
