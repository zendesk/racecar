require 'rdkafka'

class TimePeriodConsumer
  attr_reader :brokers, :group_id, :track

  def initialize(brokers:, group_id:)
    @brokers = brokers
    @group_id = group_id
    @track = {}
  end

  def consume(topic:, from:, to:)
    consumer.subscribe(topic)

    wait_for_assignment

    topic_assignments = consumer.assignment.to_h.fetch(topic)
    partitions_with_time = topic_assignments.map { |partition| [partition.partition, from] }

    tpl = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
      list.add_topic_and_partitions_with_offsets(
        topic, partitions_with_time
      )
    end

    offsets = consumer.offsets_for_times(tpl).to_h.fetch(topic)

    puts "Offsets at #{from}"
    pp offsets

    offsets.each do |partition|
      fake_msg = OpenStruct.new(topic: topic, partition: partition.partition, offset: partition.offset)
      puts "Seeking to #{partition}"
      consumer.seek(fake_msg)
    end

    consumer.each do |msg|
      p({ key: msg.key, offset: msg.offset, partition: msg.partition, timestamp: msg.timestamp })

      track[msg.partition] = true if msg.timestamp > to || msg.offset == -1

      if offsets.all? { |partition| track.key? partition.partition }
        puts 'Done with all msgs in the given time window'
        consumer.unsubscribe
        break
      end

      if track.key? msg.partition
        puts "skipping #{msg.key}"
      else
        yield msg
      end
    end
  ensure
    puts 'Closing consumer'
    consumer.close
  end

  private

  def wait_for_assignment
    10.times do
      break unless consumer.assignment.empty?

      puts 'sleeping'
      sleep 1
    end
  end

  def config
    { "bootstrap.servers": brokers.join(','), "group.id": group_id } # , debug: "cgrp,topic,fetch" }
  end

  def consumer
    @consumer ||= Rdkafka::Config.new(config).consumer
  end
end
