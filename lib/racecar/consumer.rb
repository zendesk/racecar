module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :start_from_beginning, :max_bytes_per_partition)

    class << self
      attr_accessor :max_wait_time
      attr_accessor :group_id

      def subscriptions
        @subscriptions ||= []
      end

      # Adds one or more topic subscriptions.
      #
      # start_from_beginning    - whether to start from the beginning or the end of each
      #                           partition.
      # max_bytes_per_partition - the maximum number of bytes to fetch from each partition
      #                           at a time.
      def subscribes_to(*topics, start_from_beginning: true, max_bytes_per_partition: 1048576)
        topics.each do |topic|
          subscriptions << Subscription.new(topic, start_from_beginning, max_bytes_per_partition)
        end
      end
    end

    def teardown; end
  end
end
