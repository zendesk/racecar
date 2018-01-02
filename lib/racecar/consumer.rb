module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :start_from_beginning, :max_bytes_per_partition, :responds_with,
                              :response_partition_key)

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
      # responds_with           - optional topic to which the return value of the #process or #process_batch method
      #                           is produced
      # response_partition_key  - optional string or method pointer (symbol) to use as the partition key for messages
      #                           produced, message or batch will be passed to method
      def subscribes_to(*topics, start_from_beginning: true, max_bytes_per_partition: 1048576, responds_with: nil,
                        response_partition_key: nil)
        topics.each do |topic|
          subscriptions << Subscription.new(topic, start_from_beginning, max_bytes_per_partition, responds_with,
                                            response_partition_key)
        end
      end
    end

    def teardown; end
  end
end
