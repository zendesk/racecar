module Racecar
  class Consumer
    Subscription = Struct.new(:topic, :start_from_beginning)

    class << self
      attr_accessor :max_wait_time
      attr_accessor :group_id

      def subscriptions
        @subscriptions ||= []
      end

      # Adds one or more topic subscriptions.
      def subscribes_to(*topics, start_from_beginning: true)
        topics.each do |topic|
          subscriptions << Subscription.new(topic, start_from_beginning)
        end
      end
    end
  end
end
