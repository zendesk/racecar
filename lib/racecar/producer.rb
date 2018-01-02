module Racecar
  module Producer
    class << self
      def produce(topic, message, options = {})
        DeliveryBoy.deliver(message, options.merge({ topic: topic }))
      end
    end
  end
end