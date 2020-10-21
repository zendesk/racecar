# frozen_string_literal: true

class ProducingConsumer < Racecar::Consumer
  subscribes_to "messages", start_from_beginning: false

  def process(message)
    value = message.value.reverse

    produce value, topic: "reverse-messages"
  end
end
