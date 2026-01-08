# frozen_string_literal: true

class DlqConsumer < Racecar::Consumer
  subscribes_to "messages", start_from_beginning: false
  dead_letter_queue topic: "dlq"

  def process(message)
    # simulate failure
    if message.key == 'key-10'
      raise "oops"
    end

    puts message.value
  end
end
