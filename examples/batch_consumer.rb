# frozen_string_literal: true

class BatchConsumer < Racecar::Consumer
  subscribes_to "messages", start_from_beginning: false

  def process_batch(messages)
    messages.each do |message|
      puts message.value
    end
  end
end
