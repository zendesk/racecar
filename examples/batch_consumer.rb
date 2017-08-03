class BatchConsumer < Racecar::Consumer
  subscribes_to "messages", start_from_beginning: false

  def process_batch(batch)
    batch.messages.each do |message|
      puts message.value
    end
  end
end
