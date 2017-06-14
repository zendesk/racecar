class CatConsumer
  extend Racecar::Consumer

  subscribes_to "messages", start_from_beginning: false

  def process(message)
    puts message.value
  end
end
