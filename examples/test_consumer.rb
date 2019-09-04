class TestConsumer < Racecar::Consumer
  subscribes_to "test-tokens"

  def initialize
    @logger = Logger.new(STDOUT)
    @token_path = "/token"
  end

  def process(message)
    token = message.payload

    @logger.info "Got #{token}"

    File.open(token_path, "w") do |file|
      file << token
    end
  end
end
