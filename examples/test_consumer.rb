class TestConsumer < Racecar::Consumer
  subscribes_to "test-tokens"

  def initialize
    @logger = Logger.new(STDOUT)
    @base_dir = ENV.fetch("RESULTS_DIR", "/")
  end

  def process(message)
    token = message.payload
    token_path = [@base_dir, token].join("/")

    @logger.info "Got #{token}"

    File.open(token_path, "w") do |file|
      file << "OK"
    end
  end
end
