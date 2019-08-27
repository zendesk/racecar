$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "racecar"
require "timecop"

RSpec.configure do |config|
  config.disable_monkey_patching!
end
