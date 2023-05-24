# frozen_string_literal: true

require "timecop"
require "timeout"
require "pry-byebug"

$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "racecar"

require_relative 'support/mock_env'
require_relative 'support/integration_helper'
Thread.abort_on_exception = true

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.include MockEnv
  config.include IntegrationHelper, type: :integration

  config.before do
    Racecar.config = Racecar::Config.new
  end
end
