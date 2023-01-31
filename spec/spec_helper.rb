# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "active_support"
require "racecar"
require "timecop"
require_relative 'support/mock_env'
require_relative 'support/integration_helper'

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.include MockEnv
  config.include IntegrationHelper, type: :integration

  config.after do
    IntegrationHelper.reset_signal_handlers_to_default
    IntegrationHelper.ensure_all_child_process_have_exited
  end
end

module PutsWithPIDAndThread
  def puts(arg)
    if arg.is_a?(Array)
      arg.each { |s| super(s) }
    else
      super "[#{Process.pid}] " + (Thread.current.name || Thread.current.inspect) + ":  " + arg
    end

    nil
  end
end

Object.prepend(PutsWithPIDAndThread)
$stderr.singleton_class.prepend(PutsWithPIDAndThread)
