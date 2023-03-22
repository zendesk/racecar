# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

# Pushing to rubygems is handled by a github workflow
ENV["gem_push"] = "false"

RSpec::Core::RakeTask.new(:spec)

task :default => :spec
