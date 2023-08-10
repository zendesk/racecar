# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'racecar/version'

Gem::Specification.new do |spec|
  spec.name          = "racecar"
  spec.version       = Racecar::VERSION
  spec.authors       = ["Daniel Schierbeck", "Benjamin Quorning"]
  spec.email         = ["dschierbeck@zendesk.com", "bquorning@zendesk.com"]

  spec.summary       = "A framework for running Kafka consumers"
  spec.homepage      = "https://github.com/zendesk/racecar"
  spec.license       = "Apache License Version 2.0"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.required_ruby_version = '>= 2.6'

  spec.add_runtime_dependency "king_konf", "~> 1.0.0"
  spec.add_runtime_dependency "rdkafka",   "~> 0.13.0"

  spec.add_development_dependency "bundler", [">= 1.13", "< 3"]
  spec.add_development_dependency "pry-byebug"
  spec.add_development_dependency "rake", "> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "timecop"
  spec.add_development_dependency "dogstatsd-ruby", ">= 4.0.0", "< 6.0.0"
  spec.add_development_dependency "activesupport"
end
