# frozen_string_literal: true

require "racecar/cli"

RSpec.describe Racecar::Cli do
  # has to be reset between examples otherwise it caches across examples and causes unexpected behavior
  before do
    Racecar.config = nil
  end

  it "raise exception if no consumer class is specified" do
    expect { Racecar::Cli.main([]) }.to raise_exception(Racecar::Error, "no consumer specified")
  end

  context "displaying startup message" do
    before do
      expect($stderr).to receive(:puts).with(/Starting Racecar consumer CatConsumer/)
      expect($stderr).to receive(:puts).with(/Vro+m!/)
      expect($stderr).to receive(:puts).with(/Ctrl-C to shutdown consumer/)
    end

    it "doesn't start Rails if --without-rails option is specified" do
      args = ["--require", "./examples/cat_consumer.rb", "CatConsumer", "--without-rails"]

      allow(Racecar).to receive(:run)
      expect(Racecar::RailsConfigFileLoader).not_to receive(:load!)

      Racecar::Cli.main(args)
    end

    it "starts Rails if the --without-rails option is omitted" do
      args = ["--require", "./examples/cat_consumer.rb", "CatConsumer"]

      allow(Racecar).to receive(:run)
      expect(Racecar::RailsConfigFileLoader).to receive(:load!)

      Racecar::Cli.main(args)
    end
  end

  it "raises an exception if the required library fails to load" do
    args = ["--require", "./spec/support/bad_library.rb", "BadLibrary::BadConsumer"]
    expect($stderr).to_not receive(:puts)

    expect { Racecar::Cli.main(args) }.to raise_error(StandardError, "BadLibrary failed to load")
  end
end
