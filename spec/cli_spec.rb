require "racecar/cli"

RSpec.describe Racecar::Cli do
  # has to be reset between examples otherwise it caches across examples and causes unexpected behavior
  before do
    Racecar.config = nil
  end

  it "fails if no consumer class is specified" do
    expect { Racecar::Cli.main([]) }.to raise_exception(Racecar::Error, "no consumer specified")
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

  it "outputs a helpful message and exits if the required library fails to load" do
    args = ["--require", "./spec/support/bad_library.rb", "BadLibrary::BadConsumer"]
    expected_output = "=> ./spec/support/bad_library.rb failed to load: BadLibrary failed to load\n"

    expect {
      Racecar::Cli.main(args)
    }.to raise_error(SystemExit).and output(expected_output).to_stderr
  end
end
