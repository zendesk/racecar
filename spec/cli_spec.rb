require "racecar/cli"

RSpec.describe Racecar::Cli do
  # has to be reset between examples otherwise it caches across examples and causes unexpected behavior
  before do
    Racecar.config = nil
  end

  it "fails if no consumer class is specified" do
    expect { Racecar::Cli.main([]) }.to raise_exception(Racecar::Error, "no consumer specified")
  end

  it "doesn't start Rails if --norrails option is specified" do
    args = ["--require", "./examples/cat_consumer.rb", "CatConsumer", "--norails"]

    allow(Racecar).to receive(:run)
    expect(Racecar::RailsConfigFileLoader).not_to receive(:load!)

    Racecar::Cli.main(args)
  end

  it "starts Rails if the --norails option is omitted" do
    args = ["--require", "./examples/cat_consumer.rb", "CatConsumer"]

    allow(Racecar).to receive(:run)
    expect(Racecar::RailsConfigFileLoader).to receive(:load!)

    Racecar::Cli.main(args)
  end
end
