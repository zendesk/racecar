require "racecar/cli"

RSpec.describe Racecar::Cli do
  it "fails if no consumer class is specified" do
    expect { Racecar::Cli.main([]) }.to raise_exception(Racecar::Error, "no consumer specified")
  end
end
