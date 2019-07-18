require 'spec_helper'

RSpec.describe Racecar::RailsConfigFileLoader do
  it "doesn't require rails when not present" do
    allow($stderr).to receive(:puts)

    described_class.load!

    expect($stderr).not_to have_received(:puts).with("=> Detected Rails, booting application...")
  end

  it "requires Rails when present" do
    $LOAD_PATH << File.dirname('./spec/support/rails')
    allow($stderr).to receive(:puts)

    described_class.load!

    expect($stderr).to have_received(:puts).with("=> Detected Rails, booting application...")
    $LOAD_PATH.pop
  end
end
