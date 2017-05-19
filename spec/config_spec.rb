require "racecar/config"

describe Racecar::Config do
  let(:config) { Racecar::Config.new }

  it "uses the default config if no explicit value has been set" do
    expect(config.offset_commit_interval).to eq 10
  end

  describe "#load" do
    it "sets config variables" do
      data = {
        "client_id" => "fiddle",
      }

      config.load(data)

      expect(config.client_id).to eq "fiddle"
    end

    it "fails when an unsupported variable is passed" do
      data = {
        "hammer" => "nail",
      }

      expect { config.load(data) }.to raise_exception(RuntimeError)
    end
  end
end
