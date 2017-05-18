require "racecar/config"

describe Racecar::Config do
  describe "#load" do
    let(:config) { Racecar::Config.new }

    it "sets config variables" do
      data = {
        "client_id" => "fiddle",
      }

      config.load(data)

      expect(config.client_id).to eql "fiddle"
    end

    it "fails when an unsupported variable is passed" do
      data = {
        "hammer" => "nail",
      }

      expect { config.load(data) }.to raise_exception(RuntimeError)
    end
  end
end
