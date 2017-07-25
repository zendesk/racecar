require "racecar/env_loader"

describe Racecar::EnvLoader do
  let(:env) { {} }
  let(:config) { double(:config, set: nil) }
  let(:loader) { Racecar::EnvLoader.new(env, config) }

  describe "#validate!" do
    it "raises ConfigError if an unknown RACECAR_* env variable is set" do
      env["RACECAR_HELLO"] = "world"

      expect {
        loader.validate!
      }.to raise_error(Racecar::ConfigError, "unknown config variable RACECAR_HELLO")
    end

    it "doesn't raise an error if all RACECAR_* env variables are valid" do
      env["RACECAR_HELLO"] = "world"

      loader.string(:hello)

      expect { loader.validate! }.not_to raise_error
    end
  end
end
