require "ostruct"

RSpec.describe Racecar::Pause do
  let(:pause) { described_class.new(timeout: 10) }

  before do
    Timecop.freeze(0)
  end

  after do
    Timecop.return
  end

  describe "#paused?" do
    it "returns true if we're paused" do
      pause.pause!

      expect(pause.paused?).to eq true
    end

    it "returns false if we're not paused" do
      pause.pause!
      pause.resume!

      expect(pause.paused?).to eq false
    end
  end

  describe "#expired?" do
    it "returns false on infinite timeout" do
      pause.instance_variable_set(:@timeout, nil)
      pause.pause!
      expect(pause.expired?).to eq false
    end

    it "returns false if no timeout was specified" do
      pause.pause!
      expect(pause.expired?).to eq false
    end

    it "returns false if the timeout has not yet passed" do
      pause.pause!

      Timecop.freeze(9)

      expect(pause.expired?).to eq false
    end

    it "returns true if the timeout has passed" do
      pause.pause!

      Timecop.freeze(10)

      expect(pause.expired?).to eq true
    end

    context "with exponential backoff" do
      it "doubles the timeout with each attempt" do
        pause.instance_variable_set(:@exponential_backoff, true)
        pause.pause!
        pause.resume!
        pause.pause!
        pause.resume!
        pause.pause!

        expect(pause.expired?).to eq false

        Timecop.freeze(10 + 20 + 40)

        expect(pause.expired?).to eq true
      end

      it "never pauses for more than the max timeout" do
        pause.instance_variable_set(:@exponential_backoff, true)
        pause.instance_variable_set(:@max_timeout, 30)

        pause.pause!
        pause.resume!
        pause.pause!
        pause.resume!
        pause.pause!
        pause.resume!
        pause.pause!

        Timecop.freeze(29)

        expect(pause.expired?).to eq false

        Timecop.freeze(1)

        expect(pause.expired?).to eq true
      end
    end
  end
end
