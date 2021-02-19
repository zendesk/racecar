# frozen_string_literal: true

require "spec_helper"

RSpec.describe Racecar::Signals do
  describe ".wait_for_shutdown" do
    before { described_class.setup_shutdown_handlers }

    let(:callback_mock) { double(:callback, cleanup: nil) }

    it "calls the callback after a signal is received" do
      Thread.new do
        sleep 0.2
        Process.kill(described_class::SHUTDOWN_SIGNALS.sample, 0)
      end

      expect(callback_mock).to receive(:cleanup).once

      described_class.wait_for_shutdown { callback_mock.cleanup }
    end

    context "when signal is sent more than once" do
      it "only calls the callback once" do
        Thread.new do
          sleep 0.2
          Process.kill(described_class::SHUTDOWN_SIGNALS.sample, 0)
          Process.kill(described_class::SHUTDOWN_SIGNALS.sample, 0)
          Process.kill(described_class::SHUTDOWN_SIGNALS.sample, 0)
          Process.kill(described_class::SHUTDOWN_SIGNALS.sample, 0)
        end

        described_class.wait_for_shutdown { callback_mock.cleanup }

        expect(callback_mock).to have_received(:cleanup).exactly(1).time
      end
    end
  end
end
