# frozen_string_literal: true

require "racecar/liveness_probe"
require "active_support/notifications"

RSpec.describe Racecar::LivenessProbe do
  subject(:probe) { Racecar::LivenessProbe.new(message_bus, file_path_string, max_interval) }

  let(:file_path) { Pathname.new(file_path_string) }
  let(:file_path_string) { "/tmp/liveness-probe-test-#{SecureRandom.hex(4)}" }
  let(:max_interval) { 5 }
  let(:message_bus) { ActiveSupport::Notifications }

  after do
    ActiveSupport::Notifications.notifier = ActiveSupport::Notifications::Fanout.new
    FileUtils.rm_rf(file_path)
    Process.waitall
  end

  describe "#check_liveness_within_interval!" do
    context "when the file has just been touched" do
      before do
        FileUtils.touch(file_path_string)
      end

      it "does nothing, exits successfully" do
        probe.check_liveness_within_interval!
      end
    end

    context "when the file has not been touched since before max_interval seconds ago" do
      before do
        before_max_interval = Time.now - (max_interval + 1)
        FileUtils.touch(file_path_string, mtime: before_max_interval)
      end

      it "exits the process with error status 1" do
        expect(Process).to receive(:exit).with(1)

        probe.check_liveness_within_interval!
      end
    end
  end

  describe "#install" do
    context "when the file is writeable"  do
      it "touches the file on 'start_main_loop'" do
        probe.install

        expect { message_bus.publish("start_main_loop.racecar") }.to(change { file_path.exist? }.from(false).to(true))
      end

      it "deletes the file on 'shut_down'" do
        probe.install

        FileUtils.touch(file_path)

        expect { message_bus.publish("shut_down.racecar") }.to(change { file_path.exist? }.from(true).to(false))
      end
    end

    context "when the file is not writable"  do
      context "because the directory does not exist" do
        let(:file_path_string) { "directory-does-not-exist/liveness-file" }

        it "raises a helpful message" do
          expect { probe.install }.to(raise_error(/Liveness probe configuration error/))
        end
      end

      context "because the process does not have write permissions" do
        let(:file_path_string) { "/etc" }

        it "raises a helpful message" do
          expect { probe.install }.to(raise_error(/Liveness probe configuration error/))
        end
      end
    end
  end

  describe "#uninstall" do
    before { probe.install }

    it "stops the probe " do
      probe.uninstall

      expect(file_path).not_to exist
    end

    def trigger_probe
      message_bus.instrument("start_main_loop.racecar")
    end
  end
end
