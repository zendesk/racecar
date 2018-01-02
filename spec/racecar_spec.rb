require "racecar/config"

describe Racecar do
  describe ".configure" do
    let!(:original_config) { Racecar.config }
    let(:new_brokers) { ["foo", "bar"] }

    before { allow(DeliveryBoy).to receive(:configure).and_call_original }
    after { Racecar.instance_variable_set(:@config, original_config) }

    it "configures DeliveryBoy as well" do
      Racecar.configure { |c| c.brokers = new_brokers }
      expect(DeliveryBoy).to have_received(:configure)
      expect(DeliveryBoy.config.brokers).to eq new_brokers
    end

    context "config.configure_delivery_boy set to false" do
      it "does not configure DeliveryBoy" do
        Racecar.configure do |c|
          c.brokers = new_brokers
          c.configure_delivery_boy = false
        end
        expect(DeliveryBoy).not_to have_received(:configure)
      end
    end
  end
end