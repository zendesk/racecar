require "ostruct"
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

  describe "#load_consumer_class" do
    let(:consumer_class) {
      OpenStruct.new(group_id: nil, name: "DoStuffConsumer", subscriptions: [])
    }

    it "sets the group id if one has been explicitly defined" do
      consumer_class.group_id = "fiddle"

      config.load_consumer_class(consumer_class)

      expect(config.group_id).to eq "fiddle"
    end

    it "defaults the group id to a dasherized version of the class name with the prefix" do
      config.group_id_prefix = "my-app."

      config.load_consumer_class(consumer_class)

      expect(config.group_id).to eq "my-app.do-stuff-consumer"
    end

    it "sets the subscriptions to the ones defined on the consumer class" do
      consumer_class.subscriptions = ["one", "two"]

      config.load_consumer_class(consumer_class)

      expect(config.subscriptions).to eq ["one", "two"]
    end

    it "doesn't override existing values if the consumer hasn't specified anything" do
      consumer_class.max_wait_time = nil
      consumer_class.group_id = nil

      config.max_wait_time = 10
      config.group_id = "cats"
      config.load_consumer_class(consumer_class)

      expect(config.max_wait_time).to eq 10
      expect(config.group_id).to eq "cats"
    end
  end

  describe "#validate!" do
    it "raises an exception if required variables are not set" do
      expect {
        config.validate!
      }.to raise_exception(RuntimeError, "required configuration key `brokers` not defined")

      config.load(brokers: ["a", "b", "c"])

      expect { config.validate! }.not_to raise_exception
    end
  end
end
