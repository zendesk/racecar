require "stringio"

def subscription(name)
  Racecar::Consumer::Subscription.new(name, true, 1048576, {})
end

describe Racecar::ConsumerSet do
  let(:config)              { Racecar::Config.new }
  let(:rdconsumer)          { double("rdconsumer", subscribe: true) }
  let(:rdconfig)            { double("rdconfig", consumer: rdconsumer) }
  let(:consumer_set)        { Racecar::ConsumerSet.new(config, Logger.new(StringIO.new)) }
  let(:partition_eof_error) { Rdkafka::RdkafkaError.new(-191) }

  def message_generator(messages)
    msgs = messages.dup
    proc do
      message = msgs.shift
      message.is_a?(StandardError) ? raise(message) : message
    end
  end

  before do
    allow(Rdkafka::Config).to receive(:new).and_return(rdconfig)
    allow(config).to receive(:subscriptions).and_return(subscriptions)
  end

  context "A consumer without subscription" do
    let(:subscriptions) { [] }

    it "raises an expeption" do
      expect { consumer_set.subscribe }.to raise_error(ArgumentError)
    end
  end

  context "A consumer with subscription" do
    let(:subscriptions) { [ subscription("greetings") ] }

    it "subscribes to a topic" do
      expect(rdconsumer).to receive(:subscribe).with("greetings")
      consumer_set.subscribe
    end

    context "which is subscribed" do
      before { consumer_set.subscribe }

      describe "#poll" do
        it "forwards to Rdkafka" do
          expect(rdconsumer).to receive(:poll).once.with(100).and_return(:message)
          expect(consumer_set.poll(100)).to be :message
        end

        it "returns nil on end of partition" do
          allow(rdconsumer).to receive(:poll).and_return(nil)
          expect(consumer_set.poll(100)).to be nil
        end

        it "passes through end of partition errors" do
          allow(rdconsumer).to receive(:poll).and_raise(partition_eof_error)
          expect { consumer_set.poll(100) }.to raise_error(partition_eof_error)
        end

        it "raises other Rdkafka errors" do
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          expect { consumer_set.poll(100) }.to raise_error(Rdkafka::RdkafkaError)
        end
      end

      describe "#batch_poll" do
        it "forwards to Rdkafka (as poll)" do
          config.fetch_messages = 3
          expect(rdconsumer).to receive(:poll).exactly(3).times.with(100).and_return(:msg1, :msg2, :msg3)
          expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2, :msg3]
        end

        it "returns remaining messages of current partition" do
          config.fetch_messages = 1000
          messages = [:msg1, :msg2, nil, :msgN]
          allow(rdconsumer).to receive(:poll, &message_generator(messages))

          expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
        end

        it "passes through end of partition errors" do
          config.fetch_messages = 1000
          allow(rdconsumer).to receive(:poll).and_raise(partition_eof_error)

          expect { consumer_set.batch_poll(100) }.to raise_error(partition_eof_error)
        end

        it "returns messages until nil is encountered" do
          config.fetch_messages = 3
          allow(rdconsumer).to receive(:poll).and_return(:msg1, :msg2, nil, :msg3)
          expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
        end

        it "eventually reads all messages" do
          config.fetch_messages = 1
          messages = [:msg1, :msg2, nil, nil, partition_eof_error, partition_eof_error,  :msgN]
          allow(rdconsumer).to receive(:poll, &message_generator(messages))

          polled = []
          messages.size.times do
            polled += consumer_set.batch_poll(100) rescue []
          end
          expect(polled).to eq [:msg1, :msg2, :msgN]
        end

        it "raises other Rdkafka errors" do
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          expect { consumer_set.batch_poll(100) }.to raise_error(Rdkafka::RdkafkaError)
        end
      end

      describe "#commit" do
        it "forwards to Rdkafka" do
          expect(rdconsumer).to receive(:commit).once
          consumer_set.commit
        end

        it "does not raise when there is nothing to commit" do
          expect(rdconsumer).to receive(:commit).once.and_raise(Rdkafka::RdkafkaError, -168) # no_offset
          consumer_set.commit
        end
      end

      describe "#close" do
        it "forwards to Rdkafka" do
          expect(rdconsumer).to receive(:close).once
          consumer_set.close
        end
      end

      describe "#current" do
        it "returns current rdkafka client" do
          expect(consumer_set.current).to be rdconsumer
        end
      end
    end
  end

  context "A consumer with multiple subscriptions" do
    let(:subscriptions) { [ subscription("feature"), subscription("profile"), subscription("account") ] }
    let(:rdconsumer1)   { double("rdconsumer_feature", subscribe: true) }
    let(:rdconsumer2)   { double("rdconsumer_profile", subscribe: true) }
    let(:rdconsumer3)   { double("rdconsumer_account", subscribe: true) }

    before do
      allow(rdconfig).to receive(:consumer).and_return(rdconsumer1, rdconsumer2, rdconsumer3)
    end

    it "subscribes to topics" do
      expect(rdconsumer1).to receive(:subscribe).with("feature")
      expect(rdconsumer2).to receive(:subscribe).with("profile")
      expect(rdconsumer3).to receive(:subscribe).with("account")
      consumer_set.subscribe
    end

    context "already setup" do
      before { consumer_set.subscribe }

      it "#current returns current rdkafka client" do
        expect(consumer_set.current).to be rdconsumer1
      end

      it "#poll changes rdkafka client on end of partition" do
        allow(rdconsumer1).to receive(:poll).and_return(nil)
        expect(consumer_set.poll(100)).to be nil
        expect(consumer_set.current).to be rdconsumer2
      end

      it "#poll changes rdkafka client when partition EOF is raised" do
        allow(rdconsumer1).to receive(:poll).and_raise(partition_eof_error)
        expect { consumer_set.poll(100) }.to raise_error(partition_eof_error)
        expect(consumer_set.current).to be rdconsumer2
      end

      it "#batch_poll changes rdkafka client on end of partition" do
        config.fetch_messages = 1000
        messages = [:msg1, :msg2, nil, :msgN]
        allow(rdconsumer1).to receive(:poll, &message_generator(messages))

        expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
        expect(consumer_set.current).to be rdconsumer2
      end

      it "#batch_poll changes rdkafka client when partition EOF is raised" do
        allow(rdconsumer1).to receive(:poll).and_raise(partition_eof_error)
        expect { consumer_set.batch_poll(100) }.to raise_error(partition_eof_error)
        expect(consumer_set.current).to be rdconsumer2
      end

      it "#batch_poll changes rdkafka client when encountering a nil message" do
        config.fetch_messages = 1000
        messages = [:msg1, :msg2, nil, :msgN]
        allow(rdconsumer1).to receive(:poll, &message_generator(messages))

        expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
        expect(consumer_set.current).to be rdconsumer2
      end

      it "#batch_poll eventually reads all messages" do
        config.fetch_messages = 1
        messages = [:msg1, nil, nil, partition_eof_error, partition_eof_error, :msgN]
        allow(rdconsumer1).to receive(:poll, &message_generator(messages))
        allow(rdconsumer2).to receive(:poll, &message_generator(messages))
        allow(rdconsumer3).to receive(:poll, &message_generator(messages))

        polled = []
        count = (messages.size+1)*3
        count.times { polled += consumer_set.batch_poll(100) rescue [] }
        expect(polled).to eq [:msg1, :msg1, :msg1, :msgN, :msgN, :msgN]
      end
    end
  end
end
