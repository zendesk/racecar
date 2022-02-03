# frozen_string_literal: true

require "stringio"

def subscription(name)
  Racecar::Consumer::Subscription.new(name, true, 1048576, {})
end

def tpl(subscription)
  Rdkafka::Consumer::TopicPartitionList.new.tap do |tpl|
    tpl.add_topic(subscription.topic, 1)
  end
end

RSpec.describe Racecar::ConsumerSet do
  let(:config)              { Racecar::Config.new }
  let(:rdconsumer)          { double("rdconsumer", subscribe: true) }
  let(:rdconfig)            { double("rdconfig", consumer: rdconsumer) }
  let(:logger)              { Logger.new(StringIO.new) }
  let(:instrumenter)        { Racecar::NullInstrumenter }
  let(:consumer_set)        { Racecar::ConsumerSet.new(config, logger, instrumenter) }
  let(:max_poll_exceeded_error) { Rdkafka::RdkafkaError.new(-147) }
  let(:not_coordinator_error) { Rdkafka::RdkafkaError.new(16) }

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
      expect { consumer_set }.to raise_error(ArgumentError)
    end
  end

  context "A consumer with subscription" do
    let(:subscriptions) { [ subscription("greetings") ] }

    it "subscribes to a topic upon first use" do
      allow(rdconsumer).to receive(:subscribe)

      consumer_set
      expect(rdconsumer).not_to have_received(:subscribe)

      consumer_set.current
      expect(rdconsumer).to have_received(:subscribe).with("greetings")
    end

    context "which is subscribed" do
      before { consumer_set; consumer_set.current }

      describe "pause and resume" do
        before do
          allow(rdconsumer).to receive(:assignment).and_return(tpl(subscriptions.first))
        end

        it "#pause allows to pause known partitions" do
          expect(rdconsumer).to receive(:pause) do |tpl|
            expect(tpl.count).to eq 1
            expect(tpl).to be_kind_of Rdkafka::Consumer::TopicPartitionList
          end
          expect(rdconsumer).to receive(:seek)
          consumer_set.pause("greetings", 0, 123456)
        end

        it "#pause doesn't pause unknown partitions" do
          expect(rdconsumer).not_to receive(:pause)
          expect(rdconsumer).not_to receive(:seek)

          consumer_set.pause("greetings", 1, 123456)
        end

        it "#pause seeks to given offset" do
          allow(rdconsumer).to receive(:pause)
          expect(rdconsumer).to receive(:seek) do |msg|
            expect(msg.offset).to eq 123456
          end
          consumer_set.pause("greetings", 0, 123456)
        end

        it "#resume allows to resume known partitions" do
          expect(rdconsumer).to receive(:resume) do |tpl|
            expect(tpl.count).to eq 1
            expect(tpl).to be_kind_of Rdkafka::Consumer::TopicPartitionList
          end
          consumer_set.resume("greetings", 0)
        end

        it "#resume doesn't resume unknown partitions" do
          expect(rdconsumer).not_to receive(:resume)
          consumer_set.resume("greetings", 1)
        end
      end

      describe "#poll" do
        before do
          Timecop.freeze
          allow(consumer_set).to receive(:sleep) do |val|
            Timecop.freeze(Time.now + val)
          end
        end
        after { Timecop.return }

        it "forwards to Rdkafka" do
          expect(rdconsumer).to receive(:poll).once.with(100).and_return(:message)
          expect(consumer_set.poll(100)).to be :message
        end

        it "returns nil on end of partition" do
          allow(rdconsumer).to receive(:poll).and_return(nil)
          expect(consumer_set.poll(100)).to be nil
        end

        it "retries with exponential backoff" do
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          allow(rdconsumer).to receive(:subscription)

          expect { consumer_set.poll(2**31) }.to raise_error(Rdkafka::RdkafkaError)

          expect(consumer_set).to have_received(:sleep).ordered.with(0.1)
          expect(consumer_set).to have_received(:sleep).ordered.with(0.2)
          expect(consumer_set).to have_received(:sleep).ordered.with(0.4)
          expect(consumer_set).to have_received(:sleep).ordered.with(0.8)
          expect(consumer_set).to have_received(:sleep).ordered.with(1.6)
          expect(consumer_set).to have_received(:sleep).ordered.with(3.2)
          expect(consumer_set).to have_received(:sleep).ordered.with(6.4)
          expect(consumer_set).to have_received(:sleep).ordered.with(12.8)
          expect(consumer_set).to have_received(:sleep).ordered.with(25.6)
          expect(rdconsumer).to have_received(:poll).exactly(Racecar::ConsumerSet::MAX_POLL_TRIES).times
        end

        it "instruments errors" do
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          allow(rdconsumer).to receive(:subscription)
          allow(instrumenter).to receive(:instrument).and_call_original

          expect { consumer_set.poll(2**31) }.to raise_error(Rdkafka::RdkafkaError)

          expect(instrumenter).to have_received(:instrument).with("poll_retry",
            try: kind_of(Integer),
            rdkafka_time_limit: kind_of(Integer),
            exception: kind_of(Rdkafka::RdkafkaError)
          ).exactly(Racecar::ConsumerSet::MAX_POLL_TRIES).times
        end

        it "retries over multiple calls" do
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          allow(rdconsumer).to receive(:subscription)

          # -2 because the first call tries twice, since we are freezing time
          (Racecar::ConsumerSet::MAX_POLL_TRIES - 2).times { consumer_set.poll(50) }
          expect { consumer_set.poll(50) }.to raise_error(Rdkafka::RdkafkaError)

          expect(rdconsumer).to have_received(:poll).exactly(Racecar::ConsumerSet::MAX_POLL_TRIES).times
        end

        it "skips retries if rescue block was too slow" do
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          allow(logger).to receive(:error) do
            Timecop.freeze(Time.now + 1)
          end

          expect(consumer_set.poll(1000)).to eq nil
          expect(logger).to have_received(:error).with(/Will retry on next call/)
        end
      end

      describe "#batch_poll" do
        it "honors timeout on subsequent polls" do
          Timecop.freeze do
            allow(consumer_set).to receive(:poll_current_consumer) do
              Timecop.freeze(Time.now + 0.1)
              :fake_msg
            end

            consumer_set.batch_poll(150)

            expect(consumer_set).to have_received(:poll_current_consumer).ordered.with(150)
            expect(consumer_set).to have_received(:poll_current_consumer).ordered.with(50)
            expect(consumer_set).to have_received(:poll_current_consumer).twice
          end
        end

        it "does not report errors for zero time remain edge cases" do
          Timecop.freeze do
            allow(logger).to receive(:error)
            allow(consumer_set).to receive(:remaining_time_ms).and_return(0)

            consumer_set.batch_poll(150)

            expect(logger).not_to have_received(:error)
          end
        end

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

        it "returns messages until nil is encountered" do
          config.fetch_messages = 3
          allow(rdconsumer).to receive(:poll).and_return(:msg1, :msg2, nil, :msg3)
          expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
        end

        it "eventually reads all messages" do
          config.fetch_messages = 1
          messages = [:msg1, :msg2, nil, nil, :msgN]
          allow(rdconsumer).to receive(:poll, &message_generator(messages))

          polled = []
          messages.size.times do
            polled += consumer_set.batch_poll(100) rescue []
          end
          expect(polled).to eq [:msg1, :msg2, :msgN]
        end

        it "raises other Rdkafka errors" do
          allow(consumer_set).to receive(:sleep)
          allow(rdconsumer).to receive(:poll).and_raise(Rdkafka::RdkafkaError, 10) # msg_size_too_large
          allow(rdconsumer).to receive(:subscription)
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
    let(:rdconsumer1)   { double("rdconsumer_feature", subscribe: true, assignment: tpl(subscriptions[0])) }
    let(:rdconsumer2)   { double("rdconsumer_profile", subscribe: true, assignment: tpl(subscriptions[1])) }
    let(:rdconsumer3)   { double("rdconsumer_account", subscribe: true, assignment: tpl(subscriptions[2])) }

    before do
      allow(rdconfig).to receive(:consumer).and_return(rdconsumer1, rdconsumer2, rdconsumer3)
    end

    it ".new subscribes to all topics" do
      expect(rdconsumer1).to receive(:subscribe).with("feature")
      expect(rdconsumer2).to receive(:subscribe).with("profile")
      expect(rdconsumer3).to receive(:subscribe).with("account")

      3.times do
        consumer_set.current
        consumer_set.send(:select_next_consumer)
      end
    end

    it ".new subscribes lazily" do
      expect(rdconsumer1).to receive(:subscribe).with("feature")
      expect(rdconsumer2).to receive(:subscribe).never
      expect(rdconsumer3).to receive(:subscribe).never

      consumer_set.current
      consumer_set.send(:select_next_consumer)
    end

    it "#reset_current_consumer removes the reference to the Rdkafka consumer" do
      3.times do
        consumer_set.current
        consumer_set.send(:select_next_consumer)
      end
      consumer_set.send(:select_next_consumer)
      allow(rdconsumer2).to receive(:close)

      expect do
        consumer_set.send(:reset_current_consumer)
      end.to change {
        consumer_set.instance_variable_get(:@consumers)[1]
      }.from(rdconsumer2).to(nil)
    end

    it "#reset_current_consumer closes the Rdkafka consumer" do
      3.times do
        consumer_set.current
        consumer_set.send(:select_next_consumer)
      end
      consumer_set.send(:select_next_consumer)
      expect(rdconsumer2).to receive(:close).once
      consumer_set.send(:reset_current_consumer)
    end

    it "#current recreates resetted consumers" do
      3.times do
        consumer_set.current
        consumer_set.send(:select_next_consumer)
      end
      consumer_set.send(:select_next_consumer)
      allow(consumer_set.current).to receive(:close)
      consumer_set.send(:reset_current_consumer)

      expect(consumer_set.current).not_to be_nil
    end

    it "#current returns current rdkafka client" do
      expect(consumer_set.current).to be rdconsumer1
    end

    describe "pause and resume" do
      before do
        3.times do
          consumer_set.current
          consumer_set.send(:select_next_consumer)
        end
      end

      it "#pause pauses partition in right consumer" do
        expect(rdconsumer1).not_to receive(:pause)
        expect(rdconsumer1).not_to receive(:seek)
        expect(rdconsumer2).to receive(:pause).once
        expect(rdconsumer2).to receive(:seek).once
        consumer_set.pause("profile", 0, 1233456)
      end

      it "#pause doesn't pause unknown partitions" do
        expect(rdconsumer2).not_to receive(:pause)
        consumer_set.pause("profile", 1, 1233456)
      end

      it "#pause doesn't pause unknown topics" do
        expect(rdconsumer1).not_to receive(:pause)
        expect(rdconsumer2).not_to receive(:pause)
        expect(rdconsumer3).not_to receive(:pause)
        consumer_set.pause("unknowntopic", 0, 1233456)
      end

      it "#resume resumes partition in right consumer" do
        expect(rdconsumer3).to receive(:resume).once
        consumer_set.resume("account", 0)
      end

      it "#resume doesn't resume unknown partitions" do
        expect(rdconsumer3).not_to receive(:resume)
        consumer_set.resume("account", 1)
      end

      it "#resume doesn't resume unknown topics" do
        expect(rdconsumer1).not_to receive(:resume)
        expect(rdconsumer2).not_to receive(:resume)
        expect(rdconsumer3).not_to receive(:resume)
        consumer_set.resume("unknowntopic", 0)
      end
    end

    it "#poll retries upon max poll exceeded" do
      raised = false
      allow(rdconsumer1).to receive(:poll) do
        next nil if raised
        raised = true
        raise(max_poll_exceeded_error)
      end
      allow(rdconsumer2).to receive(:poll).and_return(nil)
      allow(rdconsumer3).to receive(:poll)
      allow(consumer_set).to receive(:reset_current_consumer)
      allow(consumer_set).to receive(:sleep)

      consumer_set.poll(200)
      consumer_set.poll(200)

      expect(consumer_set).to have_received(:reset_current_consumer).once
      expect(rdconsumer1).to have_received(:poll).twice
      expect(rdconsumer2).to have_received(:poll).once
      expect(rdconsumer3).not_to have_received(:poll)
    end

    it "#poll retries upon not coordinator error" do
      raised = false
      allow(rdconsumer1).to receive(:poll) do
        next nil if raised
        raised = true
        raise(not_coordinator_error)
      end
      allow(rdconsumer2).to receive(:poll).and_return(nil)
      allow(rdconsumer3).to receive(:poll)
      allow(consumer_set).to receive(:reset_current_consumer)
      allow(consumer_set).to receive(:sleep)

      consumer_set.poll(200)
      consumer_set.poll(200)

      expect(consumer_set).to have_received(:reset_current_consumer).once
      expect(rdconsumer1).to have_received(:poll).twice
      expect(rdconsumer2).to have_received(:poll).once
      expect(rdconsumer3).not_to have_received(:poll)
    end

    it "#poll changes rdkafka client after end of partition on next poll" do
      allow(rdconsumer1).to receive(:poll).and_return(nil)
      allow(rdconsumer2).to receive(:poll).and_return(nil)

      expect(consumer_set.poll(100)).to be nil
      expect(consumer_set.current).to be rdconsumer1

      consumer_set.poll(100)
      expect(consumer_set.current).to be rdconsumer2
    end

    it "#batch_poll changes rdkafka client after end of partition on next poll" do
      config.fetch_messages = 1000
      messages = [:msg1, :msg2, nil, :msgN]
      allow(rdconsumer1).to receive(:poll, &message_generator(messages))
      allow(rdconsumer2).to receive(:poll).and_return(nil)

      expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
      expect(consumer_set.current).to be rdconsumer1

      consumer_set.batch_poll(100)
      expect(consumer_set.current).to be rdconsumer2
    end

    it "#batch_poll changes rdkafka client after encountering a nil message on next poll" do
      config.fetch_messages = 1000
      messages = [:msg1, :msg2, nil, :msgN]
      allow(rdconsumer1).to receive(:poll, &message_generator(messages))
      allow(rdconsumer2).to receive(:poll).and_return(nil)

      expect(consumer_set.batch_poll(100)).to eq [:msg1, :msg2]
      expect(consumer_set.current).to be rdconsumer1

      consumer_set.batch_poll(100)
      expect(consumer_set.current).to be rdconsumer2
    end

    it "#batch_poll eventually reads all messages" do
      config.fetch_messages = 1
      messages = [:msg1, nil, nil, :msgN]
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
