# frozen_string_literal: true

RSpec.describe Racecar::OAuthBearer do
  let(:rdkafka_config) do
    Rdkafka::Config.new(
      "bootstrap.servers" => "localhost:1",
      "security.protocol" => "sasl_plaintext",
      "sasl.mechanism" => "OAUTHBEARER",
      "group.id" => "oauth-bearer-spec",
    )
  end
  let(:token) do
    {
      token: "a-token",
      lifetime_ms: (Time.now.to_i + 900) * 1000,
      principal_name: "kafka-cluster",
    }
  end
  let(:calls) { [] }
  let(:token_provider) { -> { calls << :called; token } }
  let(:clients) { [] }

  after do
    clients.each { |client| described_class.close_client(client) }
    Rdkafka::Config.oauthbearer_token_refresh_callback = nil
  end

  def create_client(type, provider = token_provider)
    described_class.create_client(rdkafka_config, type, provider).tap { |client| clients << client }
  end

  describe ".create_client" do
    context "without a token provider" do
      it "builds the client as before" do
        rdkafka_config = instance_double(Rdkafka::Config)
        producer = instance_double(Rdkafka::Producer)
        allow(rdkafka_config).to receive(:producer).with(no_args).and_return(producer)

        expect(described_class.create_client(rdkafka_config, :producer, nil)).to be(producer)
      end

      it "leaves the rdkafka refresh callback alone" do
        rdkafka_config = instance_double(Rdkafka::Config, producer: instance_double(Rdkafka::Producer))

        described_class.create_client(rdkafka_config, :producer, nil)

        expect(Rdkafka::Config.oauthbearer_token_refresh_callback).to be_nil
      end
    end

    context "with a token provider" do
      %i[producer consumer].each do |type|
        it "sets the token on the #{type} before returning it" do
          expect_any_instance_of(Rdkafka.const_get(type.capitalize))
            .to receive(:oauthbearer_set_token).with(**token).and_call_original

          client = create_client(type)

          expect(client).to be_a(Rdkafka.const_get(type.capitalize))
          expect(calls).to eq [:called]
        end
      end

      it "reports a token failure when the provider raises" do
        expect_any_instance_of(Rdkafka::Producer)
          .to receive(:oauthbearer_set_token_failure).with("no credentials").and_call_original

        create_client(:producer, -> { raise "no credentials" })
      end

      it "reports a token failure when the provider returns unexpected keys" do
        expect_any_instance_of(Rdkafka::Producer)
          .to receive(:oauthbearer_set_token_failure).with(a_string_including("unknown keyword")).and_call_original

        create_client(:producer, -> { token.merge(colour: "red") })
      end

      it "uses each client's own provider" do
        other_calls = []
        create_client(:producer)
        create_client(:producer, -> { other_calls << :called; token })

        expect([calls, other_calls]).to eq [[:called], [:called]]
      end

      it "raises when rdkafka does not support token refresh" do
        allow(Rdkafka::Config).to receive(:respond_to?).and_call_original
        allow(Rdkafka::Config).to receive(:respond_to?).with(:oauthbearer_token_refresh_callback=).and_return(false)

        expect { described_class.create_client(rdkafka_config, :producer, token_provider) }
          .to raise_exception(Racecar::ConfigError, "`oauth_token_provider` requires rdkafka 0.16.0 or later")
      end
    end
  end

  describe ".close_client" do
    it "closes the client" do
      client = create_client(:producer)

      described_class.close_client(client)

      expect(client).to be_closed
    end

    it "closes clients created without a token provider" do
      client = double("client", close: nil)

      described_class.close_client(client)

      expect(client).to have_received(:close)
    end

    it "stops refreshing tokens for the client" do
      client = create_client(:producer)
      name = client.name

      described_class.close_client(client)
      described_class.refresh_token(nil, name)

      expect(calls).to eq [:called]
    end
  end

  describe ".refresh_token" do
    it "ignores clients it did not create" do
      expect { described_class.refresh_token(nil, "unknown#producer-1") }.not_to raise_exception
    end
  end
end
