# frozen_string_literal: true

require "rdkafka"

module Racecar
  module OAuthBearer
    @mutex = Mutex.new
    @clients = {}

    class << self
      def create_client(rdkafka_config, type, token_provider)
        return rdkafka_config.public_send(type) if token_provider.nil?

        unless Rdkafka::Config.respond_to?(:oauthbearer_token_refresh_callback=)
          raise Racecar::ConfigError, "`oauth_token_provider` requires rdkafka 0.16.0 or later"
        end

        Rdkafka::Config.oauthbearer_token_refresh_callback = method(:refresh_token)
        client = rdkafka_config.public_send(type, native_kafka_auto_start: false)
        @mutex.synchronize { @clients[client.name] = [client, token_provider] }
        client.start
        client
      end

      def close_client(client)
        @mutex.synchronize { @clients.reject! { |_, (registered, _)| registered.equal?(client) } }
        client.close
      end

      def refresh_token(_oauthbearer_config, client_name)
        client, token_provider = @mutex.synchronize { @clients[client_name] }
        return if client.nil?

        begin
          client.oauthbearer_set_token(**token_provider.call)
        rescue => e
          client.oauthbearer_set_token_failure(e.message)
        end
      end
    end
  end
end
