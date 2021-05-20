# frozen_string_literal: true

require 'tempfile'

# Heroku Kafka addon provides 4 ENVs to connect to their Kafka Broker
# KAFKA_TRUSTED_CERT, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL
# This will work only if the Heroku Kafka add-on is aliased to "KAFKA"

$stderr.puts "=> Loading configuration from Heroku Kafka ENVs"

module Racecar
  module Heroku
    def self.load_configuration!
      [
        "KAFKA_URL",
        "KAFKA_TRUSTED_CERT",
        "KAFKA_CLIENT_CERT",
        "KAFKA_CLIENT_CERT_KEY"
      ]. each do |env_name|
        if ENV[env_name].nil?
          $stderr.puts "Error: ENV #{env_name} is not set"
          exit 1
        end
      end

      Racecar.configure do |config|
        ca_cert = ENV["KAFKA_TRUSTED_CERT"]
        client_cert = ENV["KAFKA_CLIENT_CERT"]
        client_cert_key = ENV["KAFKA_CLIENT_CERT_KEY"]

        tmp_file_path = lambda do |data|
          tempfile = Tempfile.new(['', '.pem'])
          tempfile << data
          tempfile.close
          tempfile.path
        end

        config.security_protocol = :ssl
        config.ssl_ca_location = tmp_file_path.call(ca_cert)
        config.ssl_certificate_location = tmp_file_path.call(client_cert)
        config.ssl_key_location = tmp_file_path.call(client_cert_key)

        config.brokers = ENV["KAFKA_URL"].to_s.gsub('kafka+ssl://', '').split(',')
      end
    end
  end
end

Racecar::Heroku.load_configuration!
