# frozen_string_literal: true

require 'tempfile'

# Heroku Kafka addon provides 4 ENVs to connect to their Kafka Broker
# KAFKA_TRUSTED_CERT, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL
# "KAFKA" is the default name of the addon, which is configurable

$stderr.puts "=> Loading configuration from Heroku Kafka ENVs"

module Racecar
  module Heroku
    def self.kafka_name=(kafka_name, log_and_exit_if_invalid_env = true)
      [
        "#{kafka_name}_URL",
        "#{kafka_name}_TRUSTED_CERT",
        "#{kafka_name}_CLIENT_CERT",
        "#{kafka_name}_CLIENT_CERT_KEY"
      ]. each do |env_name|
        if ENV[env_name].nil? && log_and_exit_if_invalid_env
          $stderr.puts "Error: ENV #{env_name} is not set"
          exit 1
        end
      end
      Racecar.configure do |config|
        ca_cert = ENV["#{kafka_name}_TRUSTED_CERT"]
        client_cert = ENV["#{kafka_name}_CLIENT_CERT"]
        client_cert_key = ENV["#{kafka_name}_CLIENT_CERT_KEY"]

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

        config.brokers = ENV["#{kafka_name}_URL"].to_s.gsub('kafka+ssl://', '').split(',')
      end
    end
  end
end

Racecar::Heroku.send(:kafka_name=, 'KAFKA', false)
