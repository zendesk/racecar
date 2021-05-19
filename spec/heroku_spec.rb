# frozen_string_literal: true

require "racecar/config"

RSpec.describe "Racecar::Heroku" do
  let(:config) { Racecar::Config.new }

  context 'when "racecar/heroku" is required' do
    context 'when one or more of Heroku Kafka ENVs are not set' do
      it 'logs error about the missing ENV and exits with code 1' do
        expect($stderr).to receive(:puts).with("=> Loading configuration from Heroku Kafka ENVs")
        expect($stderr).to receive(:puts).with("Error: ENV KAFKA_CLIENT_CERT_KEY is not set")

        with_env('KAFKA_URL', 'valid-url') do
          with_env('KAFKA_TRUSTED_CERT', 'valid trusted cert') do
            with_env('KAFKA_CLIENT_CERT', 'valid client cert') do
              # ENV KAFKA_CLIENT_CERT_KEY is not set
              expect do
                load "racecar/heroku.rb"
              end.to raise_exception(SystemExit)
            end
          end
        end
      end
    end

    context 'when all of the Heroku Kafka ENVs are set' do
      before do
        with_env('KAFKA_URL', 'kafka+ssl://url1,kafka+ssl://url2') do
          with_env('KAFKA_TRUSTED_CERT', 'valid trusted cert') do
            with_env('KAFKA_CLIENT_CERT', 'valid client cert') do
              with_env('KAFKA_CLIENT_CERT_KEY', 'valid client cert key') do
                Racecar.config = config
                load "racecar/heroku.rb"
              end
            end
          end
        end
      end

      it 'sets the config.security_protocol to :ssl' do
        expect(Racecar.config.security_protocol).to eq :ssl
      end

      it 'sets the config.brokers to KAFKA_URL' do
        expect(Racecar.config.brokers).to eq ['url1', 'url2']
      end

      it 'sets the config.ssl_ca_location to a temporary file with contents of KAFKA_TRUSTED_CERT' do
        file_extention = Racecar.config.ssl_ca_location.split('.').last
        file_contents = File.read(Racecar.config.ssl_ca_location)

        expect(file_extention).to eq 'pem'
        expect(file_contents).to eq 'valid trusted cert'
      end

      it 'sets the config.ssl_certificate_location to a temporary file with contents of KAFKA_CLIENT_CERT' do
        file_extention = Racecar.config.ssl_certificate_location.split('.').last
        file_contents = File.read(Racecar.config.ssl_certificate_location)

        expect(file_extention).to eq 'pem'
        expect(file_contents).to eq 'valid client cert'
      end

      it 'sets the config.ssl_key_location to a temporary file with contents of KAFKA_CLIENT_CERT_KEY' do
        file_extention = Racecar.config.ssl_key_location.split('.').last
        file_contents = File.read(Racecar.config.ssl_key_location)

        expect(file_extention).to eq 'pem'
        expect(file_contents).to eq 'valid client cert key'
      end
    end
  end
end