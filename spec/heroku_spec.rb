# frozen_string_literal: true

require "racecar/config"

RSpec.describe "Racecar::Heroku" do
  let(:config) { Racecar::Config.new }

  context 'when "racecar/heroku" is required' do
    context 'when one or more of Heroku Kafka ENVs are not set' do
      context 'when Racecar::Heroku.kafka_name= is not called (default behavior)' do
        it 'doesn\'t do anything' do
          expect($stderr).not_to receive(:puts).with("Error: ENV KAFKA_CLIENT_CERT_KEY is not set")

          with_env('KAFKA_URL', 'valid-url') do
            with_env('KAFKA_TRUSTED_CERT', 'valid trusted cert') do
              with_env('KAFKA_CLIENT_CERT', 'valid client cert') do
                # ENV KAFKA_CLIENT_CERT_KEY is not set
                expect do
                  load "racecar/heroku.rb"
                end.not_to raise_exception
              end
            end
          end
        end
      end

      context 'when Racecar::Heroku.kafka_name= is called (overriding behavior)' do
        it 'logs the error and exits with status 1' do

          expect($stderr).to receive(:puts).with("=> Loading configuration from Heroku Kafka ENVs")
          expect($stderr).to receive(:puts).with("Error: ENV KAFKA2_CLIENT_CERT_KEY is not set")

          with_env('KAFKA2_URL', 'valid-url') do
            with_env('KAFKA2_TRUSTED_CERT', 'valid trusted cert') do
              with_env('KAFKA2_CLIENT_CERT', 'valid client cert') do
                # ENV KAFKA2_CLIENT_CERT_KEY is not set
                expect do
                  load "racecar/heroku.rb"
                  Racecar::Heroku.kafka_name = 'KAFKA2'
                end.to raise_exception(SystemExit)
              end
            end
          end
        end
      end
    end

    context 'when all of the Heroku Kafka ENVs are set' do
      context 'when Racecar::Heroku.kafka_name= is not called (default behavior)' do
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

      context 'when Racecar::Heroku.kafka_name= is called with \'PURPLE_KAFKA\'' do
        before do
          with_env('PURPLE_KAFKA_URL', 'kafka+ssl://purple-url1,kafka+ssl://purple-url2') do
            with_env('PURPLE_KAFKA_TRUSTED_CERT', 'valid purple trusted cert') do
              with_env('PURPLE_KAFKA_CLIENT_CERT', 'valid purple client cert') do
                with_env('PURPLE_KAFKA_CLIENT_CERT_KEY', 'valid purple client cert key') do
                  Racecar.config = config
                  load "racecar/heroku.rb"
                  Racecar::Heroku.kafka_name = 'PURPLE_KAFKA'
                end
              end
            end
          end
        end

        it 'sets the config.security_protocol to :ssl' do
          expect(Racecar.config.security_protocol).to eq :ssl
        end

        it 'sets the config.brokers to PURPLE_KAFKA_URL' do
          expect(Racecar.config.brokers).to eq ['purple-url1', 'purple-url2']
        end

        it 'sets the config.ssl_ca_location to a temporary file with contents of PURPLE_KAFKA_TRUSTED_CERT' do
          file_extention = Racecar.config.ssl_ca_location.split('.').last
          file_contents = File.read(Racecar.config.ssl_ca_location)

          expect(file_extention).to eq 'pem'
          expect(file_contents).to eq 'valid purple trusted cert'
        end

        it 'sets the config.ssl_certificate_location to a temporary file with contents of PURPLE_KAFKA_CLIENT_CERT' do
          file_extention = Racecar.config.ssl_certificate_location.split('.').last
          file_contents = File.read(Racecar.config.ssl_certificate_location)

          expect(file_extention).to eq 'pem'
          expect(file_contents).to eq 'valid purple client cert'
        end

        it 'sets the config.ssl_key_location to a temporary file with contents of PURPLE_KAFKA_CLIENT_CERT_KEY' do
          file_extention = Racecar.config.ssl_key_location.split('.').last
          file_contents = File.read(Racecar.config.ssl_key_location)

          expect(file_extention).to eq 'pem'
          expect(file_contents).to eq 'valid purple client cert key'
        end
      end
    end
  end
end