# frozen_string_literal: true

begin
  require 'rack'
  require 'prometheus/middleware/exporter'
rescue LoadError
  $stderr.puts "In order to report Kafka client metrics to Prometheus you need to install the `rack` and `prometheus-client` gems."
  raise
end

module Racecar
  module Prometheus
    class << self
      def configure
        yield self
      end

      def endpoint
        @endpoint ||= nil
      end

      def endpoint=(endpoint)
        @endpoint = endpoint
      end

      def port
        @port ||= 80
      end

      def port=(port)
        @port = port
      end

      def registry
        @registry ||= nil
      end

      def registry=(registry)
        @registry = registry
      end

      def config
        config = {}
        config.merge!(path: endpoint) if endpoint
        config.merge!(registry: registry) if registry
        config
      end

      def app
        @app ||= begin
          current_config = config

          Rack::Builder.new do
            use Rack::Deflater
            use ::Prometheus::Middleware::Exporter, current_config

            run Proc.new { |env|
              ['200', {'Content-Type' => 'text/html'}, ['OK']]
            }
          end
        end
      end

      def run
        Thread.new do
          $stderr.puts "=> Exposing Prometheus metrics on #{ endpoint.nil? ? '/metrics' : endpoint} with port #{port}"

          Rack::Handler::WEBrick.run(app, Port: port) do |server|
            ['QUIT', 'INT', 'TERM'].each do |signal|
              trap(signal) { server.shutdown }
            end
          end
        end
      end
    end
  end
end
