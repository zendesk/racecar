# frozen_string_literal: true

require "racecar/prometheus"

RSpec.describe Racecar::Prometheus do
  subject(:prometheus_klass) { described_class }

  describe '.endpoint' do
    it { expect(prometheus_klass.endpoint).to be_nil }

    context 'when value is updated' do
      let(:custom_endpoint) { '/custom_endpoint' }

      before { prometheus_klass.endpoint = custom_endpoint }

      it { expect(prometheus_klass.endpoint).to eq(custom_endpoint) }
    end
  end

  describe '.port' do
    it { expect(prometheus_klass.port).to eq(80) }

    context 'when value is updated' do
      let(:custom_port) { 8080 }

      before { prometheus_klass.port = custom_port }

      it { expect(prometheus_klass.port).to eq(custom_port) }
    end
  end

  describe '.registry' do
    it { expect(prometheus_klass.registry).to be_nil }

    context 'when value is updated' do
      let(:custom_registry) { 'registry' }

      before { prometheus_klass.registry = custom_registry }

      it { expect(prometheus_klass.registry).to eq(custom_registry) }
    end
  end

  describe '.config' do
    context 'with default values' do
      before do
        prometheus_klass.endpoint = nil
        prometheus_klass.registry = nil
      end

      it { expect(prometheus_klass.config).to eq({}) }
    end

    context 'with custom values' do
      let(:endpoint) { '/endpoint' }
      let(:registry) { 'MyRegistry' }

      before do
        prometheus_klass.endpoint = endpoint
        prometheus_klass.registry = registry
      end

      it { expect(prometheus_klass.config).to eq({ path: endpoint, registry: registry }) }
    end
  end
end
