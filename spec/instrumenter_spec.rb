# frozen_string_literal: true

RSpec.describe Racecar::Instrumenter do
  describe '#instrument' do
    let(:instrumenter) { Racecar::Instrumenter.new(backend: backend) }
    let(:backend) { double(:backend, instrument: nil) }

    it 'applies a namespace' do
      expect(backend).
        to receive(:instrument).
        with('event.racecar', any_args)

      instrumenter.instrument('event')
    end

    it 'appends a default payload' do
      expect(backend).
        to receive(:instrument).
        with('event.racecar', { client_id: 'race' })

      instrumenter.instrument('event', client_id: 'race')
    end
  end
end
