# frozen_string_literal: true

require "spec_helper"
require "racecar/parallel_runner"

CustomError = Class.new(StandardError)

RSpec.describe Racecar::ParallelRunner do
  let(:config) { double(:config, parallel_workers: parallel_workers) }
  let(:parallel_workers) { 3 }
  let(:parallel_runner) { described_class.new(runner: runner, config: config, logger: Racecar.logger) }
  let(:encoding) { Encoding.default_internal }

  def when_ready(&block)
    attempts = 0
    sleep_duration = 0.1

    Thread.new do
      Thread.current.abort_on_exception = true
      until parallel_runner.running?
        raise "Runner was not running after #{attempts} attempts" if attempts >= 10

        sleep sleep_duration
        attempts += 1
      end
      block.call
    end
  end

  before do
    @previous_internal = Encoding.default_internal
    Encoding.default_internal = encoding
  end

  after do
    Encoding.default_internal = @previous_internal
  end

  describe "#run" do
    context "when all of the workers raise an error" do
      let(:parallel_workers) { 1 }

      let(:runner_class) do
        Class.new do
          def run
            raise CustomError, "Kaboom!"
          end
        end
      end
      let(:runner) { runner_class.new }

      context "when default encoding is not set" do
        let(:encoding) { nil }

        it "raises the error from the worker and shuts down" do
          expect { parallel_runner.run }.to raise_error(CustomError) do |e|
            expect(e.message).to eq "Kaboom!"
          end
        end
      end

      context "when default encoding is set to UTF-8" do
        let(:encoding) { Encoding::UTF_8 }

        it "raises the error from the worker and shuts down" do
          expect { parallel_runner.run }.to raise_error(CustomError) do |e|
            expect(e.message).to eq "Kaboom!"
          end
        end
      end
    end

    context "when one of the workers raises an error" do
      let(:runner_class) do
        Class.new do
          def run
            trap("TERM") { @stop = true }
            trap("INT") { @explode = true }

            loop do
              break if @stop
              raise CustomError, "Kaboom!" if @explode
              sleep 0.1
            end
          end
        end
      end
      let(:runner) { runner_class.new }

      context "when default encoding is not set" do
        let(:encoding) { nil }

        it "raises the error from the worker and shuts down" do
          when_ready do
            Process.kill("INT", parallel_runner.worker_pids.first)
          end

          expect { parallel_runner.run }.to raise_error(CustomError) do |e|
            expect(e.message).to eq "Kaboom!"
          end
        end
      end

      context "when default encoding is set to UTF-8" do
        let(:encoding) { Encoding::UTF_8 }

        it "raises the error from the worker and shuts down" do
          when_ready do
            Process.kill("INT", parallel_runner.worker_pids.first)
          end

          expect { parallel_runner.run }.to raise_error(CustomError) do |e|
            expect(e.message).to eq "Kaboom!"
          end
        end
      end
    end

    context "when one of the workers is sent a shutdown signal after successful boot" do
      let(:runner_class) do
        Class.new do
          def run
            trap("TERM") { @stop = true }

            loop do
              sleep 0.1
              break if @stop
            end
          end
        end
      end

      let(:runner) { runner_class.new }

      it "stops all the child processes and closes gracefully" do
        when_ready do
          Process.kill("TERM", parallel_runner.worker_pids.first)
        end

        expect { parallel_runner.run }.not_to raise_error
      end
    end

    context "when the parent process is sent a shutdown signal after successful boot" do
      let(:runner_class) do
        Class.new do
          def run
            trap("TERM") { @stop = true }

            loop do
              sleep 0.1
              break if @stop
            end
          end
        end
      end

      let(:runner) { runner_class.new }

      it "stops all the child processes and closes gracefully" do
        when_ready do
          parallel_runner.stop
        end

        expect { parallel_runner.run }.not_to raise_error
      end
    end
  end
end
