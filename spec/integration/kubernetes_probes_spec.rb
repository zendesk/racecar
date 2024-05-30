# frozen_string_literal: true

require "timeout"
require "securerandom"
require "open3"
require "active_support/notifications"
require "racecar/cli"

RSpec.describe "kubernetes probes", type: :integration do
  describe "liveness probe" do
    before do
      set_config
      input_topic = generate_input_topic_name
      consumer_class.subscribes_to(input_topic)
    end

    after do
      ensure_liveness_file_is_deleted
      reset_probe
    end

    it "initially fails, then passes when the main loop starts" do
      expect(Pathname.new(file_path)).not_to be_readable
      expect(run_probe).to be false

      start_racecar
      wait_for_main_loop

      expect(run_probe).to be true
    end

    it "fails if processing stalls for too long" do
      start_racecar
      wait_for_main_loop

      expect(run_probe).to be true

      stall_processing
      sleep(max_interval * 1.1)

      expect(run_probe).to be false
    end

    context "even when the timeout is long" do
      let(:max_interval) { 10 }

      it "fails immediately after stopping" do
        start_racecar
        wait_for_main_loop

        stop_racecar

        expect(run_probe).to be false
      end
    end

    context "when the probe is disabled" do
      before do
        Racecar.config.liveness_probe_enabled = false
      end

      it "does not touch the file" do
        start_racecar
        wait_for_main_loop

        liveness_file = Pathname.new(Racecar.config.liveness_probe_file_path)
        expect(liveness_file).not_to be_readable
      end
    end

    describe "config loading" do
      let(:ruby_config_file) { File.expand_path("config/racecar.rb", tmp_dir) }
      let(:yaml_config_file) { File.expand_path("config/racecar.yml", tmp_dir) }
      let(:tmp_dir) { File.expand_path("/tmp/your_racecar_project", Dir.pwd) }
      let(:ruby_config_indicator_file) { "ruby_config_was_loaded.truth" }
      let(:yaml_config_indicator_file) { "yaml_config_was_loaded.truth" }
      let(:ruby_config_file_contents) { <<~RUBY }
        `touch ruby_config_was_loaded.truth`
      RUBY
      let(:yaml_config_file_contents) { <<~YAML }
        production:
          client_id: client_id
          group_id: anything
          <% `touch yaml_config_was_loaded.truth` %>
      YAML

      before do
        @original_directory = Dir.pwd
        FileUtils.mkdir_p(File.dirname(ruby_config_file))
        File.write(ruby_config_file, ruby_config_file_contents)
        File.write(yaml_config_file, yaml_config_file_contents)
        Dir.chdir(tmp_dir)
      end

      after do
        Dir.chdir(@original_directory)
        FileUtils.rm_rf(tmp_dir)
      end

      context "when config file loading is disabled" do
        let(:env_vars) do
          {
            "RACECAR_LIVENESS_PROBE_SKIP_CONFIG_FILES" => "true",
            "RAILS_ENV" => "production",
          }
        end

        it "does not load config/racecar.rb or config/racecar.yml" do
          run_probe

          aggregate_failures do
            expect(Dir.glob("*")).not_to include(ruby_config_indicator_file)
            expect(Dir.glob("*")).not_to include(yaml_config_indicator_file)
          end
        end
      end

      context "when config file loading is enabled " do
        let(:env_vars) { {} }

        it "loads config/racecar.rb" do
          run_probe

          expect(Dir.glob("*")).to include(ruby_config_indicator_file)
        end

        context "when RAILS_ENV is set" do
          let(:env_vars) { { "RAILS_ENV" => "production" } }

          it "loads config/racecar.yml" do
            run_probe

            expect(Dir.glob("*")).to include(yaml_config_indicator_file)
          end
        end

        context "when RAILS_ENV is not set" do
          it "does not load config/racecar.yml" do
            run_probe

            expect(Dir.glob("*")).not_to include(yaml_config_indicator_file)
          end
        end
      end
    end

    let(:file_path) { "/tmp/racecar-liveness-file-#{SecureRandom.hex(4)}" }
    let(:max_interval) { 1 }
    let(:racecar_cli) { Racecar::Cli.new([consumer_class.name.to_s]) }

    let(:consumer_class) do
      NoOpConsumer = Class.new(Racecar::Consumer) do
        self.group_id = "schrödingers-consumers"

        define_method :process do |_message|
        end
      end
    end

    let(:env_vars) do
      {
        "RACECAR_LIVENESS_PROBE_FILE_PATH" => file_path,
        "RACECAR_LIVENESS_PROBE_MAX_INTERVAL" => max_interval.to_s,
      }
    end

    let!(:racecarctl) { File.expand_path("exe/racecarctl", Dir.pwd) }

    def run_probe
      command = "#{racecarctl} liveness_probe"
      output, status = Open3.capture2e(env_vars, command)
      $stderr.puts "Probe output: #{output}" if ENV["DEBUG"]
      status.success?
    end

    def wait_for_main_loop
      test_thread = Thread.current
      execute_after_next_main_loop { test_thread.wakeup }
      sleep_with_timeout
    end

    def stall_processing(time = 5)
      execute_after_next_main_loop { sleep(time) }
    end

    def ensure_liveness_file_is_deleted
      File.unlink(file_path) if File.exist?(file_path)
    end
  end

  def execute_after_next_main_loop(&block)
    subscriber = ActiveSupport::Notifications.subscribe("main_loop.racecar") do |event, *_|
      ActiveSupport::Notifications.unsubscribe(subscriber)

      block.call if block
    end
  end

  def sleep_with_timeout(max_sleep = 8)
    Timeout.timeout(max_sleep) { sleep }
  end

  def set_config
    Racecar.config = Racecar::Config.new
    Racecar.config.load_consumer_class(consumer_class)
    Racecar.config.max_wait_time = 0.05
    Racecar.config.liveness_probe_enabled = true
    Racecar.config.liveness_probe_file_path = file_path
    Racecar.config.liveness_probe_max_interval = max_interval
  end

  def reset_probe
    Racecar.config.liveness_probe.uninstall
  end
end
