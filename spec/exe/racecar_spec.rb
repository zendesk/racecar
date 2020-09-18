
module Racecar
  class << self
    attr_accessor :exit_code

    def exit(code = 0)
      @exit_code = code
    end
  end
end

RSpec.describe "exe/racecar" do
  before do
    Racecar.exit_code = -1
  end

  context "when exception raised during startup" do
    before do
      @orig_argv = ::ARGV
      Object.send(:remove_const, 'ARGV')
      ::ARGV = ['--require', 'missing']
    end

    after do
      Object.send(:remove_const, 'ARGV')
      ::ARGV = @orig_argv
    end

    it "displays exception, calls exit_handler, and exits with failure status" do
      expect($stderr).to receive(:puts).with(/=> Crashed: LoadError: cannot load such file -- missing\n.*cli\.rb/)

      expect(Racecar.config.error_handler).to receive(:call) do |e|
        expect(e).to be_kind_of(LoadError)
      end

      load "./exe/racecar"

      expect(Racecar.exit_code).to eq(1)
    end
  end

  context "when SystemExit raised during startup" do
    it "displays nothing and re-raises" do
      expect($stderr).to_not receive(:puts)

      SystemExit

      expect(Racecar::Cli).to receive(:main).with(anything) { raise SystemExit }

      expect do
        load "./exe/racecar"
      end.to raise_exception(SystemExit)

      expect(Racecar.exit_code).to eq(-1)
    end
  end
end
