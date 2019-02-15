require 'tmpdir'
require 'tempfile'

describe "the racecar executable" do

  def ruby_exe
    exe = if File.symlink? "/proc/self/exe"
      File.readlink "/proc/self/exe"
    else
      `lsof -F -p #{$$} -a -d txt | grep ^n | head -1 | cut -c2-`.chomp
    end
    puts "ruby_exe = #{exe}"
    exe
  end

  def run_racecar(*args)
    # Supply an argument to Tempfile#open, because of the rather old version of ruby (2.2.6) on CircleCI
    Tempfile.open('out') do |stdout|
      Tempfile.open('err') do |stderr|
        Dir.mktmpdir do |dir|
          yield dir if block_given?
          system(
            ruby_exe,
            "-I", dir,
            "-I", File.join(File.dirname(__FILE__), "../lib"),
            File.join(File.dirname(__FILE__), "../exe/racecar"),
            *args,
            in: "/dev/null",
            out: stdout.fileno,
            err: stderr.fileno,
          )
          stdout.rewind
          stderr.rewind
          { stdout: stdout.read, stderr: stderr.read, status: $? }
        end
      end
    end
  end

  it "should show help" do
    result = run_racecar '--help'
    output = result[:stdout] + result[:stderr]
    expect(result[:status]).to be_success
    expect(output).to include("Show Racecar version")
  end

  it "should show version" do
    result = run_racecar '--version'
    output = result[:stdout] + result[:stderr]
    expect(result[:status]).to be_success
    expect(output).to match(/^Racecar [0-9]\S+$/)
    expect(output.lines.count).to eq(1)
  end

  it "load --require'd file" do
    req = "tmp_require_#{Time.now.strftime '%s_%6N'}"
    message = "This is #{req}"
    result = run_racecar('--require', req) do |dir|
      IO.write "#{dir}/#{req}.rb", "puts #{message.inspect}"
    end
    expect(result[:stdout]).to include(message)
  end

  it "should crash if --require file is missing" do
    result = run_racecar '--require', "no_such_file_to_require_#{Time.now.to_i}"
    expect(result[:stderr]).to include('cannot load such file')
    expect(result[:status]).not_to be_success
  end

  it "should propagate exceptions from a --require'd file" do
    pending "https://github.com/zendesk/racecar/issues/106"

    req = "tmp_require_#{Time.now.strftime '%s_%6N'}"
    message = "Error from #{req}"
    result = run_racecar('--require', req) do |dir|
      IO.write "#{dir}/#{req}.rb", "raise #{message.inspect}"
    end

    output = result[:stdout] + result[:stderr]
    expect(output).to include(message)
    expect(result[:status]).not_to be_success
  end

end
