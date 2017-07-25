module Racecar
  class EnvLoader
    def initialize(env, config)
      @env = env
      @config = config
      @loaded_keys = []
    end

    def string(name)
      set(name) {|value| value }
    end

    def integer(name)
      set(name) do |value|
        begin
          Integer(value)
        rescue ArgumentError
          raise ConfigError, "#{value.inspect} is not an integer"
        end
      end
    end

    def string_list(name)
      set(name) {|value| value.split(",") }
    end

    def validate!
      # Make sure the user hasn't made a typo and added a key we don't know
      # about.
      @env.keys.grep(/^RACECAR_/).each do |key|
        unless @loaded_keys.include?(key)
          raise ConfigError, "unknown config variable #{key}"
        end
      end
    end

    private

    def set(name)
      key = "RACECAR_#{name.upcase}"

      if @env.key?(key)
        value = yield @env.fetch(key)
        @config.set(name, value)
        @loaded_keys << key
      end
    end
  end
end
