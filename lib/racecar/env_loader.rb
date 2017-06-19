module Racecar
  class EnvLoader
    def initialize(env, config)
      @env = env
      @config = config
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

    private

    def set(name)
      key = "RACECAR_#{name.upcase}"

      if @env.key?(key)
        value = yield @env.fetch(key)
        @config.set(name, value)
      end
    end
  end
end
