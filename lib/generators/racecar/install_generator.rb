module Racecar
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("../../templates", __FILE__)

      def create_config_file
        copy_file "racecar.yml", "config/racecar.yml"
      end

      def create_consumers_directory
        empty_directory "app/consumers"
      end
    end
  end
end
