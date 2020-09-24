# frozen_string_literal: true

module Racecar
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("../../templates", __FILE__)

      def create_config_file
        template "racecar.yml.erb", "config/racecar.yml"
      end

      def create_consumers_directory
        empty_directory "app/consumers"
      end
    end
  end
end
