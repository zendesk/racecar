# frozen_string_literal: true

module Racecar
  module Generators
    class ConsumerGenerator < Rails::Generators::NamedBase
      source_root File.expand_path("../../templates", __FILE__)

      def create_consumer_file
        template "consumer.rb.erb", "app/consumers/#{file_name}_consumer.rb"
      end
    end
  end
end
