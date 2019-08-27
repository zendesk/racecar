# only needed when ruby < 2.4 and not using active support

unless {}.respond_to? :compact
  # https://github.com/rails/rails/blob/fc5dd0b85189811062c85520fd70de8389b55aeb/activesupport/lib/active_support/core_ext/hash/compact.rb
  class Hash
    def compact
      select { |_, value| !value.nil? }
    end
  end
end