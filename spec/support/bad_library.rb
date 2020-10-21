# frozen_string_literal: true

module BadLibrary
  begin
    raise ArgumentError, "may not be nil" # will appear as `cause` of exeception raised below
  rescue
    raise StandardError, "BadLibrary failed to load"
  end
end
