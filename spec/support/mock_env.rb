module MockEnv
  def with_env(env_name, value)
    initial_state = ENV[env_name]
    ENV[env_name] = value.to_s
    yield
    ENV[env_name] = initial_state
  end
end