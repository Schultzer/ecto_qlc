import Config

config :mnesia, debug: :none, dump_log_load_regulation: true

config :logger, level: :none

config :ecto_qlc, Postgres,
  username: "postgres",
  password: "postgres",
  database: "ecto_qlc_test#{System.get_env("MIX_TEST_PARTITION")}",
  hostname: "localhost",
  show_sensitive_data_on_connection_error: true

config :ecto_qlc, Mnesia, log: false
