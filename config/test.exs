import Config

config :mnesia, debug: :none

config :logger, level: :none

config :ecto_qlc, EctoQLC.Adapters.PostgresTest.Repo,
  username: "postgres",
  password: "postgres",
  database: "ecto_qlc_test#{System.get_env("MIX_TEST_PARTITION")}",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10
