import Config

## mnesia is configurable https://www.erlang.org/doc/man/mnesia.html#configuration-parameters
## https://streamhacker.com/2008/12/10/how-to-eliminate-mnesia-overload-events/
config :mnesia,
  access_module: :mnesia,
  auto_repair: true,
  backup_module: :mnesia_backup,
  debug: :none,
  # core_dir: false,
  dc_dump_limit: 40,
  # dir: '/',
  dump_disc_copies_at_startup: true,
  dump_log_load_regulation: true,
  dump_log_update_in_place: true,
  dump_log_write_threshold: 50000,
  dump_log_time_threshold: :timer.minutes(3),
  event_module: :mnesia_event, #EctoQLC.Adapters.Mnesia.Event,
  extra_db_nodes: [],
  # fallback_error_function: {EctoQLC.Adapters.Mnesia.Fallback, :fallback},
  max_wait_for_decision: :infinity,
  no_table_loaders: 2,
  send_compressed: 0,
  max_transfer_size: 64000
  # schema_location: :opt_disc

import_config "#{Mix.env()}.exs"
