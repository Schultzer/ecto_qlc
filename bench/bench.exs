alias EctoQLC.Accounts.User

:mnesia.stop
mnesia_dir = '#{System.tmp_dir!()}ecto-qlc-#{Ecto.UUID.generate}/'
File.mkdir(mnesia_dir)
Application.put_env(:mnesia, :dir, mnesia_dir)
:mnesia.start

defmodule Postgres do
  use Ecto.Repo, otp_app: :ecto_qlc, adapter: Ecto.Adapters.Postgres, log: false
end

defmodule Mnesia do
  use Ecto.Repo, otp_app: :ecto_qlc, adapter: EctoQLC.Adapters.Mnesia, log: false
end

defmodule ETS do
  use Ecto.Repo, otp_app: :ecto_qlc, adapter: EctoQLC.Adapters.ETS, log: false
end

migration_path = "#{File.cwd!}/priv/repo/migrations"
repos = [Mnesia, Postgres, ETS]
Enum.map(repos, &(&1.__adapter__.ensure_all_started(&1.config, :temporary)))
Enum.map(repos, &(&1.__adapter__.storage_down(&1.config)))
Enum.map(repos, &(&1.__adapter__.storage_up(&1.config)))
Enum.map(repos, &(&1.start_link(&1.config ++ [dir: mnesia_dir])))

for file <- Enum.sort(File.ls!(migration_path)) do
  {version, _rest} = Integer.parse(file)
  [{module, _}] = Code.compile_file(file, migration_path)
  for repo <- repos, do: Ecto.Migrator.up(repo, version, module, log: false)
end

Benchee.run(Map.new(repos, fn repo -> {"#{repo}", &repo.insert!(&1)} end),
  inputs: %{"struct" => struct(User, %{email: "user@example.com"}), "changeset" => User.changeset(%User{}, %{email: "user@example.com"})},
  formatters: [
    {Benchee.Formatters.HTML, file: "bench/results/insert.html"},
    Benchee.Formatters.Console
  ]
)

Enum.map(repos, &(&1.delete_all(User)))

users = Enum.map(1..5_000, &%{email: "user#{&1}@example.com", inserted_at: DateTime.utc_now, updated_at: DateTime.utc_now})
Enum.map(repos, &(&1.insert_all(User, users)))

Benchee.run(
  Map.new(repos, fn repo -> {"#{repo}", fn -> repo.all(User, limit: 5_000) end} end),
  time: 10,
  after_each: &(5_000 = length(&1)),
  formatters: [
    {Benchee.Formatters.HTML, file: "bench/results/all.html"},
    Benchee.Formatters.Console
  ]
)

Enum.map(repos, &(&1.__adapter__.storage_down(&1.config)))
