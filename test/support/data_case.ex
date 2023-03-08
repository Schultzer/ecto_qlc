defmodule EctoQLC.DataCase do
  use ExUnit.CaseTemplate

  using options do
    adapters = %{mnesia: EctoQLC.Adapters.Mnesia, ets: EctoQLC.Adapters.ETS, dets: EctoQLC.Adapters.DETS, postgres: Ecto.Adapters.Postgres}
    adapter = adapters[options[:repo]]
    migrations = [{1, EctoQLC.Repo.Migrations.CreateUser}, {2, EctoQLC.Repo.Migrations.CreateUserSession}, {3, EctoQLC.Repo.Migrations.AddTokenToUserSession}]

    quote do
      import Ecto.Query
      alias EctoQLC.Accounts.User
      alias EctoQLC.Accounts.UserSession

      defmodule Repo do
        use Ecto.Repo, otp_app: :ecto_qlc, adapter: unquote(adapter)
      end

      setup_all context do
        if unquote(options[:repo]) == :postgres do
          start_supervised(Repo)
          Repo.__adapter__.storage_up(Repo.config)
          Enum.map(unquote(migrations), fn {version, module} -> Ecto.Migrator.up(Repo, version, module, log: false) end)
        end
        Map.put(context, :migrations, Enum.reverse(unquote(migrations)))
      end

      setup context do
        Application.put_env(:ecto_qlc, Repo, log: true)
        if unquote(options[:repo]) == :postgres do
          pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not context[:async])
          on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
        else
          Process.register(self(), :__ecto_qlc__)
          tmp_dir = System.tmp_dir!()
          mnesia_dir = '#{Path.join(tmp_dir, "ecto-qlc-#{Ecto.UUID.generate}")}'
          dets_dir = '#{Path.join(tmp_dir, "ecto-qlc-#{Ecto.UUID.generate}")}'
          File.mkdir(mnesia_dir)
          File.mkdir(dets_dir)
          Application.put_env(:ecto_qlc, Repo, [dir: mnesia_dir])
          Application.put_env(:mnesia, :dir, mnesia_dir)
          Application.put_env(:dets, :dir, dets_dir)
          Repo.__adapter__.storage_down(Repo.config)
          start_supervised(Repo)
          Repo.__adapter__.storage_up(Repo.config)
          Enum.map(unquote(migrations), fn {version, module} -> Ecto.Migrator.up(Repo, version, module, log: false) end)
        end
        %{user: Repo.insert!(%User{email: "user@example.com", sessions: [%UserSession{token: "C", meta: %UserSession.Meta{remote_ip: "localhost"}}, %UserSession{token: "B", meta: %UserSession.Meta{remote_ip: "0.0.0.0.0"}}, %UserSession{token: "A", meta: %UserSession.Meta{remote_ip: "0.0.5.0.0"}}]})}
      end
    end
  end
end
