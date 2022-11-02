defmodule EctoQLC.Repo.Migrations.AddTokenToUserSession do
  use Ecto.Migration

  def change do
    alter table(:users_sessions) do
      add :token, :string
    end
  end
end
