defmodule EctoQLC.Repo.Migrations.CreateUserSession do
  use Ecto.Migration

  def change do
    create table(:users_sessions) do
      add :user_id, references(:users)
      add :meta, :map

      timestamps(updated_at: false, type: :utc_datetime_usec)
    end
  end
end
