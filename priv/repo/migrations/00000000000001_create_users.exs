defmodule EctoQLC.Repo.Migrations.CreateUser do
  use Ecto.Migration

  def change do
    create table(:users) do
      add :email, :string
      add :password_hash, :string, default: ""

      timestamps(type: :utc_datetime_usec)
    end

    create index(:users, [:email])
  end
end
