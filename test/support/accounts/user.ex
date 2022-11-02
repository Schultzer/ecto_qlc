defmodule EctoQLC.Accounts.User do
  use Ecto.Schema

  schema "users" do
    field :email, :string
    field :password, :string, virtual: true, redact: true
    field :password_confirmation, :string, virtual: true, redact: true
    field :password_hash, :string, redact: true
    has_many :sessions, EctoQLC.Accounts.UserSession, on_delete: :delete_all
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(changeset, attrs \\ %{}) do
    changeset
    |> Ecto.Changeset.cast(attrs, ~w[email password_hash password password_confirmation]a)
    |> Ecto.Changeset.unique_constraint(:id, name: :primary_key)
  end
end
