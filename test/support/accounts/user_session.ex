defmodule EctoQLC.Accounts.UserSession do
  use Ecto.Schema

  defmodule Meta do
    use Ecto.Schema
    embedded_schema do
      field :remote_ip, :string
    end
  end

  schema "users_sessions" do
    field :token, :string
    embeds_one :meta, Meta
    belongs_to :user, EctoQLC.Accounts.User
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(changeset, attrs \\ %{}) do
    changeset
    |> Ecto.Changeset.cast(attrs, ~w[meta token]a)
    |> Ecto.Changeset.unique_constraint(:user_id, name: :users_sessions_pkey)
  end
end
