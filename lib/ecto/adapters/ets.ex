defmodule EctoQLC.Adapters.ETS do
  @moduledoc """
  Adapter module for [ETS](https://www.erlang.org/doc/man/ets.html).
  """
  use EctoQLC.Adapters.QLC, driver: :ets

  @impl Ecto.Adapter.Storage
  def storage_down(_opts), do: :ok

  @impl Ecto.Adapter.Storage
  def storage_status(_opts), do: :up

  @impl Ecto.Adapter.Storage
  def storage_up(_opts), do: :ok
end
