defmodule EctoQLC.Adapters.DETS do
  @moduledoc """
  Adapter module for [DETS](https://www.erlang.org/doc/man/dets.html).
  """
  use EctoQLC.Adapters.QLC, driver: :dets

  @impl Ecto.Adapter.Storage
  def storage_down(_opts) do
    if Enum.all?(Enum.map(:dets.all(), &:dets.close/1)), do: :ok, else: {:error, :already_down}
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_opts) do
    :up
  end

  @impl Ecto.Adapter.Storage
  def storage_up(_opts) do
    :ok
  end
end
