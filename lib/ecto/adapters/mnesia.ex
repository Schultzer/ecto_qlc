defmodule EctoQLC.Adapters.Mnesia do
  @moduledoc """
  Adapter module for [Mnesia](https://www.erlang.org/doc/man/mnesia.html).

  ## Clustering

  There is currently no support for automatic clustering or unsplit. If you decide to deploy EctoQLC.Adapters.Mnesia in a cluster you might find the following resources useful:

  * https://www.erlang.org/doc/apps/mnesia/mnesia_chap5#distribution-and-fault-tolerance
  * https://github.com/danschultzer/pow/blob/master/lib/pow/store/backend/mnesia_cache.ex
  * https://github.com/danschultzer/pow/blob/master/lib/pow/store/backend/mnesia_cache/unsplit.ex
  * https://github.com/uwiger/unsplit
  """
  use EctoQLC.Adapters.QLC, driver: :mnesia

  @impl Ecto.Adapter.Storage
  def storage_down(_opts) do
    with :stopped <- :mnesia.stop(),
         :ok <- :mnesia.delete_schema([node()]) do
         :ok
    else
      {:error, reason} -> raise RuntimeError, :mnesia.error_description(reason)
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_opts) do
    case :mnesia.system_info(:is_running) do
      :no -> :down
      :yes -> :up
      value -> {:error, value}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_up(_opts) do
    with :stopped <- :mnesia.stop,
      :ok <- :mnesia.create_schema([node()]) do
      :ok
    else
      {:aborted, {:already_exists, _table, _node, _storage_type}} -> {:error, :already_up}

      {:error,  {_, {:already_exists, _}}} -> {:error, :already_up}

      {:error, reason} -> raise RuntimeError, :mnesia.error_description(reason)
    end
  end

  @impl Ecto.Adapter.Transaction
  def transaction(_adapter_meta, _opts, fun) do
    # mnesia transaction support functions with args
    # with would allow it to forward options if needed.
    case :mnesia.transaction(fun) do
      {:aborted, reason} -> {:error, reason}
      {:atomic, result} -> {:ok, result}
      result -> {:ok, result}
    end
  end

  @impl Ecto.Adapter.Transaction
  def in_transaction?(_adapter_meta), do: :mnesia.is_transaction()

  @impl Ecto.Adapter.Transaction
  def rollback(adapter_meta, %_schema{} = value) do
    if in_transaction?(adapter_meta) do
      throw(:mnesia.abort(value))
    else
      raise "cannot call rollback outside of transaction"
    end
  end
end
