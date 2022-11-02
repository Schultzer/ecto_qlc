defmodule MyEventModule do
  @moduledoc """
  This module uses implement the :gen_event behavior that the :mnesia event_module expect https://www.erlang.org/doc/apps/mnesia/mnesia_chap5#mnesia-event-handling.
  """
  @behaviour :gen_event

  @impl :gen_event
  def init(state), do: {:ok, state}

  @impl :gen_event
  def handle_event(event, state), do: __handle_event__(event, state)

  @impl :gen_event
  def handle_info(msg, state) do
    {:ok, _}  = __handle_event__(msg, state)
    {:ok, state}
  end

  @impl :gen_event
  def handle_call(msg, state) do
    {:ok, state} = __handle_event__(msg, state)
    {:ok, :ok, state}
  end

  @impl :gen_event
  def format_status(_opt, [_pdict, _s]), do: :ok

  @impl :gen_event
  def terminate(_reason, _state), do: :ok

  @impl :gen_event
  def code_change(_old_vsn, state, _extra), do: {:ok, state}

  defp __handle_event__({:mnesia_system_event, {:mnesia_up, _node}}, state) do
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:mnesia_down, _node}}, state) do
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:mnesia_checkpoint_activated, _chechpoint}}, state) do
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:mnesia_checkpoint_deactivated, _chechpoint}}, state) do
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:mnesia_overload, _details}}, state) do
    ## mnesia_overload is a common event and can be avoided by adjusting dc_dump_limit and dump_log_write_threshold to fit your hardware and mnesia usage.
    ## https://groups.google.com/g/rabbitmq-users/c/N9DYkaand9k
    ## https://issues.couchbase.com/browse/MB-3982?focusedCommentId=21168&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:inconsistent_database, type, _node}}, state) when type in ~w[running_partitioned_network starting_partitioned_network bad_decision]a do
    ## inconsistent_database useally happens when running mnesia in a cluster, the replicas can becomen inconsistent when enduring a netsplit, therefore a strategy is to be implemented by the consumer.
    ## one way to deal with this is https://github.com/uwiger/unsplit and https://github.com/danschultzer/pow/blob/master/lib/pow/store/backend/mnesia_cache/unsplit.ex
    {:ok, state}
  end

  defp __handle_event__({:mnesia_fatal, {:mnesia_fatal, _format, _args, _binary_core}}, state) do
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:mnesia_error, _format, _args}}, state) do
    {:ok, state}
  end

  defp __handle_event__({:mnesia_system_event, {:mnesia_user, _event}}, state) do
    ## some deal with splitbrain from netsplit by leveraging majority checking and do their unsplit when the minority_write_attempt event happens. https://github.com/sheharyarn/memento/pull/21
    {:ok, state}
  end

  defp __handle_event__({:mnesia_activity_event, _event}, state) do
    {:ok, state}
  end
end
