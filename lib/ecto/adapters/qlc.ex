defmodule EctoQLC.Adapters.QLC do
  @moduledoc ~S"""
  This application provides functionality for working with Erlang databases in `Ecto`.

  ## Built-in adapters

    * `EctoQLC.Adapters.DETS` for [`dets`](https://www.erlang.org/doc/man/dets.html)
    * `EctoQLC.Adapters.ETS` for [`ets`](https://www.erlang.org/doc/man/ets.html)
    * `EctoQLC.Adapters.Mnesia` for [`mnesia`](https://www.erlang.org/doc/man/mnesia.html)

  ## Migrations

  `ecto_qlc` supports `ecto_sql` database migrations, currently none of the adapters support constraints, unique index or multi column index.
  """
  require Kernel
  require Logger
  alias Ecto.Migration.Table
  alias Ecto.Migration.Index
  alias Ecto.Migration.Constraint

  @doc false
  defmacro __using__(opts) do
    quote do
      @behaviour Ecto.Adapter
      @behaviour Ecto.Adapter.Migration
      @behaviour Ecto.Adapter.Structure
      @behaviour Ecto.Adapter.Queryable
      @behaviour Ecto.Adapter.Schema
      @behaviour Ecto.Adapter.Storage
      @behaviour Ecto.Adapter.Transaction

      alias __MODULE__

      opts = unquote(opts)
      @driver Keyword.fetch!(opts, :driver)

      @impl Ecto.Adapter
      def ensure_all_started(_config, _type) do
        case Application.ensure_all_started(@driver) do
          {:ok, []} -> {:ok, [@driver]}
          {:ok, [@driver]} -> {:ok, [@driver]}
          {:error, _reason} -> {:ok, [@driver]}
        end
      end

      @impl Ecto.Adapter
      def init(config) do
        EctoQLC.Adapters.QLC.init(config, @driver)
      end

      @impl Ecto.Adapter
      def checkout(%{pid: pid}, _opts, fun) do
        Process.put({__MODULE__, pid}, true)
        result = fun.()
        Process.delete({__MODULE__, pid})
        result
      end

      @impl Ecto.Adapter
      def checked_out?(%{pid: pid} = _adapter_meta) do
        Process.get({__MODULE__, pid}) != nil
      end

      @impl Ecto.Adapter
      defmacro __before_compile__(_env), do: :ok

      @impl Ecto.Adapter
      def loaders({:map, _}, type),   do: [&Ecto.Type.embedded_load(type, &1, :json)]
      def loaders(match, type) when match in ~w[binary_id embed_id]a, do: [Ecto.UUID, type]
      def loaders(_, type), do: [type]

      @impl Ecto.Adapter
      def dumpers({:map, _}, type),   do: [&Ecto.Type.embedded_load(type, &1, :json)]
      def dumpers({:array, {:array, value}}, _type), do: [{:in, value}]
      def dumpers(match, type) when match in ~w[binary_id embed_id]a, do: [type, Ecto.UUID]
      def dumpers(_, type), do: [type]

      @impl Ecto.Adapter.Queryable
      def prepare(operation, %Ecto.Query{} = query) do
        {:nocache, {operation, query}}
      end

      @impl Ecto.Adapter.Queryable
      def execute(adapter_meta, query_meta, {:nocache, query}, params, options) do
        EctoQLC.Adapters.QLC.execute(adapter_meta, query_meta, query, params, options)
      end

      @impl Ecto.Adapter.Queryable
      def stream(adapter_meta, query_meta, {:nocache, query}, params, options) do
        EctoQLC.Adapters.QLC.stream(adapter_meta, query_meta, query, params, options)
      end

      @impl Ecto.Adapter.Schema
      def delete(adapter_meta, schema_meta, filters, returning, options) do
        EctoQLC.Adapters.QLC.delete(@driver, adapter_meta, schema_meta, filters, returning, options)
      end

      @impl Ecto.Adapter.Schema
      def insert(adapter_meta, schema_meta, fields, on_conflict, returning, options) do
        EctoQLC.Adapters.QLC.insert(@driver, adapter_meta, schema_meta, fields, on_conflict, returning, options)
      end

      @impl Ecto.Adapter.Schema
      def insert_all(adapter_meta, schema_meta, header, list, on_conflict, returning, placeholders, options) do
        EctoQLC.Adapters.QLC.insert_all(@driver, adapter_meta, schema_meta, header, list, on_conflict, returning, placeholders, options)
      end

      @impl Ecto.Adapter.Schema
      def update(adapter_meta, schema_meta, fields, filters, returning, options) do
        EctoQLC.Adapters.QLC.update(@driver, adapter_meta, schema_meta, fields, filters, returning, options)
      end

      @impl Ecto.Adapter.Schema
      def autogenerate(:id), do: System.unique_integer([:positive, :monotonic])
      def autogenerate(:embed_id), do: Ecto.UUID.generate()
      def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

      @impl Ecto.Adapter.Migration
      def execute_ddl(adapter_meta, command, options) do
        EctoQLC.Adapters.QLC.execute_ddl(adapter_meta, command, options)
      end

      @impl Ecto.Adapter.Migration
      def lock_for_migrations(adapter_meta, options, fun) do
        EctoQLC.Adapters.QLC.lock_for_migrations(adapter_meta, options, fun)
      end

      @impl Ecto.Adapter.Migration
      def supports_ddl_transaction?(), do: false

      @impl Ecto.Adapter.Transaction
      def transaction(adapter_meta, opts, fun) do
        Process.put({adapter_meta.pid, :transaction}, true)
        v = fun.()
        Process.put({adapter_meta.pid, :transaction}, false)
        {:ok, v}
      end

      @impl Ecto.Adapter.Transaction
      def in_transaction?(adapter_meta), do: Process.get({adapter_meta.pid, :transaction}, false)

      @impl Ecto.Adapter.Transaction
      def rollback(adapter_meta, %_schema{} = value) do
        if in_transaction?(adapter_meta) do
          throw(value)
        else
          raise "cannot call rollback outside of transaction"
        end
      end

      @impl Ecto.Adapter.Structure
      def dump_cmd(args, opts, config) do
        EctoQLC.Adapters.QLC.dump_cmd(args, opts, config, @driver)
      end

      @impl Ecto.Adapter.Structure
      def structure_dump(default, config) do
        EctoQLC.Adapters.QLC.structure_dump(default, config, @driver)
      end

      @impl Ecto.Adapter.Structure
      def structure_load(default, config) do
        EctoQLC.Adapters.QLC.structure_load(default, config, @driver)
      end

      defoverridable [prepare: 2, execute: 5, stream: 5, execute_ddl: 3, loaders: 2, dumpers: 2, checked_out?: 1, checkout: 3, autogenerate: 1, ensure_all_started: 2, __before_compile__: 1, lock_for_migrations: 3, supports_ddl_transaction?: 0, transaction: 3, in_transaction?: 1, rollback: 2]
    end
  end

  @doc false
  def dump_cmd(_args, _opts, _config, _driver) do
    {"not_implemnted", 1}
  end

  @doc false
  def structure_dump(_default, _config, _driver) do
    {:error, "not_implemnted"}
  end

  @doc false
  def structure_load(_default, _config, _driver) do
    {:error, "not_implemnted"}
  end

  @doc false
  def init(config, :mnesia = driver) do
    dir = '#{Keyword.fetch!(config, :dir)}'
    File.mkdir_p!(dir)
    Application.put_env(:mnesia, :dir, dir)
    log = Keyword.get(config, :log, :debug)
    stacktrace = Keyword.get(config, :stacktrace, nil)
    telemetry_prefix = Keyword.fetch!(config, :telemetry_prefix)
    telemetry = {config[:repo], log, telemetry_prefix ++ [:query]}
    {:ok, DynamicSupervisor.child_spec(strategy: :one_for_one, name: Module.concat([config[:repo], driver])), %{telemetry: telemetry, stacktrace: stacktrace, opts: config}}
  end

  def init(config, driver) do
    log = Keyword.get(config, :log, :debug)
    stacktrace = Keyword.get(config, :stacktrace, nil)
    telemetry_prefix = Keyword.fetch!(config, :telemetry_prefix)
    telemetry = {config[:repo], log, telemetry_prefix ++ [:query]}
    {:ok, DynamicSupervisor.child_spec(strategy: :one_for_one, name: Module.concat([config[:repo], driver])), %{telemetry: telemetry, stacktrace: stacktrace, opts: config}}
  end

  @aggregates ~w[avg count max min sum]a
  @operators ~w[or and > < >= <= == === != + - * /]a

  @doc false
  def execute(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, query_meta, {operator, query}, params, options) do
    prepareed = prepare(adapter_meta, query_meta, query, params, options)
    qlc = to_qlc(operator, prepareed)
    :mnesia.transaction(fn ->
      if lock = elem(prepareed, 2).lock do
        :mnesia.lock(elem(lock, 0), elem(lock, 1))
      end
      query_handle = to_query_handle(operator, prepareed, qlc)
      {query_time, values} = :timer.tc(:qlc, :eval, [query_handle, []])
      {decode_time, value} = :timer.tc(__MODULE__, :select, [values, operator, prepareed])
      log(value, get_source(query.sources), qlc, query_time, decode_time, 0, 0, operator, adapter_meta.telemetry, params, query, options ++ adapter_meta.opts)
    end)
    |> elem(1)
  end
  def execute(adapter_meta, query_meta, {operator, query}, params, options) do
    prepareed = prepare(adapter_meta, query_meta, query, params, options)
    qlc = to_qlc(operator, prepareed)
    query_handle = to_query_handle(operator, prepareed, qlc)
    {query_time, values} = :timer.tc(:qlc, :eval, [query_handle, []])
    {decode_time, value} = :timer.tc(__MODULE__, :select, [values, operator, prepareed])
    log(value, get_source(query.sources), qlc, query_time, decode_time, 0, 0, operator, adapter_meta.telemetry, params, query, options ++ adapter_meta.opts)
  end

  @doc false
  def stream(adapter_meta, query_meta, {operator, query}, params, options) do
    key = :erlang.timestamp
    prepareed = prepare(adapter_meta, query_meta, query, params, options)
    qlc = to_qlc(operator, prepareed)
    query_handle = to_query_handle(operator, prepareed, qlc)
    Stream.resource(
      fn ->
      Process.put(key, :erlang.timestamp)
      :qlc.cursor(query_handle, elem(prepareed, 4))
      end,
      fn cursor ->
        case :qlc.next_answers(cursor, options[:max_rows] || 500) do
          []   -> {:halt, cursor}
          rows ->  {[select(rows, :all, prepareed)], cursor}
        end
      end,
      fn cursor ->
        result = :qlc.delete_cursor(cursor)
        query_time = :timer.now_diff(:erlang.timestamp, Process.get(key))
        log(result, get_source(query.sources), qlc, query_time, 0, 0, 0, operator, adapter_meta.telemetry, params, query, options ++ adapter_meta.opts)
      end
    )
  end

  @doc false
  def get_source({%Ecto.SubQuery{} = subquery}), do: get_source(subquery.query.sources)
  def get_source({source, _module_, _prefix}) when is_binary(source), do: source
  def get_source(source) when is_tuple(source), do: get_source(elem(source, 0))

  @creates [:create, :create_if_not_exists]
  @drops [:drop, :drop_if_exists]

  @doc false
  def execute_ddl(adapter_meta, {_command, %Constraint{}}, _options) do
    {:ok, [{:warn, "#{adapter_meta.adapter} adapter does not support CONSTRAINT commands", []}]}
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.DETS} = adapter_meta, {command, %Table{} = table, _columns}, options) when command in @creates do
    options = Keyword.merge(adapter_meta.opts, List.wrap(table.options) ++ options)
    table = to_table(adapter_meta, table.name, table.prefix, options)
    file = if dir = Application.get_env(:dets, :dir), do: Path.join(dir, "#{table}"), else: table
    options = Keyword.take(Keyword.merge([file: '#{file}'],  options), ~w[access auto_save estimated_no_objects file max_no_slots min_no_slots keypos ram_file repair type]a)
    case :dets.open_file(table, options) do
      {:ok, ^table}   ->
        Enum.map(:dets.all, &:dets.sync/1)
        {:ok, []}
      {:error, reason} -> {:ok, [{:warn, "#{inspect(:mnesia.error_description(reason))}", []}]}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.ETS} = adapter_meta, {command, %Table{} = table, _columns}, options) when command in @creates do
    options = Keyword.merge([write_concurrency: true, read_concurrency: true], Keyword.merge(adapter_meta.opts, List.wrap(table.options) ++ options))
    options = [:set, :public, :named_table, {:heir, GenServer.whereis(:__ecto_qlc__) || self(), %{}}] ++ Keyword.take(options, ~w[write_concurrency read_concurrency keypos heir heir decentralized_counters compressed]a)
    table = to_table(adapter_meta, table.name, table.prefix, options)
    with :undefined <- :ets.info(table),
         ^table     <- :ets.new(table, options) do
      {:ok, []}
        else
        _ ->
      {:ok, []}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, {command, %Table{} = table, columns}, options) when command in @creates do
    options = if "schema_migrations" == table.name do
      [disc_only_copies: [node() | Node.list]]
    else
      Keyword.take(Keyword.merge(adapter_meta.opts, List.wrap(table.options) ++ options), ~w[disc_copies access_mode disc_only_copies index load_order majority ram_copies record_name snmp storage_properties type local_content]a)
    end
    table = to_table(adapter_meta, table.name, table.prefix, options)
    primary_keys = Enum.count(columns, fn {_command, _column, _type, options} -> options[:primary_key] == true end)
    attributes = Enum.reject(columns, fn {_command, _column, _type, options} -> options[:primary_key] == true end) |> Enum.map(&elem(&1, 1))
    attributes = if primary_keys > 1, do: [:primary_keys | attributes], else: Enum.map(columns, &elem(&1, 1))

    with :ok <- :mnesia.start(),
         {:atomic, :ok} <- :mnesia.create_table(table, [{:attributes, attributes} | options]) do
          {:ok, []}
    else
      {:aborted, {:already_exists, ^table}} -> {:ok, []}

      {status, reason} when status in ~w[error aborted]a  -> {:ok, [{:warn, "#{inspect(:mnesia.error_description(reason))}", []}]}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.DETS} = adapter_meta, {command, %Table{} = table, _columns}, options) when command in @drops do
    table = to_table(adapter_meta, table.name, table.prefix, options)
    case :dets.close(table) do
      :ok -> {:ok, []}
      {:error, resoan} -> {:ok, [{:warn, "#{inspect(resoan)}", []}]}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.ETS} = adapter_meta, {command, %Table{} = table, _columns}, options) when command in @drops do
    table = to_table(adapter_meta, table.name, table.prefix, options)
    :ets.delete(table)
    {:ok, []}
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, {command, %Table{} = table, _columns}, options) when command in @drops do
    case :mnesia.delete_table(to_table(adapter_meta, table.name, table.prefix, options)) do
      {:atomic, :ok} -> {:ok, []}
      {:aborted, resoan} -> {:ok, [{:warn, "#{inspect(resoan)}", []}]}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, {:alter, %Table{} = table, changes}, options) do
    table = to_table(adapter_meta, table.name, table.prefix, options)
    attributes = :mnesia.table_info(table, :attributes)
    new_attributes = Enum.reduce(changes, attributes, fn change, attributes -> update_attributes(change, attributes) end)
    with true <- attributes != new_attributes,
      {:atomic, :ok} <- :mnesia.transform_table(table, &Enum.reduce(changes, &1, fn change, row -> update_row(row, change, attributes) end), new_attributes) do
      {:ok, []}
    else
      false -> {:ok, []}
      {status, reason} when status in ~w[error aborted]a -> {:ok, [{:warn, "#{inspect(:mnesia.error_description(reason))}", []}]}
    end
  end
  def execute_ddl(adapter_meta, {:alter, %Table{} = _table, _changes}, _options) do
    {:ok, [{:warn, "#{adapter_meta.adapter} adapter does not support alter", []}]}
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, {command, %Index{columns: [column]} = index}, options) when command in @creates do
    case :mnesia.add_table_index(to_table(adapter_meta, index.name, index.prefix, options), column) do
      {:atomic, :ok} -> {:ok, []}
      {:aborted, {:already_exists, _table, _}} when command == :create_if_not_exists -> {:ok, []}
      {:aborted, {:already_exists, _table, _}} -> raise "index already exists"
      {:aborted, resoan} -> {:ok, [{:warn, "#{inspect(resoan)}", []}]}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, {command, %Index{columns: [column]} = index, _mode}, options) when command in @drops do
    case :mnesia.del_table_index(to_table(adapter_meta, index.name, index.prefix, options), column) do
      {:atomic, :ok} -> {:ok, []}
      {:aborted, {:no_exists, _table, _}} when command == :drop_if_exists -> {:ok, []}
      {:aborted, {:no_exists, _table, _}} -> raise "index does not exists"
      {:aborted, resoan} -> {:ok, [{:warn, "#{inspect(resoan)}", []}]}
    end
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia}, {_command, %Index{}}, _options) do
    {:ok, [{:warn, "Mnesia adapter does not support index with multiply columns", []}]}
  end
  def execute_ddl(adapter_meta, {_command, %Index{}}, _options) do
    {:ok, [{:warn, "#{adapter_meta.adapter} adapter does not support index", []}]}
  end
  def execute_ddl(adapter_meta, {_command, %Index{}, _mode}, _options) do
    {:ok, [{:warn, "#{adapter_meta.adapter} adapter does not support index", []}]}
  end
  def execute_ddl(adapter_meta, {:rename, %Table{} = _current_table, %Table{} = _new_table}, _options) do
    # Since table name always stays the same then we need to copy the table into a newley created one.
    # There might be some limitations with also being aware of indexes
    # So for now we just gonna warn
    # current_table = to_table(adapter_meta, current_table.name, current_table.prefix, options)
    # new_table = to_table(adapter_meta, new_table.name, new_table.prefix, options)
    # case :mnesia.transform_table(current_table, &:erlang.setelement(1, &1, new_table), :mnesia.table_info(current_table, :attributes), new_table) do
    #   {:atomic, :ok} -> {:ok, []}
    #   {status, reason} when status in ~w[error aborted]a -> {:ok, [{:warn, "#{inspect(:mnesia.error_description(reason))}", []}]}
    # end
    {:ok, [{:warn, "#{adapter_meta.adapter} adapter does not support RENAME table commands", []}]}
  end
  def execute_ddl(%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, {:rename, %Table{} = table, current_column, new_column}, options) do
    table = to_table(adapter_meta, table.name, table.prefix, options)
    attributes = Enum.map(:mnesia.table_info(table, :attributes), fn
      ^current_column -> new_column
      column -> column
    end)
    case :mnesia.transform_table(table, &(&1), attributes) do
      {:atomic, :ok} -> {:ok, []}
      {status, reason} when status in ~w[error aborted]a -> {:ok, [{:warn, "#{inspect(:mnesia.error_description(reason))}", []}]}
    end
  end
  def execute_ddl(adapter_meta, {:rename, %Table{} = _table, _current_column, _new_column}, _options) do
    {:ok, [{:warn, "#{adapter_meta.adapter} adapter does not support RENAME column commands", []}]}
  end
  def execute_ddl(adapter_meta, command, _options) when is_binary(command) do
    raise "#{adapter_meta.adapter} adapter does not support binary in execute"
  end
  def execute_ddl(adapter_meta, command, _options) when is_list(command) do
    raise "#{adapter_meta.adapter} adapter does not support keyword lists in execute"
  end

  @doc false
  def lock_for_migrations(%{adapter: EctoQLC.Adapters.Mnesia}, _options, fun) do
    with :ok   <- :global.sync(),
         :ok   <- :mnesia.start(),
         value when value != :aborted <- :global.trans({:lock_for_migrations, __MODULE__}, fun),
         v when v in [:ok, {:error, :no_such_log}] <- :mnesia.sync_log(),
         true  <- :global.del_lock({:lock_for_migrations, __MODULE__}) do
      value
    else
      reason -> {:error, reason}
    end
  end
  def lock_for_migrations(%{adapter: EctoQLC.Adapters.DETS}, _options, fun) do
    with :ok   <- :global.sync(),
      value when value != :aborted <- :global.trans({:lock_for_migrations, __MODULE__}, fun),
      _     <- Enum.map(:dets.all, &:dets.sync/1),
      true  <- :global.del_lock({:lock_for_migrations, __MODULE__}) do
      value
    else
      reason -> {:error, reason}
    end
  end
  def lock_for_migrations(_adapter_meta, _options, fun) do
    with :ok   <- :global.sync(),
      value when value != :aborted <- :global.trans({:lock_for_migrations, __MODULE__}, fun),
      true  <- :global.del_lock({:lock_for_migrations, __MODULE__}) do
      value
    else
      reason -> {:error, reason}
    end
  end

  defp update_attributes({command, column, _type, _options}, attributes) when command in [:remove_if_exists, :remove] do
    attributes -- [column]
  end
  defp update_attributes({command, column, _type, _options}, attributes) when command in [:add_if_not_exists, :add] do
    if column in attributes do
      attributes
    else
      attributes ++ [column]
    end
  end
  defp update_attributes(_change, attributes), do: attributes

  @doc false
  def update_row(row, {:add, _column, _type, options}, _attributes) do
    Tuple.append(row, options[:default])
  end
  def update_row(row, {:add_if_not_exists, column, _type, options}, attributes) do
    if column not in attributes, do: Tuple.append(row, options[:default]), else: row
  end
  def update_row(row, {:remove, column, _type, _options}, attributes) do
    Tuple.delete_at(row, Enum.find_index(attributes, &(&1 == column)))
  end
  def update_row(row, {:remove_if_exists, column, _type, _options}, attributes) do
    if column in attributes, do: Tuple.delete_at(row, Enum.find_index(attributes, &(&1 == column))), else: row
  end
  def update_row(row, {:modify, column, type, options}, attributes) do
    idx = Enum.find_index(attributes, &(&1 == column))
    :erlang.setelement(idx, row, cast(elem(row, idx), options[:from], type))
  end
  def update_row(schema, fields, row) do
    schema.__schema__(:fields) -- schema.__schema__(:primary_key)
    |> Enum.reduce({1, row}, fn column, {idx, row} ->
      if value = fields[column] do
        {idx + 1, :erlang.setelement(idx, row, value)}
      else
        {idx + 1, row}
      end
    end)
    |> elem(1)
  end

  defp get_key([], [primary_key | _ ], fields), do: fields[primary_key]
  defp get_key([primary_key], _columns, fields), do: fields[primary_key]
  defp get_key(primary_keys, _columns, fields), do: Enum.reduce(primary_keys, {}, &Tuple.insert_at(&2, tuple_size(&2), fields[&1]))

  defp cast(value, _from, :list), do: '#{value}'
  defp cast(value, _from, :string), do: "#{value}"
  defp cast(value, _from, :integer) when is_binary(value) or is_list(value) do
    case Integer.parse("#{value}") do
      {integer, ""} -> integer
      result -> raise "Could no parse #{value} to integer got: #{inspect(result)}"
    end
  end
  defp cast(value, _from, :float) when is_binary(value) or is_list(value) do
    case Float.parse("#{value}") do
      {float, ""} -> float
      result -> raise "Could no parse #{value} to float got: #{inspect(result)}"
    end
  end

  defp bindings(params, bindings \\ :erl_eval.new_bindings()) do
    params
    |> Enum.reduce({length(bindings), bindings}, fn v, {count, bindings} ->
      count = count + 1
      {count, :erl_eval.add_binding(:"PARAM#{count}", v, bindings)}
    end)
    |> elem(1)
  end

  @doc false
  def coalesce(nil, nil), do: nil
  def coalesce(nil, right), do: right
  def coalesce(left, nil), do: left
  def coalesce(left, _), do: left

  @doc false
  def like(left, right) do
    String.match?(left, Regex.compile!(right))
  end

  @doc false
  def ilike(left, right) do
    String.match?(left, Regex.compile!(right, [:caseless]))
  end

  @doc false
  def to_match_spec(adapter_meta, schema, filters) do
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields) -- primary_key
    key = if length(primary_key) > 1, do: Enum.reduce(primary_key, {}, &Tuple.insert_at(&2, tuple_size(&2), filters[&1])), else: filters[hd(primary_key)]
    row = if adapter_meta.adapter == EctoQLC.Adapters.Mnesia, do: {to_table(adapter_meta, schema.__schema__(:source), schema.__schema__(:prefix), []), key}, else: {key}
    head = Enum.reduce(columns, row, fn column, head -> Tuple.insert_at(head, tuple_size(head), filters[column] || :"$#{tuple_size(head)}") end)
    body = if adapter_meta.adapter == EctoQLC.Adapters.Mnesia, do: [{head}], else: [true]
    conditions = []
    match_spec = [{head, conditions, body}]
    case :ets.test_ms(head, match_spec) do
      {:error, reason} -> raise RuntimeError, "invalid MatchSpec: #{inspect reason}"
      _ -> match_spec
    end
  end

  @doc false
  def prepare(%{adapter: adapter}, _query_meta, %Ecto.Query{lock: lock} = query, _params, _options) when not is_nil(lock) and adapter != EctoQLC.Adapters.Mnesia do
    raise Ecto.QueryError, query: query, message: "#{List.last(Module.split(adapter))} adapter does not support locks"
  end
  def prepare(_adapter_meta, _query_meta, %Ecto.Query{with_ctes: with_ctes} = query, _params, _options) when not is_nil(with_ctes) do
    raise Ecto.QueryError, query: query, message: "QLC adapter does not support CTE"
  end
  def prepare(_adapter_meta, _query_meta, %Ecto.Query{windows: windows} = query, _params, _options) when windows != [] do
    raise Ecto.QueryError, query: query, message: "QLC adapter does not support windows"
  end
  def prepare(_adapter_meta, _query_meta, %Ecto.Query{combinations: combinations} = query, _params, _options) when combinations != [] do
    raise Ecto.QueryError, query: query, message: "QLC adapter does not support combinations like: #{Enum.map_join(combinations, ", ", fn {k, _} -> k end)}"
  end
  def prepare(adapter_meta, query_meta, %Ecto.Query{} = query, params, options) do
    if query.select && Enum.any?(query.select.fields, &has_fragment/1), do: raise(Ecto.QueryError, query: query, message: "QLC adapter does not support fragemnt in select clauses")
    if query.wheres |> Enum.flat_map(&(&1.subqueries)) |> Enum.any?(&has_parent_as/1), do: raise(Ecto.QueryError, query: query, message: "QLC adapter does not support parent_as in a subquery's where clauses")
    options = options(query, options)
    prefix = options[:prefix] || query.from.prefix || query.prefix
    order_bys = if query.distinct && Keyword.keyword?(query.distinct.expr), do: [query.distinct | query.order_bys], else: query.order_bys
    {adapter_meta, query_meta, %{query |
      order_bys: order_bys,
      group_bys: group_bys(query),
      updates: updates(adapter_meta, query, params),
      offset: offset(query, params),
      lock: lock(adapter_meta, query, prefix),
      limit: limit(adapter_meta, query, prefix, params),
     }, params, options}
  end

  defp lock(_adapter_meta, %Ecto.Query{lock: nil}, _prefix), do: nil
  defp lock(adapter_meta, %Ecto.Query{lock: "write", from: %{source: {source, _module}}}, prefix), do: {{:table, to_table(adapter_meta, source, prefix, [])}, :write}
  defp lock(adapter_meta, %Ecto.Query{lock: "read", from: %{source: {source, _module}}}, prefix), do: {{:table, to_table(adapter_meta, source, prefix, [])}, :read}
  defp lock(adapter_meta, %Ecto.Query{lock: "sticky_write", from: %{source: {source, _module}}}, prefix), do: {{:table, to_table(adapter_meta, source, prefix, [])}, :sticky_write}
  defp lock(_adapter_meta, %Ecto.Query{lock: lock} = query, _prefix), do: raise(Ecto.QueryError, query: query, message: "Unsupported lock: #{inspect lock}, supported locks: write, read, stickey_write")

  defp offset(%Ecto.Query{offset: %{expr: {:^, _, [idx]}}}, params), do: Enum.at(params, idx, idx)
  defp offset(%Ecto.Query{offset: %{expr: expr}}, _params), do: expr
  defp offset(%Ecto.Query{}, _params), do: 0

  defp limit(adapter_meta, %Ecto.Query{limit: nil, from: %{source: {source, _module}}}, prefix, _params) do
    mod = :"#{String.downcase(List.last(Module.split(adapter_meta.adapter)))}"
    fun = if mod == :mnesia, do: :table_info, else: :info
    case apply(mod, fun, [to_table(adapter_meta, source, prefix, adapter_meta.opts), :size]) do
      :undefined -> 1
      0 -> 500
      limit -> limit
    end
  end
  defp limit(_adapter_meta, %Ecto.Query{limit: %{expr: {:^, _, [idx]}}}, _prefix, params), do: Enum.at(params, idx, idx)
  defp limit(_adapter_meta, %Ecto.Query{limit: %{expr: expr}}, _prefix, _params), do: expr
  defp limit(_adapter_meta, %Ecto.Query{limit: limit}, _prefix, _params), do: limit || 500

  defp group_bys(%Ecto.Query{group_bys: group_bys, sources: sources}) do
    Enum.reduce(group_bys, [], fn
      %Ecto.Query.QueryExpr{expr: [{:selected_as, [], [:date]}]}, acc -> acc

      %Ecto.Query.QueryExpr{expr: expr}, acc ->
        acc ++  for {{:., _, [{:&, _, [idx]}, column]}, _, _} <- expr do
          module = elem(elem(sources, idx), 1)
          {column, get_index(%{adapter: nil}, column, module.__schema__(:fields), module.__schema__(:primary_key))}
        end
    end)
  end

  defp updates(_adapter_meta, %Ecto.Query{updates: [] = updates}, _params), do: updates
  defp updates(adapter_meta, %Ecto.Query{from: %{source: {_source, module}}, updates: updates}, params) do
    addition = if adapter_meta.adapter == EctoQLC.Adapters.Mnesia, do: 2, else: 1
    columns = module.__schema__(:fields)
    Enum.flat_map(updates, fn %Ecto.Query.QueryExpr{expr: [set: set]} ->
      Enum.map(set, fn
        {column, {:^, _, [idx]}} -> {column, {Enum.find_index(columns, &(&1 == column)) + addition, Enum.at(params, idx)}}
        {column, value} -> {column, {Enum.find_index(columns, &(&1 == column)) + addition, value}}
      end)
    end)
  end

  # Subqurries are currently not allowed to have parent_as due to having to plan which query to execute first, an example would be if the subquery would have to match on the FK from the main query or to evaluate columns from the main query.
  # in that case we would have to execute the main query before we could evaluate the subquery for then filtering the main query based on the result of the subquery
  defp has_parent_as(fields, acc \\ false)
  defp has_parent_as(%Ecto.SubQuery{query: query}, acc) do
    has_parent_as(query.select.fields, acc) || if Enum.find(query.wheres, &has_parent_as(&1.expr, acc)), do: true, else: false
  end
  defp has_parent_as({_op, _meta, children}, acc), do: has_parent_as(children, acc)
  defp has_parent_as(nil, acc), do: acc
  defp has_parent_as([], acc), do: acc
  defp has_parent_as([{{:., _, [{:parent_as, _, _}, _]}, _, _} | _fields], _acc), do: true
  defp has_parent_as([{_op, _meta, children} | fields], acc), do: has_parent_as(children, acc) || has_parent_as(fields, acc)
  defp has_parent_as([_ | fields], acc), do: has_parent_as(fields, acc)

  defp has_fragment(fields, acc \\ false)
  defp has_fragment(%Ecto.SubQuery{query: query}, acc), do: has_fragment(query.select.fields, acc)
  defp has_fragment(%Ecto.Query.Tagged{value: value}, acc), do: has_fragment(value, acc)
  defp has_fragment(nil, acc), do: acc
  defp has_fragment([], acc), do: acc
  defp has_fragment({:fragment, _meta, _children}, _acc), do: true
  defp has_fragment([{{:., _, [{:fragment, _, _}, _]}, _, _} | _fields], _acc), do: true
  defp has_fragment({_op, _meta, children}, acc), do: has_fragment(children, acc)
  defp has_fragment({_op, children}, acc), do: has_fragment(children, acc)
  defp has_fragment([{:fragment, _meta, _children} | _fields], _acc), do: true
  defp has_fragment([{_op, _meta, children} | fields], acc), do: has_fragment(children, acc) || has_fragment(fields, acc)
  defp has_fragment([_ | fields], acc), do: has_fragment(fields, acc)

  defp has_aggregates(fields, acc \\ false)
  defp has_aggregates(nil, acc), do: acc
  defp has_aggregates([], acc), do: acc
  defp has_aggregates([{_, {op, _meta, _children}} | _fields], false) when op in @aggregates, do: true
  defp has_aggregates([{op, _meta, _children} | _fields], false) when op in @aggregates, do: true
  defp has_aggregates([{_op, _meta, children} | fields], _acc) do
    if has_aggregates(children), do: true, else: has_aggregates(fields)
  end
  defp has_aggregates([_ | fields], acc), do: has_aggregates(fields, acc)

  defp options(%Ecto.Query{} = query, options) do
    unique = unique?(query)
    if options[:unique] && unique, do: raise(Ecto.QueryError, query: query, message: "QLC does not support mixing distinct in queries and unique options")
    options
    # |> Keyword.put_new(:unique, unique)
    |> Enum.take_while(fn
      {k, _v} -> k in ~w[max_lookup cache join lookup unique]a
      k -> k in ~w[cache unique]a
    end)
    |> Enum.map(fn
      {:join, join} when join not in ~w[any merge lookup nested_loop]a -> raise(Ecto.QueryError, query: query, message: "QLC only supports: :any, :merge, :lookup or :nested_loop joins, got: `#{inspect(join)}`")
      x -> x
    end)
  end

  defp unique?(%Ecto.Query{distinct: %Ecto.Query.QueryExpr{}}), do: false
  defp unique?(%Ecto.Query{}), do: false


  defp to_qlc(:subquery = operator, query), do: '[#{to_expression(operator, query)} || #{to_qualifiers(query)}]'
  defp to_qlc(operator, query), do: '[#{to_expression(operator, query)} || #{to_qualifiers(query)}].'

  defp to_expression(operator, {adapter_meta, _query_meta, query, _params, options} = q) do
    mod = :"#{String.downcase(List.last(Module.split(adapter_meta.adapter)))}"
    count = tuple_size(query.sources) - 1
    if operator in ~w[delete_all update_all]a do
      if query.select do
        '{#{Enum.map_join(query.select.fields, ", ", &expr(&1, q))}}'
      else
        '#{Enum.map_join(0..count, ", ", fn idx -> "#{String.upcase(String.first(elem(elem(query.sources, idx), 0)))}#{idx}"  end)}'
      end
     else
      '{#{Enum.map_join(0..count, ", ", fn idx ->
        case elem(query.sources, idx) do
          {<<s::binary-size(1), _::binary>> = source, nil, prefix} ->
            table = to_table(adapter_meta, source, prefix, options)
            [primary_keys | fields] = if mod == :mnesia and table in :mnesia.system_info(:tables), do: :mnesia.table_info(table, :attributes), else: [:version, :inserted_at]
            primary_keys = if source == "schema_migrations", do: [:version], else: [primary_keys]
            Enum.map_join(fields, ", ", &to_element(adapter_meta, &1, fields, primary_keys, "#{String.upcase(s)}#{idx}"))

          {<<s::binary-size(1), _::binary>>, module, _} ->
            Enum.map_join(module.__schema__(:fields), ", ", &to_element(adapter_meta, &1, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{idx}"))

          %{query: %{sources: {{<<s::binary-size(1), _::binary>>, module, _}}}} ->
            Enum.map_join(module.__schema__(:fields), ", ", &to_element(adapter_meta, &1, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{idx}"))

          %{query: query} ->
            {<<s::binary-size(1), _::binary>>, module, _} = elem(query.sources, idx)
            Enum.map_join(module.__schema__(:fields), ", ", &to_element(adapter_meta, &1, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{idx}"))

        end
        end)}}'
     end
   end

  defp to_qualifiers({adapter_meta, _query_meta, query, _params, options} = q) do
    prefix = options[:prefix] || query.from.prefix || query.prefix
    options = options(query, options)
    count = tuple_size(query.sources) - 1
    mod = :"#{String.downcase(List.last(Module.split(adapter_meta.adapter)))}"
    options = Keyword.merge(options, [n_objects: query.limit])
    take = if mod == :mnesia, do: ~w[lock traverse n_objects]a, else: ~w[traverse n_objects]a
    table_opts = Keyword.take(options, take)
    generators = Enum.map_join(0..count, ", ", fn idx ->
      case elem(query.sources, idx) do
      {<<s::binary-size(1), _::binary>> = source, _module, _} ->
      "#{String.upcase(s)}#{idx} <- #{mod}:table('#{to_table(adapter_meta, source, prefix, options)}', [#{Enum.map_join(table_opts, ", ", fn {k, v} -> "{#{k}, #{v}}" end)}])"
      %{query: %{sources: {{<<s::binary-size(1), _::binary>> = source, _module, _}}}} ->
        "#{String.upcase(s)}#{idx} <- #{mod}:table('#{to_table(adapter_meta, source, prefix, options)}', [#{Enum.map_join(table_opts, ", ", fn {k, v} -> "{#{k}, #{v}}" end)}])"

      %{query: %{sources: sources}} ->
        {<<s::binary-size(1), _::binary>> = source, _module, _} = elem(sources, idx)
        "#{String.upcase(s)}#{idx} <- #{mod}:table('#{to_table(adapter_meta, source, prefix, options)}', [#{Enum.map_join(table_opts, ", ", fn {k, v} -> "{#{k}, #{v}}" end)}])"
      end
    end)
    filters = Enum.map_join(query.joins, " andalso ", fn %Ecto.Query.JoinExpr{prefix: _prefix, on: %Ecto.Query.QueryExpr{expr: expr}} -> expr(expr, q) end)
    guards = wheres(query.wheres, q, [])
    cond do
      filters == "" and guards == "" -> generators
      guards == "" -> "#{generators}, #{filters}"
      filters == "" -> "#{generators}, #{guards}"
      true -> "#{generators}, #{filters}, #{guards}"
    end
  end

  defp wheres([], _query, [" andalso "]), do: ""
  defp wheres([], _query, acc), do: to_string(acc)
  defp wheres([%Ecto.Query.BooleanExpr{expr: expr} | rest], query, [] = acc), do: wheres(rest, query, [expr(expr, query) | acc])
  defp wheres([%Ecto.Query.BooleanExpr{op: op, expr: expr} | rest], query, acc), do: wheres(rest, query, [acc] ++ "#{to_erlang_term(op)} #{expr(expr, query)}")

  defp to_query_handle(_operator, {_adapter_meta, _query_meta, query, params, options}, qlc) do
    Enum.reduce(query.order_bys, :qlc.string_to_handle(qlc, options, bindings(params)), fn %{expr: expr}, qh ->
      Enum.reduce(expr, qh, fn {k, {{:., _, [{:&, _, [idx]}, column]}, _, _}}, qh when k in ~w[asc desc]a  ->
        module = elem(elem(query.sources, idx), 1)
        columns = module.__schema__(:fields)
        primary_key = module.__schema__(:primary_key)

        key = case primary_key do
        [_primary_key] ->
          Enum.find_index(columns, &(&1 == column)) + 1
        primary_keys ->
          if idx = Enum.find_index(columns -- primary_keys, &(&1 == column)), do: idx + length(primary_keys), else: 1
        end
        :qlc.keysort(key, qh, order: to_order(query, k))

        {k, _v}, qh ->
          :qlc.sort(qh, order: to_order(query, k))
      end)
     end)
  end

  defp to_order(_query, :asc), do: :ascending
  defp to_order(_query, :desc), do: :descending
  defp to_order(query, order), do: raise(Ecto.QueryError, query: query, message: "QLC does not support ordering by: #{inspect order}")

  defp expr({_, _,[{{_, _, [{:parent_as, _, _}, _]}, _, _}, _]}, _query), do: ""
  defp expr({_, _,[_, {{_, _, [{:parent_as, _, _}, _]}, _, _}]}, _query), do: ""
  defp expr({:exists, _, [%Ecto.SubQuery{} = subquery]}, {adapter_meta, _query_meta, query, params, options}) do
    execute(adapter_meta, query, {:all, subquery.query}, params, options)
    |> elem(1)
    |> List.flatten()
    |> Enum.empty?()
    |> Kernel.not()
    |> to_string()
  end
  defp expr({op, _, [left, {o, _, [%Ecto.SubQuery{} = subquery]}]}, {adapter_meta, _query_meta, query, params, options} = q)  when o in ~w[all any]a  do
    values =
     execute(adapter_meta, query, {:all, subquery.query}, params, options)
     |> elem(1)
     |> List.flatten()

     "apply('Elixir.Enum', '#{o}?', [#{expr(values, q)}, fun(Val) -> #{expr(left, q)} #{to_erlang_term(op)} Val end])"
  end
  defp expr({:sum, _, [expr]}, query) do
    "{sum, #{expr(expr, query)}}"
  end
  defp expr({:fragment, _, fragemnt}, query) do
    fragemnt
    |> Enum.reduce([], fn
      {:raw, raw}, acc -> acc ++ [raw]
      {:expr, {expr, _, _}}, acc -> acc ++ [expr(expr, query)]
    end)
    |> to_string()
  end
  defp expr({:not = operator, mdl, [{:in, mdr, [left, %Ecto.SubQuery{} = subquery]}]}, {adapter_meta, _query_meta, query, params, options} = q), do: expr({operator, mdl, [{:in, mdr, [left, elem(execute(adapter_meta, query, {:all, subquery.query}, params, options), 1)]}]}, q)
  defp expr({:not = operator, _, [{:in, _, _} = expr]}, query), do: unroll(expr, query, operator)
  defp expr({:not = operator, [], [{:is_nil, _, [{{:., _, [{:&, _, [index]}, column]}, _, _}]}]}, {adapter_meta, _query_meta, query, _params, _options}) do
    {<<s::binary-size(1), _::binary>>, module, _prefix} = elem(query.sources, index)
    "#{to_element(adapter_meta, column, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{index}")} #{to_erlang_term(operator)} nil"
  end
  defp expr({:not, _, [expr]}, query) do
    "(#{expr(expr, query)}) == false"
  end
  defp expr({operator, _, [{l, _, _} = left, {r, _, _} = right]}, query) when operator in ~w[> < >= <= == === !=]a and l == :datetime_add or r == :datetime_add do
    case operator do
      :>   -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) == gt"
      :<   -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) == lt"
      :>=  -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) == gt or eq"
      :<=  -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) == lt or eq"
      :==  -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) == eq"
      :=== -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) == eq"
      :!=  -> "apply('Elixir.DateTime', compare, [#{expr(left, query)}, #{expr(right, query)}]) /= eq"
    end
  end
  defp expr({operator, _, [left, right]}, query) when operator in ~w[not or and > < >= <= == === != + - * /]a do
    "#{expr(left, query)} #{to_erlang_term(operator)} #{expr(right, query)}"
  end
  defp expr(%Ecto.Query.Tagged{value: expr}, query), do: expr(expr, query)
  defp expr({:json_extract_path, _, [left, right]}, query) do
    "apply('Elixir.EctoQLC.Adapters.QLC', get_in, [#{expr(left, query)}, #{expr(right, query)}])"
  end
  defp expr({:like, _, [left, match]}, query) do
    "apply('Elixir.EctoQLC.Adapters.QLC', 'like', [#{expr(left, query)}, #{expr(match, query)}]) == true"
  end
  defp expr({:ilike, _, [left, match]}, query) do
    "apply('Elixir.EctoQLC.Adapters.QLC', 'ilike', [#{expr(left, query)}, #{expr(match, query)}]) == true"
  end
  defp expr({:datetime_add, _, [left, right, interval]}, query) do
    "apply('Elixir.DateTime', add, [#{expr(left, query)}, #{interval_to_seconds(interval) * expr(right, query)}])"
  end
  defp expr({:date_add, _, [left, right, interval]}, query) do
    "apply('Elixir.Date', add, [#{expr(left, query)}, #{round(interval_to_days(interval) * expr(right, query))}])"
  end
  defp expr({:count, _, [{expr, [], []}, :distinct]}, query), do: expr(expr, query)
  defp expr({:count, _, [{expr, [], []}]}, query), do: expr(expr, query)
  defp expr({:coalesce, _, [left, right]}, query) do
    "apply('Elixir.EctoQLC.Adapters.QLC', coalesce, [#{expr(left, query)}, #{expr(right, query)}])"
  end
  defp expr({:parent_as, _, [key]}, {_adapter_meta, query_meta, _query, _params, _options}), do: query_meta.aliases[key]
  defp expr({:., _, [{:&, _, [idx]}, column]}, {adapter_meta, _query_meta, query, _params, options}) do
    case elem(query.sources, idx) do
      {<<s::binary-size(1), _::binary>> = source, nil, prefix} when adapter_meta.adapter == EctoQLC.Adapters.Mnesia ->
        attributes = :mnesia.table_info(to_table(adapter_meta, source, prefix, options), :attributes)
        to_element(adapter_meta, column, tl(attributes), [hd(attributes)], "#{String.upcase(s)}#{idx}")

      {"schema_migrations", nil, _prefix} ->
        to_element(adapter_meta, column, Ecto.Migration.SchemaMigration.__schema__(:fields), Ecto.Migration.SchemaMigration.__schema__(:primary_key), "S#{idx}")

      {<<s::binary-size(1), _::binary>>, module, _prefix} ->
        to_element(adapter_meta, column, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{idx}")
    end
  end
  defp expr({:., _, [{:parent_as, _, [key]}, column]}, {adapter_meta, query_meta, _query, _params, _options}) do
    idx = query_meta.aliases[key]
    {<<s::binary-size(1), _::binary>>, module, _prefix} = elem(query_meta.sources, query_meta.aliases[key])
    to_element(adapter_meta, column, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{idx}")
  end
  defp expr({:in, metadata, [left, %Ecto.SubQuery{} = subquery]}, {adapter_meta, _query_meta, query, params, options} = q), do: expr({:in, metadata, [left, elem(execute(adapter_meta, query, {:all, subquery.query}, params, options), 1)]}, q)
  defp expr({:in, _, _} = expr, query), do: unroll(expr, query, :==)
  defp expr({:is_nil, _, [expr]}, query), do: "#{expr(expr, query)} == nil"
  defp expr({:^, _, [ix]}, _query), do: "PARAM#{ix + 1}"
  defp expr({expr, [], []}, query), do: expr(expr, query)
  defp expr(expr, _query), do: to_erlang_term(expr)

  defp expr({:filter, _, [expr, filter]}, group, query) do
    expr(expr, Enum.filter(group, &expr(filter, &1, query)), query)
  end
  defp expr({:count, _, []}, group, _query), do: length(group)
  defp expr({:count, _, [expr, :distinct]}, group, query) do
    group
    |> Enum.uniq_by(&expr(expr, &1, query))
    |> Enum.count()
  end
  defp expr({:avg, _, [expr]}, group, query) do
    case Enum.map(group, &expr(expr, &1, query)) do
      [] -> 0
      values -> Enum.sum(values) / length(values)
    end
  end
  defp expr({op, _, [expr]}, group, query) when op in @aggregates do
    case Enum.map(group, &expr(expr, &1, query)) do
      [] -> 0
      values -> apply(Enum, op, [values])
    end
  end
  defp expr({:in, _, [left, right]}, row, query), do: expr(left, row, query) in expr(right, row, query)
  defp expr({:and, _, [left, right]}, row, query), do: expr(left, row, query) and expr(right, row, query)
  defp expr({:or, _, [left, right]}, row, query), do: expr(left, row, query) or expr(right, row, query)
  defp expr({op, _, [left, right]}, row, query) when op in @operators, do: apply(Kernel, op, [expr(left, row, query), expr(right, row, query)])
  defp expr({op, _, _} = expr, [row | _] = group, query) when op not in @aggregates and is_tuple(row), do: expr(expr, List.first(group), query)
  defp expr({:parent_as, _, [key]}, _row, {_adapter_meta, query_meta, _query, _params, _options}), do: query_meta.aliases[key]
  defp expr({:&, _, [idx]}, _row, _query), do: idx
  defp expr({{:., _, [_expr, _column]}, _, _} = expr, row, {adapter_meta, query_meta, %{query: query}, params, options}), do: expr(expr, row, {adapter_meta, query_meta, query, params, options})
  defp expr({{:., _, [expr, column]}, _, _}, row, {_adapter_meta, _query_meta, query, _params, _options} = q) do
    idx = expr(expr, row, q)
    case elem(query.sources, idx) do
      %{query: query} ->
        {_source, module, _prefix} = elem(query.sources, idx)

        base = if idx > 0, do: Enum.reduce(0..idx - 1, 0, fn idx, acc ->
          {_source, module, _prefix} = elem(query.sources, idx)
          length(module.__schema__(:fields)) + acc
        end), else: 0
        elem(row, base + Enum.find_index(module.__schema__(:fields), &(&1 == column)))

      {"schema_migrations", nil, _prefix} -> elem(row, 0)

      {_source, module, _prefix} ->
        base = if idx > 0, do: Enum.reduce(0..idx - 1, 0, fn idx, acc ->
          {_source, module, _prefix} = elem(query.sources, idx)
          length(module.__schema__(:fields)) + acc
        end), else: 0
        elem(row, base + Enum.find_index(module.__schema__(:fields), &(&1 == column)))
    end
  end
  defp expr({:^, _, [idx]}, _row, {_adapter_meta, _query_meta, _query, params, _options}), do: Enum.at(params, idx, idx)
  defp expr({:like, _, [left, right]}, row, query), do: String.match?(expr(left, row, query), Regex.compile!(expr(right, row, query)))
  defp expr({:ilike, _, [left, right]}, row, query), do: String.match?(expr(left, row, query), Regex.compile!(expr(right, row, query), [:caseless]))
  defp expr({:json_extract_path, _, [left, right]}, row, query) do
    Enum.reduce(expr(right, row, query), expr(left, row, query), fn
    _k, nil = data -> data
    k, %_{} = struct -> Map.get(struct, String.to_existing_atom(k))
    k, data when is_integer(k) and is_list(data) -> Enum.at(data, k)
    k, data when is_integer(k) and is_tuple(data) -> elem(data, k)
    k, data -> data[k] || data[String.to_existing_atom(k)]
  end)
  end
  defp expr({operator, md, [%Ecto.Query.Tagged{value: expr}, right, interval]}, row, query) when operator in ~w[datetime_add date_add]a do
    expr({operator, md, [expr, right, interval]}, row, query)
  end
  defp expr({operator, md, [left, %Ecto.Query.Tagged{value: expr}, interval]}, row, query) when operator in ~w[datetime_add date_add]a do
    expr({operator, md, [left, expr, interval]}, row, query)
  end
  defp expr({:datetime_add, _, [left, right, interval]}, row, query) do
    DateTime.add(expr(left, row, query), interval_to_seconds(interval) * expr(right, row, query), :second)
  end
  defp expr({:date_add, _, [left, right, interval]}, row, query) do
    Date.add(expr(left, row, query), round(interval_to_days(interval) * expr(right, row, query)))
  end
  defp expr({op, _, [left, right]}, row, query), do: apply(__MODULE__, op, [expr(left, row, query), expr(right, row, query)])
  defp expr(%Ecto.Query.Tagged{value: expr}, row, query), do: expr(expr, row, query)
  defp expr({_selected_as, expr}, row, query), do: expr(expr, row, query)
  defp expr(%Ecto.SubQuery{} = subquery, _row, {adapter_meta, _query_meta, query, params, options}), do: execute(adapter_meta, query, {:all, subquery.query}, params, options) |> elem(1) |> List.flatten()
  defp expr(expr, _row, _query), do: expr

  @doc false
  def select(rows, :all, {adapter_meta, _query_meta, query, _params, _options} = q) do
    if query.select && has_aggregates(query.select.fields) do
      rows = rows
             |> Enum.group_by(&Enum.map_join(query.group_bys, ":", fn {_column, idx} -> :erlang.element(idx, &1) end))
             |> Map.values()
             |> Enum.map(fn group -> Enum.map(query.select.fields, &expr(&1, group, q)) end)
             |> distinct(query, adapter_meta)
             |> offset(query, 0)
             |> Enum.take(query.limit)

      {length(rows), rows}
    else
      rows = rows
             |> Enum.map(fn row ->
               if query.select do
                 Enum.map(query.select.fields, &expr(&1, row, q))
               else
                 row
               end
              end)
              |> distinct(query, adapter_meta)
              |> offset(query, 0)
              |> Enum.take(query.limit)

      {length(rows), rows}
    end
  end
  def select(rows, :delete_all, {adapter_meta, _query_meta, %Ecto.Query{from: %{source: {source, _module}}} = query, _params, options}) do
    mod = :"#{String.downcase(List.last(Module.split(adapter_meta.adapter)))}"
    prefix = options[:prefix] || query.from.prefix || query.prefix
    table = to_table(adapter_meta, source, prefix, options)
    rows
    |> distinct(query, adapter_meta)
    |> offset(query, 0)
    |> Enum.take(query.limit)
    |> Enum.reduce({0, nil}, fn row, {count, acc} ->
      args = if mod == :mnesia, do: [:erlang.setelement(1, row, table)], else: [table, row]
      if apply(mod, :delete_object, args) do
        {count + 1, acc}
      else
        {count, acc}
      end
    end)
  end
  def select(rows, :update_all, {%{adapter: EctoQLC.Adapters.Mnesia} = adapter_meta, _query_meta, %Ecto.Query{from: %{source: {source, _module}}} = query, _params, options}) do
    {:atomic, value} = :mnesia.transaction(fn ->
      table = to_table(adapter_meta, source, query.from.prefix || query.prefix, options)
      rows
      |> distinct(query, adapter_meta)
      |> offset(query, 0)
      |> Enum.take(query.limit)
      |> Enum.reduce({0, nil}, fn row, {count, acc} ->
        if _row = update(:mnesia, table, row, query) do
          {count + 1, acc}
        else
          {count, acc}
        end
      end)
   end)
   value
  end
  def select(rows, :update_all, {adapter_meta, _query_meta, %Ecto.Query{from: %{source: {source, _module}}} = query, _params, options}) do
    mod = :"#{String.downcase(List.last(Module.split(adapter_meta.adapter)))}"
    table = to_table(adapter_meta, source, query.from.prefix || query.prefix, options)
    rows
    |> distinct(query, adapter_meta)
    |> offset(query, 0)
    |> Enum.take(query.limit)
    |> Enum.reduce({0, nil}, fn row, {count, acc} ->
      if _row = update(mod, table, row, query) do
        {count + 1, acc}
      else
        {count, acc}
      end
    end)
  end

  @doc false
  def get_in(data, keys) do
    Enum.reduce(keys, data, fn
      k, %_{} = struct -> Map.get(struct, String.to_existing_atom(k))
      k, data -> data[k] || data[String.to_existing_atom(k)]
    end)
  end

  defp update(:ets = mod, table, row, query), do: mod.update_element(table, elem(row, 0), Keyword.values(query.updates))
  defp update(:mnesia = mod, _table, row, query), do: mod.write(Enum.reduce(Keyword.values(query.updates), row, fn {idx, value}, row -> :erlang.setelement(idx, row, value) end))
  defp update(:dets = mod, table, row, query), do: mod.insert(table, Enum.reduce(Keyword.values(query.updates), row, fn {idx, value}, row -> :erlang.setelement(idx, row, value) end))

  @doc false
  def delete(mod, adapter_meta, %{source: source, prefix: prefix, schema: schema}, filters, _returning, options) when mod in ~w[dets ets]a do
    table = to_table(adapter_meta, source, prefix, options)
    ms = to_match_spec(adapter_meta, schema, filters)
    {query_time, count} = :timer.tc(mod, :select_delete, [table, ms])
    if 0 < count do
      {:ok, []}
    else
      {:error, :stale}
    end
    |> log(source, "DELETE #{inspect source} #{inspect filters} MATCHSPEC #{inspect ms}", query_time, 0, 0, 0, :delete_all, adapter_meta.telemetry, filters, [], options ++ adapter_meta.opts)
  end
  def delete(:mnesia = mod, adapter_meta, %{source: source, prefix: prefix, schema: schema}, filters, _returning, options) do
    ms = to_match_spec(adapter_meta, schema, filters)
    fun = fn ->
            with table <- to_table(adapter_meta, source, prefix, options),
                 [row] <- mod.select(table, ms),
                   :ok <- mod.delete_object(row) do
              {:ok, []}
            else
              _ -> {:error, :stale}
            end
          end
    {query_time, {:atomic, result}} = :timer.tc(mod, :transaction, [fun])
    log(result, source, "DELETE #{inspect source} #{inspect filters} MATCHSPEC #{inspect ms}", query_time, 0, 0, 0, :delete_all, adapter_meta.telemetry, filters, [], options ++ adapter_meta.opts)
  end

  @doc false
  def insert(:dets = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, fields, _on_conflict, returning, options) do
    table = to_table(adapter_meta, source, prefix, options)
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields)
    key = get_key(primary_key, columns, fields)
    record = to_record({key}, columns, primary_key, fields)
    file = if dir = Application.get_env(mod, :dir), do: Path.join(dir, "#{table}"), else: table
    options = Keyword.take(Keyword.merge([file: '#{file}'],  options), ~w[access auto_save estimated_no_objects file max_no_slots min_no_slots keypos ram_file repair type]a)
    {query_time, result} = with {:ok, ^table} <- mod.open_file(table, options),
                                {query_time, true} <- :timer.tc(mod, :insert_new, [table, record]),
                                :ok <- mod.sync(table) do
                                {query_time, {:ok, Enum.map(returning, &fields[&1])}}
                            else
                              {query_time, false} when is_integer(query_time) -> {query_time, {:invalid, [unique: "primary_key"]}}
                            end
    log(result, source, "INSERT INTO #{inspect source} RETURNING #{inspect returning} #{inspect fields}", query_time, 0, 0, 0, :insert, adapter_meta.telemetry, fields, [], options ++ adapter_meta.opts)
  end
  def insert(:ets = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, fields, _on_conflict, returning, options) do
    table = to_table(adapter_meta, source, prefix, options)
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields)
    key = get_key(primary_key, columns, fields)
    record = to_record({key}, columns, primary_key, fields)

    {query_time, result} = case :timer.tc(mod, :insert_new, [table, record]) do
                              {query_time, true}  -> {query_time, {:ok, Enum.map(returning, &fields[&1])}}
                              {query_time, false} -> {query_time, {:invalid, [unique: "primary_key"]}}
                            end
    log(result, source, "INSERT INTO #{inspect source} RETURNING #{inspect returning} #{inspect fields}", query_time, 0, 0, 0, :insert, adapter_meta.telemetry, fields, [], options ++ adapter_meta.opts)
  end
  def insert(:mnesia = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, fields, _on_conflict, returning, options) do
    table = to_table(adapter_meta, source, prefix, options)
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields)
    key = get_key(primary_key, columns, fields)
    record = to_record({table, key}, columns, primary_key, fields)
    fun  = fn ->
                with [] <- mod.wread({table, key}),
                    :ok <- mod.write(record) do
                  {:ok, []}
                else
                  [_record] -> {:invalid, [unique: "primary_key"]}

                  {:aborted, {:no_exists, ^table}} -> {:invalid, [no_exists: table]}

                  {:aborted, {:bad_type, _}} -> {:invalid, [bad_type: record]}
                end
            end

   {query_time, result} = case :timer.tc(mod, :transaction, [fun]) do
                            {query_time, {:aborted, {:bad_type, ^record}}} -> {query_time, {:invalid, [bad_type: inspect(record)]}}
                            {query_time, {:atomic, :ok}}                   -> {query_time, {:ok, Enum.map(fields, &Enum.map(returning, fn k -> &1[k] end))}}
                            {query_time, {:atomic, result}}                -> {query_time, result}
                          end

    log(result, source, "INSERT INTO #{inspect source} RETURNING #{inspect returning} #{inspect fields}", query_time, 0, 0, 0, :insert, adapter_meta.telemetry, fields, [], options ++ adapter_meta.opts)
  end

  @doc false
  def insert_all(:dets = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, _header, rows, _on_conflict, returning, _placeholders, options) do
    table = to_table(adapter_meta, source, prefix, options)
    {query_time, rows} = if is_list(rows), do: {0, rows}, else: :timer.tc(adapter_meta.repo, :all, [rows])
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields)
    records = for row <- rows do
      key = get_key(primary_key, columns, row)
      to_record({key}, columns, primary_key, row)
    end
    file = if dir = Application.get_env(mod, :dir), do: Path.join(dir, "#{table}"), else: table
    options = Keyword.take(Keyword.merge([file: '#{file}'],  options), ~w[access auto_save estimated_no_objects file max_no_slots min_no_slots keypos ram_file repair type]a)

    {query_time, result}  = with {open_time, {:ok, ^table}} <- :timer.tc(mod, :open_file, [table, options]),
         {insert_time, true} <- :timer.tc(mod, :insert_new, [table, records]),
         {sync_time, :ok} <- :timer.tc(mod, :sync, [table]) do
         result = unless returning == [], do: Enum.map(rows, &Enum.map(returning, fn k -> &1[k] end))
         {query_time + open_time + insert_time + sync_time, {length(records), result}}
    else
      {insert_time, false} when is_integer(insert_time) ->
        {insert_time, {0, nil}}
    end


    log(result, source, "INSERT INTO #{inspect source} RETURNING #{inspect returning} #{inspect rows}", query_time, 0, 0, 0, :insert, adapter_meta.telemetry, rows, [], options ++ adapter_meta.opts)
  end
  def insert_all(:ets = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, _header, rows, _on_conflict, returning, _placeholders, options) do
    table = to_table(adapter_meta, source, prefix, options)
    {query_time, rows} = if is_list(rows), do: {0, rows}, else: :timer.tc(adapter_meta.repo, :all, [rows])
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields)
    records = for row <- rows, do: to_record({get_key(primary_key, columns, row)}, columns, primary_key, row)
    result = unless returning == [], do: Enum.map(rows, &Enum.map(returning, fn k -> &1[k] end))
    {insert_time, value} = :timer.tc(mod, :insert_new, [table, records])
    query_time = query_time + insert_time
    if value do
      {length(records), result}
    else
      {0, result}
    end
    |> log(source, "INSERT INTO #{inspect source} RETURNING #{inspect returning} #{inspect rows}", query_time, 0, 0, 0, :insert, adapter_meta.telemetry, rows, [], options ++ adapter_meta.opts)
  end
  def insert_all(:mnesia = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, _header, rows, _on_conflict, returning, _placeholders, options) do
    table = to_table(adapter_meta, source, prefix, options)
    {query_time, rows} = if is_list(rows), do: {0, rows}, else: :timer.tc(adapter_meta.repo, :all, [rows])
    primary_key = schema.__schema__(:primary_key)
    columns = schema.__schema__(:fields)
    {:atomic, result} = mod.transaction(fn ->
      Enum.reduce(rows, {0, []}, fn row, {count, acc} ->
        key = get_key(primary_key, columns, row)
        record = to_record({table, key}, columns, primary_key, row)
        {insert_time, value} = :timer.tc(mod, :write, [record])
        query_time = query_time + insert_time
        if value == :ok do
          acc = if returning != [], do: acc ++ [Enum.map(returning, fn k -> row[k] end)]
          log({count + 1, acc}, source, "INSERT INTO #{inspect source} RETURNING #{inspect returning} #{inspect row}", query_time, 0, 0, 0, :insert, adapter_meta.telemetry, rows, [], options ++ adapter_meta.opts)
        else
          {count, acc}
        end
      end)
    end)
    result
  end

  @doc false
  def update(:dets = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, fields, params, returning, options) do
    table = to_table(adapter_meta, source, prefix, options)
    key = to_key(params)
    file = if dir = Application.get_env(mod, :dir), do: Path.join(dir, "#{table}"), else: table
    options = Keyword.take(Keyword.merge([file: '#{file}'],  options), ~w[access auto_save estimated_no_objects file max_no_slots min_no_slots keypos ram_file repair type]a)
    {query_time, result} = with {:ok, ^table} <- mod.open_file(table, options),
                                        [row] <- mod.lookup(table, key),
                            {query_time, :ok} <- :timer.tc(mod, :insert, [table, update_row(schema, fields, row)]),
                                          :ok <- mod.sync(table),
                           {decode_time, row} <- :timer.tc(Enum, :map, [returning, &fields[&1]]) do
                             {query_time + decode_time, {:ok, row}}
                           else
                             {query_time, _error} when is_integer(query_time) ->  {query_time, {:invalid, [unique: "primary_key"]}}
                           end
    log(result, source, "UPDATE INTO #{inspect source} RETURNING #{inspect returning} #{inspect fields}", query_time, 0, 0, 0, :update_all, adapter_meta.telemetry, params, [], options ++ adapter_meta.opts)
  end
  def update(:ets = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, fields, params, returning, options) do
    key = to_key(params)
    {records, _count} = Enum.reduce(schema.__schema__(:fields) -- schema.__schema__(:primary_key), {[], 1}, fn k, {acc, count} -> if Keyword.has_key?(fields, k), do: {[{count, fields[k]} | acc], count + 1}, else: {acc, count + 1}  end)
    table = to_table(adapter_meta, source, prefix, options)
    {query_time, result} = with {query_time, true} <- :timer.tc(mod, :update_element, [table, key, records]),
                                {decode_time, row} <- :timer.tc(Enum, :map, [returning, &fields[&1]]) do
                             {query_time + decode_time, {:ok, row}}
                           else
                             {query_time, _} -> {query_time, {:invalid, [unique: "primary_key"]}}
                           end
    log(result, source, "UPDATE INTO #{inspect source} RETURNING #{inspect returning} #{inspect fields}", query_time, 0, 0, 0, :update_all, adapter_meta.telemetry, params, [], options ++ adapter_meta.opts)
  end
  def update(:mnesia = mod, adapter_meta, %{schema: schema, source: source, prefix: prefix}, fields, params, returning, options) do
   table = to_table(adapter_meta, source, prefix, options)
   key = to_key(params)
   fun = fn ->
            with [row] <- mod.wread({table, key}),
            :ok <- mod.write(update_row(schema, fields, row)) do
              {:ok, []}
            else
            {:error, _resaon} ->
              {:invalid, [unique: "primary_key"]}
            end
          end

   {query_time, result} = case :timer.tc(mod, :transaction, [fun]) do
                            {query_time, {:atomic, :ok}}     -> {query_time, {:ok, Enum.map(fields, &Enum.map(returning, fn k -> &1[k] end))}}
                            {query_time, {:atomic, result}}  -> {query_time, result}
                          end
   log(result, source, "UPDATE INTO #{inspect source} RETURNING #{inspect returning} #{inspect fields}", query_time, 0, 0, 0, :update_all, adapter_meta.telemetry, params, [], options ++ adapter_meta.opts)
 end

  defp distinct(rows, %Ecto.Query{distinct: nil}, _adapter_meta), do: rows
  defp distinct(rows, %Ecto.Query{distinct: true}, _adapter_meta), do: Enum.uniq(rows)
  defp distinct(rows, %Ecto.Query{distinct: %Ecto.Query.QueryExpr{expr: true}}, _adapter_meta), do: Enum.uniq(rows)
  defp distinct(rows, %Ecto.Query{distinct: %Ecto.Query.QueryExpr{expr: expr}} = query, adapter_meta) do
    Enum.reduce(expr, rows, fn
      {sorter, {{:., _, [{:&, _, [index]}, column]}, _, _}}, rows ->
        {_source, module, _prefix} = elem(query.sources, index)
        idx = get_index(adapter_meta, column, module.__schema__(:fields), module.__schema__(:primary_key))
        rows
        |> Enum.sort_by(fn
          [v] -> v
          row -> Enum.at(row, idx)
        end, sorter)
        |> Enum.uniq_by(fn
          [v] -> v
          row -> Enum.at(row, idx)
        end)

      {sorter, {:json_extract_path, _, [{{:., _, [{:&, _, [index]}, column]}, _, _}, keys]}}, rows ->
          {_source, module, _prefix} = elem(query.sources, index)
          idx = get_index(adapter_meta, column, module.__schema__(:fields), module.__schema__(:primary_key))

          rows
          |> Enum.sort_by(fn
            [v] -> v
            row ->
              Enum.reduce(keys, Enum.at(row, idx), fn
                k, %_{} = struct -> Map.get(struct, String.to_existing_atom(k))
                k, data -> data[k] || data[String.to_existing_atom(k)]
              end)
          end, sorter)
          |> Enum.uniq_by(fn
            [v] -> v
            row ->
              Enum.reduce(keys, Enum.at(row, idx), fn
                k, %_{} = struct -> Map.get(struct, String.to_existing_atom(k))
                k, data -> data[k] || data[String.to_existing_atom(k)]
              end)
          end)
      end)
  end

  defp offset(rows, %{offset: offset}, offset), do: rows
  defp offset([_ | rows], query, offset), do: offset(rows, query, offset + 1)

  ## TBD There is a cavet here and we should properly use ex_cldr to do proper calculation
  ## E.g months in second is the same as 30 days in seconds, but some months don't have 30 days.
  defp interval_to_seconds("year"), do: 31557600
  defp interval_to_seconds("month"), do: 2629800
  defp interval_to_seconds("week"), do: 604800
  defp interval_to_seconds("day"), do: 86400
  defp interval_to_seconds("hour"), do: 3600
  defp interval_to_seconds("minute"), do: 60
  defp interval_to_seconds("seconds"), do: 1
  defp interval_to_seconds("millisecond"), do: 0.001
  defp interval_to_seconds("microsecond"), do: 0.000001

  defp interval_to_days("year"), do: 365.25
  defp interval_to_days("month"), do: 30.4375
  defp interval_to_days("week"), do: 7
  defp interval_to_days("day"), do: 1
  defp interval_to_days("hour"), do: 0.04166667
  defp interval_to_days("minute"), do: 6.944444e-4
  defp interval_to_days("seconds"), do: 1.157407e-5
  defp interval_to_days("millisecond"), do: 1.157407e-8
  defp interval_to_days("microsecond"), do: 1.157407e-11

  defp unroll({:in, _, [{{:., [], [{:&, [], [index]}, column]}, _, _}, values]}, {adapter_meta, _query_meta, query, _params, _options} = q, operator) do
    [v | values] = unbind(values, q)
    {<<s::binary-size(1), _::binary>>, module, _prefix} = elem(query.sources, index)
    el = to_element(adapter_meta, column, module.__schema__(:fields), module.__schema__(:primary_key), "#{String.upcase(s)}#{index}")
    values
    |> Enum.reduce(["#{el} #{to_erlang_term(operator)} #{v}"], fn v, acc -> acc ++ [" orelse #{el} #{to_erlang_term(operator)} #{v}"] end)
    |> to_string()
  end

  defp unbind({:^, _, [index]}, {_adapter_meta, _query_meta, _query, params, _options}), do: to_erlang_term(Enum.at(params, index))
  defp unbind({:^, _, values}, {_adapter_meta, _query_meta, _query, params, _options}), do: Enum.map(values, &to_erlang_term(Enum.at(params, &1, &1)))
  defp unbind([_ | _] = values, query), do: Enum.map(values, &unbind(&1, query))
  defp unbind(value, _meta), do: to_erlang_term(value)

  defp to_erlang_term(<<value::binary>>), do: '<<"#{value}">>'
  defp to_erlang_term(:or), do: :orelse
  defp to_erlang_term(:and), do: :andalso
  defp to_erlang_term(:<=), do: :"=<"
  defp to_erlang_term(:===), do: :"=:="
  defp to_erlang_term(op) when op in ~w[!= not]a, do: :"/="
  defp to_erlang_term(op) when op in ~w[> < >= == + - * /]a, do: "#{op}"
  defp to_erlang_term(value) when is_list(value), do: "[#{Enum.map_join(value, ", ", &to_erlang_term(&1))}]"
  defp to_erlang_term(value) when is_tuple(value), do: "#{value |> Tuple.to_list() |> Enum.map(&to_erlang_term/1) |> List.to_tuple() |> inspect}"
  defp to_erlang_term(value) when is_atom(value), do: "'#{value}'"
  defp to_erlang_term(value) when is_number(value), do: value
  defp to_erlang_term(value) when is_map(value), do: "#" <> "{" <> Enum.map_join(value, ", ", fn {k, v} -> "#{to_erlang_term(k)} => #{to_erlang_term(v)}" end) <> "}"
  defp to_erlang_term(value), do: value


  defp to_record(tuple, [_ | columns], [], fields), do: Enum.reduce(columns, tuple, &Tuple.insert_at(&2, tuple_size(&2), fields[&1]))
  defp to_record(tuple, columns, primary_key, fields), do: Enum.reduce(columns -- primary_key, tuple, &Tuple.insert_at(&2, tuple_size(&2), fields[&1]))

  defp to_element(adapter_meta, column, columns, [_primary_key] = primary_keys, var) do
    "element(#{get_index(adapter_meta, column, columns, primary_keys)}, #{var})"
  end
  defp to_element(adapter_meta, column, columns, primary_keys, var) do
   idx = get_index(adapter_meta, column, columns, primary_keys)
   if column in primary_keys do
      "element(#{idx}, element(#{if adapter_meta.adapter == EctoQLC.Adapters.Mnesia, do: 2, else: 1}, #{var}))"
   else
     "element(#{idx}, #{var})"
    end
  end

  defp get_index(adapter_meta, column, columns, [_primary_key]) do
    Enum.find_index(columns, &(&1 == column))
    |> Kernel.||(0)
    |> Kernel.+(if(adapter_meta.adapter == EctoQLC.Adapters.Mnesia, do: 2, else: 1))
  end
  defp get_index(adapter_meta, column, columns, primary_keys) do
    if column in primary_keys do
      Enum.find_index(primary_keys, &(&1 == column)) + 1
    else
      Enum.find_index(columns -- primary_keys, &(&1 == column)) + length(primary_keys) + if(adapter_meta.adapter == EctoQLC.Adapters.Mnesia, do: 1, else: 1)
    end
  end

  defp to_key(params) do
    case Keyword.values(params) do
      [k] -> k
      values -> List.to_tuple(values)
    end
  end

  defp to_table(adapter_meta, source, prefix, options) do
    Module.concat([adapter_meta.adapter, options[:prefix] || prefix, source])
  end

  defp log(:ok, source, query, query_time, decode_time, queue_time, idle_time, operator, telemetry, params, columns, opts) do
    log({0, []}, source, query, query_time, decode_time, queue_time, idle_time, operator, telemetry, params, columns, opts)
  end
  defp log({num_rows, rows} = result, source, query, query_time, decode_time, queue_time, idle_time, operator, {repo, log, event_name} = _telemetry, params, columns, opts) do
    columns = if is_struct(columns) and columns.select, do: columns.select.fields, else: []
    query = String.Chars.to_string(query)
    stacktrace = Keyword.get(opts, :stacktrace)
    if event_name = Keyword.get(opts, :telemetry_event, event_name) do
      :telemetry.execute(event_name,
                        %{query_time: query_time, decode_time: decode_time, queue_time: queue_time, idle_time: idle_time, total_time: query_time + decode_time + queue_time + idle_time},
                        %{type: :ecto_qlc_query, repo: repo, result: {:ok, %{command: to_command(operator), rows: rows, num_rows: num_rows, columns: columns}}, params: params, query: query, source: source, stacktrace: stacktrace, options: Keyword.get(opts, :telemetry_options, [])})
    end
    fun = fn -> log_iodata(query_time, decode_time, queue_time, idle_time, repo, source, query, opts[:cast_params] || params, result, stacktrace) end
    case Keyword.get(opts, :log, log) do
      true  ->
        Logger.log(:debug, fun, ansi_color: operator_to_color(operator))
        result
      false ->
        :ok
        result
      level ->
        Logger.log(level, fun, ansi_color: operator_to_color(operator))
        result
    end
  end

  defp to_command(:all), do: :select
  defp to_command(command), do: command

  defp log_iodata(query_time, decode_time, queue_time, idle_time, repo, source, query, params, result, stacktrace) do
    result =  if is_tuple(result) and is_atom(elem(result, 0)), do: String.upcase("#{elem(result, 0)}"), else: "OK"
    stacktrace = case stacktrace do
      [_ | _] = stacktrace ->
        {module, function, arity, info} = last_non_ecto(Enum.reverse(stacktrace), repo, nil)
        [?\n, IO.ANSI.light_black(), "↳ ", Exception.format_mfa(module, function, arity), log_stacktrace_info(info), IO.ANSI.reset()]
        _ -> []
    end
    ['QUERY ',
     result,
     " source=#{inspect(source)}",
     format_time("db", query_time),
     format_time("decode", decode_time),
     format_time("queue", queue_time),
     format_time("idle_time", idle_time),
     ?\n,
     query,
     ?\s,
     inspect(params, charlists: false),
     List.wrap(stacktrace)]
  end

  defp format_time(label, time) when time > 999, do: [?\s, label, ?=, :io_lib_format.fwrite_g(time / 1000), ?m, ?s]
  defp format_time(label, time) when time <= 999, do: [?\s, label, ?=, "#{time}", ?μ, ?s]

  defp log_stacktrace_info([file: file, line: line] ++ _rest), do: [", at: ", file, ?:, Integer.to_string(line)]
  defp log_stacktrace_info(_), do: []

  defp last_non_ecto([{mod, _, _, _} | _stacktrace], repo, last) when mod == repo or mod in [Ecto.Repo.Queryable, Ecto.Repo.Schema, Ecto.Repo.Transaction], do: last
  defp last_non_ecto([last | stacktrace], repo, _last), do: last_non_ecto(stacktrace, repo, last)
  defp last_non_ecto([], _repo, last), do: last

  defp operator_to_color(:all), do: :cyan
  defp operator_to_color(:update_all), do: :yellow
  defp operator_to_color(:delete_all), do: :red
  defp operator_to_color(:insert), do: :green
  defp operator_to_color(_op), do: nil
end
