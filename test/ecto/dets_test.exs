defmodule EctoQLC.Adapters.DETSTest do
  use EctoQLC.DataCase, repo: :dets

  describe "Ecto.Query.API" do

    test "!=/2" do
      assert %User{} = Repo.one!(where(User, [u], u.email != "body"))
    end

    test "*/2"  do
      assert %User{} = Repo.one!(where(User, [u], u.id * 1 == u.id))
    end

    test "+/2", %{user: user} do
      assert %{id: user.id} == Repo.one!(select(User, [u], %{id: u.id + 0}))
    end

    test "-/2", %{user: user} do
      assert user.id == Repo.one!(select(User, [u], u.id - 0))
    end

    test "//2", %{user: user}  do
      assert user.id == Repo.one!(select(User, [u], u.id / 1))
    end

    test "</2" do
      assert Repo.one!(select(User, [u], 0 < u.id))
    end

    test "<=/2" do
      assert Repo.one!(select(User, [u], u.id))
      assert Repo.one!(select(User, [u], u.id <= u.id))
    end

    test "==/2" do
      assert Repo.one!(select(User, [u], u.id == u.id))
    end

    test ">/2" do
      assert Repo.one!(select(User, [u], u.id > 0))
    end

    test ">=/2" do
      assert Repo.one!(select(User, [u], u.id >= u.id))
    end

    test "ago/2" do
      assert %User{} = Repo.one(where(User, [u], u.inserted_at > ago(3, "month")))
    end

    test "all/1" do
      assert Repo.one(from(us in UserSession, select: avg(us.id), group_by: [us.user_id]))
      assert %User{} = Repo.one(from u in User, where: u.id <= all(from(us in UserSession, select: avg(us.user_id), group_by: [us.user_id])) and u.id >= all(from(us in UserSession, select: avg(us.user_id), group_by: [us.user_id])))
      assert %User{email: "user@example.com"} = Repo.one(from u in User, where: u.id == all(from(u in User, select: max(u.id))))
    end

    test "and/2"  do
      assert %User{} = Repo.one(where(User, [u], u.email == "user@example.com" and u.id != 69))
    end

    test "any/1"  do
      assert %User{email: "user@example.com"} = Repo.one(from u in User, where: u.id == any(from(u in User, select: [u.id], where: u.email == "user@example.com")))
    end

    test "as/2"  do
      assert %User{} = Repo.one(from(User, as: :user))
    end

    test "avg/1"  do
      assert %User{} = Repo.one(group_by(select(User, [u], %{u | id: avg(u.id)}), [:email, :password_hash, :inserted_at, :updated_at]))
    end

    test "coalesce/2"  do
      assert "user@example.com" == Repo.one(select(User, [u], u.email |> coalesce("NULL") |> coalesce("1") |> coalesce("0")))
    end

    test "count/0"  do
      assert 1 = Repo.one(select(User, [u], count()))
    end

    test "count/1"  do
      assert 1 = Repo.one(select(User, [u], count(u.id)))
    end

    test "count/3"  do
      Repo.insert!(%User{email: "user@example.com"})
      assert 2 = Repo.one(select(User, [u], count(u.id, :distinct)))
      assert 1 = Repo.one(select(User, [u], count(u.email, :distinct)))
    end

    test "date_add/3", %{user: user}  do
      assert %{inserted_at: inserted_at} = Repo.one(select(User, [u], %{inserted_at: date_add(^Date.utc_today(), 1, "month")}))
      assert 30 == Date.diff(inserted_at, user.inserted_at)
      assert %{inserted_at: inserted_at} = Repo.one(select(User, [u], %{inserted_at: date_add(type(u.inserted_at, :date), 1, "month")}))
      assert 30 == Date.diff(inserted_at, user.inserted_at)
    end

    test "datetime_add/3", %{user: user} do
      assert %{inserted_at: inserted_at} = Repo.one(select(User, [u], %{u | inserted_at: datetime_add(type(^DateTime.utc_now(), :utc_datetime_usec), 1, "month")}))
      assert 30 == DateTime.diff(inserted_at, user.inserted_at, :day)
      assert %{inserted_at: inserted_at} = Repo.one(select(User, [u], %{u | inserted_at: datetime_add(u.inserted_at, 1, "month")}))
      assert 30 == DateTime.diff(inserted_at, user.inserted_at, :day)
    end

    test "exists/1" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support parent_as in a subquery's where clauses/, fn ->
        assert %User{} = Repo.one(from(User, as: :user, where: exists(from(us in UserSession, where: parent_as(:user).id == us.user_id and parent_as(:user).email != "email", select: 1))))
      end
      assert %User{} = Repo.one(from(User, as: :user, where: exists(from(us in UserSession))))
    end

    test "field/2"  do
      assert %User{} = Repo.one(where(User, [u], fragment("byte_size(?)", field(u, :email)) == 16))
    end

    test "filter/2" do
      assert 0 == Repo.one(from u in User, select: avg(u.id) |> filter(u.id < -1))
      assert 0 < Repo.one(from u in User, select: avg(u.id) |> filter(u.id >= -1))
    end

    test "fragment/1"  do
      refute Repo.one(where(User, [u], fragment("byte_size(?)", u.email) == 50))
      assert %User{} = Repo.one(where(User, [u], fragment("byte_size(?)", u.email) == 16))
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support fragemnt in select clauses in query/, fn ->
        assert %User{email: "user@example.com"} =
          User
          |> select([u], %{u | email: fragment("? ?", u.email, u.email)})
          |> where([u], fragment("byte_size(?)", u.email) == 16)
          |> Repo.one()
      end
    end

    test "from_now/2"  do
      assert %User{} = Repo.one(where(User, [u], u.inserted_at < from_now(3, "month")))
    end

    test "ilike/2" do
      refute Repo.one(where(User, [u], ilike(u.email, "aaaaa")))
      assert %User{} = Repo.one(where(User, [u], ilike(u.email, "USER@example.com")))
    end

    test "in/2" do
      assert %User{} = Repo.one(where(User, [u], u.email in ["user@example.com", "USER@example.com"]))
    end

    test "is_nil/2"  do
      assert %User{} = Repo.one(where(User, [u], not is_nil(u.email)))
    end

    test "json_extract_path/2" do
      assert ~w[localhost 0.0.0.0.0 0.0.5.0.0] == Repo.all(from(u in UserSession, order_by: u.meta["remote_ip"], select: u.meta["remote_ip"]))
    end

    test "like/2" do
      refute Repo.one(where(User, [u], ilike(u.email, "aaaaa")))
      assert %User{} = Repo.one(where(User, [u], like(u.email, "user@example.com")))
    end

    test "map/2" do
      assert %{email: "user@example.com"} == Repo.one(from u in User, select: map(u, [:email]))
    end

    test "max/1"  do
      assert %User{email: "user@example.com"} = Repo.one(group_by(select(User, [u], %{u | id: max(u.email)}), [:email, :password_hash, :inserted_at, :updated_at]))
    end

    test "merge/2" do
      assert %{left: "left", email: "user@example.com"} == Repo.one(from u in User, select: merge(%{left: "left"}, %{email: u.email}))
    end

    test "min/1" do
      assert %User{email: "user@example.com"} = Repo.one(group_by(select(User, [u], %{u | email: min(u.email)}), [:id, :email, :password_hash, :inserted_at, :updated_at]))
    end

    test "not/1" do
      assert %User{email: "user@example.com"} = Repo.one(where(User, [u], not(u.id == 69)))
    end

    test "or/2" do
      assert %User{email: "user@example.com"} = Repo.one(where(User, [u], u.id == 69 or u.email == "user@example.com"))
    end

    test "selected_as/2", %{user: %{inserted_at: posted}} do
      query = from u in User,
        select: %{
          posted: selected_as(u.inserted_at, :date),
          sum_visits: u.id |> coalesce(0) |> sum() |> selected_as(:sum_visits)
        },
        group_by: selected_as(:date),
        order_by: selected_as(:sum_visits)

      assert %{posted: ^posted} = Repo.one(query)
    end

    test "struct/2" do
      assert %User{email: "user@example.com", inserted_at: nil} = Repo.one(select(User, [u], struct(u, [:email])))
    end

    test "sum/1"  do
      assert %User{email: "user@example.com"} = Repo.one(group_by(select(User, [u], %{u | id: sum(u.id)}), [:email, :password_hash, :inserted_at, :updated_at]))
    end

    test "type/2" do
      email = "string"
      assert %User{email: "string"} = Repo.one(select(User, [u], %{u | email: type(^email, u.email)}))
    end
  end

  describe "Ecto.Query" do

    test "distinct/3" do
      assert ~w[localhost 0.0.0.0.0 0.0.5.0.0] == Repo.all(from(us in UserSession, distinct: true, order_by: [us.meta["remote_ip"]], select: us.meta["remote_ip"]))
      assert ~w[0.0.0.0.0 0.0.5.0.0 localhost] == Repo.all(from(us in UserSession, distinct: us.meta["remote_ip"], order_by: [us.inserted_at], select: us.meta["remote_ip"]))
      assert ~w[localhost 0.0.5.0.0 0.0.0.0.0] == Repo.all(from(us in UserSession, distinct: [desc: us.meta["remote_ip"]], order_by: [us.inserted_at], select: us.meta["remote_ip"]))
    end

    test "dynamic/2" do
      assert %User{} = Repo.one(where(User, ^dynamic([u], u.id != 69)))
    end

    test "except/2" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support combinations like: except/, fn ->
      assert nil == Repo.one(from u in User, select: u.email, except: ^(from u in User, select: u.email))
      end
    end

    test "except_all/2" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support combinations like: except_all/, fn ->
      assert nil == Repo.one(from u in User, select: u.email, except_all: ^(from u in User, select: u.email))
      end
    end

    test "first/2" do
      assert %User{email: "user@example.com"} = Repo.one(first(User))
    end

    test "from/2" do
      assert %User{email: "user@example.com"} = Repo.one(from(User))
    end

    test "group_by/2" do
      assert {"user@example.com", 1} == Repo.one(from(u in User, group_by: u.email, select: {u.email, count(u.id)}))
    end

    test "having/3" do
      assert {"user@example.com", 1} == Repo.one(from(u in User, group_by: u.email, having: avg(u.id) > 10, select: {u.email, count(u.id)}))
    end

    test "intersect/2" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support combinations like: intersect/, fn ->
      assert ["user@example.com"] == Repo.all(from u in User, select: u.email, intersect: ^(from u in User, select: u.email))
      end
    end

    test "intersect_all/1" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support combinations like: intersect_all/, fn ->
        assert ["user@example.com"] == Repo.all(from u in User, select: u.email, intersect_all: ^(from u in User, select: u.email))
      end
    end

    test "join/5" do
      assert [%User{email: "user@example.com"}] =
        from(User, as: :user)
        |> join(:left, [user: user], session in assoc(user, :sessions), on: not is_nil(session.meta["remote_ip"]), as: :sessions)
        |> preload([sessions: sessions], [sessions: sessions])
        |> where([user: user], user.email == "user@example.com")
        |> Repo.all()
    end

    test "last/1" do
      assert %User{email: "user@example.com"} = Repo.one(last(User))
    end

    test "limit/2" do
      limit = 2
      assert [%UserSession{}, %UserSession{}] = Repo.all(order_by(limit(UserSession, 2), [:user_id]))
      assert [%UserSession{}, %UserSession{}] = Repo.all(order_by(limit(UserSession, ^limit), [:user_id]))
    end

    test "lock/2" do
      assert_raise Ecto.QueryError, ~r/DETS adapter does not support locks in query/, fn ->
        assert [] == Repo.all(from(u in User, where: u.email == "user@example.com", lock: "write"))
      end
    end

    test "offset/2" do
      offset = 2
      assert %UserSession{token: "C"} = Repo.one(order_by(offset(UserSession, 2), [:token]))
      assert %UserSession{token: "C"} = Repo.one(order_by(offset(UserSession, ^offset), [:token]))
    end

    test "or_having/2" do
      assert %User{email: "user@example.com"} =  User |> having([u], not is_nil(u.email)) |> or_having([u], u.email == "C") |> or_having([u], u.email == "user@example.com") |> group_by([:id]) |> Repo.one()
    end

    test "or_where/0" do
      assert %User{email: "user@example.com"} =  User |> or_where(email: "B") |> where(email: "A") |> or_where(email: "user@example.com") |> Repo.one()
    end

    test "order_by/3" do
      assert [%{token: "C"}, %{token: "B"}, %{token: "A"}] = Repo.all(select(order_by(UserSession, [desc: :token, asc: :id]), [u], u))
      assert [%{token: "A"}, %{token: "B"}, %{token: "C"}] = Repo.all(order_by(UserSession, [u], [asc: u.token, desc: u.id]))
    end

    test "preload/3", %{user: user} do
      assert %User{email: "user@example.com", sessions: [%UserSession{}, %UserSession{}, %UserSession{}]} = Repo.preload(user, [:sessions], force: true)
      assert %User{email: "user@example.com", sessions: [%UserSession{}, %UserSession{}, %UserSession{}]} = Repo.preload(user, [:sessions], in_parallel: false)
      assert %User{email: nil, sessions: [%UserSession{}, %UserSession{}, %UserSession{}]} = Repo.preload(%User{id: user.id}, [:sessions])
    end

    test "reverse_order/3" do
      assert Repo.all(reverse_order(order_by(User, asc: :id))) == Repo.all(order_by(User, desc: :id))
    end

    test "select/1" do
      assert ["user@example.com"] == Repo.all(select(User, [u], u.email))
      assert [2] == Repo.all(select(User, [u], 2))
    end

    test "select_merge/2" do
      assert %{email: "body"} == Repo.one(select_merge(select(User, [u], %{email: u.email}), %{email: "body"}))
    end

    test "subquery/2" do
      assert Repo.one(where(User, [u], u.email in subquery(select(User, [:email]))))
      refute Repo.one(where(User, [u], u.email not in subquery(select(User, [:email]))))
      assert Repo.one(select(User, [u], u.email in subquery(select(User, [:email]))))
      assert 0 < Repo.one(from u in subquery(where(User, email: "user@example.com")), select: avg(u.id))
      assert [%User{email: "user@example.com"}] = Repo.all(join(User, :inner, [u], t in subquery(where(User, email: "user@example.com")), as: :users))
    end

    test "union/1" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support combinations like: union/, fn ->
        assert ["user@example.com"] == Repo.all(from u in User, select: u.email, union: ^(from u in User, select: u.email))
      end
    end

    test "union_all/1" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support combinations like: union_all/, fn ->
        assert ["user@example.com", "user@example.com"] == Repo.all(from u in User, select: u.email, union_all: ^(from u in User, select: u.email))
      end
    end

    test "update/3" do
      now = DateTime.utc_now()
      assert {1, nil} = Repo.update_all(update(User, [set: [email: "user@example.com", updated_at: ^now]]), [])
      assert [%{email: "user@example.com", updated_at: ^now}] = Repo.all(where(User, [email: "user@example.com"]))
    end

    test "where/2" do
      assert [%User{email: "user@example.com"}] = Repo.all(where(User, [email: "user@example.com"]))
    end

    test "windows/2" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support windows/, fn ->
        assert [{"user@example.com", _decimal}] = Repo.all(from u in User, select: {u.email, over(avg(u.id), :email)}, windows: [email: [partition_by: u.email]])
      end
    end

    test "with_cte/2" do
      assert_raise Ecto.QueryError, ~r/QLC adapter does not support CTE/, fn ->
        assert [
          %User{},
          %User{},
          %User{}
        ] = User
          |> recursive_ctes(true)
          |> with_cte("sessions", as: ^from(UserSession))
          |> join(:left, [u], us in "users_sessions", on: us.user_id == u.id)
          |> Repo.all()
      end
    end
  end

  describe "Repo" do

    test "aggregate/3" do
      assert 1 == Repo.aggregate(User, :count)
      assert 1 == Repo.aggregate(User, :count)
    end

    test "aggregate/4" do
      Repo.aggregate(User, :count, :id)
      assert 1 == Repo.aggregate(User, :count, :id)
    end

    test "all/2" do
      assert [%User{}] = Repo.all(User)
      assert [%User{}] = Repo.all(User)
    end

    test "delete_all/2" do
      assert {3, nil} == Repo.delete_all(UserSession)
      assert [%User{}] = Repo.all(User)
      assert {1, nil} = Repo.delete_all(where(User, [email: "user@example.com"]))
      assert [] == Repo.all(User)
    end

    test "delete!/2", %{user: user} do
      assert Repo.delete!(user)
    end

    test "delete/2", %{user: user} do
      assert {:ok, %User{}} = Repo.delete(user)
    end

    test "insert!/2" do
      assert Repo.insert!(%User{})
      assert Repo.insert!(%User{sessions: [%UserSession{}]})
    end

    test "insert/2" do
      assert {:ok, %User{}} = Repo.insert(%User{})
      assert {:ok, %User{}} = Repo.insert(%User{})
    end

    test "insert_all/3" do
      now = DateTime.utc_now()
      assert {2, [%User{inserted_at: %DateTime{}, updated_at: %DateTime{}}, %User{inserted_at: %DateTime{}, updated_at: %DateTime{}}]} = Repo.insert_all(User, [%{inserted_at: now, updated_at: now}, %{inserted_at: now, updated_at: now}], returning: true)
      assert {2, nil} = Repo.insert_all(User, [%{inserted_at: now, updated_at: now}, %{inserted_at: now, updated_at: now}])
      assert {2, nil} = Repo.insert_all(User, [%{inserted_at: {:placeholder, :now}, updated_at: {:placeholder, :now}}, %{inserted_at: {:placeholder, :now}, updated_at: {:placeholder, :now}}], placeholders: %{now: now})
    end

    test "insert_or_update!/2" do
      changeset = User.changeset(%User{}, %{})
      assert user = Repo.insert_or_update!(changeset)
      assert %User{password_hash: "password_hash"} = Repo.insert_or_update!(User.changeset(user, %{password_hash: "password_hash"}))
    end

    test "insert_or_update/2" do
      changeset = User.changeset(%User{}, %{})
      assert {:ok, user} = Repo.insert_or_update(changeset)
      assert {:ok, %User{password_hash: "password_hash"}} = Repo.insert_or_update(User.changeset(user, %{password_hash: "password_hash"}))
    end

    test "load/2"  do
      assert %User{email: "test"} = Repo.load(User, [email: "test"])
    end

    test "preload/3", %{user: user} do
      assert ^user = Repo.preload(user, [:sessions])
    end

    test "reload!/2", %{user: %{sessions: [session | _]}} do
      assert ^session = Repo.reload!(session)
    end

    test "reload/2", %{user: %{sessions: [session | _]}} do
      assert ^session = Repo.reload!(session)
    end

    test "update!/2", %{user: user} do
      changeset = User.changeset(user, %{password_hash: "password_hash"})
      assert %User{password_hash: "password_hash"} = Repo.update!(changeset)
    end

    test "update/2", %{user: user} do
      changeset = User.changeset(user, %{password_hash: "password_hash"})
      assert {:ok, %User{password_hash: "password_hash"}} = Repo.update(changeset)
    end

    test "checked_out?/0" do
      refute Repo.checked_out?()
    end

    test "checkout/2" do
      assert %User{} = Repo.checkout(fn ->
        assert Repo.checked_out?()
        Repo.one(User) end)
    end

    test "exists?/2"  do
      assert Repo.exists?(User)
      assert Repo.exists?(where(User, email: "user@example.com"))
    end

    test "get!/3", %{user: user} do
      assert Repo.get!(User, user.id)
    end

    test "get/3", %{user: user} do
      assert Repo.get(User, user.id)
    end

    test "get_by!/3" do
      assert Repo.get_by!(User, [email: "user@example.com"])
    end

    test "get_by/3"  do
      assert Repo.get_by(User, [email: "user@example.com"])
    end

    test "one!/2" do
      assert %User{} = Repo.one!(User)
    end

    test "one/2" do
      assert %User{} = Repo.one(User)
    end

    test "stream/2" do
      assert {:ok, [%UserSession{}, %UserSession{}, %UserSession{}]} = Repo.transaction(fn -> Enum.to_list(Repo.stream(UserSession, max_rows: 2)) end)
    end

    test "update_all/2" do
      now = DateTime.utc_now()
      assert {1, nil} == Repo.update_all(User, [set: [password_hash: "password_hash", updated_at: now]])
      assert [%User{password_hash: "password_hash", updated_at: ^now}] = Repo.all(User)
    end
  end

  describe "migrations" do
    test "down", %{migrations: migrations} do
      assert [:ok, :ok, :ok] == Enum.map(migrations, fn {version, module} -> Ecto.Migrator.down(Repo, version, module, log: false) end)
    end
  end
end
