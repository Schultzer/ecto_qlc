# Ecto QLC

Have you ever wondered if you could just write one Ecto query and use it both for an SQL database and Erlang's ETS, DETS or Mnesia?

Whether you are doing early prototyping or looking for an easy way to do pass-through cache, let Ecto do the heavy lifting for you, with one schema and multiple backends!

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ecto_qlc` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ecto_qlc, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ecto_qlc>.

## Limitations

The following are the current limitations. Most of these can be implemented others might not even make sense to implement, like placeholders.

* Subqueries are only supported without parent_as since that would require implemetation of a query planner.
* Combinations are not supported, but could be supported in a similiar way as suqueries.
* Windows are not supported.
* Joins are limted to merge_join or nested_join, the qual: :left, :right, :inner, and etc will not affect the result.
* CTE are not supported.
* Query locks are only supported in the mnesia adapter, and only read, write or sticky_write. global locks are not supported.
* Placeholders are not supported.
* On_conflict is not supported.
* Fragments are not supported in select and has limited support in where clauses where the string can only contain valid Erlang code.
* Queries are not cached
* No automatic clustering

## Examples

Can be found under the examples directory.

- [Building a simple cache with Ecto in 66 lines](/examples/building_a_simple_cache_with_ecto_in_66_lines.livemd) or [![Run in Livebook](https://livebook.dev/badge/v1/black.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2FSchultzer%2Fecto_qlc%2Fmain%2Fexamples%2Fbuilding_a_simple_cache_with_ecto_in_66_lines.livemd%3Ftoken%3DGHSAT0AAAAAABYTWG4GREE7SJQWSL5INJZSY44YZWQ)
- [Building a simple job queue with Ecto in 46 lines](/examples/building_a_simple_job_queue_with_ecto_in_46_lines.livemd) or [![Run in Livebook](https://livebook.dev/badge/v1/black.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2FSchultzer%2Fecto_qlc%2Fmain%2Fexamples%2Fbuilding_a_simple_job_queue_with_ecto_in_46_lines.livemd%3Ftoken%3DGHSAT0AAAAAABYTWG4GZLPAJDZ4WIIF6LAWY44Y2OQ)

## Prior Arts

Most of the inspiration for this adapter comes from [Ecto SQL](https://github.com/elixir-ecto/ecto_sql) and [Etso](https://github.com/evadne/etso).

Below is a non-exhustive list of similiar projects:

- https://github.com/elixir-ecto/ecto_sql
- https://github.com/meh/amnesia
- https://github.com/sheharyarn/memento
- https://github.com/Nebo15/ecto_mnesia
- https://github.com/evadne/etso
- https://github.com/wojtekmach/ets_ecto
- https://gitlab.com/patatoid/ecto3_mnesia
- https://github.com/Logflare/ecto3_mnesia

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.