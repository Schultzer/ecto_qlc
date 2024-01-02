defmodule EctoQlc.MixProject do
  use Mix.Project

  @source_url "https://github.com/schultzer/ecto_qlc"
  @version "0.2.0"

  def project do
    [
      app: :ecto_qlc,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "QLC-based adapters for Ecto",
      package: package(),
      name: "Ecto QLC",
      docs: docs(),
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :mnesia],
      mod: {EctoQLC.Adapters.QLC.Application, []}
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ~w[lib test/support]
  defp elixirc_paths(:bench), do: ~w[lib test/support]
  defp elixirc_paths(_), do: ~w[lib]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      ecto_dep(),
      ecto_sql_dep(),
      {:telemetry, "~> 0.4.0 or ~> 1.0"},
      {:ex_doc, "~> 0.29", only: :dev},
      {:benchee, "~> 1.1.0", only: :bench},
      {:benchee_html, "~> 1.0", only: :bench},
      {:postgrex, ">= 0.0.0", only: [:bench, :test]},
      {:jason, ">= 0.0.0", only: [:bench, :test]}
    ]
  end

  defp ecto_dep do
    if path = System.get_env("ECTO_PATH") do
      {:ecto, path: path}
    else
      {:ecto, "~> 3.11.0"}
    end
  end

  defp ecto_sql_dep do
    if path = System.get_env("ECTO_SQL_PATH") do
      {:ecto_sql, path: path}
    else
      {:ecto_sql, "~> 3.11.0"}
    end
  end

  defp package do
    [
      maintainers: ["Benjamin Schultzer"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files:  ~w(.formatter.exs mix.exs README.md CHANGELOG.md lib)
    ]
  end

  defp docs do
    [
      main: "EctoQLC.Adapters.QLC",
      source_ref: "v#{@version}",
      canonical: "http://hexdocs.pm/ecto_qlc",
      source_url: @source_url,
      extras: ["CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      groups_for_modules: [
        "Built-in adapters": [
          EctoQLC.Adapters.DETS,
          EctoQLC.Adapters.ETS,
          EctoQLC.Adapters.Mnesia
        ]
      ]
    ]
  end
end
