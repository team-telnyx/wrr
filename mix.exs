defmodule WeightedRoundRobin.MixProject do
  use Mix.Project

  def project do
    [
      app: :wrr,
      version: "0.1.8",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "WeightedRoundRobin",
      source_url: "https://github.com/team-telnyx/wrr",
      description: description()
    ]
  end

  def application do
    [
      mod: {WeightedRoundRobin.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp description do
    """
    A local, decentralized and scalable weighted round-robin generator.
    """
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Guilherme Balena Versiani <guilherme@telnyx.com>"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/team-telnyx/wrr"},
      files: ~w"lib mix.exs README.md LICENSE"
    ]
  end
end
