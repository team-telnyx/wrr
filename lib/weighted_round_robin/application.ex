defmodule WeightedRoundRobin.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children =
      if Application.get_env(:wrr, :start, true) == true,
        do: [{WeightedRoundRobin, name: WeightedRoundRobin}],
        else: []

    Supervisor.start_link(children, strategy: :one_for_one, name: WeightedRoundRobin.Supervisor)
  end
end
