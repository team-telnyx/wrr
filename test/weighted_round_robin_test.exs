defmodule WeightedRoundRobinTest do
  use ExUnit.Case, async: false
  doctest WeightedRoundRobin

  test "reset removes all the configured pools" do
    pools = ~w(foo bar)

    for p <- pools do
      WeightedRoundRobin.new_pool(p, a: 0.1, b: 0.9)
      refute {:error, :not_found} == WeightedRoundRobin.take(p)
    end

    WeightedRoundRobin.reset()

    for p <- pools do
      assert {:error, :not_found} == WeightedRoundRobin.take(p)
    end

    for p <- pools do
      WeightedRoundRobin.new_pool(p, a: 0.1, b: 0.9)
      refute {:error, :not_found} == WeightedRoundRobin.take(p)
    end
  end
end
