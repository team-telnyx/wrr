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

  test "remove keys with 0 weight" do
    pool = :test
    WeightedRoundRobin.new_pool(pool, base: 1.0, canary: 0.0)
    results = for _ <- 1..1_000, do: WeightedRoundRobin.take(pool)
    assert Enum.all?(results, &(&1 == :base))
  end
end
