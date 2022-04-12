defmodule WeightedRoundRobinTest do
  use ExUnit.Case

  test "remove keys with 0 weight" do
    pool = :test
    WeightedRoundRobin.new_pool(pool, base: 1.0, canary: 0.0)
    results = for _ <- 1..1_000, do: WeightedRoundRobin.take(pool)
    assert Enum.all?(results, &(&1 == :base))
  end
end
