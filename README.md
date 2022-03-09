# Weighted round-robin generator

Copyright (c) 2022 Telnyx LLC.

**wrr** is a local, decentralized and scalable weighted round-robin generator.

## Usage

Simply start a pool at the generator passing the elements and their respective
weights (in the interval `[0.0, 1.0]`), then consume anywhere in your code
(including between multiple processes) using `WeightedRoundRobin.take/2`:

```elixir
:ok = WeightedRoundRobin.new_pool(:pool, [a: 0.1, b: 0.2, c: 1.0])
dist = Enum.map(1..10_000, fn _ -> WeightedRoundRobin.take(:pool) end)
%{a: 768, b: 1542, c: 7690} = Enum.frequencies(dist)
```

## Installation

The package can be installed by adding `wrr` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:wrr, "~> 0.1.0"}
  ]
end
```

Further docs can be found at
[https://hexdocs.pm/wrr](https://hexdocs.pm/wrr).
