defmodule WeightedRoundRobin do
  @doc ~S"""
  A local, decentralized and scalable weighted round-robin generator.

  It allows developers to generate a sequence, evenly distributed, attending a
  predefined set of weights attributed to elements of any type. The `take`
  function is guaranteed to be atomic and isolated.

  Generators can have any number of pools, each under a different `pool_name`.
  The `precision` indicates how many precision digits you want at the generator
  output (so 100 indicates you want a two digits precision).

  The application can have multiple instances of the generator, but in this
  case every function needs to be prefixed with the generator name, indicated
  as `wrr`.
  """
  use GenServer

  @type wrr :: atom
  @type pool_name :: any
  @type key_weights :: [{key, weight}]
  @type key :: any
  @type weight :: float
  @type precision :: non_neg_integer
  @type start_option :: {:name, generator_name :: atom}

  @doc """
  Create a new pool under the generator.
  """
  @spec new_pool(wrr, pool_name, key_weights, precision) :: :ok
  def new_pool(wrr \\ __MODULE__, pool_name, key_weights, precision \\ 100)
      when is_list(key_weights) do
    total = Enum.reduce(key_weights, 0, &(&2 + 1 / elem(&1, 1)))

    kw_counts = Enum.map(key_weights, fn {k, w} -> {k, 1 / w, 0} end)

    weighted_dist =
      0..(trunc(Float.round(total)) * precision)
      |> Enum.reduce({[], kw_counts}, fn _, {acc, kw_counts} ->
        kw_counts = Enum.sort_by(kw_counts, fn {_, inv_w, count} -> count * inv_w end)
        {k, inv_w, count} = hd(kw_counts)
        {[k | acc], [{k, inv_w, count + 1} | tl(kw_counts)]}
      end)
      |> elem(0)

    :ets.insert(key_ets!(wrr), {pool_name, length(weighted_dist) - 1, weighted_dist})
    :ets.insert(counter_ets!(wrr), {pool_name, 0})

    :ok
  end

  @doc """
  Delete a new pool from the generator.
  """
  @spec delete_pool(wrr, pool_name) :: :ok
  def delete_pool(wrr \\ __MODULE__, pool_name) do
    :ets.delete(key_ets!(wrr), pool_name)
    :ets.delete(counter_ets!(wrr), pool_name)

    :ok
  end

  @doc """
  Take elements from the pool in a round-robin fashion.

  ## Examples

      iex> :ok = WeightedRoundRobin.new_pool(:pool, [a: 0.1, b: 0.2, c: 1.0])
      iex> dist = Enum.map(1..10_000, fn _ -> WeightedRoundRobin.take(:pool) end)
      iex> %{a: 768, b: 1542, c: 7690} = Enum.frequencies(dist)
  """
  @spec take(wrr, pool_name) :: any
  def take(wrr \\ __MODULE__, pool_name) do
    case :ets.lookup(key_ets!(wrr), pool_name) do
      [] ->
        {:error, :not_found}

      [{_pool_name, threshold, weighted_distribution}] ->
        index =
          :ets.update_counter(
            counter_ets!(wrr),
            pool_name,
            {2, 1, threshold, 0}
          )

        Enum.at(weighted_distribution, index)
    end
  end

  @doc """
  Starts the weighted round-robin generator.

  You typically don't need to start the weighted round-robin generator, one
  is started automatically at application start, except if you explicitly
  say to not start one at your config:

      config :wrr, start: false

  So, manually it can be started as:

      WeightedRoundRobin.start_link(name: MyApp.WeightedRoundRobin)

  In your supervisor tree, you would write:

      Supervisor.start_link([
        {WeightedRoundRobin, name: MyApp.WeightedRoundRobin}
      ], strategy: :one_for_one)

  ## Options

  The weighted round-robin generator requires the following key:

    * `:name` - the name of the generator and its tables

  """
  @spec start_link([start_option]) :: {:ok, pid} | {:error, term}
  def start_link(options) do
    name =
      case Keyword.fetch(options, :name) do
        {:ok, name} when is_atom(name) ->
          name

        {:ok, other} ->
          raise ArgumentError, "expected :name to be an atom, got: #{inspect(other)}"

        :error ->
          raise ArgumentError, "expected :name option to be present"
      end

    GenServer.start_link(__MODULE__, name, name: name)
  end

  @impl true
  def init(wrr) do
    :ets.new(key_ets!(wrr), [
      :named_table,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(counter_ets!(wrr), [
      :named_table,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, []}
  end

  defp key_ets!(name), do: Module.concat([name, "Keys"])

  defp counter_ets!(name), do: Module.concat([name, "Counters"])
end
