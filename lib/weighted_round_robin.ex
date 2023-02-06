defmodule WeightedRoundRobin do
  @moduledoc ~S"""
  A local, decentralized and scalable weighted round-robin generator.

  It allows developers to generate a sequence, evenly distributed, attending a
  predefined set of weights attributed to elements of any type. The `take/2`
  function is guaranteed to be atomic and isolated.

  Generators can have any number of pools, each under a different `pool_name`.
  The `precision` indicates how many precision digits you want at the generator
  output (so 100 indicates you want a two digits precision).

  The application can have multiple instances of the generator, but in this
  case every function needs to be prefixed with the generator name, indicated
  as `wrr`.

  Internally the pools are versioned using an ETS table for each version of the
  pool created with `new_pool`. Accesses hit the newer version first, and
  migrate from the older version to the newer version along the time. When a
  new generation is started, the oldest one is deleted by an internal GC.
  """
  use GenServer

  @type wrr :: atom
  @type pool_name :: any
  @type key_weights :: [{key, weight}]
  @type key :: any
  @type weight :: float
  @type precision :: non_neg_integer
  @type start_option :: {:name, generator_name :: atom}
  @type option :: {:precision, precision}

  @threshold_pos 2
  @counter_pos 3
  @dist_pos 4
  @version_autoincr -1
  @version_pos 2
  @default_precision 100
  @default_gc_interval :timer.minutes(1)
  @default_gc_cleanup_min_timeout :timer.seconds(10)

  @doc """
  Create a new pool under the generator.

  It is safe to reconfigure pools by calling `new_pool` with different
  parameters, while `take` is being served at other processes.

  Keys with weight equal to 0.0 will be filtered out.
  """
  @spec new_pool(pool_name, key_weights) :: :ok
  def new_pool(pool_name, key_weights) when is_list(key_weights),
    do: new_pool(__MODULE__, pool_name, key_weights, [])

  @spec new_pool(pool_name, key_weights, [option]) :: :ok
  def new_pool(pool_name, key_weights, options) when is_list(key_weights) and is_list(options),
    do: new_pool(__MODULE__, pool_name, key_weights, options)

  @spec new_pool(wrr, pool_name, key_weights, [option]) :: :ok
  def new_pool(wrr, pool_name, key_weights, options \\ [])
      when is_atom(wrr) and is_list(key_weights) and is_list(options) do
    key_weights = Enum.reject(key_weights, &(elem(&1, 1) == 0))
    total = Enum.reduce(key_weights, 0, &(&2 + 1 / elem(&1, 1)))

    precision = Keyword.get(options, :precision, @default_precision)
    kw_cw = Enum.map(key_weights, fn {k, w} -> {k, w, 0} end)

    weighted_dist =
      0..(trunc(Float.round(total)) * precision)
      |> Enum.reduce({kw_cw, []}, fn _, {kw_cw, acc} ->
        # NGINX smooth weighted round-robin algorithm
        # https://github.com/phusion/nginx/commit/27e94984486058d73157038f7950a0a36ecc6e35
        kw_cw = Enum.sort_by(kw_cw, fn {_, _, cw} -> cw end, :desc)

        sum_weights = Enum.reduce(tl(kw_cw), 0, fn {_, w, _}, acc -> acc + w end)

        {k, w, cw} = hd(kw_cw)
        tail = Enum.map(tl(kw_cw), fn {k, w, cw} -> {k, w, cw + w} end)

        {[{k, w, cw - sum_weights} | tail], [k | acc]}
      end)
      |> elem(1)
      |> Enum.reverse()

    threshold = length(weighted_dist) - 1

    version =
      :ets.update_counter(
        version_ets!(wrr),
        @version_autoincr,
        {@version_pos, 1},
        # we put default here to mitigate the impact of `reset/1` removing the key
        {@version_autoincr, 1}
      )

    object = List.to_tuple([{pool_name, version}, threshold, -1 | weighted_dist])

    :ets.insert(key_ets!(wrr), object)
    :ets.insert(version_ets!(wrr), {{pool_name, version}, :erlang.monotonic_time()})

    :ok
  end

  @doc """
  Delete a new pool from the generator.

  It is not safe to call this function while serving other processes using
  `take` or concurrently with `new_pool` for the same pool.
  """
  @spec delete_pool(wrr, pool_name) :: :ok
  def delete_pool(wrr \\ __MODULE__, pool_name) do
    for [version] <- :ets.match(version_ets!(wrr), {{pool_name, :"$1"}, :_}) do
      :ets.delete(version_ets!(wrr), {pool_name, version})
      :ets.delete(key_ets!(wrr), {pool_name, version})
    end

    :ok
  end

  @doc """
  Resets the wrr state.

  This drops all previously configured pools and resets version counter.
  It is not safe to call this function while serving other processes using
  `take` or concurrently with `new_pool` for any pool.
  """
  @spec reset(wrr) :: :ok
  def reset(wrr \\ __MODULE__) do
    # it removes the `{@version_autoincr, pos_integer()}` object
    # it will be inserted using a :ets.update_counter/4 default value on either first `take/2`
    # or first `new_pool/4` call
    :ets.delete_all_objects(version_ets!(wrr))
    :ets.delete_all_objects(key_ets!(wrr))
    :ok
  end

  @doc """
  Take elements from the pool in a round-robin fashion.

  ## Examples

      iex> :ok = WeightedRoundRobin.new_pool(:pool, [a: 0.1, b: 0.2, c: 1.0])
      iex> dist = Enum.map(1..10_000, fn _ -> WeightedRoundRobin.take(:pool) end)
      iex> %{a: 775, b: 1537, c: 7688} = Enum.frequencies(dist)
  """
  @spec take(wrr, pool_name) :: key | {:error, :not_found}
  def take(wrr \\ __MODULE__, pool_name) do
    case :ets.select(version_ets!(wrr), [{{{pool_name, :"$1"}, :_}, [], [:"$1"]}]) do
      [] ->
        {:error, :not_found}

      versions ->
        version = Enum.max(versions)

        try do
          threshold = :ets.lookup_element(key_ets!(wrr), {pool_name, version}, @threshold_pos)

          index =
            :ets.update_counter(
              key_ets!(wrr),
              {pool_name, version},
              {@counter_pos, 1, threshold, 0},
              # we put default here to mitigate the impact of `reset/1` removing the key
              {@version_autoincr, 1}
            )

          :ets.lookup_element(key_ets!(wrr), {pool_name, version}, @dist_pos + index)
        catch
          :error, :badarg -> take(wrr, pool_name)
        end
    end
  end

  @doc """
  Executes the garbage collector.

  Used when the automatic GC is disabled by passing `:gc_interval` as
  `:infinity` to `start_link`.
  """
  @spec gc(wrr) :: :ok
  def gc(wrr \\ __MODULE__, gc_cleanup_min_timeout \\ @default_gc_cleanup_min_timeout) do
    :ets.select(version_ets!(wrr), [{{{:"$1", :"$2"}, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
    |> Enum.group_by(fn {pool_name, _, _} -> pool_name end)
    |> Enum.flat_map(fn {_, candidates} ->
      [_ | candidates] = Enum.sort_by(candidates, fn {_, version, _} -> version end, :desc)

      candidates
      |> Enum.filter(fn {_, _, created_timestamp} ->
        :erlang.monotonic_time() - created_timestamp >= gc_cleanup_min_timeout
      end)
      |> Enum.map(fn {pool_name, version, _} -> {pool_name, version} end)
    end)
    |> Enum.each(fn {pool_name, version} ->
      :ets.delete(version_ets!(wrr), {pool_name, version})
      :ets.delete(key_ets!(wrr), {pool_name, version})
    end)
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

    * `:gc_interval` - If it is set, an integer > 0 is expected defining the
      interval time in milliseconds to garbage collection to run, deleting the
      older versions. If this option is not set, garbage collection is executed
      every 1 minute. If set to :infinity, garbage collection is never executed
      automatically and `gc` will need to be executed explicitly.

    * `:gc_cleanup_min_timeout` - An integer > 0 defining the min timeout in
      milliseconds for triggering the next cleanup.

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

    gc_interval = Keyword.get(options, :gc_interval, @default_gc_interval)
    gc_cleanup_min_timeout = Keyword.get(options, :gc_interval, @default_gc_cleanup_min_timeout)

    GenServer.start_link(__MODULE__, {name, gc_interval, gc_cleanup_min_timeout}, name: name)
  end

  @impl true
  def init({wrr, gc_interval, gc_cleanup_min_timeout}) do
    :ets.new(version_ets!(wrr), [
      :named_table,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.insert(version_ets!(wrr), {@version_autoincr, 0})

    :ets.new(key_ets!(wrr), [
      :named_table,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, {wrr, gc_interval, gc_cleanup_min_timeout}, gc_interval}
  end

  @impl true
  def handle_info(:timeout, {wrr, gc_interval, gc_cleanup_min_timeout}) do
    gc(wrr, gc_cleanup_min_timeout)

    {:noreply, {wrr, gc_interval, gc_cleanup_min_timeout}, gc_interval}
  end

  @compile {:inline, version_ets!: 1, key_ets!: 1}

  defp version_ets!(name), do: Module.concat([name, "Versions"])

  defp key_ets!(name), do: Module.concat([name, "Keys"])
end
