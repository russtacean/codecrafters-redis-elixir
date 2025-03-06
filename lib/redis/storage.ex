defmodule Redis.Storage do
  use GenServer

  @impl GenServer
  def init(_state) do
    {:ok, create_table()}
  end

  def start_link(state \\ []) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  @doc """
  Set a value in the storage with an optional time-to-live (TTL) in milliseconds.
  """
  def set_val(key, val, ttl \\ nil) do
    GenServer.cast(__MODULE__, {:set_val, key, val, ttl})
  end

  @doc """
  Retrieves a value from storage, returns nil if the key does not exist or has expired.
  """
  def get_val(key) do
    GenServer.call(__MODULE__, {:get_val, key})
  end

  @impl GenServer
  def handle_cast({:set_val, key, val, ttl}, _state) do
    {:noreply, write_to_table(key, val, ttl)}
  end

  @impl GenServer
  def handle_call({:get_val, key}, _from, state) do
    {:reply, read_from_table(key), state}
  end

  defp create_table do
    :ets.new(:redis, [:set, :protected, :named_table])
  end

  defp write_to_table(key, val, nil) do
    :ets.insert(:redis, {key, val, nil})
  end

  defp write_to_table(key, val, ttl) do
    expiry = :os.system_time(:millisecond) + ttl
    :ets.insert(:redis, {key, val, expiry})
  end

  defp read_from_table(key) do
    case :ets.lookup(:redis, key) do
      [result | _] -> check_expiry(result)
      [] -> nil
    end
  end

  defp check_expiry({key, val, expiry}) do
    cond do
      expiry == nil -> {key, val}
      expiry > :os.system_time(:millisecond) -> {key, val}
      :else -> nil
    end
  end
end
