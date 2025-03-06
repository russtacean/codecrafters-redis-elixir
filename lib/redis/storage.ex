defmodule Redis.Storage do
  use GenServer

  @impl GenServer
  def init(_state) do
    {:ok, create_table()}
  end

  def start_link(state \\ []) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  def set_val(key, val) do
    GenServer.cast(__MODULE__, {:set_val, key, val})
  end

  def get_val(key) do
    GenServer.call(__MODULE__, {:get_val, key})
  end

  @impl GenServer
  def handle_cast({:set_val, key, val}, _state) do
    {:noreply, write_to_table(key, val)}
  end

  @impl GenServer
  def handle_call({:get_val, key}, _from, state) do
    {:reply, read_from_table(key), state}
  end

  defp create_table do
    :ets.new(:redis, [:set, :protected, :named_table])
  end

  defp write_to_table(key, val) do
    :ets.insert(:redis, {key, val})
  end

  defp read_from_table(key) do
    :ets.lookup(:redis, key)
  end
end
