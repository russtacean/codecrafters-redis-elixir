defmodule Redis.RDB do
  use GenServer

  @impl GenServer
  def init(_) do
    # If the file isn't present at the specified location, we can
    # assume the file hasn't been created yet and we'll start a new one
    try do
      rdb_file =
        Redis.RDB.File.open!("#{Redis.Config.get_dir()}/#{Redis.Config.get_dbfilename()}")

      {:ok, rdb_file}
    rescue
      _ -> {:ok, Redis.RDB.File.empty_file()}
    end
  end

  def start_link(state \\ []) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  def get_dir do
    Redis.Config.get_dir()
  end

  def get_dbfilename do
    Redis.Config.get_dbfilename()
  end

  def keys() do
    GenServer.call(__MODULE__, {:get_keys})
  end

  def set_val(key, val, expiry \\ nil) do
    GenServer.cast(__MODULE__, {:set_val, key, val, expiry})
  end

  def get_val(key) do
    GenServer.call(__MODULE__, {:get_val, key})
  end

  @impl GenServer
  def handle_call({:get_keys}, _from, rdb_file) do
    {:reply, Redis.RDB.File.keys(rdb_file), rdb_file}
  end

  @impl GenServer
  def handle_call({:get_val, key}, _from, rdb_file) do
    {:reply, Redis.RDB.File.get_val(rdb_file, key), rdb_file}
  end

  @impl GenServer
  def handle_cast({:set_val, key, val, expiry}, rdb_file) do
    {:noreply, Redis.RDB.File.set_val(rdb_file, key, val, expiry)}
  end
end
