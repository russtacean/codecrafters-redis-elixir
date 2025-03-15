defmodule Redis.Replication do
  use GenServer
  require Logger

  alias Redis.Replication.Info

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    info = Info.from_config()
    Logger.info(starting_replication_info: info)
    {:ok, info}
  end

  def get_replication_info do
    GenServer.call(__MODULE__, :get_replication_info)
  end

  def handle_call(:get_replication_info, _from, state) do
    info_string = Info.to_string(state)
    Logger.info(replication_info: info_string)
    {:reply, info_string, state}
  end
end
