defmodule Redis.Replication do
  use GenServer
  require Logger

  alias Redis.Replication.Info
  alias Redis.Replication.Replica

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    info = Info.from_config()
    Logger.info(starting_replication_info: info)

    case Redis.Config.get_replicaof() do
      {host, port} ->
        spawn_link(fn -> initiate_replica_handshake(host, port) end)

      nil ->
        :ok
    end

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

  defp initiate_replica_handshake(host, port) do
    case Replica.start_handshake(host, port) do
      {:ok, _socket} ->
        Logger.info("Successfully completed replica handshake with master")

      {:error, reason} ->
        Logger.error("Failed to complete replica handshake: #{inspect(reason)}")
    end
  end
end
