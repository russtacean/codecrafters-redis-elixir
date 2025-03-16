defmodule Redis.Replication do
  @moduledoc """
  Manages Redis replication, handling both master and replica roles.
  This module coordinates the replication process, maintains connection state,
  and manages the replication topology.
  """

  use GenServer
  require Logger

  alias Redis.Replication.Info
  alias Redis.Replication.{Master, Replica}

  @type role :: :master | :replica
  @type state :: %{
          info: Info.t(),
          role: role(),
          replicas: %{optional(port()) => Master.replica_info()},
          master_socket: port() | nil
        }

  @doc """
  Starts the replication manager as a GenServer.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Retrieves the current replication information.
  """
  @spec get_replication_info() :: String.t()
  def get_replication_info do
    GenServer.call(__MODULE__, :get_replication_info)
  end

  @doc """
  Handles a new replica connection attempt.
  """
  @spec handle_replica_connection(port()) :: :ok | {:error, term()}
  def handle_replica_connection(socket) do
    GenServer.call(__MODULE__, {:handle_replica_connection, socket})
  end

  @impl true
  @spec init(term()) :: {:ok, state()}
  def init(_) do
    info = Info.from_config()
    Logger.info(starting_replication_info: info)

    state = %{
      info: info,
      role: :master,
      # Map of socket -> replica_info
      replicas: %{},
      master_socket: nil
    }

    case Redis.Config.get_replicaof() do
      {host, port} ->
        spawn_link(fn -> initiate_replica_handshake(host, port) end)
        {:ok, %{state | role: :replica}}

      nil ->
        {:ok, %{state | role: :master}}
    end
  end

  @impl GenServer
  def handle_call({:handle_replica_connection, socket}, _from, %{role: :master} = state) do
    case Master.handle_replica_connection(socket) do
      {:ok, replica_info} ->
        new_state = put_in(state, [:replicas, socket], replica_info)
        {:reply, :ok, new_state}

      {:error, reason} = error ->
        Logger.error("Failed to handle replica connection", error: reason)
        {:reply, error, state}
    end
  end

  def handle_call({:handle_replica_connection, _socket}, _from, state) do
    {:reply, {:error, :not_master}, state}
  end

  def handle_call(:get_replication_info, _from, state) do
    info_string = Info.to_string(state.info)
    Logger.info(replication_info: info_string)
    {:reply, info_string, state}
  end

  @impl GenServer
  def handle_cast({:master_connection_established, socket, repl_id, offset}, state) do
    new_info = %{state.info | master_replid: repl_id, master_repl_offset: offset}
    {:noreply, %{state | master_socket: socket, info: new_info}}
  end

  @spec initiate_replica_handshake(String.t(), integer()) :: :ok
  defp initiate_replica_handshake(host, port) do
    case Replica.start_handshake(host, port) do
      {:ok, socket, repl_id, offset} ->
        Logger.info("Successfully completed replica handshake with master",
          replication_id: repl_id,
          offset: offset
        )

        GenServer.cast(__MODULE__, {:master_connection_established, socket, repl_id, offset})

      {:error, reason} ->
        Logger.error("Failed to complete replica handshake: #{inspect(reason)}")
    end
  end
end
