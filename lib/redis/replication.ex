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
  alias Redis.Replication.Replconf

  @type role :: :master | :replica
  @type state :: %{
          info: Info.t(),
          role: role(),
          master_socket: port() | nil,
          replicas: %{port() => Replconf.t()}
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
  Handles a REPLCONF command from a replica.
  """
  @spec handle_replconf(port(), [String.t()]) :: {:ok, String.t()} | {:error, String.t()}
  def handle_replconf(args, client) do
    GenServer.call(__MODULE__, {:handle_replconf, client, args})
  end

  @doc """
  Propagates a command to all connected replicas.
  """
  @spec propagate_command(Redis.Command.t()) :: :ok
  def propagate_command(command) do
    Logger.info(propagating_command: command)
    GenServer.cast(__MODULE__, {:propagate_command, command})
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
      master_socket: nil,
      replica_configs: %{}
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
  def handle_call({:handle_replconf, client, args}, _from, state) do
    case state.role do
      :master ->
        replconf = Map.get(state.replicas, client, Replconf.new(nil, []))

        case Master.handle_replconf(replconf, args) do
          {:ok, updated_replconf} ->
            new_state = %{
              state
              | replicas: Map.put(state.replicas, client, updated_replconf)
            }

            Logger.info(updated_replconf: updated_replconf)

            {:reply, :ok, new_state}

          {:error, error} ->
            {:reply, {:error, error}, state}
        end

      :replica ->
        {:reply, {:error, "ERR This instance is not a master"}, state}
    end
  end

  @impl GenServer
  def handle_call({:handle_replica_connection, client}, _from, state) do
    case state.role do
      :master ->
        {:reply, {:ok, client}, state}

      :replica ->
        {:reply, {:error, :not_master}, state}
    end
  end

  @impl GenServer
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

  @impl GenServer
  def handle_cast({:propagate_command, command}, state) do
    case state.role do
      :master ->
        encoded_command = Redis.Protocol.encode(command)

        Logger.info(replicas: state.replicas)

        Enum.each(state.replicas, fn {socket, _info} ->
          :gen_tcp.send(socket, encoded_command)
        end)

        {:noreply, state}

      :replica ->
        {:noreply, state}
    end
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
