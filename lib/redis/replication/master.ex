defmodule Redis.Replication.Master do
  @moduledoc """
  Handles the master side of Redis replication, managing replica connections
  and implementing the replication handshake protocol.
  """

  require Logger

  alias Redis.Protocol
  alias Redis.Replication.Info

  @type replica_info :: %{
          port: integer() | nil,
          capabilities: list(atom())
        }

  @doc """
  Handles an incoming replica connection and performs the replication handshake.

  ## Parameters
    * socket - The TCP socket connected to the replica

  ## Returns
    * `{:ok, replica_info()}` - Successfully completed handshake with replica information
    * `{:error, term()}` - Failed to complete handshake with reason
  """
  @spec handle_replica_connection(port()) :: {:ok, replica_info()} | {:error, term()}
  def handle_replica_connection(socket) do
    with :ok <- handle_ping(socket),
         {:ok, replica_info} <- handle_replconf(socket),
         :ok <- send_fullsync(socket) do
      Logger.info("Successfully completed handshake with replica", replica_info: replica_info)
      {:ok, replica_info}
    end
  end

  @spec handle_ping(port()) :: :ok | {:error, term()}
  defp handle_ping(socket) do
    case recv_command(socket) do
      {:ok, %Redis.Command{command: "PING"}} ->
        Logger.info("Received PING from replica")

        case :gen_tcp.send(socket, Protocol.pong()) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      other ->
        Logger.error(unexpected_ping_command: other)
        {:error, :unexpected_command}
    end
  end

  @spec handle_replconf(port()) :: {:ok, replica_info()} | {:error, term()}
  defp handle_replconf(socket) do
    replica_info = %{port: nil, capabilities: []}

    # Handle listening-port
    with {:ok, %Redis.Command{command: "REPLCONF", args: ["listening-port", port]}} <-
           recv_command(socket),
         :ok <- :gen_tcp.send(socket, Protocol.ok()),
         # Handle capabilities
         {:ok, %Redis.Command{command: "REPLCONF", args: ["capa", "eof", "capa", "psync2"]}} <-
           recv_command(socket),
         :ok <- :gen_tcp.send(socket, Protocol.ok()) do
      replica_info = %{
        replica_info
        | port: String.to_integer(port),
          capabilities: [:eof, :psync2]
      }

      {:ok, replica_info}
    else
      error ->
        Logger.error(replconf_error: error)
        {:error, :invalid_replconf}
    end
  end

  @spec send_fullsync(port()) :: :ok | {:error, term()}
  defp send_fullsync(socket) do
    case recv_command(socket) do
      {:ok, %Redis.Command{command: "PSYNC", args: ["?", "-1"]}} ->
        replication_id = Info.generate_replid()
        offset = "0"
        response = Protocol.encode(["FULLRESYNC", replication_id, offset])

        case :gen_tcp.send(socket, response) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      other ->
        Logger.error(unexpected_psync_command: other)
        {:error, :unexpected_command}
    end
  end

  @spec recv_command(port()) :: {:ok, Redis.Command.t()} | {:error, term()}
  defp recv_command(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} -> Protocol.decode(data)
      error -> error
    end
  end
end
