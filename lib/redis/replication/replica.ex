defmodule Redis.Replication.Replica do
  require Logger

  alias Redis.Command
  alias Redis.Protocol

  def start_handshake(host, port) do
    Logger.debug("Starting handshake with master at #{host}:#{port}")

    host = String.to_charlist(host)

    case :gen_tcp.connect(host, port, [:binary, active: false]) do
      {:ok, socket} ->
        Logger.info("Connected to master at #{host}:#{port}")

        with {:ok, _} <- handshake_ping(socket),
             {:ok, _} <- handshake_replconf(socket),
             {:ok, socket, repl_id, offset} <- handshake_psync(socket) do
          {:ok, socket, repl_id, offset}
        end

      {:error, reason} ->
        Logger.error(master_connect_failed: reason)
        {:error, reason}
    end
  end

  defp handshake_ping(socket) do
    Logger.info("Sending PING to master")
    send_and_receive(socket, Protocol.ping(), Protocol.pong())
  end

  defp handshake_replconf(socket) do
    port = Redis.Config.get_port()
    ok_msg = Protocol.ok()

    # Send listening port
    with {:ok, _} <-
           send_and_receive(
             socket,
             Protocol.encode(%Command{
               command: "REPLCONF",
               args: ["listening-port", Integer.to_string(port)]
             }),
             ok_msg
           ),
         Logger.info("REPLCONF listening-port acknowledged"),
         # Send capabilities
         {:ok, _} <-
           send_and_receive(
             socket,
             Protocol.encode(%Command{
               command: "REPLCONF",
               args: ["capa", "eof", "capa", "psync2"]
             }),
             ok_msg
           ) do
      Logger.info("REPLCONF capabilities acknowledged")
      {:ok, socket}
    end
  end

  defp handshake_psync(socket) do
    Logger.info("Sending PSYNC command to master")
    psync_cmd = Protocol.encode(["PSYNC", "?", "-1"])

    case send_and_receive(socket, psync_cmd) do
      {:ok, response} ->
        case Protocol.decode(response) do
          {:ok, %Redis.Command{command: "FULLRESYNC", args: [replication_id, offset]}} ->
            Logger.info("Received FULLRESYNC from master",
              replication_id: replication_id,
              offset: offset
            )

            {:ok, socket, replication_id, String.to_integer(offset)}

          other ->
            Logger.error(fullresync_unexpected_response: other)
            {:error, :unexpected_response}
        end

      error ->
        error
    end
  end

  # Private helper for send + receive with expected response
  defp send_and_receive(socket, command, expected_response \\ nil) do
    case :gen_tcp.send(socket, command) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, ^expected_response} when not is_nil(expected_response) ->
            {:ok, expected_response}

          {:ok, response} when is_nil(expected_response) ->
            {:ok, response}

          {:ok, other} ->
            Logger.error(unexpected_response: other)
            {:error, :unexpected_response}

          {:error, reason} ->
            Logger.error(receive_failed: reason)
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error(send_failed: reason)
        {:error, reason}
    end
  end
end
