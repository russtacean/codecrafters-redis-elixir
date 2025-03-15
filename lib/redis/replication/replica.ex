defmodule Redis.Replication.Replica do
  require Logger

  alias Redis.Protocol

  def start_handshake(host, port) do
    Logger.debug("Starting handshake with master at #{host}:#{port}")

    host = String.to_charlist(host)

    case :gen_tcp.connect(host, port, [
           :binary,
           active: false
         ]) do
      {:ok, socket} ->
        Logger.info("Connected to master at #{host}:#{port}")
        send_ping(socket)

      {:error, reason} ->
        Logger.error("Failed to connect to master: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp send_ping(socket) do
    ping_request = Protocol.ping_request()

    case :gen_tcp.send(socket, ping_request) do
      :ok ->
        Logger.info("Sent PING to master")
        receive_pong(socket)

      {:error, reason} ->
        Logger.error("Failed to send PING: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp receive_pong(socket) do
    pong_response = Protocol.pong_response()

    case :gen_tcp.recv(socket, 0) do
      {:ok, ^pong_response} ->
        Logger.info("Received PONG from master")
        {:ok, socket}

      {:ok, other} ->
        Logger.error("Unexpected response from master: #{inspect(other)}")
        {:error, :unexpected_response}

      {:error, reason} ->
        Logger.error("Failed to receive PONG: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
