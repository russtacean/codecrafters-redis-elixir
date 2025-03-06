defmodule Redis do
  alias Redis.Command
  alias Redis.Protocol
  alias Redis.Storage
  require Logger

  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    children = [
      Storage,
      {Task, fn -> Redis.listen() end}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def listen() do
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    accept_loop(socket)
  end

  defp accept_loop(listening_socket) do
    {:ok, client} = :gen_tcp.accept(listening_socket)

    spawn(fn -> serve(client) end)
    accept_loop(listening_socket)
  end

  defp serve(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, line} ->
        line
        |> Protocol.parse_message()
        |> handle_command()
        |> write_line(client)

        serve(client)

      {:error, :closed} ->
        Logger.info("Client closed connection")

      {:error, reason} ->
        Logger.error("Error: #{reason}")
    end
  end

  defp handle_command({:ok, %Command{command: "SET", args: [key, val]}}) do
    Storage.set_val(key, val)
    Protocol.ok()
  end

  defp handle_command({:ok, %Command{command: "SET", args: [key, val, "px", expiry]}}) do
    Storage.set_val(key, val, String.to_integer(expiry))
    Protocol.ok()
  end

  defp handle_command({:ok, %Command{command: "GET", args: [key]}}) do
    val = Storage.get_val(key)
    Logger.debug(storage_get: val)

    case val do
      nil -> Protocol.null_bulk_string()
      {_, val} -> Protocol.bulk_string(val)
    end
  end

  defp handle_command({:ok, %Command{command: "ECHO", args: [msg]}}),
    do: Protocol.bulk_string(msg)

  defp handle_command({:ok, %Command{command: "PING"}}), do: Protocol.pong()

  defp handle_command({:error, :invalid_protocol}),
    do: Protocol.error_response("Invalid protocol")

  defp handle_command({:error, :unknown_message_type}),
    do: Protocol.error_response("Unknown message type")

  defp write_line(line, client) do
    Logger.info(sent_message: line)
    :gen_tcp.send(client, line)
  end
end
