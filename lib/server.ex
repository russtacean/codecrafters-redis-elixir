defmodule Server do
  require Logger

  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
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
        |> parse_line()
        |> handle_command()
        |> write_line(client)

        serve(client)

      {:error, :closed} ->
        Logger.info("Client closed connection")

      {:error, reason} ->
        Logger.error("Error: #{reason}")
    end
  end

  defp parse_line(line) do
    parsed = String.split(line, "\r\n", trim: true)
    Logger.info("Parsed: #{inspect(parsed)}")
    parsed
  end

  defp handle_command([_, _, "ECHO", length, msg]) do
    "#{length}\r\n#{msg}\r\n"
  end

  defp handle_command([_, _, "PING"]), do: "+PONG\r\n"
  defp handle_command(_), do: "Err: unknown command\r\n"

  defp write_line(line, client) do
    :gen_tcp.send(client, line)
  end
end
