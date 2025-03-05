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
      {:ok, data} ->
        Logger.info("Received: #{data}")
        write_line(data, client)
        serve(client)

      {:error, :closed} ->
        Logger.info("Client closed connection")

      {:error, reason} ->
        Logger.error("Error: #{reason}")
    end
  end

  defp write_line(_line, client) do
    # Hardcode for now per instuctions
    :gen_tcp.send(client, "+PONG\r\n")
  end
end
