defmodule Redis do
  require Logger

  alias Redis.Commands
  alias Redis.MultiResponse
  alias Redis.Protocol

  @moduledoc """
  Implementation of a subset of Redis server functionality
  """

  use Application

  def start(_type, _args) do
    children = [
      Redis.Config,
      Redis.Replication,
      Redis.Storage,
      Redis.RDB,
      {Task, fn -> Redis.listen() end}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def listen() do
    {:ok, socket} =
      :gen_tcp.listen(Redis.Config.get_port(), [:binary, active: false, reuseaddr: true])

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
        |> Protocol.decode()
        |> Commands.execute()
        |> respond(client)

        serve(client)

      {:error, :closed} ->
        Logger.info("Client closed connection")

      {:error, reason} ->
        Logger.error("Error: #{reason}")
    end
  end

  defp write_line(line, client) do
    Logger.info(sent_message: line)
    :gen_tcp.send(client, line)
  end

  defp respond(%MultiResponse{} = multi_response, client) do
    Logger.info(multi_response: multi_response)
    responses = MultiResponse.read_responses(multi_response)

    Enum.each(responses, fn response -> respond(response, client) end)
  end

  defp respond(message, client) do
    Logger.info(pre_encode: message)

    message
    |> Protocol.encode()
    |> write_line(client)
  end
end
