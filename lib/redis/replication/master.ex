defmodule Redis.Replication.Master do
  @moduledoc """
  Handles the master side of Redis replication, managing replica connections
  and implementing the replication handshake protocol.
  """

  require Logger

  alias Redis.Command
  alias Redis.MultiResponse

  def handle_replconf(args) do
    case args do
      ["listening-port", port] ->
        Logger.info("Received REPLCONF listening-port", port: port)
        :ok

      ["capa", "eof"] ->
        Logger.info("Received REPLCONF capabilities: eof")
        :ok

      ["capa", "psync2"] ->
        Logger.info("Received REPLCONF capabilities: psync2")
        :ok

      _ ->
        {:error, "ERR Unknown REPLCONF subcommand or wrong number of arguments"}
    end
  end

  def handle_psync(args) do
    case args do
      ["?", "-1"] ->
        # Handle initial sync request
        replication_id = Redis.Replication.Info.generate_replid()

        Logger.info("Initiating multi-response FULLRESYNC for new replica",
          replication_id: replication_id
        )

        MultiResponse.new()
        |> MultiResponse.add_response(%Command{
          command: "FULLRESYNC",
          args: [replication_id, "0"]
        })
        |> MultiResponse.add_response(Redis.RDB.File.empty_file_binary())

      [replid, offset] when is_binary(replid) and is_binary(offset) ->
        # For now, we'll always respond with FULLRESYNC
        # TODO: Implement partial sync when we have replication backlog
        new_replid = Redis.Replication.Info.generate_replid()

        MultiResponse.new()
        |> MultiResponse.add_response(%Command{
          command: "FULLRESYNC",
          args: [new_replid, "0"]
        })
        |> MultiResponse.add_response(Redis.RDB.File.empty_file_binary())

      _ ->
        {:error, "ERR wrong number of arguments for 'PSYNC' command"}
    end
  end
end
