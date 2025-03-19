defmodule Redis.Commands do
  require Logger

  alias Redis.Command
  alias Redis.Storage
  alias Redis.RDB

  def execute(%Redis.Request{command: command, client: client}) do
    handle_command(command, client)
  end

  def execute({:error, err_atom}) do
    err_msg =
      err_atom
      |> Atom.to_string()
      |> String.replace("_", " ")
      |> String.capitalize()

    {:error, err_msg}
  end

  defp handle_command(%Command{command: "PING"}, _client), do: :pong

  defp handle_command(%Command{command: "GET", args: [key]}, _client) do
    val = Storage.get_val(key)
    Logger.info(storage_get: val)

    case val do
      nil ->
        case RDB.get_val(key) do
          nil ->
            Logger.debug(rdb_miss: key)
            nil

          {val, expiry} ->
            Logger.info(rdb_get: {key, val, expiry}, sys_time: :os.system_time(:millisecond))
            Storage.set_val(key, val, expiry)
            val
        end

      {_, val} ->
        val
    end
  end

  defp handle_command(%Command{command: "REPLCONF", args: args}, client) do
    Redis.Replication.handle_replconf(args, client)
  end

  defp handle_command(%Command{command: "PSYNC", args: args}, _client) do
    Redis.Replication.Master.handle_psync(args)
  end

  defp handle_command(%Command{command: "CONFIG", args: [subcommand | rest]}, _client) do
    Logger.debug(config_subcommand: {subcommand, rest})

    case subcommand do
      "GET" -> get_config(rest)
      _ -> {:error, "Invalid CONFIG subcommand"}
    end
  end

  defp handle_command(%Command{command: "INFO", args: [subcommand | rest]}, _client) do
    Logger.debug(info_subcommand: {subcommand, rest})

    case String.upcase(subcommand) do
      "REPLICATION" ->
        info = Redis.Replication.get_replication_info()
        Logger.info(replication_info: info)
        info

      _ ->
        {:error, "Invalid INFO subcommand"}
    end
  end

  defp handle_command(%Command{command: "ECHO", args: [msg]}, _client), do: msg

  defp handle_command(%Command{command: "SET", args: args}, client), do: set_command(args, client)

  defp handle_command(%Command{command: "KEYS", args: [pattern]}, _client) do
    pattern = String.replace(pattern, "*", ".*")
    Logger.debug(keys_pattern: pattern)

    try do
      all_keys = Redis.RDB.keys()

      case pattern do
        "*" -> all_keys
        _ -> Enum.filter(all_keys, &String.match?(&1, ~r/#{pattern}/))
      end
    rescue
      error -> {:error, "Error fetching keys: #{error}"}
    end
  end

  # Add catch-all for unknown commands
  defp handle_command(%Command{command: unknown_command}, _client) do
    {:error, "Unknown command '#{unknown_command}'"}
  end

  defp set_command([key, val], _client) do
    Storage.set_val(key, val)
    Redis.Replication.propagate_command(%Command{command: "SET", args: [key, val]})
    :ok
  end

  defp set_command([key, val, "px", expiry], _client) do
    Storage.set_val(key, val, String.to_integer(expiry))
    Redis.Replication.propagate_command(%Command{command: "SET", args: [key, val, "px", expiry]})
    :ok
  end

  defp get_config(["dir"]), do: ["dir", Redis.RDB.get_dir()]
  defp get_config(["dbfilename"]), do: ["dbfilename", Redis.RDB.get_dbfilename()]
end
