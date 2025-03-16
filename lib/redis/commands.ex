defmodule Redis.Commands do
  require Logger

  alias Redis.Command
  alias Redis.Storage
  alias Redis.RDB

  def execute({:ok, %Command{command: "PING"}}), do: :pong

  def execute({:ok, %Command{command: "GET", args: [key]}}) do
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

  def execute({:ok, %Command{command: "REPLCONF", args: args}}) do
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

  def execute({:ok, %Command{command: "CONFIG", args: args}}) do
    [subcommand | rest] = args
    Logger.debug(config_subcommand: {subcommand, rest})

    case subcommand do
      "GET" -> get_config(rest)
      _ -> {:error, "Invalid CONFIG subcommand"}
    end
  end

  def execute({:ok, %Command{command: "INFO", args: args}}) do
    [subcommand | rest] = args
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

  def execute({:ok, %Command{command: "ECHO", args: [msg]}}), do: msg

  def execute({:error, err_atom}) do
    err_msg =
      err_atom
      |> Atom.to_string()
      |> String.replace("_", " ")
      |> String.capitalize()

    {:error, err_msg}
  end

  def execute({:ok, %Command{command: "SET", args: args}}), do: set_command(args)

  def execute({:ok, %Command{command: "KEYS", args: args}}) do
    [pattern] = args
    pattern = String.replace(pattern, "*", ".*")
    Logger.debug(keys_pattern: pattern)

    try do
      all_keys = Redis.RDB.keys()

      case args do
        ["*"] -> all_keys
        _ -> Enum.filter(all_keys, &String.match?(&1, ~r/#{pattern}/))
      end
    rescue
      error -> {:error, "Error fetching keys: #{error}"}
    end
  end

  defp set_command([key, val]) do
    Storage.set_val(key, val)
    :ok
  end

  defp set_command([key, val, "px", expiry]) do
    Storage.set_val(key, val, String.to_integer(expiry))
    :ok
  end

  defp get_config(["dir"]), do: ["dir", Redis.RDB.get_dir()]
  defp get_config(["dbfilename"]), do: ["dbfilename", Redis.RDB.get_dbfilename()]
end
