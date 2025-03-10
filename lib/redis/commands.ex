defmodule Redis.Commands do
  require Logger

  alias Redis.Command
  alias Redis.Storage

  def execute({:ok, %Command{command: "PING"}}), do: :pong

  def execute({:ok, %Command{command: "GET", args: [key]}}) do
    val = Storage.get_val(key)
    Logger.debug(storage_get: val)

    case val do
      nil -> nil
      {_, val} -> val
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
      all_keys = Redis.DB.keys!()

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

  defp get_config(["dir"]), do: ["dir", Redis.DB.get_dir()]
  defp get_config(["dbfilename"]), do: ["dbfilename", Redis.DB.get_dbfilename()]
end
