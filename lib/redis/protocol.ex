defmodule Redis.Protocol do
  @moduledoc """
  This module is responsible for parsing the Redis protocol.

  Redis messages can come in several different forms (non-exhaustive list):
  - Arrays: *<number-of-elements>\r\n<element-1>...<element-n>
  - Bulk strings: $<number-of-bytes>\r\n<bytes>\r\n
  - Simple strings: +<string>\r\n

  For additional details, consult the redis protocol documentation:
  https://redis.io/docs/latest/develop/reference/protocol-spec/
  """
  require Logger
  alias Redis.Command
  alias Redis.Protocol.RDB
  @clrf "\r\n"

  @doc """
  Parses a Redis message and returns a struct with the command and arguments.

  On success, returns {:ok, %Redis.Command{}}
  On failure, returns {:error, :<error_type>}
  """
  def decode(message) do
    Logger.debug(raw_message: message)

    command =
      case message do
        "*" <> rest -> decode_array_string(rest)
        "$" <> rest -> decode_encode_bulk_string(rest)
        "+" <> rest -> decode_simple_string(rest)
        _ -> {:error, :unknown_message_type}
      end

    Logger.info(decoded: command)
    command
  end

  defp decode_array_string(rest) do
    [count, rest] = String.split(rest, @clrf, parts: 2)
    count = String.to_integer(count)
    parsed_array = decode_array(count, rest, [])

    case parsed_array do
      {:ok, args} ->
        [command | args] = args
        {:ok, %Command{command: command, args: args}}

      _ ->
        {:error, :invalid_protocol}
    end
  end

  defp decode_array(0, _data, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_array(count, data, acc) do
    case data do
      "$" <> rest ->
        [length, remainder] = String.split(rest, @clrf, parts: 2)
        length = String.to_integer(length)
        {array_element, rest} = String.split_at(remainder, length)
        rest = String.trim_leading(rest, @clrf)
        decode_array(count - 1, rest, [array_element | acc])

      _ ->
        {:error, :invalid_protocol}
    end
  end

  defp decode_encode_bulk_string(rest) do
    [length, rest] = String.split(rest, @clrf, parts: 2)
    length = String.to_integer(length)
    {msg, _} = String.split_at(rest, length)
    {:ok, %Command{command: "", args: [msg]}}
  end

  defp decode_simple_string(rest) do
    command = String.trim_trailing(rest, @clrf)
    {:ok, %Command{command: command, args: []}}
  end

  @doc """
  Encodes a value into the Redis protocol format.

  Simple responses should use atoms, such as :ok, :pong, etc.
  Strings will be encoded as RESP bulk strings.
  Lists will be encoded as RESP arrays, where each element will be encoded in turn.
  {:error, "reason"} will be encoded as a simple error message.
  Command structs will be encoded as RESP arrays with command and args.
  """
  def encode(nil), do: "$-1\r\n"
  def encode(atom) when is_atom(atom), do: encode_simple_string(atom)
  def encode(string) when is_binary(string), do: encode_bulk_string(string)
  def encode(list) when is_list(list), do: encode_array(list)
  def encode({:error, reason}), do: encode_simple_error(reason)
  def encode(%Command{} = command), do: encode_command(command)

  def encode(%RDB{binary_value: rdb_data}) do
    "$#{byte_size(rdb_data)}\r\n#{rdb_data}"
  end

  defp encode_simple_string(atom) when is_atom(atom) do
    msg =
      atom
      |> Atom.to_string()
      |> String.upcase()

    "+#{msg}\r\n"
  end

  defp encode_simple_string(string) when is_binary(string) do
    "+#{string}\r\n"
  end

  defp encode_simple_error(reason) do
    "-ERR: #{reason}\r\n"
  end

  defp encode_bulk_string(msg) do
    "$#{String.length(msg)}\r\n#{msg}\r\n"
  end

  defp encode_array([]) do
    "*0\r\n"
  end

  defp encode_array(list) do
    encoded_elements =
      list
      |> Enum.map(&encode(&1))
      |> Enum.join()

    "*#{length(list)}\r\n#{encoded_elements}"
  end

  defp encode_command(%Command{command: "PING"}) do
    encode(["PING"])
  end

  defp encode_command(%Command{command: "FULLRESYNC", args: args}) do
    [replication_id, offset] = args
    encode_simple_string("FULLRESYNC #{replication_id} #{offset}")
  end

  defp encode_command(%Command{command: command, args: args}) do
    [String.upcase(command) | args]
    |> encode_array()
  end

  def ok do
    encode(:ok)
  end

  def ping do
    encode(["PING"])
  end

  def pong do
    encode(:pong)
  end
end
