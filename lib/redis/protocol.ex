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

  @clrf "\r\n"

  @doc """
  Parses a Redis message and returns a struct with the command and arguments.

  On success, returns {:ok, %Redis.Command{}}
  On failure, returns {:error, :<error_type>}
  """
  def parse_message(message) do
    Logger.info(parser_message: message)

    case message do
      "*" <> rest -> parse_array_string(rest)
      "$" <> rest -> parse_bulk_string(rest)
      "+" <> rest -> parse_simple_string(rest)
      _ -> {:error, :unknown_message_type}
    end
  end

  defp parse_array_string(rest) do
    [count, rest] = String.split(rest, @clrf, parts: 2)
    count = String.to_integer(count)
    parsed_array = parse_array(count, rest, [])

    case parsed_array do
      {:ok, args} ->
        [command | args] = args
        {:ok, %Command{command: command, args: args}}

      _ ->
        {:error, :invalid_protocol}
    end
  end

  defp parse_array(0, _data, acc), do: {:ok, Enum.reverse(acc)}

  defp parse_array(count, data, acc) do
    case data do
      "$" <> rest ->
        [length, remainder] = String.split(rest, @clrf, parts: 2)
        length = String.to_integer(length)
        {array_element, rest} = String.split_at(remainder, length)
        rest = String.trim_leading(rest, @clrf)
        parse_array(count - 1, rest, [array_element | acc])

      _ ->
        {:error, :invalid_protocol}
    end
  end

  defp parse_bulk_string(rest) do
    [length, rest] = String.split(rest, @clrf, parts: 2)
    length = String.to_integer(length)
    {msg, _} = String.split_at(rest, length)
    {:ok, %Command{command: "", args: [msg]}}
  end

  defp parse_simple_string(rest) do
    command = String.trim_trailing(rest, @clrf)
    {:ok, %Command{command: command, args: []}}
  end

  def simple_string(msg) do
    "+#{msg}\r\n"
  end

  def pong, do: simple_string("PONG")
  def ok, do: simple_string("OK")

  def error_response(reason) do
    "-ERR: #{reason}\r\n"
  end

  def bulk_string(msg) do
    "$#{String.length(msg)}\r\n#{msg}\r\n"
  end
end
