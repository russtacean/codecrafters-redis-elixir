defmodule Redis.ProtocolTest do
  use ExUnit.Case, async: true

  test "Simple String: PING" do
    {:ok, command} = Redis.Protocol.parse_message("*1\r\n$4\r\nPING\r\n")
    assert command == %Redis.Command{command: "PING", args: []}
  end

  test "Array: Echo" do
    {:ok, command} = Redis.Protocol.parse_message("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")
    assert command == %Redis.Command{command: "ECHO", args: ["hey"]}
  end

  test "Array: Set" do
    {:ok, command} =
      Redis.Protocol.parse_message("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")

    assert command == %Redis.Command{command: "SET", args: ["key", "value"]}
  end

  test "Array: Get" do
    {:ok, command} = Redis.Protocol.parse_message("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
    assert command == %Redis.Command{command: "GET", args: ["key"]}
  end

  test "Error: unknown message type" do
    # Improper carriage return after key
    {:error, :unknown_message_type} =
      Redis.Protocol.parse_message("1Foo\r\n")
  end
end
