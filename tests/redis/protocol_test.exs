defmodule Redis.ProtocolTest do
  use ExUnit.Case, async: true

  test "Decode simple string PING" do
    {:ok, command} = Redis.Protocol.decode("*1\r\n$4\r\nPING\r\n")
    assert command == %Redis.Command{command: "PING", args: []}
  end

  test "Decode array: ECHO" do
    {:ok, command} = Redis.Protocol.decode("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")
    assert command == %Redis.Command{command: "ECHO", args: ["hey"]}
  end

  test "Decode array: SET" do
    {:ok, command} =
      Redis.Protocol.decode("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")

    assert command == %Redis.Command{command: "SET", args: ["key", "value"]}
  end

  test "Decode array: GET" do
    {:ok, command} = Redis.Protocol.decode("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
    assert command == %Redis.Command{command: "GET", args: ["key"]}
  end

  test "Decode array: CONFIG GET" do
    {:ok, command} =
      Redis.Protocol.decode("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n")

    assert command == %Redis.Command{command: "CONFIG", args: ["GET", "dir"]}
  end

  test "Decode array: KEYS wildcard" do
    {:ok, command} =
      Redis.Protocol.decode("*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")

    assert command == %Redis.Command{command: "KEYS", args: ["*"]}
  end

  test "Decode array: KEYS multiple" do
    {:ok, command} =
      Redis.Protocol.decode("*3\r\n$4\r\nKEYS\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

    assert command == %Redis.Command{command: "KEYS", args: ["foo", "bar"]}
  end

  test "Error: unknown message type" do
    # Improper carriage return after key
    {:error, :unknown_message_type} =
      Redis.Protocol.decode("1Foo\r\n")
  end

  test "Encode nil" do
    assert Redis.Protocol.encode(nil) == "$-1\r\n"
  end

  test "Encode OK" do
    assert Redis.Protocol.encode(:ok) == "+OK\r\n"
  end

  test "Encode PONG" do
    assert Redis.Protocol.encode(:pong) == "+PONG\r\n"
  end

  test "Encode bulk string" do
    assert Redis.Protocol.encode("foobar") == "$6\r\nfoobar\r\n"
  end

  test "Encode array" do
    assert Redis.Protocol.encode(["foo", "bar"]) == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
  end

  test "Encode simple error" do
    assert Redis.Protocol.encode({:error, "Test error"}) == "-ERR: Test error\r\n"
  end
end
