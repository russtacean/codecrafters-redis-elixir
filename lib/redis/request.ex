defmodule Redis.Request do
  defstruct command: nil, client: nil

  @type t() :: %__MODULE__{
          command: Redis.Command.t(),
          client: port()
        }

  @spec new(Redis.Command.t(), port()) :: Redis.Request.t()
  def new(command, client) do
    %__MODULE__{command: command, client: client}
  end

  @spec from_client(port()) :: {:ok, Redis.Request.t()} | {:error, term()}
  def from_client(client) do
    request = %__MODULE__{command: nil, client: client}

    with {:ok, line} <- :gen_tcp.recv(client, 0),
         {:ok, command} <- Redis.Protocol.decode(line) do
      {:ok, %{request | command: command}}
    else
      error -> {:error, error}
    end
  end
end
