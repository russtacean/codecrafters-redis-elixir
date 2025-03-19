defmodule Redis.Replication.Replconf do
  defstruct(port: nil, capabilities: [])

  @type t :: %__MODULE__{
          port: integer(),
          capabilities: [String.t()]
        }

  def new(port, capabilities), do: %__MODULE__{port: port, capabilities: capabilities}

  def add_capability(replconf, capability) do
    %{replconf | capabilities: [capability | replconf.capabilities]}
  end
end
