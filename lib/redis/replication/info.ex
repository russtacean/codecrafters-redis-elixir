defmodule Redis.Replication.Info do
  defstruct [:role, :master_replid, :master_repl_offset]

  @type t :: %__MODULE__{
          role: String.t(),
          master_replid: String.t(),
          master_repl_offset: integer()
        }

  @doc """
  Generates a new replication ID.
  Returns a 40-character string that can be used as a replication ID.
  """
  @spec generate_replid() :: String.t()
  def generate_replid do
    :crypto.strong_rand_bytes(20)
    |> Base.encode16(case: :lower)
  end

  def from_config do
    role = get_role()

    %__MODULE__{
      role: role,
      master_replid: get_master_replid(role),
      master_repl_offset: 0
    }
  end

  def to_string(info) do
    info
    |> Map.from_struct()
    |> Enum.map_join("\r", fn {key, value} -> "#{key}:#{value}" end)
  end

  defp get_role do
    case Redis.Config.get_replicaof() do
      nil -> "role:master"
      {host, port} when is_binary(host) and is_integer(port) -> "role:slave"
      _ -> "role:master"
    end
  end

  defp get_master_replid(role) do
    case role do
      "role:master" -> generate_replid()
      _ -> nil
    end
  end
end
