defmodule Redis.Replication.Info do
  defstruct [:role, :master_replid, :master_repl_offset]

  @type t :: %__MODULE__{
          role: String.t(),
          master_replid: String.t(),
          master_repl_offset: integer()
        }

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

  defp generate_replid do
    # Generates a random 40 character string
    :crypto.strong_rand_bytes(40) |> Base.url_encode64(padding: false)
  end
end
