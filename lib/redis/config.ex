defmodule Redis.Config do
  use Agent

  require Logger

  @options [dir: :string, dbfilename: :string, port: :integer, replicaof: :string]
  @default_dir "/tmp"
  @default_file_name "dump.rdb"
  @default_port 6379

  def start_link(_) do
    args = System.argv()
    {parsed_options, _, _} = OptionParser.parse(args, strict: @options)

    directory = Keyword.get(parsed_options, :dir, @default_dir)
    dbfilename = Keyword.get(parsed_options, :dbfilename, @default_file_name)
    port = Keyword.get(parsed_options, :port, @default_port)
    replicaof = Keyword.get(parsed_options, :replicaof, nil)

    state = %{
      dir: directory,
      dbfilename: dbfilename,
      port: port,
      replicaof: replicaof
    }

    Logger.debug(initial_config: state)
    Agent.start_link(fn -> state end, name: __MODULE__)
  end

  def get_dir do
    Agent.get(__MODULE__, fn state -> state.dir end)
  end

  def get_dbfilename do
    Agent.get(__MODULE__, fn state -> state.dbfilename end)
  end

  def get_port do
    Agent.get(__MODULE__, fn state -> state.port end)
  end

  def get_replication_info do
    role =
      case get_replicaof() do
        nil -> "role:master"
        [_host, _port] -> "role:slave"
      end

    Logger.info(replication_info: role)
    role
  end

  defp get_replicaof do
    replicaof = Agent.get(__MODULE__, fn state -> state.replicaof end)

    case replicaof do
      nil -> nil
      _ -> String.split(replicaof, " ")
    end
  end
end
