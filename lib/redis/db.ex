defmodule Redis.DB do
  def get_dir do
    Redis.Config.get_dir()
  end

  def get_dbfilename do
    Redis.Config.get_dbfilename()
  end

  def keys!() do
    path = "#{get_dir()}/#{get_dbfilename()}"

    Redis.DB.File.open!(path)
    |> Redis.DB.File.keys()
  end
end
