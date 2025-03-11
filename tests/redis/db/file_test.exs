defmodule Redis.RDB.FileTest do
  use ExUnit.Case, async: true

  defp load_rdb(file_name) do
    path = Path.join([File.cwd!(), "tests/fixtures", file_name])
    Redis.RDB.File.open!(path)
  end

  test "Load basic key/value file" do
    rdb = load_rdb("key_value.rdb")

    assert rdb.version == "0011"
    assert rdb.metadata["redis-ver"] == "7.2.7"

    assert rdb.database.stats.total_size == 1
    assert rdb.database.stats.expiry_size == 0
    assert rdb.database.kv_store == %{"mykey" => {"myval", nil}}
  end
end
