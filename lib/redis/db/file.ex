defmodule Redis.DB.File do
  @moduledoc """
  Provides functions for reading and writing RDB files.
  See https://rdb.fnordig.de/file_format.html for details on the RDB file format.
  """
  require Logger

  defstruct version: 1, metadata: %{}, database: %{}
  @type t :: %__MODULE__{version: integer(), metadata: map(), database: map()}

  # Can have one or more metadata subsection marked by 0xFA
  @metadata_start 0xFA

  # DB sections
  @db_start 0xFE
  @db_resize 0xFB
  @db_kv_expiry_seconds 0xFD
  @db_kv_expiry_ms 0xFC
  @db_end 0xFF

  @val_types %{
    0x00 => :string
    # TODO Implement other types
  }

  def keys(db_file) do
    db_file.database.kv_store
    |> Map.keys()
  end

  def open!(path) do
    path
    |> read!()
    |> decode!()
  end

  defp read!(path) do
    case File.read(path) do
      {:ok, content} -> content
      {:error, reason} -> raise "Error reading file: #{reason}"
    end
  end

  defp decode!(bytes) do
    db_file = %__MODULE__{}
    decode_header!(bytes, db_file)
  end

  defp decode_header!(<<"REDIS", version::binary-size(4), rest::bytes>>, db_file) do
    db_file = Map.put(db_file, :version, version)
    decode_metadata!(rest, db_file)
  end

  defp decode_header!(_, _db_file), do: raise("Invalid RDB file")

  defp decode_metadata!(bytes, db_file, metadata \\ %{}) do
    case bytes do
      <<@metadata_start, rest::binary>> ->
        {key, rest} = decode_string!(rest)
        {val, rest} = decode_string!(rest)
        metadata = Map.put(metadata, key, val)
        Logger.debug(metadata: metadata)
        decode_metadata!(rest, db_file, metadata)

      <<@db_start, _rest::binary>> ->
        Logger.debug(parsed_metadata: metadata)
        db_file = Map.put(db_file, :metadata, metadata)
        decode_database!(bytes, db_file)
    end
  end

  defp decode_database!(bytes, db_file, database \\ %{stats: %{}, kv_store: %{}}) do
    case bytes do
      <<@db_start, rest::binary>> ->
        stats = database.stats
        {db_number, rest} = decode_size!(rest)
        stats = Map.put(stats, :db_number, db_number)
        database = Map.put(database, :stats, stats)
        decode_database!(rest, db_file, database)

      <<@db_resize, rest::binary>> ->
        stats = database.stats
        {total_size, rest} = decode_size!(rest)
        stats = Map.put(stats, :total_size, total_size)
        {expiry_size, rest} = decode_size!(rest)
        stats = Map.put(stats, :expiry_size, expiry_size)
        database = Map.put(database, :stats, stats)
        Logger.debug(db_resize: database)
        decode_database!(rest, db_file, database)

      <<@db_kv_expiry_seconds, timestamp_s::little-integer-size(64), 0, rest::binary>> ->
        expiry = DateTime.from_unix(timestamp_s, :second)
        {database, rest} = parse_db_kv!(rest, expiry, database)
        decode_database!(rest, db_file, database)

      <<@db_kv_expiry_ms, timestamp_ms::little-integer-size(32), 0, rest::binary>> ->
        expiry = DateTime.from_unix(timestamp_ms, :millisecond)
        {database, rest} = parse_db_kv!(rest, expiry, database)
        decode_database!(rest, db_file, database)

      <<@db_end::size(8), _::binary>> ->
        # Ignore 8-bit checksum for now
        db_file = Map.put(db_file, :database, database)
        Logger.info(parsed_db_file: db_file)
        db_file

      _ ->
        {database, rest} = parse_db_kv!(bytes, nil, database)
        decode_database!(rest, db_file, database)
    end
  end

  defp decode_size!(<<0b00::size(2), size::big-integer-size(6), rest::binary>>), do: {size, rest}
  defp decode_size!(<<0b01::size(2), size::big-integer-size(14), rest::binary>>), do: {size, rest}

  defp decode_size!(<<0b10::size(2), _::6, size::big-integer-size(4 * 8), rest::binary>>),
    do: {size, rest}

  defp decode_size!(<<0b11::2, 0::6, rest::binary>>) do
    {:int8, rest}
  end

  defp decode_size!(<<0b11::2, 1::6, rest::binary>>) do
    {:int16, rest}
  end

  defp decode_size!(<<0b11::2, 2::6, rest::binary>>) do
    {:int32, rest}
  end

  defp decode_size!(_bytes) do
    raise "Invalid size encoding"
  end

  defp decode_string!(bytes) do
    {size, rest} = decode_size!(bytes)
    # TODO: Does not read LZF compressed strings yet
    case rest do
      <<int::little-integer-size(8), rest::binary>> when size == :int8 ->
        {Integer.to_string(int), rest}

      <<int::little-integer-size(16), rest::binary>> when size == :int16 ->
        {Integer.to_string(int), rest}

      <<int::little-integer-size(32), rest::binary>> when size == :int32 ->
        {Integer.to_string(int), rest}

      <<string::binary-size(size), rest::binary>> when is_integer(size) ->
        {string, rest}

      _ ->
        raise "Invalid string encoding"
    end
  end

  defp parse_db_kv!(bytes, expiry, database_map) do
    {val_type, rest} = parse_val_type!(bytes)
    {key, rest} = decode_string!(rest)
    {value, rest} = parse_value!(val_type, rest)

    kv_store = database_map.kv_store
    kv_store = Map.put(kv_store, key, {value, expiry})
    database_map = Map.put(database_map, :kv_store, kv_store)
    {database_map, rest}
  end

  defp parse_val_type!(<<val_type::size(8), rest::binary>>) do
    case Map.fetch(@val_types, val_type) do
      {:ok, parsed_type} -> {parsed_type, rest}
      {:error, _} -> raise "Invalid value type"
    end
  end

  defp parse_value!(:string, bytes), do: decode_string!(bytes)

  # TODO: Implement other value types
end
