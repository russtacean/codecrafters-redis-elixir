defmodule Redis.Protocol.RDB do
  defstruct binary_value: nil

  def new(binary_value) do
    %__MODULE__{binary_value: binary_value}
  end
end
