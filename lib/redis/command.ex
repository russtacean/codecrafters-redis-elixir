defmodule Redis.Command do
  defstruct command: "", args: []
  @type t :: %__MODULE__{command: String.t(), args: [String.t()]}
end
