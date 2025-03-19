defmodule Redis.MultiResponse do
  @moduledoc """
  A struct that represents a multi-response from Redis. Use to send multiple responses to a client,
  such as when sending an empty RDB file to a replica after a PSYNC.
  """

  defstruct responses: []
  @type t :: %__MODULE__{responses: [any()]}

  def new do
    %__MODULE__{}
  end

  def add_response(multi_response, response) do
    %{multi_response | responses: [response | multi_response.responses]}
  end

  def read_responses(multi_response) do
    Enum.reverse(multi_response.responses)
  end
end
