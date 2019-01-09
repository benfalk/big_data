defmodule BigData.MockApi do
  @moduledoc """
  This is a simple dummy API that we can use to generate fake records
  for the purpose of our examples and assert everything is working
  correct in our examples.
  """

  use GenServer
  import Logger, only: [debug: 1, warn: 1]
  alias BigData.MockApi.Data

  defstruct data: Data.new, warnings: []

  @mock_api_pid __MODULE__

  @min_item_id 1_000_000
  @max_item_id 1_999_999

  @min_container_id 100_000
  @max_container_id 300_000

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: @mock_api_pid)
  end

  @doc """
  Given a list of ids, this will return found ones by their id.  Internally
  these calls are checked and tracked and produce warnings if dupcliate or
  invalid ids are requested.
  """
  def get_items_by_id(ids) do
    GenServer.call(@mock_api_pid, {:items, :by_id, ids})
  end

  def get_containers_by_id(ids) do
    GenServer.call(@mock_api_pid, {:containers, :by_id, ids})
  end

  def get_containers(opts \\ []) do
    pagination =
      opts
      |> Keyword.put_new(:limit, 1000)
      |> Keyword.put_new(:offset, 0)

    GenServer.call(@mock_api_pid, {:get_containers, pagination})
  end

  def container_id_stream do
    Stream.resource(
      fn -> %{limit: 1000, offset: 0} end,
      fn %{limit: limit, offset: offset} = pagination ->
        case get_containers(limit: limit, offset: offset) do
          [] -> {:halt, :ok}
          containers when is_list(containers) ->
            ids = Enum.map(containers, & &1.id)
            {ids, %{ pagination | offset: offset + limit}}
        end
      end,
      fn _ -> :ok end
    )
  end

  @doc false
  def init(_) do
    debug("Starting MockApi Initialization")

    data = Data.new(
      item_id_range: @min_item_id..@max_item_id,
      container_id_range: @min_container_id..@max_container_id
    )

    debug("Finished MockApi Initialization")

    {:ok, %__MODULE__{data: data}}
  end

  @doc false
  def handle_call({what, :by_id, []}, _from, state) do
    {:reply, [], add_warning(state, "#{what}_by_id_called_with_empty_list")}
  end
  def handle_call({what, :by_id, ids}, _from, state) when is_list(ids) do
    real_ids = Enum.uniq(ids)
    items =
      case what do
        :items -> Data.items_by_ids(state.data, real_ids)
        :containers -> Data.containers_by_ids(state.data, real_ids)
      end
    real_id_len = length(real_ids)

    state =
      if real_id_len != length(items) do
        add_warning(state, :unfound_ids_requested)
      else
        state
      end

    state =
      if length(ids) != real_id_len do
        add_warning(state, :duplicate_ids_requested_in_single_batch)
      else
        state
      end

    {:reply, items, state}
  end

  def handle_call({:get_containers, pagination}, _from, state) do
    containers = Data.containers_by_page(state.data, pagination)
    {:reply, containers, state}
  end

  defp add_warning(state = %__MODULE__{warnings: warnings}, warning) do
    warn("[MockApi] #{warning}")
    %{ state | warnings: [warning|warnings] }
  end
end
