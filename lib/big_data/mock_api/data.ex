defmodule BigData.MockApi.Data do
  @moduledoc """
  This a simple helper for working with the internal data for BigData.MockApi
  """

  @default_item_id_range 1..100
  @default_container_id_range 1..10

  defstruct \
    items: %{},
    containers: %{},
    item_id_range: @default_item_id_range,
    container_id_range: @default_container_id_range

  @doc """
  Creates a new data structure fully initialized with items and containers
  loaded up randomly in an un-uniform fashion with items.  Supports a set
  of options
    * `:item_id_range` : the id range pool with which to create items
    * `:container_id_range` : the container id range pool to create and fill
  """
  def new(opts \\ []) do
    item_ids = Keyword.get(opts, :item_id_range, @default_item_id_range)
    container_ids = Keyword.get(opts, :container_id_range, @default_container_id_range)

    %__MODULE__{item_id_range: item_ids, container_id_range: container_ids}
    |> init_items()
    |> init_containers()
    |> load_containers()
  end

  @doc """
  Retrieves all found items with a provided list of ids
  """
  def items_by_ids(%__MODULE__{items: items}, ids) when is_list(ids) do
    Map.take(items, ids) |> Map.values
  end

  def containers_by_ids(%__MODULE__{containers: containers}, ids) when is_list(ids) do
    Map.take(containers, ids) |> Map.values
  end

  @doc """
  """
  def containers_by_page(%__MODULE__{containers: containers, container_id_range: range}, pagination) do
    limit = Keyword.fetch!(pagination, :limit)
    offset = Keyword.fetch!(pagination, :offset)
    ids = Enum.slice(range, offset..(offset+limit-1))

    Map.take(containers, ids) |> Map.values
  end

  @doc false
  defp init_items(state = %__MODULE__{item_id_range: range}) do
    items =
      range
      |> Stream.map(& {&1, %{id: &1, name: "Item ##{&1}"}})
      |> Enum.into(%{})

    %{ state | items: items }
  end

  @doc false
  defp init_containers(state = %__MODULE__{container_id_range: range}) do
    containers =
      range
      |> Stream.map(& {&1, %{id: &1, item_ids: [], name: "Container ##{&1}"}})
      |> Enum.into(%{})

    %{ state | containers: containers }
  end

  @doc false
  defp load_containers(state = %__MODULE__{items: items, containers: containers, container_id_range: range}) do
    item_ids = items |> Map.keys

    loaded_containers =
      Enum.reduce(item_ids, %{}, fn id, acc ->
        container_id = Enum.random(range)
        %{item_ids: ids} = container = acc[container_id] || containers[container_id]
        updated_container = put_in(container.item_ids, [id|ids])
        put_in(acc[container_id], updated_container)
      end)

    %{ state | containers: Map.merge(containers, loaded_containers) }
  end
end
