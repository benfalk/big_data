defmodule BigData do
  @moduledoc """
  Just a series of examples
  """
  alias BigData.MockApi, as: API
  alias Flow.Window
  require Logger

  def playground do
    every_100 = Window.global |> Window.trigger_every(100)

    API.container_id_stream
    |> Stream.chunk_every(10, 10, [])
    |> Flow.from_enumerable(min_demand: 1, max_demand: 20, stages: 4)
    |> Flow.flat_map(fn container_ids ->
      container_ids
      |> API.get_containers_by_id
      |> Enum.map(& &1.item_ids)
      |> List.flatten
    end)
    |> Flow.partition(window: every_100)
    |> Flow.reduce(fn -> [] end, &[&1 | &2])
    |> Flow.on_trigger(& {[API.get_items_by_id(&1)], []})
  end
end
