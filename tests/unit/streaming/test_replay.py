from collections.abc import Iterator

from smartlogix.streaming.replay import round_robin_records


def _source(values: list[int]) -> Iterator[dict[str, int]]:
    for value in values:
        yield {"value": value}


def test_round_robin_records_interleaves_sources() -> None:
    records = list(
        round_robin_records(
            {
                "a": _source([1, 3, 5]),
                "b": _source([2, 4]),
            }
        )
    )

    assert [record["value"] for record in records] == [1, 2, 3, 4, 5]
