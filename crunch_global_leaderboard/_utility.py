from collections import defaultdict
from datetime import date
from typing import Callable, Dict, Generator, Generic, Iterable, List, Optional, TypedDict, TypeVar, overload

from pandas import date_range

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
RankableId = TypeVar("RankableId", bound=int)


def identity(x: T) -> T:
    return x


def group_by(
    items: Iterable[T],
    *,
    key: Callable[[T], K],
) -> Dict[K, List[T]]:
    grouped: Dict[K, List[T]] = defaultdict(list)
    for item in items:
        item_key = key(item)
        grouped[item_key].append(item)

    return dict(grouped)


@overload
def to_dict(
    items: Iterable[T],
    *,
    key: Callable[[T], K],
    value: None = None,
    merge: Optional[Callable[[T, T], T]] = None,
) -> Dict[K, T]: ...


@overload
def to_dict(
    items: Iterable[T],
    *,
    key: Callable[[T], K],
    value: Callable[[T], V],
    merge: Optional[Callable[[V, V], V]] = None,
) -> Dict[K, V]: ...


def to_dict(
    items: Iterable[T],
    *,
    key: Callable[[T], K],
    value: Optional[Callable[[T], V]] = None,
    merge: Optional[Callable[[V, V], V]] = None,
) -> Dict[K, V]:
    key_getter: Callable[[T], K] = key
    value_getter: Callable[[T], V] = value or identity  # type: ignore

    grouped: Dict[K, V] = {}
    for item in items:
        item_key = key_getter(item)
        item_value = value_getter(item)

        previous_value = grouped.get(item_key)
        if previous_value is not None:
            if merge is None:
                raise ValueError(f"merge is not defined, cannot decide between previous=`{previous_value}` and new=`{item_value}` value")

            item_value = merge(previous_value, item_value)

        grouped[item_key] = item_value

    return grouped


class RankableItem(TypedDict, Generic[T]):
    id: T
    points: int


class RankedItem(TypedDict, Generic[T]):
    id: T
    points: int
    rank: int


def rank_by_points(
    items: List[RankableItem[RankableId]],
) -> Dict[RankableId, RankedItem[RankableId]]:
    return {
        item["id"]: {
            "id": item["id"],
            "points": item["points"],
            "rank": rank,
        }
        for rank, item in enumerate(sorted(
            items,
            key=lambda row: (row["points"], -row["id"]),
            reverse=True,
        ), 1)
    }


def daily_date_range(
    start: date,
    end: date,
) -> Generator[date, None, None]:
    for today in date_range(start, end, freq="D"):
        yield today.date()
