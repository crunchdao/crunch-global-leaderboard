from datetime import date
from typing import List, TypedDict

import pytest

from crunch_global_leaderboard._utility import RankableItem, daily_date_range, group_by, identity, rank_by_points, to_dict


class Human(TypedDict):
    name: str
    age: int


def test_identity_returns_same_value():
    assert identity(5) == 5
    assert identity("hello") == "hello"
    assert identity([1, 2, 3]) == [1, 2, 3]


def test_group_by_groups_items_by_key():
    items: List[Human] = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 30},
    ]

    result = group_by(items, key=lambda x: x["age"])

    assert result == {
        30: [
            {"name": "Alice", "age": 30},
            {"name": "Charlie", "age": 30}
        ],
        25: [
            {"name": "Bob", "age": 25}
        ]
    }


def test_group_by_returns_empty_dict_for_empty_list():
    items: List[Human] = []

    result = group_by(items, key=lambda x: x)

    assert result == {}


def test_to_dict_creates_dict_from_items():
    items: List[Human] = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]

    result = to_dict(items, key=lambda x: x["age"])

    assert result == {
        30: {"name": "Alice", "age": 30},
        25: {"name": "Bob", "age": 25},
    }


def test_to_dict_with_value_function():
    items: List[Human] = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]

    result = to_dict(items, key=lambda x: x["age"], value=lambda x: x["name"])

    assert result == {30: "Alice", 25: "Bob"}


def test_to_dict_raises_error_on_duplicate_without_merge():
    items: List[Human] = [
        {"name": "Alice", "age": 30},
        {"name": "Alice", "age": 30},
    ]

    with pytest.raises(ValueError, match="merge is not defined"):
        to_dict(items, key=lambda x: x["age"])


def test_to_dict_merges_duplicates_with_merge_function():
    items: List[Human] = [
        {"name": "Alice", "age": 30},
        {"name": "Alice", "age": 31},
        {"name": "Bob", "age": 25},
    ]

    result = to_dict(
        items,
        key=lambda x: x["name"],
        value=lambda x: x["age"],
        merge=lambda a, b: a,  # take first
    )

    assert result == {
        "Alice": 30,
        "Bob": 25
    }


def test_rank_by_points_orders_by_points():
    items: list[RankableItem[int]] = [
        {"id": 1, "points": 100},
        {"id": 2, "points": 200},
        {"id": 3, "points": 150},
    ]

    result = rank_by_points(items)

    assert result[2]["rank"] == 1
    assert result[3]["rank"] == 2
    assert result[1]["rank"] == 3


def test_rank_by_points_breaks_ties_by_id():
    items: list[RankableItem[int]] = [
        {"id": 1, "points": 100},
        {"id": 2, "points": 100},
        {"id": 3, "points": 100},
    ]

    result = rank_by_points(items)

    assert result[1]["rank"] == 1
    assert result[2]["rank"] == 2
    assert result[3]["rank"] == 3


def test_rank_by_points_empty_list():
    items: list[RankableItem[int]] = []

    result = rank_by_points(items)

    assert result == {}


def test_daily_date_range_generates_dates():
    start = date(2024, 1, 1)
    end = date(2024, 1, 5)

    result = list(daily_date_range(start, end))

    assert len(result) == 5
    assert result[0] == date(2024, 1, 1)
    assert result[4] == date(2024, 1, 5)


def test_daily_date_range_single_day():
    start = date(2024, 1, 1)
    end = date(2024, 1, 1)

    result = list(daily_date_range(start, end))

    assert len(result) == 1
    assert result[0] == date(2024, 1, 1)
