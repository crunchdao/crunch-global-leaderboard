from typing import TypedDict

from crunch_global_leaderboard._database import to_column_names, to_table_name
from crunch_global_leaderboard._model import Crunch, LeaderboardDefinition, University, User


def test_to_table_name_converts_camel_case():
    assert to_table_name(User) == "users"
    assert to_table_name(University) == "universities"


def test_to_table_name_handles_word_ending_in_y():
    assert to_table_name(University) == "universities"


def test_to_table_name_handles_word_ending_in_s():
    class Address(TypedDict):
        id: int

    assert to_table_name(Address) == "addresses"


def test_to_table_name_handles_word_ending_in_h():
    assert to_table_name(Crunch) == "crunches"


def test_to_table_name_handles_word_ending_in_x():
    class Box(TypedDict):
        id: int

    assert to_table_name(Box) == "boxes"


def test_to_table_name_handles_multi_word_camel_case():
    assert to_table_name(LeaderboardDefinition) == "leaderboard_definitions"


def test_to_column_names_without_table_name():
    result = to_column_names(User)
    assert result == "`id`, `login`, `university`"


def test_to_column_names_with_table_name():
    result = to_column_names(User, table_name="users")
    assert result == "`users`.`id`, `users`.`login`, `users`.`university`"


def test_to_column_names_single_column():
    class SingleColumn(TypedDict):
        id: int

    result = to_column_names(SingleColumn)
    assert result == "`id`"


def test_to_column_names_single_column_with_table_name():
    class SingleColumn(TypedDict):
        id: int

    result = to_column_names(SingleColumn, table_name="items")
    assert result == "`items`.`id`"


def test_to_column_names_multiple_columns_with_table_name():
    result = to_column_names(University, table_name="universities")
    assert result == "`universities`.`id`, `universities`.`name`, `universities`.`url`, `universities`.`country_alpha3`"
