if True:
    import mysql.connector as mysql_connector

import re
from functools import cache
from typing import Any, List, Mapping, Optional, Sequence, Tuple, Type, TypeVar, cast

T = TypeVar("T", bound=Mapping[str, Any])


class Database:

    def __init__(
        self,
        *,
        host: str,
        user: str,
        password: str,
        account_service_name: str,
        competition_service_name: str,
        enable_caching: bool = False,
        commit_on_close: bool = False,
    ):
        self._host = host
        self._user = user
        self._password = password
        self.cashe_enabled = enable_caching
        self.commit_on_close = commit_on_close

        self._account_access = DatabaseAccess(self, account_service_name)
        self._competition_access = DatabaseAccess(self, competition_service_name)

        self.current_database_name: Optional[str] = None
        self._connection: Optional[mysql_connector.MySQLConnection] = None

    @property
    def account(self) -> "DatabaseAccess":
        return self._account_access.use()

    @property
    def competition(self) -> "DatabaseAccess":
        return self._competition_access.use()

    @property
    def connection(self):
        connection = self._connection

        if connection is None:
            raise ValueError("connection is not ready")

        return connection

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def __enter__(self):
        if self._connection is not None:
            raise ValueError("connection is already ready")

        self._connection = cast(
            mysql_connector.MySQLConnection,
            mysql_connector.connect(
                host=self._host,
                user=self._user,
                password=self._password,
                autocommit=False,
            )
        )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        connection = self._connection
        if connection is not None:
            if self.commit_on_close:
                if exc_type is not None:
                    connection.rollback()
                else:
                    connection.commit()

            connection.close()

        self._connection = None


class DatabaseAccess:

    def __init__(
        self,
        database: Database,
        database_name: str,
    ):
        self._database = database
        self._database_name = database_name

        if database.cashe_enabled:
            cached_query_all = cache(self.query_many)
            self.query_many = lambda statement, **kwargs: [  # type: ignore
                row.copy()
                for row in cached_query_all(statement, **kwargs)  # type: ignore
            ]

    @property
    def _mysql(self):
        return self._database.connection

    def use(self):
        if self._database.current_database_name != self._database_name:
            self._mysql.cursor().execute(f"USE `{self._database_name}`;")
            self._database.current_database_name = self._database_name

        return self

    def query_many(
        self,
        statement: str,
        *,
        params: Optional[Tuple[Any]] = None,
        type: Type[T] = dict,
    ) -> List[T]:
        cursor = self._mysql.cursor()
        cursor.execute(statement, params)

        column_names = [
            column[0]
            for column in cursor._description  # type: ignore
        ]

        return [
            cast(T, dict(zip(column_names, row)))  # type: ignore
            for row in cursor  # type: ignore
        ]

    def query_first(
        self,
        statement: str,
        *,
        type: Type[T] = dict,
    ) -> Optional[T]:
        return next(
            iter(self.query_many(
                statement,
                type=type,
            )),
            None,
        )

    def insert(
        self,
        statement: str,
        params: Optional[Tuple[Any]] = None,
    ) -> int:
        cursor = self._mysql.cursor()
        cursor.execute(statement, params)

        return cursor.lastrowid  # type: ignore

    def query_many_objects(
        self,
        type: Type[T],
        *,
        table_name: Optional[str] = None,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> List[T]:
        columns = to_column_names(type)
        table_name = table_name or to_table_name(type)

        statement = f"SELECT {columns} FROM `{table_name}`"

        if where:
            statement += f" WHERE {where}"

        if order_by:
            statement += f" ORDER BY {order_by}"

        return self.query_many(
            statement=statement,
            type=type,
        )

    def query_first_object(
        self,
        type: Type[T],
        *,
        table_name: Optional[str] = None,
        where: Optional[str] = None,
    ) -> Optional[T]:
        return next(
            iter(self.query_many_objects(
                type=type,
                table_name=table_name,
                where=where,
            )),
            None,
        )

    def insert_object(
        self,
        table: str,
        object: Mapping[str, Any],
    ):
        columns = "`, `".join(object.keys())
        values = ", ".join(["%s"] * len(object))

        statement = f"""
            INSERT INTO `{self._database_name}`.`{table}` (`{columns}`)
            VALUES ({values})
        """

        cursor = self._mysql.cursor()
        cursor.execute(statement, tuple(object.values()))

        last_row_id = cursor.lastrowid

        return last_row_id

    def insert_many_object(
        self,
        table: str,
        objects: Sequence[Mapping[str, Any]],
    ):
        if not objects:
            return None

        first_keys = objects[0].keys()
        if any(object.keys() != first_keys for object in objects):
            raise ValueError("all objects must have the same keys")

        columns = "`, `".join(first_keys)
        values = ", ".join(["%s"] * len(first_keys))

        statement = f"""
            INSERT INTO `{self._database_name}`.`{table}` (`{columns}`)
            VALUES ({values})
        """

        cursor = self._mysql.cursor()
        cursor.executemany(statement, tuple(tuple(object.values()) for object in objects))

        return cursor.lastrowid


def to_table_name(
    type: Type[T],
) -> str:
    name = "_".join(re.findall("[A-Z][^A-Z]*", type.__name__)).lower()

    if name.endswith("y"):
        return f"{name[:-1]}ies"
    elif name.endswith("s") or name.endswith("h") or name.endswith("x"):
        return f"{name}es"
    else:
        return f"{name}s"


def to_column_names(
    type: Type[T],
    *,
    table_name: Optional[str] = None,
) -> str:
    if table_name:
        columns = f"`, `{table_name}`.`".join(type.__annotations__)
        return f"`{table_name}`.`{columns}`"
    else:
        columns = f"`, `".join(type.__annotations__)
        return f"`{columns}`"
