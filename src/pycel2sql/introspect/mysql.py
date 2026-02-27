"""MySQL schema introspection."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pycel2sql._errors import IntrospectionError
from pycel2sql.schema import FieldSchema, Schema


@runtime_checkable
class MySQLCursor(Protocol):
    """Minimal cursor protocol for MySQL drivers."""

    def execute(self, query: str, params: Any = ..., /) -> Any: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...


@runtime_checkable
class MySQLConnection(Protocol):
    """Minimal connection protocol for MySQL drivers."""

    def cursor(self) -> MySQLCursor: ...


def introspect_mysql(
    conn: MySQLConnection,
    *,
    table_names: list[str],
    database: str | None = None,
) -> dict[str, Schema]:
    """Introspect MySQL table schemas.

    Args:
        conn: A MySQL connection.
        table_names: Tables to introspect.
        database: Database name. If ``None``, uses the current database
            (``DATABASE()``).

    Returns:
        Mapping of table name to :class:`~pycel2sql.schema.Schema`.

    Raises:
        IntrospectionError: If a requested table is not found.
    """
    if not table_names:
        return {}

    cur = conn.cursor()
    try:
        return _introspect(cur, table_names, database)
    finally:
        cur.close()


def _introspect(
    cur: MySQLCursor,
    table_names: list[str],
    database: str | None,
) -> dict[str, Schema]:
    placeholders = ", ".join(["%s"] * len(table_names))

    if database is not None:
        query = f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name IN ({placeholders})
            ORDER BY table_name, ordinal_position
        """
        cur.execute(query, [database, *table_names])
    else:
        query = f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name IN ({placeholders})
            ORDER BY table_name, ordinal_position
        """
        cur.execute(query, table_names)

    rows = cur.fetchall()

    columns_by_table: dict[str, list[FieldSchema]] = {}
    for table_name, column_name, data_type in rows:
        field = _map_column(str(column_name), str(data_type))
        columns_by_table.setdefault(str(table_name), []).append(field)

    result: dict[str, Schema] = {}
    for name in table_names:
        if name not in columns_by_table:
            raise IntrospectionError(
                f"table not found: {name!r}",
                internal_details=f"table {name!r} not found in MySQL database",
            )
        result[name] = Schema(columns_by_table[name])

    return result


def _map_column(column_name: str, data_type: str) -> FieldSchema:
    is_json = data_type.lower() == "json"
    return FieldSchema(name=column_name, is_json=is_json)
