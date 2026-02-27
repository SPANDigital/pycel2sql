"""PostgreSQL schema introspection."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pycel2sql._errors import IntrospectionError
from pycel2sql.schema import FieldSchema, Schema


@runtime_checkable
class PgCursor(Protocol):
    """Minimal cursor protocol for PostgreSQL drivers."""

    def execute(self, query: str, params: Any = ..., /) -> Any: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...


@runtime_checkable
class PgConnection(Protocol):
    """Minimal connection protocol for PostgreSQL drivers."""

    def cursor(self) -> PgCursor: ...


def introspect_postgres(
    conn: PgConnection,
    *,
    table_names: list[str],
    schema_name: str = "public",
) -> dict[str, Schema]:
    """Introspect PostgreSQL table schemas.

    Args:
        conn: A PostgreSQL connection (e.g. ``psycopg.Connection``).
        table_names: Tables to introspect.
        schema_name: Schema name (default ``"public"``).

    Returns:
        Mapping of table name to :class:`~pycel2sql.schema.Schema`.

    Raises:
        IntrospectionError: If a requested table is not found.
    """
    if not table_names:
        return {}

    cur = conn.cursor()
    try:
        return _introspect(cur, table_names, schema_name)
    finally:
        cur.close()


def _introspect(
    cur: PgCursor,
    table_names: list[str],
    schema_name: str,
) -> dict[str, Schema]:
    placeholders = ", ".join(["%s"] * len(table_names))
    query = f"""
        SELECT table_name, column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name IN ({placeholders})
        ORDER BY table_name, ordinal_position
    """
    cur.execute(query, [schema_name, *table_names])
    rows = cur.fetchall()

    columns_by_table: dict[str, list[FieldSchema]] = {}
    for table_name, column_name, data_type, udt_name in rows:
        field = _map_column(str(column_name), str(data_type), str(udt_name))
        columns_by_table.setdefault(str(table_name), []).append(field)

    result: dict[str, Schema] = {}
    for name in table_names:
        if name not in columns_by_table:
            raise IntrospectionError(
                f"table not found: {name!r}",
                internal_details=f"table {name!r} not found in schema {schema_name!r}",
            )
        result[name] = Schema(columns_by_table[name])

    return result


def _map_column(column_name: str, data_type: str, udt_name: str) -> FieldSchema:
    is_json = False
    is_jsonb = False
    repeated = False

    if data_type.upper() == "ARRAY":
        repeated = True
    if udt_name == "jsonb":
        is_json = True
        is_jsonb = True
    elif udt_name == "json":
        is_json = True

    return FieldSchema(name=column_name, is_json=is_json, is_jsonb=is_jsonb, repeated=repeated)
