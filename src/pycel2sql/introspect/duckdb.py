"""DuckDB schema introspection."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pycel2sql._errors import IntrospectionError
from pycel2sql.schema import FieldSchema, Schema


@runtime_checkable
class DuckDBConnection(Protocol):
    """Minimal connection protocol for DuckDB."""

    def execute(self, query: str, parameters: Any = ..., /) -> Any: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


def introspect_duckdb(
    conn: DuckDBConnection,
    *,
    table_names: list[str],
) -> dict[str, Schema]:
    """Introspect DuckDB table schemas.

    Args:
        conn: A DuckDB connection.
        table_names: Tables to introspect.

    Returns:
        Mapping of table name to :class:`~pycel2sql.schema.Schema`.

    Raises:
        IntrospectionError: If a requested table is not found.
    """
    if not table_names:
        return {}

    placeholders = ", ".join(["?"] * len(table_names))
    query = f"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_name IN ({placeholders})
        ORDER BY table_name, ordinal_position
    """
    result = conn.execute(query, table_names)
    rows: list[tuple[Any, ...]] = result.fetchall()

    columns_by_table: dict[str, list[FieldSchema]] = {}
    for table_name, column_name, data_type in rows:
        field = _map_column(str(column_name), str(data_type))
        columns_by_table.setdefault(str(table_name), []).append(field)

    schemas: dict[str, Schema] = {}
    for name in table_names:
        if name not in columns_by_table:
            raise IntrospectionError(
                f"table not found: {name!r}",
                internal_details=f"table {name!r} not found in DuckDB",
            )
        schemas[name] = Schema(columns_by_table[name])

    return schemas


def _map_column(column_name: str, data_type: str) -> FieldSchema:
    is_json = False
    repeated = False

    upper = data_type.upper()
    if "[]" in data_type:
        repeated = True
    elif upper == "JSON":
        is_json = True

    return FieldSchema(name=column_name, is_json=is_json, repeated=repeated)
