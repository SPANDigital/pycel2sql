"""SQLite schema introspection."""

from __future__ import annotations

import re
from typing import Any, Protocol, runtime_checkable

from pycel2sql._errors import IntrospectionError
from pycel2sql.schema import FieldSchema, Schema

_VALID_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@runtime_checkable
class SQLiteConnection(Protocol):
    """Minimal connection protocol for SQLite."""

    def execute(self, query: str, params: Any = ..., /) -> Any: ...


@runtime_checkable
class SQLiteCursor(Protocol):
    """Minimal cursor protocol for SQLite."""

    def fetchall(self) -> list[tuple[Any, ...]]: ...


def introspect_sqlite(
    conn: SQLiteConnection,
    *,
    table_names: list[str],
    json_columns: dict[str, list[str]] | None = None,
) -> dict[str, Schema]:
    """Introspect SQLite table schemas.

    Since SQLite has no dedicated JSON column type, JSON columns can be
    identified in two ways:

    1. Column type contains ``"json"`` (case-insensitive), e.g. ``JSON``,
       ``json``, ``jsontext``.
    2. Explicitly listed in the ``json_columns`` parameter.

    Args:
        conn: A SQLite connection.
        table_names: Tables to introspect.
        json_columns: Optional mapping of table name to list of column
            names that should be treated as JSON. Merged with type-based
            detection.

    Returns:
        Mapping of table name to :class:`~pycel2sql.schema.Schema`.

    Raises:
        IntrospectionError: If a requested table is not found or a table
            name is invalid.
    """
    if not table_names:
        return {}

    json_columns = json_columns or {}
    result: dict[str, Schema] = {}

    for name in table_names:
        if not _VALID_TABLE_NAME.match(name):
            raise IntrospectionError(
                f"invalid table name: {name!r}",
                internal_details=(
                    f"table name {name!r} does not match "
                    f"pattern {_VALID_TABLE_NAME.pattern}"
                ),
            )

        # PRAGMA does not support parameterized table names.
        cursor = conn.execute(f"PRAGMA table_info({name})")
        rows: list[tuple[Any, ...]] = cursor.fetchall()

        if not rows:
            raise IntrospectionError(
                f"table not found: {name!r}",
                internal_details=f"PRAGMA table_info({name!r}) returned no rows",
            )

        explicit_json = set(json_columns.get(name, []))
        fields: list[FieldSchema] = []
        for row in rows:
            # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
            col_name = str(row[1])
            col_type = str(row[2])
            is_json = "json" in col_type.lower() or col_name in explicit_json
            fields.append(FieldSchema(name=col_name, is_json=is_json))

        result[name] = Schema(fields)

    return result
