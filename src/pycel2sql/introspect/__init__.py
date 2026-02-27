"""Schema introspection for CEL-to-SQL conversion.

Auto-discover table schemas from live database connections.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pycel2sql.schema import Schema

__all__ = [
    "introspect",
    "introspect_bigquery",
    "introspect_duckdb",
    "introspect_mysql",
    "introspect_postgres",
    "introspect_sqlite",
]


def introspect(
    dialect_name: str,
    conn: Any,
    *,
    table_names: list[str],
    **kwargs: Any,
) -> dict[str, Schema]:
    """Introspect table schemas from a live database connection.

    Dispatches to the dialect-specific introspection function based on
    ``dialect_name``.

    Args:
        dialect_name: Dialect name (``"postgresql"``, ``"duckdb"``,
            ``"bigquery"``, ``"mysql"``, ``"sqlite"``).
        conn: A database connection (or BigQuery ``Client``).
        table_names: Tables to introspect.
        **kwargs: Forwarded to the dialect-specific function (e.g.
            ``schema_name`` for PostgreSQL, ``json_columns`` for SQLite).

    Returns:
        Mapping of table name to :class:`~pycel2sql.schema.Schema`.

    Raises:
        IntrospectionError: If introspection fails.
        ValueError: If the dialect name is unknown.
    """
    if dialect_name == "postgresql":
        from pycel2sql.introspect.postgres import introspect_postgres

        return introspect_postgres(conn, table_names=table_names, **kwargs)
    if dialect_name == "duckdb":
        from pycel2sql.introspect.duckdb import introspect_duckdb

        return introspect_duckdb(conn, table_names=table_names, **kwargs)
    if dialect_name == "bigquery":
        from pycel2sql.introspect.bigquery import introspect_bigquery

        return introspect_bigquery(conn, table_names=table_names, **kwargs)
    if dialect_name == "mysql":
        from pycel2sql.introspect.mysql import introspect_mysql

        return introspect_mysql(conn, table_names=table_names, **kwargs)
    if dialect_name == "sqlite":
        from pycel2sql.introspect.sqlite import introspect_sqlite

        return introspect_sqlite(conn, table_names=table_names, **kwargs)

    raise ValueError(
        f"unknown dialect: {dialect_name!r}. "
        f"Available: bigquery, duckdb, mysql, postgresql, sqlite"
    )


def __getattr__(name: str) -> Any:
    """Lazy re-exports of per-dialect introspection functions."""
    if name == "introspect_postgres":
        from pycel2sql.introspect.postgres import introspect_postgres

        return introspect_postgres
    if name == "introspect_duckdb":
        from pycel2sql.introspect.duckdb import introspect_duckdb

        return introspect_duckdb
    if name == "introspect_bigquery":
        from pycel2sql.introspect.bigquery import introspect_bigquery

        return introspect_bigquery
    if name == "introspect_mysql":
        from pycel2sql.introspect.mysql import introspect_mysql

        return introspect_mysql
    if name == "introspect_sqlite":
        from pycel2sql.introspect.sqlite import introspect_sqlite

        return introspect_sqlite
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
