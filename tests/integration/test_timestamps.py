"""Integration tests for timestamp operations — PG and DuckDB."""

from __future__ import annotations

import pytest

from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.postgres import PostgresDialect

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration

TIMESTAMP_DBS = [
    pytest.param(("pg", PostgresDialect()), id="pg", marks=pytest.mark.postgres),
    pytest.param(("duckdb", DuckDBDialect()), id="duckdb", marks=pytest.mark.duckdb),
    pytest.param(("bq", BigQueryDialect()), id="bq", marks=pytest.mark.bigquery),
]


@pytest.fixture(params=TIMESTAMP_DBS)
def ts_db(request):
    db_name, dialect = request.param
    conn = request.getfixturevalue(f"{db_name}_db")
    return conn, dialect, db_name


class TestTimestamps:
    def test_timestamp_comparison(self, ts_db):
        conn, dialect, name = ts_db
        rows = execute_cel(
            conn,
            'created_at > timestamp("2024-06-01T00:00:00Z")',
            dialect,
            name,
        )
        assert get_names(rows) == {"Diana", "Eve"}

    def test_get_full_year(self, ts_db):
        conn, dialect, name = ts_db
        rows = execute_cel(
            conn,
            'created_at.getFullYear() == 2024',
            dialect,
            name,
        )
        assert get_names(rows) == {"Alice", "Bob", "Diana", "Eve"}
