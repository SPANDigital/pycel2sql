"""Integration tests for regex matches() — PG, DuckDB, MySQL only (not SQLite)."""

from __future__ import annotations

import pytest

from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration

# SQLite does not support regex
REGEX_DBS = [
    pytest.param(("pg", PostgresDialect()), id="pg", marks=pytest.mark.postgres),
    pytest.param(("duckdb", DuckDBDialect()), id="duckdb", marks=pytest.mark.duckdb),
    pytest.param(("mysql", MySQLDialect()), id="mysql", marks=pytest.mark.mysql),
    pytest.param(("bq", BigQueryDialect()), id="bq", marks=pytest.mark.bigquery),
]


@pytest.fixture(params=REGEX_DBS)
def regex_db(request):
    db_name, dialect = request.param
    conn = request.getfixturevalue(f"{db_name}_db")
    return conn, dialect, db_name


class TestRegex:
    def test_matches_start(self, regex_db):
        conn, dialect, name = regex_db
        rows = execute_cel(conn, 'name.matches("^[A-D]")', dialect, name)
        assert get_names(rows) == {"Alice", "Bob", "Charlie", "Diana"}

    def test_matches_end(self, regex_db):
        conn, dialect, name = regex_db
        rows = execute_cel(conn, 'name.matches("e$")', dialect, name)
        assert get_names(rows) == {"Alice", "Charlie", "Eve"}
