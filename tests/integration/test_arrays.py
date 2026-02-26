"""Integration tests for array operations — PG and DuckDB only (native arrays)."""

from __future__ import annotations

import pytest

from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.schema import FieldSchema, Schema

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration

ARRAY_DBS = [
    pytest.param(("pg", PostgresDialect()), id="pg", marks=pytest.mark.postgres),
    pytest.param(("duckdb", DuckDBDialect()), id="duckdb", marks=pytest.mark.duckdb),
    pytest.param(("bq", BigQueryDialect()), id="bq", marks=pytest.mark.bigquery),
]

ARRAY_SCHEMAS = {"t": Schema([FieldSchema("tags", repeated=True)])}


@pytest.fixture(params=ARRAY_DBS)
def array_db(request):
    db_name, dialect = request.param
    conn = request.getfixturevalue(f"{db_name}_db")
    return conn, dialect, db_name


class TestArrays:
    def test_array_size(self, array_db):
        conn, dialect, name = array_db
        rows = execute_cel(
            conn, 't.tags.size() > 1', dialect, name,
            schemas=ARRAY_SCHEMAS, table_alias="t",
        )
        assert get_names(rows) == {"Alice", "Bob", "Charlie"}

    def test_array_membership(self, array_db):
        conn, dialect, name = array_db
        rows = execute_cel(
            conn, '"python" in t.tags', dialect, name,
            schemas=ARRAY_SCHEMAS, table_alias="t",
        )
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_array_index(self, array_db):
        conn, dialect, name = array_db
        rows = execute_cel(
            conn, 't.tags[0] == "python"', dialect, name,
            schemas=ARRAY_SCHEMAS, table_alias="t",
        )
        assert get_names(rows) == {"Alice", "Charlie"}
