"""Integration tests for schema introspection against real databases."""

from __future__ import annotations

import pytest

from pycel2sql import convert
from pycel2sql._errors import IntrospectionError
from pycel2sql.introspect import introspect
from pycel2sql.introspect.bigquery import introspect_bigquery
from pycel2sql.introspect.duckdb import introspect_duckdb
from pycel2sql.introspect.mysql import introspect_mysql
from pycel2sql.introspect.postgres import introspect_postgres
from pycel2sql.introspect.sqlite import introspect_sqlite
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


class TestPostgresIntrospect:
    def test_introspect_test_data(self, pg_db) -> None:
        schemas = introspect_postgres(pg_db, table_names=["test_data"])
        assert "test_data" in schemas
        s = schemas["test_data"]

        # Verify expected field types
        name_f = s.find_field("name")
        assert name_f is not None
        assert not name_f.is_json
        assert not name_f.repeated

        metadata_f = s.find_field("metadata")
        assert metadata_f is not None
        assert metadata_f.is_json
        assert metadata_f.is_jsonb

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.repeated
        assert not tags_f.is_json

    def test_table_not_found(self, pg_db) -> None:
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_postgres(pg_db, table_names=["nonexistent_table"])

    def test_dispatch(self, pg_db) -> None:
        schemas = introspect("postgresql", pg_db, table_names=["test_data"])
        assert "test_data" in schemas

    def test_roundtrip(self, pg_db) -> None:
        """Introspect schema, use it with convert(), execute SQL."""
        schemas = introspect_postgres(pg_db, table_names=["test_data"])
        sql = convert(
            'test_data.metadata.role == "admin"',
            dialect=PostgresDialect(),
            schemas=schemas,
        )
        assert "->>" in sql  # JSONB access operator

        cur = pg_db.cursor()
        cur.execute(f"SELECT name FROM test_data WHERE {sql}")
        names = {row[0] for row in cur.fetchall()}
        cur.close()
        assert names == {"Alice", "Charlie"}


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


class TestDuckDBIntrospect:
    def test_introspect_test_data(self, duckdb_db) -> None:
        schemas = introspect_duckdb(duckdb_db, table_names=["test_data"])
        assert "test_data" in schemas
        s = schemas["test_data"]

        metadata_f = s.find_field("metadata")
        assert metadata_f is not None
        assert metadata_f.is_json

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.repeated

    def test_table_not_found(self, duckdb_db) -> None:
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_duckdb(duckdb_db, table_names=["nonexistent_table"])

    def test_dispatch(self, duckdb_db) -> None:
        schemas = introspect("duckdb", duckdb_db, table_names=["test_data"])
        assert "test_data" in schemas

    def test_roundtrip(self, duckdb_db) -> None:
        schemas = introspect_duckdb(duckdb_db, table_names=["test_data"])
        sql = convert(
            'test_data.metadata.role == "admin"',
            dialect=DuckDBDialect(),
            schemas=schemas,
        )
        result = duckdb_db.execute(
            f"SELECT name FROM test_data WHERE {sql}"
        )
        names = {row[0] for row in result.fetchall()}
        assert names == {"Alice", "Charlie"}


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


class TestMySQLIntrospect:
    def test_introspect_test_data(self, mysql_db) -> None:
        schemas = introspect_mysql(mysql_db, table_names=["test_data"])
        assert "test_data" in schemas
        s = schemas["test_data"]

        metadata_f = s.find_field("metadata")
        assert metadata_f is not None
        assert metadata_f.is_json

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.is_json  # MySQL stores arrays as JSON
        assert not tags_f.repeated

    def test_table_not_found(self, mysql_db) -> None:
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_mysql(mysql_db, table_names=["nonexistent_table"])

    def test_dispatch(self, mysql_db) -> None:
        schemas = introspect("mysql", mysql_db, table_names=["test_data"])
        assert "test_data" in schemas

    def test_roundtrip(self, mysql_db) -> None:
        schemas = introspect_mysql(mysql_db, table_names=["test_data"])
        sql = convert(
            'test_data.metadata.role == "admin"',
            dialect=MySQLDialect(),
            schemas=schemas,
        )
        cur = mysql_db.cursor()
        cur.execute(f"SELECT name FROM test_data WHERE {sql}")
        names = {row[0] for row in cur.fetchall()}
        cur.close()
        assert names == {"Alice", "Charlie"}


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


class TestSQLiteIntrospect:
    def test_introspect_test_data(self, sqlite_db) -> None:
        # SQLite test_data uses TEXT for tags/metadata, so we need
        # json_columns to identify them.
        schemas = introspect_sqlite(
            sqlite_db,
            table_names=["test_data"],
            json_columns={"test_data": ["tags", "metadata"]},
        )
        assert "test_data" in schemas
        s = schemas["test_data"]

        metadata_f = s.find_field("metadata")
        assert metadata_f is not None
        assert metadata_f.is_json

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.is_json

    def test_table_not_found(self, sqlite_db) -> None:
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_sqlite(sqlite_db, table_names=["nonexistent_table"])

    def test_dispatch(self, sqlite_db) -> None:
        schemas = introspect(
            "sqlite",
            sqlite_db,
            table_names=["test_data"],
            json_columns={"test_data": ["tags", "metadata"]},
        )
        assert "test_data" in schemas

    def test_roundtrip(self, sqlite_db) -> None:
        schemas = introspect_sqlite(
            sqlite_db,
            table_names=["test_data"],
            json_columns={"test_data": ["tags", "metadata"]},
        )
        sql = convert(
            'test_data.metadata.role == "admin"',
            dialect=SQLiteDialect(),
            schemas=schemas,
        )
        cur = sqlite_db.execute(f"SELECT name FROM test_data WHERE {sql}")
        names = {row[0] for row in cur.fetchall()}
        assert names == {"Alice", "Charlie"}


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------


class TestBigQueryIntrospect:
    def test_introspect_test_data(self, bq_db) -> None:
        schemas = introspect_bigquery(
            bq_db,
            table_names=["test_data"],
            dataset="test_dataset",
        )
        assert "test_data" in schemas
        s = schemas["test_data"]

        metadata_f = s.find_field("metadata")
        assert metadata_f is not None
        assert metadata_f.is_json

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.repeated

    def test_table_not_found(self, bq_db) -> None:
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_bigquery(
                bq_db,
                table_names=["nonexistent_table"],
                dataset="test_dataset",
            )

    def test_dispatch(self, bq_db) -> None:
        schemas = introspect(
            "bigquery",
            bq_db,
            table_names=["test_data"],
            dataset="test_dataset",
        )
        assert "test_data" in schemas

    def test_roundtrip(self, bq_db) -> None:
        schemas = introspect_bigquery(
            bq_db,
            table_names=["test_data"],
            dataset="test_dataset",
        )
        sql = convert(
            'test_data.metadata.role == "admin"',
            dialect=BigQueryDialect(),
            schemas=schemas,
        )
        result = bq_db.query(
            f"SELECT name FROM test_dataset.test_data WHERE {sql}"
        ).result()
        names = {row["name"] for row in result}
        assert names == {"Alice", "Charlie"}
