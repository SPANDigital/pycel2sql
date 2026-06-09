"""Cross-dialect parametrized tests for expressions that should be identical."""

import pytest

from pycel2sql import convert
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.spark import SparkDialect
from pycel2sql.dialect.sqlite import SQLiteDialect
from pycel2sql.schema import FieldSchema, Schema

ALL_DIALECTS = [
    pytest.param(PostgresDialect(), id="postgres"),
    pytest.param(DuckDBDialect(), id="duckdb"),
    pytest.param(BigQueryDialect(), id="bigquery"),
    pytest.param(MySQLDialect(), id="mysql"),
    pytest.param(SparkDialect(), id="spark"),
    pytest.param(SQLiteDialect(), id="sqlite"),
]

_DIALECTS_BY_ID = {
    "postgres": PostgresDialect(),
    "duckdb": DuckDBDialect(),
    "bigquery": BigQueryDialect(),
    "mysql": MySQLDialect(),
    "spark": SparkDialect(),
    "sqlite": SQLiteDialect(),
}


def _expected_params(expected: dict[str, str]):
    """Build parametrize args mapping a dialect to its expected SQL string."""
    return [pytest.param(_DIALECTS_BY_ID[name], sql, id=name) for name, sql in expected.items()]


class TestNullComparisons:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_is_null(self, dialect):
        assert convert("null_var == null", dialect=dialect) == "null_var IS NULL"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_is_not_null(self, dialect):
        assert convert("null_var != null", dialect=dialect) == "null_var IS NOT NULL"


class TestBoolComparisons:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_is_true(self, dialect):
        assert convert("adult == true", dialect=dialect) == "adult IS TRUE"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_is_not_true(self, dialect):
        assert convert("adult != true", dialect=dialect) == "adult IS NOT TRUE"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_is_false(self, dialect):
        assert convert("adult == false", dialect=dialect) == "adult IS FALSE"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_is_not_false(self, dialect):
        assert convert("adult != false", dialect=dialect) == "adult IS NOT FALSE"


class TestLogicalOperators:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_not(self, dialect):
        assert convert("!active", dialect=dialect) == "NOT active"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_and(self, dialect):
        assert convert("age > 10 && age < 30", dialect=dialect) == "age > 10 AND age < 30"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_or(self, dialect):
        assert convert("age < 10 || age > 30", dialect=dialect) == "age < 10 OR age > 30"


class TestArithmetic:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_addition(self, dialect):
        assert convert("1 + 2 == 3", dialect=dialect) == "1 + 2 = 3"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_subtraction(self, dialect):
        assert convert("5 - 3 == 2", dialect=dialect) == "5 - 3 = 2"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_multiplication(self, dialect):
        assert convert("2 * 3 == 6", dialect=dialect) == "2 * 3 = 6"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_division(self, dialect):
        assert convert("6 / 2 == 3", dialect=dialect) == "6 / 2 = 3"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_modulo(self, dialect):
        assert convert("5 % 3 == 2", dialect=dialect) == "MOD(5, 3) = 2"


class TestTernary:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_ternary(self, dialect):
        result = convert("age > 18 ? true : false", dialect=dialect)
        assert result == "CASE WHEN age > 18 THEN TRUE ELSE FALSE END"


class TestComparisonOperators:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_less_than(self, dialect):
        assert convert("age < 20", dialect=dialect) == "age < 20"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_greater_equal(self, dialect):
        assert convert("age >= 20", dialect=dialect) == "age >= 20"

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_inequality(self, dialect):
        assert convert("age != 20", dialect=dialect) == "age != 20"


class TestNegation:
    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_negative_number(self, dialect):
        assert convert("-5 == x", dialect=dialect) == "-5 = x"


class TestJSONArrayMembership:
    """`x in <json array field>` routes to a dialect-specific membership predicate.

    Ported from cel2sql4j PR #20 (commit 1835215): the converter now passes the
    candidate element to the dialect, so each dialect owns the full predicate
    instead of the converter emitting a broken `elem = <dialect output>` inline.
    """

    _JSONB = {"t": Schema([FieldSchema(name="tags", type="jsonb", is_json=True, is_jsonb=True)])}
    _JSON = {"t": Schema([FieldSchema(name="tags", type="json", is_json=True, is_jsonb=False)])}
    _NESTED = {"t": Schema([FieldSchema(name="metadata", type="jsonb", is_json=True, is_jsonb=True)])}

    @pytest.mark.parametrize("dialect, expected", _expected_params({
        "postgres": "'x' = ANY(ARRAY(SELECT jsonb_array_elements_text(t.tags)))",
        "duckdb": "EXISTS (SELECT 1 FROM json_each(t.tags) WHERE value = 'x')",
        "bigquery": "'x' IN UNNEST(JSON_VALUE_ARRAY(t.tags))",
        "mysql": "JSON_OVERLAPS(JSON_ARRAY('x'), t.tags)",
        "spark": "array_contains(from_json(t.tags, 'ARRAY<STRING>'), 'x')",
        "sqlite": "EXISTS (SELECT 1 FROM json_each(t.tags) WHERE value = 'x')",
    }))
    def test_direct_jsonb_field(self, dialect, expected):
        assert convert('"x" in t.tags', dialect=dialect, schemas=self._JSONB) == expected

    def test_direct_json_field_uses_json_func(self):
        # A plain (non-JSONB) JSON field selects json_array_elements_text on PG.
        assert (
            convert('"x" in t.tags', dialect=PostgresDialect(), schemas=self._JSON)
            == "'x' = ANY(ARRAY(SELECT json_array_elements_text(t.tags)))"
        )

    @pytest.mark.parametrize("dialect, expected", _expected_params({
        "postgres": "'x' = ANY(ARRAY(SELECT jsonb_array_elements_text(t.metadata->>'tags')))",
        "duckdb": "EXISTS (SELECT 1 FROM json_each(t.metadata->>'tags') WHERE value = 'x')",
        "bigquery": "'x' IN UNNEST(JSON_VALUE_ARRAY(JSON_VALUE(t.metadata, '$.tags')))",
        "mysql": "JSON_OVERLAPS(JSON_ARRAY('x'), t.metadata->>'$.tags')",
        "spark": "array_contains(from_json(get_json_object(t.metadata, '$.tags'), 'ARRAY<STRING>'), 'x')",
        "sqlite": "EXISTS (SELECT 1 FROM json_each(json_extract(t.metadata, '$.tags')) WHERE value = 'x')",
    }))
    def test_nested_json_access(self, dialect, expected):
        assert convert('"x" in t.metadata.tags', dialect=dialect, schemas=self._NESTED) == expected

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_non_json_field_unaffected(self, dialect):
        # No schema → plain array membership, never the JSON-membership predicate.
        # (SQLite/DuckDB plain membership uses `IN (SELECT value FROM json_each(...))`,
        # which is distinct from the new `EXISTS (... WHERE value = ...)` JSON path.)
        result = convert("x in y", dialect=dialect)
        for marker in (
            "EXISTS (SELECT 1 FROM json_each",
            "JSON_VALUE_ARRAY",
            "JSON_OVERLAPS",
            "jsonb_array_elements",
            "from_json",
        ):
            assert marker not in result
