"""Cross-dialect parametrized tests for expressions that should be identical."""

import pytest

from pycel2sql import convert
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect

ALL_DIALECTS = [
    pytest.param(PostgresDialect(), id="postgres"),
    pytest.param(DuckDBDialect(), id="duckdb"),
    pytest.param(BigQueryDialect(), id="bigquery"),
    pytest.param(MySQLDialect(), id="mysql"),
    pytest.param(SQLiteDialect(), id="sqlite"),
]


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
