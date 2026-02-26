"""DuckDB dialect-specific tests."""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.schema import FieldSchema, Schema


@pytest.fixture
def d():
    return DuckDBDialect()


class TestDuckDBLiterals:
    def test_string_literal(self, d):
        assert convert('name == "alice"', dialect=d) == "name = 'alice'"

    def test_int_literal(self, d):
        assert convert("age == 25", dialect=d) == "age = 25"

    def test_bool_literal(self, d):
        assert convert("active == true", dialect=d) == "active IS TRUE"

    def test_bytes_literal(self, d):
        result = convert('b"abc" == data', dialect=d)
        assert "\\x" in result


class TestDuckDBParams:
    def test_param_placeholder(self, d):
        result = convert_parameterized('name == "alice"', dialect=d)
        assert result.sql == "name = $1"
        assert result.parameters == ["alice"]

    def test_multiple_params(self, d):
        result = convert_parameterized("age > 10 && age < 30", dialect=d)
        assert "$1" in result.sql
        assert "$2" in result.sql


class TestDuckDBArrays:
    def test_array_literal(self, d):
        assert convert("[1, 2, 3]", dialect=d) == "[1, 2, 3]"

    def test_array_membership(self, d):
        result = convert("x in [1, 2, 3]", dialect=d)
        assert result == "x = ANY([1, 2, 3])"

    def test_array_index_const(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr[0]", dialect=d, schemas=schemas)
        assert "[1]" in result

    def test_array_length(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.size()", dialect=d, schemas=schemas)
        assert "COALESCE(array_length(" in result


class TestDuckDBStringFunctions:
    def test_contains(self, d):
        result = convert('name.contains("test")', dialect=d)
        assert result == "CONTAINS(name, 'test')"

    def test_starts_with(self, d):
        result = convert('name.startsWith("a")', dialect=d)
        assert "LIKE 'a%'" in result
        assert "ESCAPE '\\'" in result
        assert "ESCAPE E" not in result  # No E prefix

    def test_split(self, d):
        result = convert('"a,b,c".split(",")', dialect=d)
        assert "STRING_SPLIT(" in result

    def test_join(self, d):
        result = convert('[1, 2, 3].join(",")', dialect=d)
        # DuckDB ARRAY_TO_STRING has only 2 args (no empty string third arg)
        assert "ARRAY_TO_STRING(" in result


class TestDuckDBRegex:
    def test_regex_match(self, d):
        result = convert('name.matches("^[a-z]+$")', dialect=d)
        assert "regexp_matches(name, '^[a-z]+$')" == result

    def test_regex_case_insensitive(self, d):
        result = convert('name.matches("(?i)test")', dialect=d)
        assert "regexp_matches(name, 'test', 'i')" == result


class TestDuckDBTypeCasting:
    def test_cast_to_numeric(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("t.data.num > 5", dialect=d, schemas=schemas)
        assert "::DOUBLE" in result

    def test_type_name_string(self, d):
        result = convert('string(42)', dialect=d)
        assert "VARCHAR" in result

    def test_type_name_double(self, d):
        result = convert('double(42)', dialect=d)
        assert "DOUBLE" in result

    def test_epoch_extract(self, d):
        result = convert("int(created_at)", dialect=d)
        assert "EXTRACT(EPOCH FROM" in result
        assert "::BIGINT" in result

    def test_timestamp_cast(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z")', dialect=d)
        assert "TIMESTAMPTZ" in result


class TestDuckDBComprehensions:
    def test_map(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.map(x, x + 1)", dialect=d, schemas=schemas)
        assert "ARRAY(SELECT" in result
        assert "UNNEST(" in result

    def test_filter(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.filter(x, x > 0)", dialect=d, schemas=schemas)
        assert "ARRAY(SELECT" in result

    def test_exists(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.exists(x, x > 5)", dialect=d, schemas=schemas)
        assert "EXISTS" in result


class TestDuckDBStruct:
    def test_struct(self, d):
        result = convert('{"a": 1}', dialect=d)
        assert result == "ROW(1)"


class TestDuckDBJSON:
    def test_json_field_access(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("t.data.name", dialect=d, schemas=schemas)
        assert "->>" in result

    def test_json_existence(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("has(t.data.name)", dialect=d, schemas=schemas)
        assert "json_exists(" in result
