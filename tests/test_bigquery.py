"""BigQuery dialect-specific tests."""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.schema import FieldSchema, Schema


@pytest.fixture
def d():
    return BigQueryDialect()


class TestBigQueryLiterals:
    def test_string_literal(self, d):
        result = convert('name == "alice"', dialect=d)
        assert result == "name = 'alice'"

    def test_string_with_quote(self, d):
        result = convert("name == \"it's\"", dialect=d)
        # BigQuery escapes with backslash
        assert "\\'" in result

    def test_bytes_literal(self, d):
        result = convert('b"abc" == data', dialect=d)
        assert 'b"' in result


class TestBigQueryParams:
    def test_param_placeholder(self, d):
        result = convert_parameterized('name == "alice"', dialect=d)
        assert result.sql == "name = @p1"
        assert result.parameters == ["alice"]

    def test_multiple_params(self, d):
        result = convert_parameterized("age > 10 && age < 30", dialect=d)
        assert "@p1" in result.sql
        assert "@p2" in result.sql


class TestBigQueryArrays:
    def test_array_literal(self, d):
        assert convert("[1, 2, 3]", dialect=d) == "[1, 2, 3]"

    def test_array_membership(self, d):
        result = convert("x in [1, 2, 3]", dialect=d)
        assert "IN UNNEST(" in result

    def test_array_index_const(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr[0]", dialect=d, schemas=schemas)
        # BigQuery: 0-indexed with OFFSET
        assert "OFFSET(0)" in result

    def test_array_length(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.size()", dialect=d, schemas=schemas)
        assert "ARRAY_LENGTH(" in result
        # No COALESCE wrapper in BigQuery
        assert "COALESCE" not in result

    def test_empty_typed_array(self, d):
        result = convert('"a,b".split(",", 0)', dialect=d)
        assert "ARRAY<STRING>[]" in result


class TestBigQueryStringFunctions:
    def test_contains(self, d):
        result = convert('name.contains("test")', dialect=d)
        assert "STRPOS(name, 'test') > 0" == result

    def test_starts_with(self, d):
        result = convert('name.startsWith("a")', dialect=d)
        assert "LIKE 'a%'" in result
        # No ESCAPE clause in BigQuery
        assert "ESCAPE" not in result

    def test_split(self, d):
        result = convert('"a,b,c".split(",")', dialect=d)
        assert "SPLIT(" in result

    def test_split_with_limit(self, d):
        result = convert('"a,b,c".split(",", 2)', dialect=d)
        assert "UNNEST(SPLIT(" in result
        assert "OFFSET < 2" in result

    def test_join(self, d):
        result = convert('[1, 2, 3].join(",")', dialect=d)
        assert "ARRAY_TO_STRING(" in result


class TestBigQueryRegex:
    def test_regex_match(self, d):
        result = convert('name.matches("^[a-z]+$")', dialect=d)
        assert "REGEXP_CONTAINS(" in result

    def test_regex_case_insensitive(self, d):
        result = convert('name.matches("(?i)test")', dialect=d)
        assert "REGEXP_CONTAINS(" in result
        assert "(?i)" in result


class TestBigQueryTypeCasting:
    def test_type_name_string(self, d):
        result = convert('string(42)', dialect=d)
        assert "STRING" in result

    def test_type_name_int(self, d):
        result = convert('int(42)', dialect=d)
        assert "INT64" in result

    def test_epoch_extract(self, d):
        result = convert("int(created_at)", dialect=d)
        assert "UNIX_SECONDS(" in result

    def test_timestamp_cast(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z")', dialect=d)
        assert "CAST(" in result
        assert "AS TIMESTAMP)" in result


class TestBigQueryTimestampArithmetic:
    def test_timestamp_add(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z") + duration("1h")', dialect=d)
        assert "TIMESTAMP_ADD(" in result

    def test_timestamp_sub(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z") - duration("1h")', dialect=d)
        assert "TIMESTAMP_SUB(" in result


class TestBigQueryStruct:
    def test_struct(self, d):
        result = convert('{"a": 1}', dialect=d)
        assert result == "STRUCT(1)"


class TestBigQueryComprehensions:
    def test_map(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.map(x, x + 1)", dialect=d, schemas=schemas)
        assert "ARRAY(SELECT" in result

    def test_exists(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.exists(x, x > 5)", dialect=d, schemas=schemas)
        assert "EXISTS" in result
