"""MySQL dialect-specific tests."""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.schema import FieldSchema, Schema


@pytest.fixture
def d():
    return MySQLDialect()


class TestMySQLLiterals:
    def test_string_literal(self, d):
        assert convert('name == "alice"', dialect=d) == "name = 'alice'"

    def test_bytes_literal(self, d):
        result = convert('b"abc" == data', dialect=d)
        assert "X'" in result


class TestMySQLParams:
    def test_param_placeholder(self, d):
        result = convert_parameterized('name == "alice"', dialect=d)
        assert result.sql == "name = ?"
        assert result.parameters == ["alice"]

    def test_multiple_params(self, d):
        result = convert_parameterized("age > 10 && age < 30", dialect=d)
        # MySQL uses positional ? for all params
        assert result.sql.count("?") == 2


class TestMySQLStringConcat:
    def test_string_concat(self, d):
        result = convert('"hello" + " world"', dialect=d)
        assert "CONCAT(" in result
        assert "'hello'" in result
        assert "' world'" in result


class TestMySQLArrays:
    def test_array_literal(self, d):
        result = convert("[1, 2, 3]", dialect=d)
        assert "JSON_ARRAY(" in result

    def test_array_membership(self, d):
        result = convert("x in [1, 2, 3]", dialect=d)
        assert "JSON_CONTAINS(" in result

    def test_array_index_const(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr[0]", dialect=d, schemas=schemas)
        assert "JSON_EXTRACT(" in result
        assert "$[0]" in result

    def test_array_length(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.size()", dialect=d, schemas=schemas)
        assert "JSON_LENGTH(" in result


class TestMySQLStringFunctions:
    def test_contains(self, d):
        result = convert('name.contains("test")', dialect=d)
        assert "LOCATE(" in result
        assert "> 0" in result

    def test_starts_with(self, d):
        result = convert('name.startsWith("a")', dialect=d)
        assert "LIKE 'a%'" in result
        assert "ESCAPE '\\\\'" in result

    def test_split(self, d):
        # MySQL split is simplified
        result = convert('"a,b,c".split(",")', dialect=d)
        assert "JSON_ARRAY(" in result


class TestMySQLRegex:
    def test_regex_match(self, d):
        result = convert('name.matches("^[a-z]+$")', dialect=d)
        assert "REGEXP" in result

    def test_regex_case_insensitive(self, d):
        result = convert('name.matches("(?i)test")', dialect=d)
        assert "REGEXP" in result


class TestMySQLTypeCasting:
    def test_cast_to_numeric(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("t.data.num > 5", dialect=d, schemas=schemas)
        assert "+ 0" in result

    def test_type_name_int(self, d):
        result = convert('int(42)', dialect=d)
        assert "SIGNED" in result

    def test_type_name_string(self, d):
        result = convert('string(42)', dialect=d)
        assert "CHAR" in result

    def test_epoch_extract(self, d):
        result = convert("int(created_at)", dialect=d)
        assert "UNIX_TIMESTAMP(" in result

    def test_timestamp_cast(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z")', dialect=d)
        assert "CAST(" in result
        assert "DATETIME" in result


class TestMySQLTimestamps:
    def test_timestamp_arithmetic(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z") + duration("1h")', dialect=d)
        assert "INTERVAL" in result

    def test_extract_dow(self, d):
        result = convert("created_at.getDayOfWeek()", dialect=d)
        assert "DAYOFWEEK(" in result
        assert "+ 5" in result
        assert "% 7" in result


class TestMySQLJSON:
    def test_json_field_access(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("t.data.name", dialect=d, schemas=schemas)
        assert "->>" in result
        assert "$." in result

    def test_json_existence(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("has(t.data.name)", dialect=d, schemas=schemas)
        assert "JSON_CONTAINS_PATH(" in result


class TestMySQLComprehensions:
    def test_map(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.map(x, x + 1)", dialect=d, schemas=schemas)
        assert "JSON_ARRAYAGG(" in result
        assert "JSON_TABLE(" in result

    def test_filter(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.filter(x, x > 0)", dialect=d, schemas=schemas)
        assert "JSON_ARRAYAGG(" in result

    def test_exists(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.exists(x, x > 5)", dialect=d, schemas=schemas)
        assert "EXISTS" in result
        assert "JSON_TABLE(" in result


class TestMySQLStruct:
    def test_struct(self, d):
        result = convert('{"a": 1}', dialect=d)
        assert result == "ROW(1)"
