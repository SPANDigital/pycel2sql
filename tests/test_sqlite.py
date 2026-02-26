"""SQLite dialect-specific tests."""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql._errors import UnsupportedDialectFeatureError
from pycel2sql.dialect.sqlite import SQLiteDialect
from pycel2sql.schema import FieldSchema, Schema


@pytest.fixture
def d():
    return SQLiteDialect()


class TestSQLiteLiterals:
    def test_string_literal(self, d):
        assert convert('name == "alice"', dialect=d) == "name = 'alice'"

    def test_bytes_literal(self, d):
        result = convert('b"abc" == data', dialect=d)
        assert "X'" in result


class TestSQLiteParams:
    def test_param_placeholder(self, d):
        result = convert_parameterized('name == "alice"', dialect=d)
        assert result.sql == "name = ?"
        assert result.parameters == ["alice"]

    def test_multiple_params(self, d):
        result = convert_parameterized("age > 10 && age < 30", dialect=d)
        assert result.sql.count("?") == 2


class TestSQLiteArrays:
    def test_array_literal(self, d):
        result = convert("[1, 2, 3]", dialect=d)
        assert "json_array(" in result

    def test_array_membership(self, d):
        result = convert("x in [1, 2, 3]", dialect=d)
        assert "IN (SELECT value FROM json_each(" in result

    def test_array_index_const(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr[0]", dialect=d, schemas=schemas)
        assert "json_extract(" in result
        assert "$[0]" in result

    def test_array_length(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.size()", dialect=d, schemas=schemas)
        assert "json_array_length(" in result


class TestSQLiteStringFunctions:
    def test_contains(self, d):
        result = convert('name.contains("test")', dialect=d)
        assert "INSTR(" in result
        assert "> 0" in result

    def test_starts_with(self, d):
        result = convert('name.startsWith("a")', dialect=d)
        assert "LIKE 'a%'" in result
        assert "ESCAPE '\\'" in result

    def test_string_concat(self, d):
        result = convert('"hello" + " world"', dialect=d)
        assert "||" in result


class TestSQLiteUnsupportedFeatures:
    def test_regex_not_supported(self, d):
        with pytest.raises(UnsupportedDialectFeatureError, match="regex"):
            convert('name.matches("test")', dialect=d)

    def test_split_not_supported(self, d):
        with pytest.raises(UnsupportedDialectFeatureError, match="split"):
            convert('"a,b,c".split(",")', dialect=d)

    def test_join_not_supported(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(UnsupportedDialectFeatureError, match="join"):
            convert("t.arr.join(',')", dialect=d, schemas=schemas)


class TestSQLiteTypeCasting:
    def test_cast_to_numeric(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("t.data.num > 5", dialect=d, schemas=schemas)
        assert "+ 0" in result

    def test_type_name_int(self, d):
        result = convert('int(42)', dialect=d)
        assert "INTEGER" in result

    def test_type_name_double(self, d):
        result = convert('double(42)', dialect=d)
        assert "REAL" in result

    def test_epoch_extract(self, d):
        result = convert("int(created_at)", dialect=d)
        assert "strftime('%s'" in result
        assert "AS INTEGER" in result

    def test_timestamp_cast(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z")', dialect=d)
        assert "datetime(" in result


class TestSQLiteTimestamps:
    def test_duration(self, d):
        result = convert('timestamp("2021-01-01T00:00:00Z") + duration("1h")', dialect=d)
        assert "datetime(" in result

    def test_extract_year(self, d):
        result = convert("created_at.getFullYear()", dialect=d)
        assert "strftime('%Y'" in result
        assert "AS INTEGER" in result

    def test_extract_month(self, d):
        result = convert("created_at.getMonth()", dialect=d)
        assert "strftime('%m'" in result

    def test_extract_dow(self, d):
        result = convert("created_at.getDayOfWeek()", dialect=d)
        assert "strftime('%w'" in result


class TestSQLiteJSON:
    def test_json_field_access(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("t.data.name", dialect=d, schemas=schemas)
        assert "json_extract(" in result

    def test_json_existence(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert("has(t.data.name)", dialect=d, schemas=schemas)
        assert "json_type(" in result
        assert "IS NOT NULL" in result


class TestSQLiteComprehensions:
    def test_map(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.map(x, x + 1)", dialect=d, schemas=schemas)
        assert "json_group_array(" in result
        assert "json_each(" in result

    def test_filter(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.filter(x, x > 0)", dialect=d, schemas=schemas)
        assert "json_group_array(" in result

    def test_exists(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.exists(x, x > 5)", dialect=d, schemas=schemas)
        assert "EXISTS" in result
        assert "json_each(" in result


class TestSQLiteStruct:
    def test_struct(self, d):
        result = convert('{"a": 1}', dialect=d)
        assert "json_object(" in result
