"""Parameterized query tests - ported from parameterized_test.go."""

import pytest

from pycel2sql import convert_parameterized
from pycel2sql._errors import ConversionError
from pycel2sql.schema import FieldSchema, Schema


class TestParameterizedBasic:
    def test_string_equality(self):
        result = convert_parameterized('name == "John"')
        assert result.sql == "name = $1"
        assert result.parameters == ["John"]

    def test_multiple_string_params(self):
        result = convert_parameterized('name == "John" && name != "Jane"')
        assert result.sql == "name = $1 AND name != $2"
        assert result.parameters == ["John", "Jane"]

    def test_string_with_escaped_quotes(self):
        result = convert_parameterized("name == \"O'Brien\"")
        assert result.sql == "name = $1"
        assert result.parameters == ["O'Brien"]

    def test_integer_equality(self):
        result = convert_parameterized("age == 18")
        assert result.sql == "age = $1"
        assert result.parameters == [18]

    def test_integer_comparison(self):
        result = convert_parameterized("age > 21 && age < 65")
        assert result.sql == "age > $1 AND age < $2"
        assert result.parameters == [21, 65]

    def test_double_equality(self):
        result = convert_parameterized("salary == 50000.50")
        assert result.sql == "salary = $1"
        assert result.parameters == [50000.50]

    def test_double_comparison(self):
        result = convert_parameterized("salary >= 30000.0 && salary <= 100000.0")
        assert result.sql == "salary >= $1 AND salary <= $2"
        assert result.parameters == [30000.0, 100000.0]


class TestParameterizedBooleans:
    def test_bool_true_never_parameterized(self):
        result = convert_parameterized("active == true")
        assert result.sql == "active IS TRUE"
        assert result.parameters == []

    def test_bool_false_never_parameterized(self):
        result = convert_parameterized("active == false")
        assert result.sql == "active IS FALSE"
        assert result.parameters == []

    def test_mixed_bool_and_params(self):
        result = convert_parameterized("active == true && age == 18")
        assert result.sql == "active IS TRUE AND age = $1"
        assert result.parameters == [18]

    def test_null_never_parameterized(self):
        result = convert_parameterized("null_var == null")
        assert result.sql == "null_var IS NULL"
        assert result.parameters == []

    def test_only_booleans(self):
        result = convert_parameterized("active == true && active != false")
        assert result.sql == "active IS TRUE AND active IS NOT FALSE"
        assert result.parameters == []


class TestParameterizedBytes:
    def test_bytes_equality(self):
        result = convert_parameterized('data == b"hello"')
        assert result.sql == "data = $1"
        assert result.parameters == [b"hello"]


class TestParameterizedComplex:
    def test_complex_and(self):
        result = convert_parameterized('name == "John" && age >= 18 && salary > 50000.0')
        assert result.sql == "name = $1 AND age >= $2 AND salary > $3"
        assert result.parameters == ["John", 18, 50000.0]

    def test_complex_or(self):
        result = convert_parameterized('name == "John" || name == "Jane" || age == 25')
        assert result.sql == "name = $1 OR name = $2 OR age = $3"
        assert result.parameters == ["John", "Jane", 25]

    def test_nested_parens(self):
        result = convert_parameterized('(name == "John" && age == 18) || (name == "Jane" && age == 21)')
        assert result.sql == "(name = $1 AND age = $2) OR (name = $3 AND age = $4)"
        assert result.parameters == ["John", 18, "Jane", 21]

    def test_param_ordering(self):
        result = convert_parameterized('name == "First" && age == 1 && salary == 100.0 && name != "Second"')
        assert result.sql == "name = $1 AND age = $2 AND salary = $3 AND name != $4"
        assert result.parameters == ["First", 1, 100.0, "Second"]


class TestParameterizedStringFunctions:
    def test_starts_with_not_parameterized(self):
        result = convert_parameterized('name.startsWith("Jo")')
        assert result.sql == "name LIKE 'Jo%' ESCAPE E'\\\\'"
        assert result.parameters == []

    def test_ends_with_not_parameterized(self):
        result = convert_parameterized('name.endsWith("hn")')
        assert result.sql == "name LIKE '%hn' ESCAPE E'\\\\'"
        assert result.parameters == []

    def test_contains_parameterized(self):
        result = convert_parameterized('name.contains("oh")')
        assert result.sql == "POSITION($1 IN name) > 0"
        assert result.parameters == ["oh"]


class TestParameterizedInOperator:
    def test_in_array_literal(self):
        result = convert_parameterized("age in [18, 21, 25]")
        assert result.sql == "age = ANY(ARRAY[$1, $2, $3])"
        assert result.parameters == [18, 21, 25]

    def test_string_in_array(self):
        result = convert_parameterized('name in ["John", "Jane", "Bob"]')
        assert result.sql == "name = ANY(ARRAY[$1, $2, $3])"
        assert result.parameters == ["John", "Jane", "Bob"]


class TestParameterizedTernary:
    def test_ternary(self):
        result = convert_parameterized('age > 18 ? "adult" : "minor"')
        assert result.sql == "CASE WHEN age > $1 THEN $2 ELSE $3 END"
        assert result.parameters == [18, "adult", "minor"]


class TestParameterizedTypeCast:
    def test_cast_with_param(self):
        result = convert_parameterized('string(age) == "18"')
        assert result.sql == "CAST(age AS TEXT) = $1"
        assert result.parameters == ["18"]


class TestParameterizedNoParams:
    def test_field_comparison(self):
        result = convert_parameterized("x == y")
        assert result.sql == "x = y"
        assert result.parameters == []

    def test_field_arithmetic(self):
        result = convert_parameterized("x + y > x * y")
        assert result.sql == "x + y > x * y"
        assert result.parameters == []

    def test_boolean_constants(self):
        result = convert_parameterized("x > y && true")
        assert result.sql == "x > y AND TRUE"
        assert result.parameters == []


class TestParameterizedComprehensions:
    def test_all_with_param(self):
        result = convert_parameterized("scores.all(x, x > 50)")
        assert result.sql == "NOT EXISTS (SELECT 1 FROM UNNEST(scores) AS x WHERE NOT (x > $1))"
        assert result.parameters == [50]

    def test_exists_with_param(self):
        result = convert_parameterized("scores.exists(x, x == 100)")
        assert result.sql == "EXISTS (SELECT 1 FROM UNNEST(scores) AS x WHERE x = $1)"
        assert result.parameters == [100]

    def test_exists_one_with_param(self):
        result = convert_parameterized("scores.exists_one(x, x == 42)")
        assert result.sql == "(SELECT COUNT(*) FROM UNNEST(scores) AS x WHERE x = $1) = 1"
        assert result.parameters == [42]


class TestParameterizedRegex:
    def test_regex_not_parameterized(self):
        result = convert_parameterized(r'email.matches("[a-z]+@[a-z]+\.[a-z]+")')
        assert "~" in result.sql
        assert result.parameters == []


class TestParameterizedJSON:
    def test_json_field_comparison(self):
        schemas = {
            "usr": Schema([
                FieldSchema(name="metadata", type="jsonb", is_json=True, is_jsonb=True),
            ]),
        }
        result = convert_parameterized(
            'usr.metadata.username == "john_doe"',
            schemas=schemas,
        )
        assert result.sql == "usr.metadata->>'username' = $1"
        assert result.parameters == ["john_doe"]

    def test_nested_json_comparison(self):
        schemas = {
            "usr": Schema([
                FieldSchema(name="metadata", type="jsonb", is_json=True, is_jsonb=True),
            ]),
        }
        result = convert_parameterized(
            'usr.metadata.settings.theme == "dark"',
            schemas=schemas,
        )
        assert result.sql == "usr.metadata->'settings'->>'theme' = $1"
        assert result.parameters == ["dark"]

    def test_json_and_regular_field(self):
        schemas = {
            "usr": Schema([
                FieldSchema(name="metadata", type="jsonb", is_json=True, is_jsonb=True),
            ]),
        }
        result = convert_parameterized(
            'usr.name == "John" && usr.metadata.age == "25"',
            schemas=schemas,
        )
        assert result.sql == "usr.name = $1 AND usr.metadata->>'age' = $2"
        assert result.parameters == ["John", "25"]


class TestParameterizedErrors:
    def test_null_byte_error(self):
        with pytest.raises(ConversionError):
            convert_parameterized('name == "test\\x00"')
