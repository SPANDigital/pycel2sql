"""Core conversion tests - ported from cel2sql_test.go TestConvert."""

import pytest

from pycel2sql import convert
from pycel2sql._errors import ConversionError


class TestBasicOperators:
    def test_equality(self):
        assert convert('name == "a"') == "name = 'a'"

    def test_inequality_int(self):
        assert convert("age != 20") == "age != 20"

    def test_is_null(self):
        assert convert("null_var == null") == "null_var IS NULL"

    def test_is_not_null(self):
        assert convert("null_var != null") == "null_var IS NOT NULL"

    def test_is_not_true(self):
        assert convert("adult != true") == "adult IS NOT TRUE"

    def test_is_true(self):
        assert convert("adult == true") == "adult IS TRUE"

    def test_is_false(self):
        assert convert("adult == false") == "adult IS FALSE"

    def test_is_not_false(self):
        assert convert("adult != false") == "adult IS NOT FALSE"

    def test_less_than(self):
        assert convert("age < 20") == "age < 20"

    def test_greater_equal(self):
        assert convert("height >= 1.6180339887") == "height >= 1.6180339887"

    def test_greater_than(self):
        assert convert("age > 10") == "age > 10"

    def test_less_equal(self):
        assert convert("age <= 30") == "age <= 30"


class TestLogicalOperators:
    def test_and(self):
        result = convert('name.startsWith("a") && name.endsWith("z")')
        assert result == "name LIKE 'a%' ESCAPE E'\\\\' AND name LIKE '%z' ESCAPE E'\\\\'"

    def test_or(self):
        result = convert('name.startsWith("a") || name.endsWith("z")')
        assert result == "name LIKE 'a%' ESCAPE E'\\\\' OR name LIKE '%z' ESCAPE E'\\\\'"

    def test_not(self):
        assert convert("!adult") == "NOT adult"

    def test_parenthesized(self):
        result = convert('age >= 10 && (name.startsWith("a") || name.endsWith("z"))')
        assert result == "age >= 10 AND (name LIKE 'a%' ESCAPE E'\\\\' OR name LIKE '%z' ESCAPE E'\\\\')"


class TestArithmetic:
    def test_add(self):
        assert convert("1 + 2 == 3") == "1 + 2 = 3"

    def test_subtract(self):
        assert convert("5 - 3 == 2") == "5 - 3 = 2"

    def test_multiply(self):
        assert convert("2 * 3 == 6") == "2 * 3 = 6"

    def test_divide(self):
        assert convert("6 / 2 == 3") == "6 / 2 = 3"

    def test_modulo(self):
        assert convert("5 % 3 == 2") == "MOD(5, 3) = 2"

    def test_negation(self):
        assert convert("-1") == "-1"

    def test_string_concat(self):
        assert convert('"a" + "b" == "ab"') == "'a' || 'b' = 'ab'"


class TestTernary:
    def test_ternary(self):
        result = convert('name == "a" ? "a" : "b"')
        assert result == "CASE WHEN name = 'a' THEN 'a' ELSE 'b' END"


class TestListLiterals:
    def test_list_index(self):
        assert convert("[1, 2, 3][0] == 1") == "ARRAY[1, 2, 3][1] = 1"

    def test_list_var_index(self):
        assert convert('string_list[0] == "a"') == "string_list[1] = 'a'"

    def test_array_index_negative(self):
        with pytest.raises(ConversionError):
            convert("string_list[-1]")

    def test_list_concat(self):
        result = convert("1 in [1] + [2, 3]")
        assert result == "1 = ANY(ARRAY[1] || ARRAY[2, 3])"


class TestMapLiterals:
    def test_map_var_index(self):
        result = convert('string_int_map["one"] == 1')
        assert result == "string_int_map.one = 1"


class TestFieldAccess:
    def test_field_select(self):
        assert convert('page.title == "test"') == "page.title = 'test'"

    def test_field_starts_with(self):
        result = convert('page.title.startsWith("test")')
        assert result == "page.title LIKE 'test%' ESCAPE E'\\\\'"


class TestContains:
    def test_contains(self):
        assert convert('name.contains("abc")') == "POSITION('abc' IN name) > 0"


class TestStartsWith:
    def test_starts_with(self):
        assert convert('name.startsWith("a")') == "name LIKE 'a%' ESCAPE E'\\\\'"


class TestEndsWith:
    def test_ends_with(self):
        assert convert('name.endsWith("z")') == "name LIKE '%z' ESCAPE E'\\\\'"


class TestMatches:
    def test_matches_method(self):
        assert convert('name.matches("a+")') == "name ~ 'a+'"

    def test_matches_function_style(self):
        assert convert('matches(name, "^[0-9]+$")') == "name ~ '^[0-9]+$'"

    def test_matches_word_boundary(self):
        result = convert(r'name.matches("\\btest\\b")')
        assert result == "name ~ '\\ytest\\y'"

    def test_matches_digit_class(self):
        result = convert(r'name.matches("\\d{3}-\\d{4}")')
        assert result == "name ~ '[[:digit:]]{3}-[[:digit:]]{4}'"

    def test_matches_word_class(self):
        result = convert(r'name.matches("\\w+@\\w+\\.\\w+")')
        assert result == "name ~ '[[:alnum:]_]+@[[:alnum:]_]+\\.[[:alnum:]_]+'"


class TestTypeCasting:
    def test_cast_bool(self):
        assert convert("bool(0) == false") == "CAST(0 AS BOOLEAN) IS FALSE"

    def test_cast_bytes(self):
        assert convert('bytes("test")') == "CAST('test' AS BYTEA)"

    def test_cast_int(self):
        assert convert("int(true) == 1") == "CAST(TRUE AS BIGINT) = 1"

    def test_cast_string(self):
        assert convert('string(true) == "true"') == "CAST(TRUE AS TEXT) = 'true'"

    def test_cast_string_from_timestamp(self):
        assert convert("string(created_at)") == "CAST(created_at AS TEXT)"

    def test_cast_int_epoch(self):
        assert convert("int(created_at)") == "EXTRACT(EPOCH FROM created_at)::bigint"


class TestSize:
    def test_size_string(self):
        assert convert('size("test")') == "LENGTH('test')"

    def test_size_bytes(self):
        assert convert('size(bytes("test"))') == "LENGTH(CAST('test' AS BYTEA))"


class TestNullByteSecurity:
    def test_null_byte_in_string(self):
        with pytest.raises(ConversionError, match="null bytes"):
            convert('name == "test\\x00value"')

    def test_null_byte_in_starts_with(self):
        with pytest.raises(ConversionError, match="null bytes"):
            convert('name.startsWith("\\x00test")')

    def test_null_byte_in_ends_with(self):
        with pytest.raises(ConversionError, match="null bytes"):
            convert('name.endsWith("test\\x00")')

    def test_null_byte_in_matches(self):
        with pytest.raises(ConversionError, match="null bytes"):
            convert('name.matches("\\x00")')

    def test_valid_string(self):
        assert convert('name == "valid"') == "name = 'valid'"


class TestInOperator:
    def test_in_list(self):
        result = convert("age in [18, 21, 25]")
        assert result == "age = ANY(ARRAY[18, 21, 25])"

    def test_in_string_list(self):
        result = convert('name in ["John", "Jane"]')
        assert result == "name = ANY(ARRAY['John', 'Jane'])"

    def test_in_variable(self):
        result = convert('"test" in string_list')
        assert result == "'test' = ANY(string_list)"


class TestHasFunction:
    def test_has_field(self):
        result = convert("has(page.title)")
        assert result == "page.title IS NOT NULL"
