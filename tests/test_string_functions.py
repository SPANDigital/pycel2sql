"""String function tests - ported from string_functions_test.go."""

import pytest

from pycel2sql import convert
from pycel2sql._errors import UnsupportedOperationError


class TestLowerAscii:
    def test_method_call(self):
        result = convert("person.name.lowerAscii() == 'john'")
        assert result == "LOWER(person.name) = 'john'"

    def test_with_comparison(self):
        result = convert("person.email.lowerAscii() == person.username.lowerAscii()")
        assert result == "LOWER(person.email) = LOWER(person.username)"


class TestUpperAscii:
    def test_method_call(self):
        result = convert("person.name.upperAscii() == 'JOHN'")
        assert result == "UPPER(person.name) = 'JOHN'"

    def test_with_starts_with(self):
        result = convert("person.name.upperAscii().startsWith('J')")
        assert result == "UPPER(person.name) LIKE 'J%' ESCAPE E'\\\\'"


class TestTrim:
    def test_method_call(self):
        result = convert("person.name.trim() == 'John'")
        assert result == "TRIM(person.name) = 'John'"

    def test_in_comparison(self):
        result = convert("person.name.trim().size() > 0")
        assert result == "LENGTH(TRIM(person.name)) > 0"


class TestCharAt:
    def test_constant_index(self):
        result = convert("person.name.charAt(0) == 'J'")
        assert result == "SUBSTRING(person.name, 1, 1) = 'J'"

    def test_dynamic_index(self):
        result = convert("person.name.charAt(person.position) == 'x'")
        assert result == "SUBSTRING(person.name, person.position + 1, 1) = 'x'"


class TestIndexOf:
    def test_simple(self):
        result = convert("person.email.indexOf('@') > 0")
        assert result == "CASE WHEN POSITION('@' IN person.email) > 0 THEN POSITION('@' IN person.email) - 1 ELSE -1 END > 0"

    def test_with_offset(self):
        result = convert("person.text.indexOf('test', 5) >= 0")
        assert result == "CASE WHEN POSITION('test' IN SUBSTRING(person.text, 6)) > 0 THEN POSITION('test' IN SUBSTRING(person.text, 6)) + 5 - 1 ELSE -1 END >= 0"


class TestLastIndexOf:
    def test_simple(self):
        result = convert("person.path.lastIndexOf('/') > 0")
        assert result == "CASE WHEN POSITION(REVERSE('/') IN REVERSE(person.path)) > 0 THEN LENGTH(person.path) - POSITION(REVERSE('/') IN REVERSE(person.path)) - LENGTH('/') + 1 ELSE -1 END > 0"


class TestSubstring:
    def test_start_only_constant(self):
        result = convert("person.name.substring(5) == 'test'")
        assert result == "SUBSTRING(person.name, 6) = 'test'"

    def test_start_and_end_constant(self):
        result = convert("person.name.substring(0, 4) == 'John'")
        assert result == "SUBSTRING(person.name, 1, 4) = 'John'"

    def test_dynamic_start(self):
        result = convert("person.name.substring(person.startpos, person.endpos) == 'test'")
        assert result == "SUBSTRING(person.name, person.startpos + 1, person.endpos - (person.startpos)) = 'test'"


class TestReplace:
    def test_without_limit(self):
        result = convert("person.text.replace('old', 'new') == 'test'")
        assert result == "REPLACE(person.text, 'old', 'new') = 'test'"

    def test_with_limit_minus_one(self):
        result = convert("person.text.replace('a', 'b', -1) == 'test'")
        assert result == "REPLACE(person.text, 'a', 'b') = 'test'"

    def test_with_limit_error(self):
        with pytest.raises(UnsupportedOperationError, match="replace.*limit"):
            convert("person.text.replace('a', 'b', 1) == 'test'")


class TestReverse:
    def test_simple(self):
        result = convert("person.name.reverse() == 'nhoJ'")
        assert result == "REVERSE(person.name) = 'nhoJ'"


class TestSplit:
    def test_basic(self):
        result = convert("'a,b,c'.split(',') == ['a', 'b', 'c']")
        assert result == "STRING_TO_ARRAY('a,b,c', ',') = ARRAY['a', 'b', 'c']"

    def test_with_limit_minus_one(self):
        result = convert("'a,b,c,d'.split(',', -1) == ['a', 'b', 'c', 'd']")
        assert result == "STRING_TO_ARRAY('a,b,c,d', ',') = ARRAY['a', 'b', 'c', 'd']"

    def test_with_limit_zero(self):
        result = convert("'a,b,c'.split(',', 0).size() == 0")
        assert result == "COALESCE(ARRAY_LENGTH(ARRAY[]::text[], 1), 0) = 0"

    def test_with_limit_one(self):
        result = convert("'a,b,c'.split(',', 1) == ['a,b,c']")
        assert result == "ARRAY['a,b,c'] = ARRAY['a,b,c']"

    def test_with_limit_two(self):
        result = convert("'a,b,c,d'.split(',', 2).size() == 2")
        assert result == "COALESCE(ARRAY_LENGTH((STRING_TO_ARRAY('a,b,c,d', ','))[1:2], 1), 0) = 2"

    def test_with_limit_three(self):
        result = convert("'one;two;three;four'.split(';', 3) == ['one', 'two', 'three']")
        assert result == "(STRING_TO_ARRAY('one;two;three;four', ';'))[1:3] = ARRAY['one', 'two', 'three']"

    def test_with_space_delimiter(self):
        result = convert("'hello world'.split(' ') == ['hello', 'world']")
        assert result == "STRING_TO_ARRAY('hello world', ' ') = ARRAY['hello', 'world']"

    def test_negative_limit_error(self):
        with pytest.raises(UnsupportedOperationError, match="split.*negative limit"):
            convert("'a,b,c'.split(',', -2)")


class TestJoin:
    def test_basic_with_delimiter(self):
        result = convert("['a', 'b', 'c'].join(',') == 'a,b,c'")
        assert result == "ARRAY_TO_STRING(ARRAY['a', 'b', 'c'], ',', '') = 'a,b,c'"

    def test_without_delimiter(self):
        result = convert("['a', 'b', 'c'].join() == 'abc'")
        assert result == "ARRAY_TO_STRING(ARRAY['a', 'b', 'c'], '', '') = 'abc'"

    def test_with_space(self):
        result = convert("['hello', 'world'].join(' ') == 'hello world'")
        assert result == "ARRAY_TO_STRING(ARRAY['hello', 'world'], ' ', '') = 'hello world'"
