"""Invalid arguments error tests.

Tests for InvalidArgumentsError which has ~30 raise sites in the converter.
"""

import pytest

from pycel2sql import convert
from pycel2sql._errors import (
    InvalidArgumentsError,
    UnsupportedOperationError,
)
from pycel2sql.schema import FieldSchema, Schema


class TestStartsWith:
    def test_non_string_arg_raises(self):
        with pytest.raises(InvalidArgumentsError, match="startsWith"):
            convert("name.startsWith(123)")

    def test_too_many_args(self):
        # CEL parser may reject this; if it does, that's fine
        with pytest.raises(Exception):
            convert('name.startsWith("a", "b")')


class TestEndsWith:
    def test_non_string_arg_raises(self):
        with pytest.raises(InvalidArgumentsError, match="endsWith"):
            convert("name.endsWith(123)")


class TestMatches:
    def test_non_string_arg_raises(self):
        with pytest.raises(InvalidArgumentsError, match="matches"):
            convert("name.matches(123)")


class TestCharAt:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("name.charAt()")

    def test_too_many_args_raises(self):
        with pytest.raises(Exception):
            convert("name.charAt(1, 2)")


class TestIndexOf:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("name.indexOf()")

    def test_too_many_args_raises(self):
        with pytest.raises(InvalidArgumentsError):
            convert('name.indexOf("a", 0, 5)')


class TestLastIndexOf:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("name.lastIndexOf()")


class TestSubstring:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("name.substring()")

    def test_too_many_args_raises(self):
        with pytest.raises(InvalidArgumentsError, match="substring"):
            convert("name.substring(0, 5, 10)")


class TestReplace:
    def test_too_few_args(self):
        with pytest.raises(InvalidArgumentsError, match="replace"):
            convert('name.replace("a")')

    def test_unsupported_limit(self):
        with pytest.raises(UnsupportedOperationError, match="replace"):
            convert('name.replace("a", "b", 2)')


class TestSplit:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("name.split()")

    def test_too_many_args_raises(self):
        with pytest.raises(InvalidArgumentsError, match="split"):
            convert('name.split(",", 2, "extra")')

    def test_non_int_limit_raises(self):
        with pytest.raises(InvalidArgumentsError, match="split"):
            convert('name.split(",", "x")')

    def test_negative_limit_raises(self):
        with pytest.raises(UnsupportedOperationError, match="split"):
            convert('name.split(",", -5)')


class TestJoin:
    def test_too_many_args_raises(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(InvalidArgumentsError, match="join"):
            convert('t.arr.join(",", "extra")', schemas=schemas)


class TestTimestampFunc:
    def test_too_many_args_raises(self):
        with pytest.raises(InvalidArgumentsError, match="timestamp"):
            convert('timestamp("a", "b", "c")')


class TestDurationFunc:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("duration()")

    def test_non_string_raises(self):
        with pytest.raises(InvalidArgumentsError, match="duration"):
            convert("duration(123)")


class TestIntervalFunc:
    def test_wrong_arg_count_raises(self):
        with pytest.raises(InvalidArgumentsError, match="interval"):
            convert("interval(1)")


class TestArrayIndex:
    def test_negative_index_raises(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(InvalidArgumentsError, match="negative"):
            convert("t.arr[-1]", schemas=schemas)

    def test_overflow_index_raises(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(InvalidArgumentsError, match="overflow"):
            convert("t.arr[9999999999]", schemas=schemas)


class TestTypeCast:
    def test_cast_wrong_arg_count(self):
        with pytest.raises(Exception):
            convert("int()")


class TestContains:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("name.contains()")


class TestFormatFunc:
    def test_non_string_format(self):
        # An identifier that is not a string literal format
        with pytest.raises(InvalidArgumentsError, match="format"):
            convert("name.format([1])")


class TestHas:
    def test_no_args_raises(self):
        with pytest.raises(Exception):
            convert("has()")


class TestComprehensionArgs:
    def test_all_wrong_arg_count(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(Exception):
            convert("t.arr.all(x)", schemas=schemas)

    def test_exists_wrong_arg_count(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(Exception):
            convert("t.arr.exists(x)", schemas=schemas)

    def test_filter_wrong_arg_count(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(Exception):
            convert("t.arr.filter(x)", schemas=schemas)
