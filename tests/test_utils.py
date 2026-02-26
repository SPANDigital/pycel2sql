"""Utility function tests."""

import pytest

from pycel2sql._errors import InvalidFieldNameError
from pycel2sql._utils import (
    convert_re2_to_posix,
    escape_like_pattern,
    escape_string_literal,
    validate_field_name,
    validate_no_null_bytes,
)


class TestValidateFieldName:
    def test_valid_name(self):
        validate_field_name("my_field")

    def test_empty_name(self):
        with pytest.raises(InvalidFieldNameError):
            validate_field_name("")

    def test_too_long(self):
        with pytest.raises(InvalidFieldNameError):
            validate_field_name("a" * 64)

    def test_invalid_chars(self):
        with pytest.raises(InvalidFieldNameError):
            validate_field_name("my field")

    def test_reserved_keyword(self):
        with pytest.raises(InvalidFieldNameError):
            validate_field_name("select")

    def test_starts_with_number(self):
        with pytest.raises(InvalidFieldNameError):
            validate_field_name("1field")


class TestEscapeLikePattern:
    def test_no_special_chars(self):
        assert escape_like_pattern("hello") == "hello"

    def test_percent(self):
        assert escape_like_pattern("100%") == "100\\%"

    def test_underscore(self):
        assert escape_like_pattern("a_b") == "a\\_b"

    def test_backslash(self):
        assert escape_like_pattern("a\\b") == "a\\\\b"

    def test_single_quote(self):
        assert escape_like_pattern("it's") == "it''s"


class TestEscapeStringLiteral:
    def test_no_special_chars(self):
        assert escape_string_literal("hello") == "hello"

    def test_single_quote(self):
        assert escape_string_literal("it's") == "it''s"


class TestConvertRE2ToPOSIX:
    def test_simple_pattern(self):
        pattern, ci = convert_re2_to_posix("a+")
        assert pattern == "a+"
        assert ci is False

    def test_case_insensitive(self):
        pattern, ci = convert_re2_to_posix("(?i)hello")
        assert pattern == "hello"
        assert ci is True

    def test_digit_class(self):
        pattern, ci = convert_re2_to_posix(r"\d+")
        assert pattern == "[[:digit:]]+"

    def test_word_class(self):
        pattern, ci = convert_re2_to_posix(r"\w+")
        assert pattern == "[[:alnum:]_]+"

    def test_space_class(self):
        pattern, ci = convert_re2_to_posix(r"\s+")
        assert pattern == "[[:space:]]+"

    def test_word_boundary(self):
        pattern, ci = convert_re2_to_posix(r"\bword\b")
        assert pattern == r"\yword\y"

    def test_non_capturing_group(self):
        pattern, ci = convert_re2_to_posix("(?:abc)")
        assert pattern == "(abc)"


class TestValidateNoNullBytes:
    def test_valid_string(self):
        validate_no_null_bytes("hello")

    def test_null_byte(self):
        with pytest.raises(InvalidFieldNameError, match="null bytes"):
            validate_no_null_bytes("hel\x00lo")
