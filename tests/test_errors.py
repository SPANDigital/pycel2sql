"""Error handling tests."""

import pytest

from pycel2sql import convert
from pycel2sql._errors import (
    ConversionError,
    InvalidFieldNameError,
    InvalidRegexPatternError,
    UnsupportedOperationError,
)


class TestInvalidFieldNames:
    def test_reserved_keyword(self):
        with pytest.raises(InvalidFieldNameError):
            convert('select == "test"')

    def test_field_name_with_spaces(self):
        with pytest.raises(ConversionError):
            convert('string_int_map["on e"] == 1')


class TestReDoSProtection:
    def test_nested_quantifier(self):
        with pytest.raises(InvalidRegexPatternError, match="ReDoS"):
            convert('name.matches("(a+)+")')

    def test_pattern_too_long(self):
        with pytest.raises(InvalidRegexPatternError, match="too long"):
            convert(f'name.matches("{"a" * 600}")')

    def test_nesting_too_deep(self):
        pattern = "(" * 15 + "a" + ")" * 15
        with pytest.raises(InvalidRegexPatternError, match="nesting"):
            convert(f'name.matches("{pattern}")')


class TestDualMessaging:
    def test_user_message_is_sanitized(self):
        try:
            convert('select == "test"')
        except ConversionError as e:
            assert "field name is a reserved SQL keyword" in str(e)
            assert "select" not in str(e)
            assert "select" in e.internal()


class TestFormatErrors:
    def test_unsupported_specifier_b(self):
        with pytest.raises(UnsupportedOperationError, match="unsupported format specifier %b"):
            convert("'Binary: %b'.format([5])")

    def test_unsupported_specifier_x(self):
        with pytest.raises(UnsupportedOperationError, match="unsupported format specifier %x"):
            convert("'Hex: %x'.format([255])")
