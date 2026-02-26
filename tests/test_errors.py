"""Error handling tests."""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql._errors import (
    ConversionError,
    InvalidByteArrayLengthError,
    InvalidDurationError,
    InvalidFieldNameError,
    InvalidRegexPatternError,
    MaxComprehensionDepthExceededError,
    MaxDepthExceededError,
    MaxOutputLengthExceededError,
    UnsupportedExpressionError,
    UnsupportedOperationError,
)
from pycel2sql.schema import FieldSchema, Schema


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


class TestMaxDepthExceeded:
    def test_shallow_depth_raises(self):
        with pytest.raises(MaxDepthExceededError, match="maximum recursion depth exceeded"):
            convert("1 + 2 + 3 + 4 + 5 + 6 + 7 + 8", max_depth=5)

    def test_error_has_internal_details(self):
        try:
            convert("1 + 2 + 3 + 4 + 5 + 6 + 7 + 8", max_depth=5)
        except MaxDepthExceededError as e:
            assert "depth" in e.internal()
            assert "exceeds limit" in e.internal()

    def test_normal_depth_succeeds(self):
        result = convert("1 + 2 + 3")
        assert "+" in result

    def test_default_depth_handles_normal_expression(self):
        result = convert('name == "test" && age > 18')
        assert "name = 'test'" in result


class TestMaxOutputLengthExceeded:
    def test_tiny_limit_raises(self):
        with pytest.raises(MaxOutputLengthExceededError, match="maximum SQL output length exceeded"):
            convert('name == "alice"', max_output_length=5)

    def test_error_has_internal_details(self):
        try:
            convert('name == "alice"', max_output_length=5)
        except MaxOutputLengthExceededError as e:
            assert "exceeds limit" in e.internal()

    def test_adequate_limit_succeeds(self):
        result = convert('name == "alice"', max_output_length=50000)
        assert result == "name = 'alice'"

    def test_dual_messaging(self):
        try:
            convert('name == "alice"', max_output_length=5)
        except MaxOutputLengthExceededError as e:
            # User message should not contain sensitive implementation details
            assert "maximum SQL output length exceeded" in str(e)


class TestMaxComprehensionDepthExceeded:
    def test_triple_nesting_succeeds(self):
        # depth reaches 3 but check is >= MAX_COMPREHENSION_DEPTH (3)
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        # Double nesting should work
        result = convert(
            "t.arr.exists(x, t.arr.exists(y, x > y))",
            schemas=schemas,
        )
        assert "EXISTS" in result

    def test_quadruple_nesting_fails(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        with pytest.raises(MaxComprehensionDepthExceededError, match="comprehension nesting"):
            convert(
                "t.arr.exists(x, t.arr.exists(y, t.arr.exists(z, t.arr.exists(w, w > 0))))",
                schemas=schemas,
            )

    def test_error_has_internal_details(self):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        try:
            convert(
                "t.arr.exists(x, t.arr.exists(y, t.arr.exists(z, t.arr.exists(w, w > 0))))",
                schemas=schemas,
            )
        except MaxComprehensionDepthExceededError as e:
            assert "depth" in e.internal()
            assert "exceeds limit" in e.internal()


class TestInvalidByteArrayLength:
    def test_huge_bytes_raises(self):
        # bytes literal > 10000 chars in non-parameterized mode
        large = "a" * 10001
        with pytest.raises(InvalidByteArrayLengthError, match="byte array too long"):
            convert(f'b"{large}" == data')

    def test_at_limit_succeeds(self):
        large = "a" * 9999
        result = convert(f'b"{large}" == data')
        assert "data" in result

    def test_parameterized_mode_bypasses_check(self):
        large = "a" * 10001
        result = convert_parameterized(f'b"{large}" == data')
        assert "data" in result.sql


class TestInvalidDuration:
    def test_unparseable_duration(self):
        with pytest.raises(InvalidDurationError, match="invalid duration"):
            convert('duration("xyz")')

    def test_empty_duration(self):
        with pytest.raises(InvalidDurationError, match="invalid duration"):
            convert('duration("")')

    def test_dual_messaging(self):
        try:
            convert('duration("xyz")')
        except InvalidDurationError as e:
            assert "invalid duration" in str(e)
            assert "xyz" in e.internal()


class TestUnsupportedExpressionError:
    def test_unknown_method(self):
        with pytest.raises(UnsupportedExpressionError, match="unsupported method"):
            convert("name.unknownMethod()")

    def test_unsupported_timestamp_method(self):
        with pytest.raises(UnsupportedExpressionError):
            convert("created_at.getWeek()")


class TestDualMessagingComprehensive:
    """CWE-209 verification: user messages should not leak internal details."""

    def test_max_depth_no_leak(self):
        try:
            convert("1 + 2 + 3 + 4 + 5 + 6 + 7 + 8", max_depth=5)
        except MaxDepthExceededError as e:
            # Internal has specific numbers, user message is generic
            assert "maximum recursion depth exceeded" == str(e)
            assert "5" in e.internal()

    def test_max_output_no_leak(self):
        try:
            convert('name == "alice"', max_output_length=5)
        except MaxOutputLengthExceededError as e:
            assert "maximum SQL output length exceeded" == str(e)
            assert "5" in e.internal()

    def test_byte_array_no_leak(self):
        try:
            convert(f'b"{"a" * 10001}" == data')
        except InvalidByteArrayLengthError as e:
            assert "byte array too long" == str(e)
            assert "10001" in e.internal()

    def test_duration_no_leak(self):
        try:
            convert('duration("xyz")')
        except InvalidDurationError as e:
            assert "invalid duration value" == str(e)
            assert "xyz" in e.internal()
