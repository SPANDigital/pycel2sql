"""Error class hierarchy tests."""

import pytest

from pycel2sql._errors import (
    ConversionError,
    InvalidArgumentsError,
    InvalidByteArrayLengthError,
    InvalidComprehensionError,
    InvalidDurationError,
    InvalidFieldNameError,
    InvalidJSONPathError,
    InvalidOperatorError,
    InvalidRegexPatternError,
    InvalidSchemaError,
    InvalidTimestampOperationError,
    MaxComprehensionDepthExceededError,
    MaxDepthExceededError,
    MaxOutputLengthExceededError,
    UnsupportedDialectFeatureError,
    UnsupportedExpressionError,
    UnsupportedOperationError,
    UnsupportedTypeError,
)


class TestConversionErrorBase:
    def test_str_returns_user_message(self):
        err = ConversionError("user msg", "internal detail")
        assert str(err) == "user msg"

    def test_internal_returns_details(self):
        err = ConversionError("user msg", "internal detail")
        assert err.internal() == "internal detail"

    def test_internal_defaults_to_user_message(self):
        err = ConversionError("same message")
        assert err.internal() == "same message"

    def test_wrapped_exception(self):
        cause = ValueError("root cause")
        err = ConversionError("user msg", wrapped=cause)
        assert err.wrapped is cause

    def test_is_exception(self):
        err = ConversionError("test")
        assert isinstance(err, Exception)


class TestErrorHierarchy:
    """Test that all 17 subclasses are subclasses of ConversionError."""

    ALL_ERROR_CLASSES = [
        UnsupportedExpressionError,
        InvalidFieldNameError,
        InvalidSchemaError,
        InvalidRegexPatternError,
        MaxDepthExceededError,
        MaxOutputLengthExceededError,
        InvalidComprehensionError,
        MaxComprehensionDepthExceededError,
        InvalidArgumentsError,
        UnsupportedOperationError,
        InvalidTimestampOperationError,
        InvalidDurationError,
        InvalidJSONPathError,
        InvalidOperatorError,
        UnsupportedTypeError,
        InvalidByteArrayLengthError,
        UnsupportedDialectFeatureError,
    ]

    @pytest.mark.parametrize("cls", ALL_ERROR_CLASSES)
    def test_is_subclass_of_conversion_error(self, cls):
        assert issubclass(cls, ConversionError)

    @pytest.mark.parametrize("cls", ALL_ERROR_CLASSES)
    def test_instantiation(self, cls):
        err = cls("test message", "internal detail")
        assert str(err) == "test message"
        assert err.internal() == "internal detail"

    @pytest.mark.parametrize("cls", ALL_ERROR_CLASSES)
    def test_is_catchable_as_conversion_error(self, cls):
        with pytest.raises(ConversionError):
            raise cls("test")


class TestForwardLookingErrors:
    """Test defined-but-not-yet-commonly-raised error types."""

    def test_invalid_schema_error(self):
        err = InvalidSchemaError("bad schema", "field 'x' has invalid type")
        assert "bad schema" in str(err)
        assert "field 'x'" in err.internal()

    def test_invalid_timestamp_operation_error(self):
        err = InvalidTimestampOperationError("bad timestamp op")
        assert isinstance(err, ConversionError)

    def test_invalid_json_path_error(self):
        err = InvalidJSONPathError("invalid JSON path", "path '$.x[' is malformed")
        assert err.user_message == "invalid JSON path"
        assert "malformed" in err.internal()

    def test_invalid_operator_error(self):
        err = InvalidOperatorError("invalid operator")
        assert isinstance(err, ConversionError)

    def test_unsupported_type_error(self):
        err = UnsupportedTypeError("unsupported type", "type 'map' not supported")
        assert "unsupported type" in str(err)

    def test_unsupported_dialect_feature_error(self):
        err = UnsupportedDialectFeatureError("feature not supported", "regex not available in SQLite")
        assert "feature not supported" in str(err)
        assert "SQLite" in err.internal()
