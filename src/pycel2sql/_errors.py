"""Exception hierarchy for CEL-to-SQL conversion."""


class ConversionError(Exception):
    """Base exception for CEL-to-SQL conversion errors.

    Provides dual messaging: a sanitized user-facing message and
    internal details for logging (CWE-209 prevention).
    """

    def __init__(
        self,
        user_message: str,
        internal_details: str = "",
        wrapped: Exception | None = None,
    ) -> None:
        super().__init__(user_message)
        self.user_message = user_message
        self.internal_details = internal_details or user_message
        self.wrapped = wrapped

    def internal(self) -> str:
        return self.internal_details


class UnsupportedExpressionError(ConversionError):
    """Raised when a CEL expression type is not supported."""


class InvalidFieldNameError(ConversionError):
    """Raised when a field name is invalid or empty."""


class InvalidSchemaError(ConversionError):
    """Raised when there is a problem with the provided schema."""


class InvalidRegexPatternError(ConversionError):
    """Raised when a regex pattern is invalid."""


class MaxDepthExceededError(ConversionError):
    """Raised when recursion depth limit is exceeded."""


class MaxOutputLengthExceededError(ConversionError):
    """Raised when SQL output length limit is exceeded."""


class InvalidComprehensionError(ConversionError):
    """Raised when a comprehension expression is invalid."""


class MaxComprehensionDepthExceededError(ConversionError):
    """Raised when comprehension nesting is too deep."""


class InvalidArgumentsError(ConversionError):
    """Raised when function arguments are invalid."""


class UnsupportedOperationError(ConversionError):
    """Raised when an operation cannot be converted to SQL."""


class InvalidTimestampOperationError(ConversionError):
    """Raised when a timestamp operation is invalid."""


class InvalidDurationError(ConversionError):
    """Raised when a duration value is invalid."""


class InvalidJSONPathError(ConversionError):
    """Raised when a JSON path expression is invalid."""


class InvalidOperatorError(ConversionError):
    """Raised when an operator is invalid."""


class UnsupportedTypeError(ConversionError):
    """Raised when a type is not supported."""


class InvalidByteArrayLengthError(ConversionError):
    """Raised when a byte array exceeds the maximum length."""


class UnsupportedDialectFeatureError(ConversionError):
    """Raised when a feature is not supported by the dialect."""


class IntrospectionError(ConversionError):
    """Raised when schema introspection fails."""


# Sanitized user-facing error message constants
ERR_MSG_UNSUPPORTED_EXPRESSION = "unsupported expression type"
ERR_MSG_INVALID_OPERATOR = "invalid operator"
ERR_MSG_UNSUPPORTED_TYPE = "unsupported type"
ERR_MSG_UNSUPPORTED_COMPREHENSION = "unsupported comprehension type"
ERR_MSG_COMPREHENSION_DEPTH_EXCEEDED = "comprehension nesting depth exceeded"
ERR_MSG_INVALID_FIELD_ACCESS = "invalid field access"
ERR_MSG_CONVERSION_FAILED = "expression conversion failed"
ERR_MSG_INVALID_TIMESTAMP_OP = "invalid timestamp operation"
ERR_MSG_INVALID_DURATION = "invalid duration value"
ERR_MSG_INVALID_ARGUMENTS = "invalid function arguments"
ERR_MSG_UNKNOWN_TYPE = "unknown type"
ERR_MSG_INVALID_PATTERN = "invalid pattern"
