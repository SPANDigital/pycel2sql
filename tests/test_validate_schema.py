"""Tests for validate_schema parameter."""

import pytest

from pycel2sql import analyze, convert, convert_parameterized
from pycel2sql._errors import InvalidSchemaError
from pycel2sql.schema import FieldSchema, Schema


def _make_schemas() -> dict[str, Schema]:
    return {
        "usr": Schema([
            FieldSchema(name="name", type="text"),
            FieldSchema(name="age", type="integer"),
            FieldSchema(name="metadata", type="jsonb", is_jsonb=True),
            FieldSchema(name="tags", type="text", repeated=True),
        ]),
    }


class TestValidateSchemaConfig:
    """Tests for validate_schema configuration."""

    def test_default_false_allows_unknown_fields(self):
        """Default validate_schema=False allows unknown fields."""
        result = convert("usr.nonexistent == 'foo'")
        assert "usr.nonexistent" in result

    def test_true_with_no_schemas_raises(self):
        """validate_schema=True with no schemas raises immediately."""
        with pytest.raises(InvalidSchemaError):
            convert("usr.name == 'foo'", validate_schema=True)

    def test_true_with_empty_schemas_raises(self):
        """validate_schema=True with empty schemas dict raises immediately."""
        with pytest.raises(InvalidSchemaError):
            convert("usr.name == 'foo'", schemas={}, validate_schema=True)

    def test_false_with_schemas_allows_unknown(self):
        """validate_schema=False with schemas still allows unknown fields."""
        schemas = _make_schemas()
        result = convert("usr.nonexistent == 'foo'", schemas=schemas, validate_schema=False)
        assert "usr.nonexistent" in result


class TestValidateSchemaFieldAccess:
    """Tests for field access validation."""

    def test_valid_field_succeeds(self):
        """Known field passes validation."""
        schemas = _make_schemas()
        result = convert("usr.name == 'alice'", schemas=schemas, validate_schema=True)
        assert "usr.name" in result

    def test_unknown_field_raises(self):
        """Unknown field raises InvalidSchemaError."""
        schemas = _make_schemas()
        with pytest.raises(InvalidSchemaError) as exc_info:
            convert("usr.email == 'test@example.com'", schemas=schemas, validate_schema=True)
        assert "field not found in schema" in str(exc_info.value)
        assert "email" in exc_info.value.internal_details

    def test_unknown_table_raises(self):
        """Unknown table raises InvalidSchemaError."""
        schemas = _make_schemas()
        with pytest.raises(InvalidSchemaError) as exc_info:
            convert("orders.total > 100", schemas=schemas, validate_schema=True)
        assert "field not found in schema" in str(exc_info.value)
        assert "orders" in exc_info.value.internal_details

    def test_multiple_valid_fields(self):
        """Multiple known fields pass validation."""
        schemas = _make_schemas()
        result = convert(
            "usr.name == 'alice' && usr.age > 18",
            schemas=schemas,
            validate_schema=True,
        )
        assert "usr.name" in result
        assert "usr.age" in result

    def test_error_dual_messaging(self):
        """Error uses sanitized user message and detailed internal message."""
        schemas = _make_schemas()
        with pytest.raises(InvalidSchemaError) as exc_info:
            convert("usr.missing == 1", schemas=schemas, validate_schema=True)
        # User-facing message is sanitized
        assert str(exc_info.value) == "field not found in schema"
        # Internal detail has specifics
        assert "missing" in exc_info.value.internal_details
        assert "usr" in exc_info.value.internal_details


class TestValidateSchemaJSON:
    """Tests for JSON field validation."""

    def test_json_first_field_validated(self):
        """JSON field validates that the first field exists in schema."""
        schemas = _make_schemas()
        result = convert(
            "usr.metadata.key == 'val'",
            schemas=schemas,
            validate_schema=True,
        )
        assert result  # Should succeed since 'metadata' is in schema

    def test_json_nested_keys_not_over_validated(self):
        """Nested JSON keys beyond first field are not validated."""
        schemas = _make_schemas()
        # 'metadata' exists but 'settings' and 'theme' are nested JSON keys — should pass
        result = convert(
            "usr.metadata.settings.theme == 'dark'",
            schemas=schemas,
            validate_schema=True,
        )
        assert result

    def test_json_unknown_first_field_raises(self):
        """Unknown first field with JSON-like access raises."""
        schemas = _make_schemas()
        with pytest.raises(InvalidSchemaError):
            convert("usr.config.key == 'val'", schemas=schemas, validate_schema=True)


class TestValidateSchemaComprehensions:
    """Tests for comprehension variable handling."""

    def test_comprehension_var_not_validated(self):
        """Comprehension variables are not validated against schema."""
        schemas = _make_schemas()
        result = convert(
            "usr.tags.all(t, t == 'admin')",
            schemas=schemas,
            validate_schema=True,
        )
        assert result

    def test_comprehension_with_field_access(self):
        """Comprehension on a valid field succeeds."""
        schemas = _make_schemas()
        result = convert(
            "usr.tags.exists(t, t == 'admin')",
            schemas=schemas,
            validate_schema=True,
        )
        assert result


class TestValidateSchemaBareIdents:
    """Tests for bare identifiers (no table prefix)."""

    def test_bare_ident_not_validated(self):
        """Bare identifiers without table prefix are not validated."""
        schemas = _make_schemas()
        result = convert(
            "age > 10",
            schemas=schemas,
            validate_schema=True,
        )
        assert "age" in result


class TestValidateSchemaConvertParameterized:
    """Tests for validate_schema with convert_parameterized."""

    def test_valid_field_succeeds(self):
        schemas = _make_schemas()
        result = convert_parameterized(
            "usr.name == 'alice'",
            schemas=schemas,
            validate_schema=True,
        )
        assert result.sql
        assert result.parameters

    def test_unknown_field_raises(self):
        schemas = _make_schemas()
        with pytest.raises(InvalidSchemaError):
            convert_parameterized(
                "usr.email == 'test'",
                schemas=schemas,
                validate_schema=True,
            )


class TestValidateSchemaAnalyze:
    """Tests for validate_schema with analyze."""

    def test_valid_field_succeeds(self):
        schemas = _make_schemas()
        result = analyze(
            "usr.name == 'alice'",
            schemas=schemas,
            validate_schema=True,
        )
        assert result.sql

    def test_unknown_field_raises(self):
        schemas = _make_schemas()
        with pytest.raises(InvalidSchemaError):
            analyze(
                "usr.email == 'test'",
                schemas=schemas,
                validate_schema=True,
            )
