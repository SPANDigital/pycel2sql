"""Resource limit constants for CEL-to-SQL conversion."""

DEFAULT_MAX_RECURSION_DEPTH = 100
"""Maximum AST visit recursion depth (CWE-674 prevention)."""

MAX_COMPREHENSION_DEPTH = 3
"""Maximum nesting depth for comprehension subqueries (CWE-400 prevention)."""

MAX_BYTE_ARRAY_LENGTH = 10000
"""Maximum byte array length in non-parameterized mode."""

DEFAULT_MAX_SQL_OUTPUT_LENGTH = 50000
"""Maximum generated SQL string length."""
