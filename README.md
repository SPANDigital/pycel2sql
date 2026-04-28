# pycel2sql

[![CI](https://github.com/SPANDigital/pycel2sql/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SPANDigital/pycel2sql/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pycel2sql)](https://pypi.org/project/pycel2sql/)
[![Python](https://img.shields.io/pypi/pyversions/pycel2sql)](https://pypi.org/project/pycel2sql/)
[![License: MIT](https://img.shields.io/pypi/l/pycel2sql)](https://github.com/SPANDigital/pycel2sql/blob/main/LICENSE)

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

Convert [CEL (Common Expression Language)](https://cel.dev/) expressions to SQL WHERE clauses.

Python port of [cel2sql](https://github.com/SPANDigital/cel2sql) (Go).

## Installation

```bash
pip install pycel2sql
```

Requires Python 3.12+.

## Quick Start

```python
from pycel2sql import convert

# Default dialect is PostgreSQL
sql = convert('name == "alice" && age > 30')
# => name = 'alice' AND age > 30

sql = convert('status == "active" || tags.size() > 0')
# => status = 'active' OR ARRAY_LENGTH(tags, 1) > 0
```

## Dialects

Six SQL dialects are supported:

```python
from pycel2sql import convert
from pycel2sql.dialect import get_dialect

# Using get_dialect() factory
sql = convert('name == "alice"', dialect=get_dialect("postgresql"))
sql = convert('name == "alice"', dialect=get_dialect("mysql"))
sql = convert('name == "alice"', dialect=get_dialect("sqlite"))
sql = convert('name == "alice"', dialect=get_dialect("duckdb"))
sql = convert('name == "alice"', dialect=get_dialect("bigquery"))
sql = convert('name == "alice"', dialect=get_dialect("spark"))

# Or instantiate directly
from pycel2sql import (
    PostgresDialect, MySQLDialect, SQLiteDialect, DuckDBDialect,
    BigQueryDialect, SparkDialect,
)

sql = convert('name == "alice"', dialect=MySQLDialect())
```

## Parameterized Queries

Use `convert_parameterized()` to produce parameterized SQL with bind placeholders:

```python
from pycel2sql import convert_parameterized, MySQLDialect

result = convert_parameterized('name == "alice" && age > 30')
# result.sql => 'name = $1 AND age > $2'  (PostgreSQL default)
# result.parameters => ['alice', 30]

result = convert_parameterized('name == "alice"', dialect=MySQLDialect())
# result.sql => 'name = ?'
# result.parameters => ['alice']
```

Placeholder styles per dialect:

| Dialect       | Placeholder        |
|---------------|--------------------|
| PostgreSQL    | `$1`, `$2`, ...    |
| DuckDB        | `$1`, `$2`, ...    |
| BigQuery      | `@p1`, `@p2`, ...  |
| MySQL         | `?` (positional)   |
| SQLite        | `?` (positional)   |
| Apache Spark  | `?` (positional)   |

## Conversion Options

### `json_variables`

Declare CEL variable names that correspond to flat JSONB columns. Field access via dot notation or bracket notation emits dialect-specific JSON extraction:

```python
from pycel2sql import convert

# PostgreSQL: dot and bracket notation both produce ->> operators
sql = convert("context.host == 'a'", json_variables={"context"})
# => context->>'host' = 'a'

sql = convert('context["host"] == "a"', json_variables={"context"})
# => context->>'host' = 'a'

# Nested paths: intermediate keys use ->, final key uses ->>
sql = convert("tags.corpus.section == 'x'", json_variables={"tags"})
# => tags->'corpus'->>'section' = 'x'
```

`json_variables` takes precedence over schema-declared JSON. Comprehension iter vars shadow `json_variables` (collisions are not treated as JSON inside the comprehension body).

### `column_aliases`

Map CEL identifier names to SQL column names. Useful when database columns use prefixed names while user-facing CEL expressions use clean names:

```python
sql = convert("name == 'a'", column_aliases={"name": "usr_name"})
# => usr_name = 'a'
```

The alias is validated against the dialect's identifier rules. The original CEL name remains the schema key — alias is output-only.

### `param_start_index`

Shift the placeholder counter for `convert_parameterized()` when embedding the generated fragment into a larger pre-parameterized query:

```python
result = convert_parameterized(
    "name == 'a' && age > 30",
    param_start_index=5,
)
# result.sql => 'name = $5 AND age > $6'
# result.parameters => ['a', 30]
```

Values less than 1 are clamped to 1. For positional-`?` dialects (MySQL, SQLite, Apache Spark) the placeholder text is unchanged but the parameter ordering is preserved.

### `format()` per-dialect mapping

CEL's `string.format(args)` dispatches to dialect-specific SQL:

| Dialect       | Output                  |
|---------------|-------------------------|
| PostgreSQL    | `FORMAT('...', ...)`    |
| BigQuery      | `FORMAT('...', ...)`    |
| SQLite        | `printf('...', ...)`    |
| DuckDB        | `printf('...', ...)`    |
| Apache Spark  | `format_string('...', ...)` |
| MySQL         | raises `UnsupportedDialectFeatureError` |

## JSON Fields

Provide schemas to enable JSON field detection:

```python
from pycel2sql import convert, PostgresDialect
from pycel2sql.schema import Schema, FieldSchema

schemas = {
    "usr": Schema([FieldSchema("metadata", is_jsonb=True)])
}

sql = convert(
    'usr.metadata.role == "admin"',
    dialect=PostgresDialect(),
    schemas=schemas,
)
# => usr.metadata->>'role' = 'admin'
```

## Schema Validation

Enable strict validation to catch typos and references to nonexistent fields:

```python
from pycel2sql import convert, InvalidSchemaError
from pycel2sql.schema import Schema, FieldSchema

schemas = {
    "usr": Schema([
        FieldSchema("name"),
        FieldSchema("age", type="integer"),
        FieldSchema("metadata", is_jsonb=True),
    ])
}

# Valid field — works normally
sql = convert('usr.name == "alice"', schemas=schemas, validate_schema=True)

# Unknown field — raises InvalidSchemaError
convert('usr.email == "test"', schemas=schemas, validate_schema=True)
# => InvalidSchemaError: field not found in schema
```

Validation scope:
- **Validates**: `table.field` references — table must exist in `schemas`, field must exist in that table's `Schema`
- **Skips**: Nested JSON paths beyond the first field (e.g., `usr.metadata.settings.theme` validates `metadata` exists, not `settings`)
- **Skips**: Comprehension variables (`t` in `tags.all(t, t > 0)`)
- **Skips**: Bare identifiers without a table prefix (`age > 10`)

Works with all three public API functions: `convert()`, `convert_parameterized()`, and `analyze()`.

## Schema Introspection

Auto-discover table schemas from a live database connection instead of building `Schema` objects manually:

```python
from pycel2sql import convert, introspect
from pycel2sql.dialect.postgres import PostgresDialect
import psycopg

conn = psycopg.connect("postgresql://localhost/mydb")

# Introspect specific tables — detects JSON, JSONB, and array columns
schemas = introspect("postgresql", conn, table_names=["users", "orders"])

sql = convert(
    'users.metadata.role == "admin"',
    dialect=PostgresDialect(),
    schemas=schemas,
)
# => users.metadata->>'role' = 'admin'
```

Per-dialect functions are also available:

```python
from pycel2sql.introspect import introspect_postgres, introspect_sqlite

# PostgreSQL — detects JSONB, JSON, and ARRAY columns
schemas = introspect_postgres(conn, table_names=["users"], schema_name="public")

# SQLite — explicit json_columns since SQLite has no JSON type
schemas = introspect_sqlite(
    conn,
    table_names=["events"],
    json_columns={"events": ["payload", "tags"]},
)
```

All five JDBC-style dialects are supported: `introspect_postgres`, `introspect_duckdb`, `introspect_bigquery`, `introspect_mysql`, `introspect_sqlite`. Apache Spark introspection is not provided — construct `Schema` directly.

## Supported CEL Features

- **Comparisons**: `==`, `!=`, `<`, `<=`, `>`, `>=`
- **Logic**: `&&`, `||`, `!`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **String functions**: `contains()`, `startsWith()`, `endsWith()`, `matches()`, `size()`, `split()`, `join()`
- **Type casting**: `int()`, `uint()`, `double()`, `string()`, `bool()`, `bytes()`, `timestamp()`, `duration()`
- **Collections**: `in` operator, list literals, `size()`, index access
- **Timestamps & durations**: arithmetic, `getFullYear()`, `getMonth()`, `getDayOfMonth()`, `getHours()`, `getMinutes()`, `getSeconds()`
- **Macros**: `exists()`, `all()`, `exists_one()`, `map()`, `filter()`
- **Ternary**: conditional expressions
- **JSON**: field access, `has()` existence checks, nested paths
- **Regex**: `matches()` with RE2 syntax
- **Structs**: struct construction

## Index Analysis

Analyze expressions for PostgreSQL index recommendations:

```python
from pycel2sql import analyze

result = analyze('name == "alice" && age > 30')
# result.sql => "name = 'alice' AND age > 30"
# result.recommendations => [IndexRecommendation(...), ...]
```

## Security Limits

Configurable resource limits prevent abuse:

```python
from pycel2sql import convert

sql = convert(
    cel_expr,
    max_depth=100,           # AST recursion depth (default: 100)
    max_output_length=50000, # Max SQL output bytes (default: 50000)
)
```

Additional built-in limits: comprehension nesting (3 levels), regex pattern length (500 chars), field name length (63 chars), byte array size (10,000).

## Development

```bash
# Setup
uv venv && uv pip install -e ".[dev]"

# Tests
uv run pytest tests/ --ignore=tests/integration -v

# Integration tests (requires Docker/Podman)
uv pip install -e ".[integration]"
uv run pytest tests/integration/ -v

# Lint & type check
uv run ruff check src/ tests/
uv run mypy src/pycel2sql/
```

## License

MIT
