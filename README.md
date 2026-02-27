# pycel2sql

[![CI](https://github.com/SPANDigital/pycel2sql/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SPANDigital/pycel2sql/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pycel2sql)](https://pypi.org/project/pycel2sql/)
[![Python](https://img.shields.io/pypi/pyversions/pycel2sql)](https://pypi.org/project/pycel2sql/)
[![License: MIT](https://img.shields.io/pypi/l/pycel2sql)](https://github.com/SPANDigital/pycel2sql/blob/main/LICENSE)

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

Five SQL dialects are supported:

```python
from pycel2sql import convert
from pycel2sql.dialect import get_dialect

# Using get_dialect() factory
sql = convert('name == "alice"', dialect=get_dialect("postgresql"))
sql = convert('name == "alice"', dialect=get_dialect("mysql"))
sql = convert('name == "alice"', dialect=get_dialect("sqlite"))
sql = convert('name == "alice"', dialect=get_dialect("duckdb"))
sql = convert('name == "alice"', dialect=get_dialect("bigquery"))

# Or instantiate directly
from pycel2sql import PostgresDialect, MySQLDialect, SQLiteDialect, DuckDBDialect, BigQueryDialect

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

| Dialect    | Placeholder |
|------------|-------------|
| PostgreSQL | `$1`, `$2`, ... |
| DuckDB     | `$1`, `$2`, ... |
| BigQuery   | `@p1`, `@p2`, ... |
| MySQL      | `?` |
| SQLite     | `?` |

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

All five dialects are supported: `introspect_postgres`, `introspect_duckdb`, `introspect_bigquery`, `introspect_mysql`, `introspect_sqlite`.

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
