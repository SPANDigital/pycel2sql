# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

pycel2sql converts CEL (Common Expression Language) expressions into SQL WHERE clauses. It supports five SQL dialects: PostgreSQL, DuckDB, BigQuery, MySQL, and SQLite.

## Commands

```bash
# Setup
uv venv && uv pip install -e ".[dev]"

# Run all unit tests
uv run pytest tests/ --ignore=tests/integration -v

# Run a single test file or test
uv run pytest tests/test_bigquery.py -v
uv run pytest tests/test_bigquery.py::TestBigQueryBasicOps::test_equality -v

# Integration tests (requires Docker/Podman for PostgreSQL, MySQL containers)
uv pip install -e ".[integration]"
uv run pytest tests/integration/ -v

# Lint
uv run ruff check src/ tests/

# Type check (strict mode; pre-existing lark type errors are expected)
uv run mypy src/pycel2sql/
```

## Architecture

### Conversion Pipeline

CEL string → `cel-python` parser → Lark parse tree → `Converter` (tree walker) → SQL string

The `Converter` class (`_converter.py`, ~2000 lines) extends `lark.visitors.Interpreter` for top-down AST traversal. It writes SQL fragments to a `StringIO` buffer, delegating all dialect-specific syntax to the `Dialect` ABC.

### Dialect ABC + WriteFunc Callback Pattern

The central design pattern: dialect methods receive `WriteFunc` callbacks (closures that write sub-expressions to the shared buffer) rather than pre-rendered strings. This enables dialects to wrap expressions differently:

```python
# Suffix-style (PostgreSQL): expr::numeric
def write_cast_to_numeric(self, w, write_expr):
    write_expr()
    w.write("::numeric")

# Function-style (BigQuery): CAST(expr AS FLOAT64)
def write_cast_to_numeric(self, w, write_expr):
    w.write("CAST(")
    write_expr()
    w.write(" AS FLOAT64)")
```

`WriteFunc = Callable[[], None]` — defined in `dialect/_base.py`.

### Lark Tree Structure

Lark grammar rule names encode operators: `relation_eq`, `addition_add`, `multiplication_mod`. Operator prefix nodes contain the LHS as a child; the RHS is a sibling at the parent level. CEL macros (`all`, `exists`, `map`, `filter`) are parsed as regular method calls (`member_dot_arg`).

### Key Modules

| Module | Role |
|--------|------|
| `__init__.py` | Public API: `convert()`, `convert_parameterized()`, `analyze()`, `introspect()` |
| `_converter.py` | Core Converter — Lark Interpreter with visitor methods for every grammar rule |
| `dialect/_base.py` | `Dialect` ABC (40+ abstract methods), `WriteFunc` type alias, `IndexAdvisor` protocol |
| `dialect/{postgres,duckdb,bigquery,mysql,sqlite}.py` | Concrete dialect implementations |
| `schema.py` | `Schema` / `FieldSchema` for JSON/array field detection |
| `_analysis.py` | `IndexAnalyzer` — second-pass tree walker for index recommendations |
| `_utils.py` | Validation, escaping, RE2→SQL regex conversion |
| `_errors.py` | `ConversionError` hierarchy with dual messaging (sanitized user + internal detail) |
| `_constants.py` | Resource limits (max depth 100, max output 50KB, etc.) |
| `introspect/` | Schema introspection — auto-discover `Schema` from live DB connections (one module per dialect) |

### Dialect Differences

- **PostgreSQL**: `$N` params, `ARRAY[...]`, `~ / ~*` regex, `->>/->` JSON, `POSITION()` for contains
- **DuckDB**: `$N` params, `[...]` arrays, RE2 regex, `CONTAINS()`, `STRING_SPLIT()`
- **BigQuery**: `@pN` params, `[...]` arrays, `REGEXP_CONTAINS()`, `JSON_VALUE()`, `TIMESTAMP_ADD/SUB()`
- **MySQL**: `?` params, `JSON_ARRAY()`, `REGEXP`, `JSON_TABLE()` for unnest
- **SQLite**: `?` params, `json_array()`, no regex/split/join, `json_each()` for unnest

### Test Organization

Unit tests (`tests/test_*.py`) cover each feature area per dialect. Integration tests (`tests/integration/`) run generated SQL against real databases via testcontainers. BigQuery integration tests are opt-in (`PYCEL2SQL_TEST_BIGQUERY=1`) due to emulator reliability.

## Important Conventions

- Temporal arithmetic checks must happen BEFORE string concatenation detection (temporal functions contain string literals)
- JSON numeric cast applies only for numeric comparisons, not string comparisons
- `has()` on a JSON column = `IS NOT NULL`; `has()` on a JSON key = JSONB `?` operator (PG)
- `size()` dispatches to `ARRAY_LENGTH` for arrays, `LENGTH` for strings
- Depth tracking: `_visit_child()` increments/decrements `_depth` and checks limits
- Error types use dual messaging pattern to prevent information disclosure (CWE-209)
- Ruff for linting, mypy strict for type checking, line length 100, target Python 3.12+
