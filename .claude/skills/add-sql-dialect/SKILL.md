---
name: add-sql-dialect
description: Adds a new SQL dialect to pycel2sql by creating src/pycel2sql/dialect/<name>.py (subclass of the Dialect ABC), registering in the DialectName enum and the get_dialect() factory, threading new test cases through every parametrized test class, and updating the README badge grid and dialect-comparison tables. Use when porting a new database backend (Trino, Snowflake, ClickHouse, MS SQL, Athena, Oracle) or any new analytics engine.
---

# Add SQL Dialect

Adding a SQL dialect is the largest contribution shape in this repo (~1500-line PR, ~18 file touches in lockstep). The pattern is well-established — six dialects already follow it (PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Apache Spark). This skill captures the procedure so the engineer can follow the template instead of reverse-engineering the layout from existing dialects.

## Quick start

```bash
# 1. Pick the closest analogue (see "Picking the analogue" below).
# 2. Scaffold by copying — the script stubs every Dialect ABC method.
python .claude/skills/add-sql-dialect/scripts/scaffold_dialect.py duckdb cockroach Cockroach
#                                                                  ^^^^^^^ template
#                                                                          ^^^^^^^^^ folder/identifier
#                                                                                    ^^^^^^^^^ class prefix

# 3. Then:
#    a. Fill SQL bodies in src/pycel2sql/dialect/cockroach.py (replace NotImplementedError stubs).
#    b. Add SPARK = "spark" → COCKROACH = "cockroach" to DialectName in dialect/_base.py.
#    c. Register CockroachDialect in dialect/__init__.py (_REGISTRY + __all__).
#    d. Export from src/pycel2sql/__init__.py.
#    e. Add cockroach_dialect fixture + CockroachDialect() to ALL_DIALECTS in tests/conftest.py.
#    f. Add to tests/test_dialect_parametrized.py ALL_DIALECTS list.
#    g. Create tests/test_cockroach.py mirroring tests/test_duckdb.py shape.
#    h. Update README badge grid + dialect count + comparison table; bump CLAUDE.md.
#    i. Run: uv run ruff check src/ tests/ && uv run pytest tests/ --ignore=tests/integration
```

## Picking the analogue

Decide by which existing dialect's syntax shape your target most resembles:

| Question | Operator-style → use DuckDB | Function-style → use BigQuery |
|---|---|---|
| Regex match | `target ~ 'p'` (Postgres, DuckDB) | `REGEXP_CONTAINS(target, 'p')` (BigQuery); `target RLIKE 'p'` (Spark) |
| JSON access | `b->>'f'` (Postgres, DuckDB, MySQL) | `JSON_VALUE(b, '$.f')` (BigQuery); `get_json_object(b, '$.f')` (Spark); `json_extract(b, '$.f')` (SQLite) |
| Array literal | `ARRAY[…]` (Postgres); `[…]` (DuckDB, BigQuery) | `array(…)` (Spark) |
| Array index | 1-indexed (Postgres, DuckDB) | 0-indexed (BigQuery via `OFFSET`, Spark direct) |
| Param placeholder | `$N` (Postgres, DuckDB) | `?` (MySQL, SQLite, Spark) or `@pN` (BigQuery) |
| Cast to numeric | `::numeric` postfix (Postgres) | `+ 0` arithmetic coercion (MySQL, SQLite, Spark); `CAST(... AS FLOAT64)` (BigQuery) |
| Format function | `FORMAT('...', ...)` (Postgres, BigQuery) | `printf('...', ...)` (SQLite, DuckDB); `format_string('...', ...)` (Spark); raises (MySQL) |

For the full Dialect-method-by-method matrix across the existing six dialects, see [references/dialect-method-checklist.md](references/dialect-method-checklist.md). When in doubt, copy DuckDB and patch — its layout is the cleanest.

## Critical surface

These methods on the `Dialect` ABC (`src/pycel2sql/dialect/_base.py`) are where dialects diverge most. Plan how to implement them before writing any code:

- `write_regex_match` — operator vs function call vs `RLIKE`.
- `write_json_field_access` — operator (`->>`) vs function wrapper (`JSON_VALUE`, `get_json_object`); whether intermediate vs final access uses different forms (Postgres `->` vs `->>`; Spark uses the same function for both).
- `write_array_literal_open` / `write_array_literal_close` — `ARRAY[`, `[`, `array(`.
- `write_list_index` / `write_list_index_const` — 0-indexed vs 1-indexed; bare `[i]` vs `[OFFSET(i)]` vs `+ 1`.
- `write_param_placeholder` — `$N`, `?`, `@pN`. Positional `?` dialects ignore the index argument.
- `write_extract` for DOW — Sunday=1 (BigQuery, Spark) vs Sunday=0 (Postgres/DuckDB convention) — adjust by `(dayofweek(t) - 1)` etc.
- `write_cast_to_numeric` — postfix `::TYPE` vs arithmetic coercion `+ 0` vs `CAST(... AS NUMERIC)`.
- `write_json_array_elements` — must be a **set-returning expression** (used in `FROM <here> AS iter`); use the engine's `EXPLODE` / `UNNEST` / `json_each` / `from_json` form.
- `write_json_array_membership` / `write_nested_json_array_membership` — must produce a valid RHS for `lhs = ` (subquery form, like SQLite's `(SELECT value FROM json_each(...))`). If your engine cannot construct a boolean predicate without the candidate element, raise `UnsupportedDialectFeatureError` (mirrors `SparkDialect`).
- `write_format` — per-dialect format() dispatch added in PR #8. Pick `FORMAT(...)`, `printf(...)`, `format_string(...)`, or raise.

## Capabilities methods are not just informational

The four `supports_*()` methods on `Dialect` drive Converter routing. Set them honestly:

```python
def supports_native_arrays(self) -> bool: return True
def supports_jsonb(self) -> bool: return False  # Postgres-style JSONB only
```

## Optional: IndexAdvisor

Implement the `IndexAdvisor` Protocol (in `dialect/_base.py`) only if the engine has user-controllable indexes (BTREE, GIN, ART, CLUSTERING). Skip for storage-layer-driven engines like Spark (Delta Z-order, Iceberg sort) — `get_index_advisor()` returns `None` for non-`IndexAdvisor` dialects, which gives an empty recommendation list (the right semantic for "no SQL-level recommendations").

## Doc refresh

When the implementation is green, refresh:

- `README.md` — bump dialect count (currently "Six SQL dialects"), add badge after the existing six in the badge grid, add column to the comparison table near the placeholder list, add row to the introspect-supported list (only if you also add an introspect module under `src/pycel2sql/introspect/`).
- `CLAUDE.md` — bump the dialect count near line 7, add a bullet under "Dialect Differences", append `dialect/<name>.py` to the dialect-files list.

The full file-by-file checklist is in [references/test-files.md](references/test-files.md).

## Verification

```bash
# Lint
uv run ruff check src/ tests/

# Type check (lark generic-arg notes are pre-existing — see CLAUDE.md)
uv run mypy src/pycel2sql/

# Unit tests — must pass for the new dialect plus all six existing ones
uv run pytest tests/ --ignore=tests/integration -v

# Optional integration (if you add Docker fixtures in tests/integration/conftest.py)
uv pip install -e ".[integration]"
uv run pytest tests/integration/ -v -k <dialect>

# Skill lint
python .claude/skills/skill-authoring/scripts/lint_skill.py .claude/skills/add-sql-dialect/
```

The Dialect ABC is enforced at instantiation time — calling `<New>Dialect()` with any abstract method missing raises `TypeError: Can't instantiate abstract class`. CI's `tests/conftest.py` instantiates every dialect in `ALL_DIALECTS`, so a missing method is caught immediately.

## Scripts

- **Run** `python .claude/skills/add-sql-dialect/scripts/scaffold_dialect.py <template> <new-name> <NewClassPrefix>` — copies an existing dialect file, renames the class to `<NewClassPrefix>Dialect`, replaces every method body with a `raise NotImplementedError(...)` stub, and prints the list of files created plus the next manual steps. Does not register the dialect anywhere — that's left to the engineer to do consciously.

## References

- [references/dialect-method-checklist.md](references/dialect-method-checklist.md) — every method on the `Dialect` ABC grouped by category, with one-line "what to emit" guidance per method drawn from the six existing implementations.
- [references/test-files.md](references/test-files.md) — exhaustive file-by-file checklist for a new dialect (code, tests, docs).
