---
name: add-cel-feature
description: Adds a new CEL function, operator, macro, or comprehension form to pycel2sql by extending Converter visit methods in src/pycel2sql/_converter.py, deciding whether a new Dialect ABC abstract method is needed, implementing it across all six dialects when SQL diverges, and adding parametrized test cases. Use when wiring a new CEL string method (toLowerCase, isAlpha), timestamp method (toISOString), free-function helper, operator overload, or comprehension form across PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, and Apache Spark.
---

# Add CEL Feature

Adding a new CEL surface area (function, operator, macro, comprehension) is the second-most-common contribution shape after adding a dialect. The work is the same shape every time — this skill captures it.

Recent examples in pycel2sql's history: per-dialect `format()` dispatch (PR #8), `validate_schema` parameter (PR #1), `json_variables` / `column_aliases` / `param_start_index` options (PR #8). Each followed the pattern below.

## Quick start

1. **Classify the feature** — function call (e.g. `string.lowerAscii()`), operator (e.g. `&&`), macro (e.g. `has(x.y)`), or comprehension (e.g. `list.exists(v, pred)`).
2. **Identify the visitor** in `_converter.py` that owns the surface area — see [references/converter-file-map.md](references/converter-file-map.md).
3. **Decide whether a new `Dialect` ABC method is needed** — see "New method or inline?" below.
4. **If a new method is needed**, walk the dialect-method checklist at `.claude/skills/add-sql-dialect/references/dialect-method-checklist.md` (shared between both skills — open it directly).
5. **Add a test case** to the appropriate `tests/test_*.py` with parametrized coverage across `ALL_DIALECTS` from `tests/conftest.py`.
6. **Run** `uv run pytest tests/ --ignore=tests/integration -v` and `uv run ruff check src/ tests/`.

## New method or inline?

The `Dialect` ABC at `src/pycel2sql/dialect/_base.py` is already large (~50 abstract methods). Add a new method only when needed; otherwise inline the SQL in the visitor.

| Situation | Verdict |
|---|---|
| The SQL is identical across all six dialects | Inline in the visitor (e.g. `LOWER(x)` for `lowerAscii`, `MOD(a, b)` for `%`). |
| Any dialect needs different syntax | New `Dialect` ABC `@abstractmethod`. |
| A subset of dialects can't support the feature at all | New method + each unsupported dialect raises `UnsupportedDialectFeatureError` in its implementation. |
| The feature is JSON-related, regex-related, or array-related | Look for an existing `write_…` method first — JSON / array / regex method coverage is broad. |

If you add an `@abstractmethod`, the **`Dialect` ABC enforces it at instantiation time** — calling `<Dialect>()` with any abstract method missing raises `TypeError: Can't instantiate abstract class`. CI's `tests/conftest.py` instantiates every dialect in `ALL_DIALECTS`, so a missing method is caught on the first test collection. mypy strict mode also flags missing methods.

## Where features live

`_converter.py` is large (~80 KB / ~2000 lines) and uses Lark `Interpreter` visitor methods. The grammar rule names map to method names — `member_dot_arg` for method calls (`obj.method(args)`), `ident_arg` for free-function calls (`func(args)`), `addition_add` for `+`, `relation_eq` for `==`, etc.

For a method-call feature like `string.lowerAscii()`:
- Entry point is `member_dot_arg` (`_converter.py:607`).
- The method name dispatches via the long if/elif chain at lines 619–691.
- `LOWER(...)` is universal SQL → inline (no new `Dialect` method).

For a method-call feature like `string.format([args])`:
- Entry point is also `member_dot_arg`.
- SQL diverges — Postgres/BigQuery use `FORMAT`, SQLite/DuckDB use `printf`, Spark uses `format_string`, MySQL has no equivalent → new `Dialect.write_format()` method (added in PR #8). Each dialect implements its own form; MySQL raises.

For a free-function feature like `has(x.y)`:
- Entry point is `ident_arg` (`_converter.py:757`).
- Dispatch is via the `func_name` if/elif at lines 763–820.
- `has()` is dialect-divergent (Postgres JSONB `?` operator vs others' `IS NOT NULL`) → uses `Dialect.write_json_existence`.

For an operator like `+`:
- Entry point is `addition` (`_converter.py:415`).
- Operator-name dispatch via `op_name` (`addition_add` / `addition_sub`).
- Cross-dialect handling routes through `Dialect.write_string_concat` for string contexts, plain `+` for numeric.

For a macro like `exists` / `all` / `filter` / `map`:
- Entry point is `member_dot_arg`, but the chain immediately routes to `_visit_comprehension` (`_converter.py:615`).
- Comprehensions use the `Dialect` `write_unnest` / `write_array_subquery_open` / `write_array_subquery_expr_close` triple plus per-method scaffolding.

For the file-by-file map of which CEL surface area each section of `_converter.py` owns, see [references/converter-file-map.md](references/converter-file-map.md).

## Parser-level vs visitor-level

CEL features that pycel2sql can support are limited by what `cel-python`'s Lark grammar parses. If the grammar doesn't recognise a syntax (e.g. some CEL-extension functions), the feature can't be added at the visitor level — `_parser.parse(cel_expr)` will fail before reaching the converter. Test the parse first:

```python
from celpy.celparser import CELParser
CELParser().parse('"hello".isAlpha()')  # raises CELParseError if unsupported
```

If parsing fails, the feature requires upstream work in `cel-python`, not in pycel2sql.

## Test coverage

Add a representative test to the appropriate file in `tests/`:

| CEL surface | Test file |
|---|---|
| String methods | `tests/test_string_functions.py` |
| Timestamps / durations | `tests/test_timestamps.py` |
| JSON access / `has()` | `tests/test_json.py` |
| Comprehensions (`exists`, `all`, `filter`, `map`) | `tests/test_comprehensions.py` |
| Type casts (`int`, `string`, `bool`, etc.) | `tests/test_convert.py` (general) or a dialect-specific file |
| Universal SQL (operators, comparisons) | `tests/test_dialect_parametrized.py` |
| Parameterized output | `tests/test_parameterized.py` |
| New conversion options | `tests/test_options.py` |

When the SQL is identical across dialects, parametrize over `ALL_DIALECTS` from `tests/conftest.py`:

```python
import pytest
from tests.conftest import ALL_DIALECTS

@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_lower_ascii(dialect):
    assert convert("name.lowerAscii()", dialect=dialect) == "LOWER(name)"
```

When the SQL diverges, write per-dialect cases (mirror `tests/test_string_functions.py::TestFormatPerDialect`).

## Verification

```bash
uv run ruff check src/ tests/
uv run mypy src/pycel2sql/  # pre-existing lark errors expected
uv run pytest tests/ --ignore=tests/integration -v

python .claude/skills/skill-authoring/scripts/lint_skill.py .claude/skills/add-cel-feature/
```

## References

- [references/converter-file-map.md](references/converter-file-map.md) — which section of `_converter.py` owns which CEL surface area, with line-number anchors.
- The `Dialect` ABC method checklist lives in the `add-sql-dialect` skill at `.claude/skills/add-sql-dialect/references/dialect-method-checklist.md` — when you add a new `@abstractmethod`, open and update that file too.
