# Java (cel2sql4j) → Python Cross-References

When the upstream Go diff doesn't translate cleanly to Python, cel2sql4j often took the same step pycel2sql will. This file lists places where cel2sql4j's Java solution was a closer template for pycel2sql than the Go original.

## Contents

- Per-dialect `format()` dispatch
- ConvertOptions shape
- Single-functional-interface lambdas vs Go closures
- Single ConversionException base + dual messaging
- Skipped concerns (Java + Python both diverged from Go)

## Per-dialect `format()` dispatch

**Upstream Go** (cel2sql `_visit_format` in `cel2sql.go`) emits SQL `FORMAT(...)` directly because Postgres is the default dialect and FORMAT is correct there. The Go dialect interface didn't grow a `WriteFormat` method until much later.

**cel2sql4j** (`Dialect.writeFormat` in `dialect/Dialect.java` + per-dialect implementations) added the abstract method when Spark landed — Postgres/BigQuery emit `FORMAT('...', ...)`, SQLite/DuckDB emit `printf('...', ...)`, Spark emits `format_string('...', ...)`, MySQL throws `ConversionException`.

**pycel2sql** (PR #8) followed the cel2sql4j shape exactly — `Dialect.write_format(w, fmt_string, write_args)` in `dialect/_base.py` plus per-dialect implementations. The `_converter.py:_visit_format` was refactored to call `self._dialect.write_format(...)` instead of writing `FORMAT(` directly.

When porting any feature where SQL diverges across dialects, cel2sql4j's Java implementation usually shows the per-dialect dispatch shape pycel2sql wants.

## ConvertOptions shape

**Upstream Go** uses functional options: `cel2sql.WithJSONVariables(...)`, `cel2sql.WithColumnAliases(...)`, etc.

**cel2sql4j** uses a builder-style `ConvertOptions` class with `withJsonVariables(String...)`, `withColumnAliases(Map<String, String>)`, `withParamStartIndex(int)` methods.

**pycel2sql** uses keyword arguments on `convert()` / `convert_parameterized()` / `analyze()` — `json_variables: set[str] | frozenset[str] | list[str] | None`, `column_aliases: dict[str, str] | None`, `param_start_index: int | None`. Internally normalised to `frozenset` / `dict` and threaded through `Converter.__init__` as `self._json_variables` etc.

The Java naming (`jsonVariables`, `columnAliases`, `paramStartIndex`) inspired the snake_case Python forms (`json_variables`, `column_aliases`, `param_start_index`) more directly than the Go method names (`WithJSONVariables` etc.).

## Single-functional-interface lambdas vs Go closures

**Upstream Go**: `func() error` (closure returning error).

**cel2sql4j**: `interface SqlWriter { void write() throws ConversionException; }` (single-method functional interface). Java lambdas implementing `SqlWriter` look syntactically identical to Go closures: `() -> visit(child)`.

**pycel2sql**: `WriteFunc = Callable[[], None]` (Python type alias). Errors propagate via raised exceptions, like Java but unlike Go's return-error convention.

Net: Java's solution is more similar to Python's than Go's is. When Go uses `func() error`, look at how cel2sql4j wrote the same call site — that's usually the right Python shape.

## Single `ConversionException` base + dual messaging

**Upstream Go** has 16 sentinel errors: `ErrUnsupportedExpression`, `ErrInvalidFieldName`, `ErrInvalidRegexPattern`, `ErrUnsupportedDialectFeature`, etc.

**cel2sql4j** uses one `ConversionException` class with `userMessage` + `internalDetails` fields. Sub-typing happens via the user-visible message; runtime callers do `instanceof ConversionException` and read `userMessage`.

**pycel2sql** uses ~16 typed subclasses on a single `ConversionError` base — `UnsupportedDialectFeatureError`, `InvalidFieldNameError`, `InvalidRegexPatternError`, etc. The base class has the dual-messaging fields (`user_message` + `internal_details`); subclasses serve mainly as pytest-rich `with pytest.raises(...)` markers.

This is a **hybrid** — pycel2sql gets cel2sql4j's dual-messaging discipline AND keeps Go's typed-error discrimination. Don't port new sentinel errors as a flat list; add a typed subclass on the base.

## Skipped concerns (Java + Python both diverged from Go)

Some upstream features were rejected by cel2sql4j and pycel2sql for the same reason. If you find these in an upstream commit, don't port; just document the skip in CLAUDE.md.

| Upstream | Why both ports skip |
|---|---|
| JDBC schema providers (Go's `pg/provider.go`, `mysql/provider.go`) | Both Java and Python users construct `Schema` directly; runtime introspection has its own subsystem (Java users do it via app metadata, pycel2sql has `src/pycel2sql/introspect/`). |
| 16 sentinel errors | Both ports use single base + structured detail. |
| Name-based numeric-cast heuristic | Java never had it (was only briefly in upstream Go, removed in cel2sql commit c68ab70f). pycel2sql also never had it. |
| Comprehension pattern-match tightening | Both ports use structurally different visitors that don't have the false-positive surface upstream Go did. |

## Workflow when consulting cel2sql4j

1. Identify the upstream Go commit you're porting.
2. `git -C /Users/richardwooding/Code/SPAN/cel2sql4j log --grep="<keyword>"` — see if cel2sql4j ported it.
3. If it did, read the cel2sql4j commit/PR alongside the Go commit. The Java diff is usually a closer template than the Go diff.
4. Map Java → Python (this is mostly mechanical: `Foo` → `foo`, `withFoo` → `foo` kwarg, `SqlWriter` → `WriteFunc`, `throws ConversionException` → `raise ConversionError-subclass`).
5. Cross-reference the cel2sql4j PR's commit message — the Java port often documented the *why* of a design choice that the Go original left implicit.
