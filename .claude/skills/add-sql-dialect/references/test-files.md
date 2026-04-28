# Test Files Touchpoints

Exhaustive file-by-file checklist for landing a new dialect. The dialect is wired correctly only when every file in this list is updated.

## Contents

- Code
- Tests
- Docs
- Optional: integration tests
- Optional: introspect support

## Code

| File | Change |
|---|---|
| `src/pycel2sql/dialect/<name>.py` | New file. The class + module-level helpers (`_<NAME>_RESERVED`, regex validators, type maps). |
| `src/pycel2sql/dialect/_base.py` | Add `<NAME> = "<name>"` to the `DialectName` enum (alphabetical or trailing — both existing dialects show both patterns). |
| `src/pycel2sql/dialect/__init__.py` | (a) Add `from pycel2sql.dialect.<name> import <Name>Dialect`. (b) Add `"<Name>Dialect"` to `__all__`. (c) Add `DialectName.<NAME>: <Name>Dialect` to `_REGISTRY`. |
| `src/pycel2sql/__init__.py` | (a) Add `from pycel2sql.dialect.<name> import <Name>Dialect`. (b) Add `"<Name>Dialect"` to `__all__`. |

## Tests

| File | Change |
|---|---|
| `tests/conftest.py` | Add `@pytest.fixture` named `<name>_dialect` returning `<Name>Dialect()`. Append `<Name>Dialect()` to `ALL_DIALECTS`. |
| `tests/test_dialect_parametrized.py` | Append `pytest.param(<Name>Dialect(), id="<name>")` to the local `ALL_DIALECTS` list. The parametrized tests cover universal SQL only (null/bool/logic/arithmetic/ternary/comparisons/negation), so no skip marks should be needed. If they are, the dialect's universal-SQL handling has a gap — fix the dialect, not the test. |
| `tests/test_<name>.py` | New file. Mirror the `tests/test_duckdb.py` structure: `TestXxxLiterals`, `TestXxxParams`, `TestXxxArrays`, `TestXxxStringFunctions`, `TestXxxRegex`, `TestXxxTimestamps`, `TestXxxJSON`, `TestXxxValidation`, `TestXxxTypeCasting`, `TestXxxComprehensions`, `TestXxxStructs`. Aim for 30–50 cases. |

If you add a method to the `Dialect` ABC as part of this dialect, every existing dialect file plus the dialect-method-checklist reference (in the `add-cel-feature` skill) must also be updated.

## Docs

| File | Change |
|---|---|
| `README.md` | (a) Bump dialect-count phrasing in the intro (currently "Six SQL dialects"). (b) Add a badge after the existing six in the badge grid. (c) Add an entry in the "Dialects" code example showing `get_dialect("<name>")`. (d) Add a row to the placeholder-style table (`?` / `$N` / `@pN`). (e) If introspect support is added (see below), update the introspect-supported list. |
| `CLAUDE.md` | (a) Bump the "five → six" / "six → seven" count near line 7. (b) Append the new dialect file to the `dialect/{...}.py` list. (c) Add a one-line bullet under "Dialect Differences" describing the dialect's params/arrays/regex/JSON/format conventions. |

## Optional: integration tests

`tests/integration/` runs generated SQL against real databases via testcontainers. The test runner adapts conftest fixtures per dialect.

| File | Change |
|---|---|
| `tests/integration/conftest.py` | Add a Docker fixture for the dialect's container (port, image tag, health check). Look at the existing Postgres/MySQL fixtures for the pattern. |
| `tests/integration/test_*.py` | The shared integration suite parametrizes over connection fixtures. If the dialect's connector library has a different cursor / parameter-style API, you may need a thin adapter. |

Apache Spark integration tests are deliberately deferred (see PR #8) — heavy testcontainers dependency. Same applies for new dialects until there's a working containerized target.

## Optional: introspect support

If you want users to be able to auto-discover schemas from a live connection:

| File | Change |
|---|---|
| `src/pycel2sql/introspect/<name>.py` | New file implementing `introspect_<name>(connection, ...) → dict[str, Schema]`. Mirror `introspect/postgres.py` for relational engines, `introspect/sqlite.py` for engines that need column-list parsing from a `PRAGMA`-style command. |
| `src/pycel2sql/introspect/__init__.py` | Re-export the new function. |
| `tests/test_introspect.py` | Add a unit test mocking the connection and asserting the parsed Schema. |
| `tests/integration/test_introspect.py` | Add an integration test against the real container — only if the dialect has integration tests already. |
