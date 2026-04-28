---
name: port-from-upstream
description: Ports features and bug fixes from upstream cel2sql Go (the canonical source) and cross-references cel2sql4j Java into pycel2sql. Use when syncing upstream changes — the workflow enumerates candidate commits since the last sync, verifies whether each change is already applied (many turn out to be done already), maps Go and Java idioms to Python equivalents, ports the test, and updates relevant docs.
---

# Port from Upstream

pycel2sql is a Python port of `github.com/spandigital/cel2sql` (Go). cel2sql4j is the parallel Java port. Upstream Go ships features and fixes regularly; this skill captures the workflow for syncing them across — and uses cel2sql4j as a cross-reference for "how a non-Go port handled this idiomatically."

The recent v3.7.0/v3.7.1 sync (PR #8: Spark dialect + observe-fork options + BigQuery COALESCE fix) followed this exact shape.

## Quick start

```bash
# 1. Enumerate candidate upstream commits since the last sync.
bash .claude/skills/port-from-upstream/scripts/list_upstream_changes.sh

# 2. For each candidate: grep pycel2sql first — many fixes are already done.
#    See "Pre-port verification" below.

# 3. For real porting work: read the upstream diff, map Go to Python
#    (and cross-reference cel2sql4j), write a test asserting the same
#    expected SQL, file the PR.
```

## pycel2sql tracks two upstreams

| Repo | Role | Local path |
|---|---|---|
| `cel2sql` (Go) | Canonical source. The reference behaviour. | `/Users/richardwooding/Code/SPAN/cel2sql` |
| `cel2sql4j` (Java) | Cross-reference. Helpful when Go's idiom doesn't translate cleanly to Python — Java often took the same step pycel2sql will. | `/Users/richardwooding/Code/SPAN/cel2sql4j` |

Both repos are siblings in the user's workspace. The script `list_upstream_changes.sh` reads from both. If either is missing, the script reports clearly which path is expected.

## Pre-port verification: always grep first

A surprising fraction of upstream "fixes" turn out to be already applied in pycel2sql — the original Python port pulled some patches preemptively, or the Python implementation never had the bug. **Don't write code before verifying.**

Examples from the v3.7.1 backport (PR #8):

| Upstream commit / fix | Already done in pycel2sql? |
|---|---|
| `getDayOfWeek` modulo correction | Yes — `_visit_timestamp_extract` had the right shape. |
| `EXTRACT(... AT TIME ZONE ...)` syntax | Yes — every dialect's `write_extract` already used the correct form. |
| `ARRAY_LENGTH` wrapped in `COALESCE` | 4 of 5 dialects already correct; only BigQuery needed the fix (and it was a *latent* pycel2sql bug, not an upstream regression — the wrap was added in PR #8). |
| Removal of name-based numeric-cast heuristic | N/A — pycel2sql never had this heuristic. |
| 16 sentinel-error refactor | N/A — pycel2sql uses typed `ConversionError` subclasses with dual-message pattern; intentional, mirrors cel2sql4j's `ConversionException`. |

Probes for the most common items:

```bash
# Is X wrapped in COALESCE?
grep -nA4 "write_array_length\|write_json_array_length" \
  src/pycel2sql/dialect/*.py

# Is "AT TIME ZONE" used (vs just "AT")?
grep -rn "AT TIME ZONE" src/pycel2sql/

# Does day-of-week emit the modulo / -1 adjustment?
grep -nA3 "DOW\|getDayOfWeek\|dayofweek" \
  src/pycel2sql/_converter.py src/pycel2sql/dialect/*.py
```

Note any "already done" finding in the PR description rather than silently skipping it — the next port can re-use that grep result.

## Out of scope (mirrors upstream rejections + cel2sql4j divergences)

These upstream concerns intentionally do **not** port to pycel2sql. If you encounter them, add a note to CLAUDE.md's "Important Conventions" section rather than implementing them:

- **JDBC / cgo schema providers** — pycel2sql has its own `src/pycel2sql/introspect/` module with a clean Python connection-object interface; runtime Go-style introspection isn't a portable contract.
- **16 sentinel error types** (`ErrUnsupportedExpression`, `ErrInvalidFieldName`, …) — pycel2sql uses typed exception subclasses on top of a single `ConversionError` base with dual `user_message` / `internal_details` (CWE-209 prevention). Idiomatic for Python; mirrors cel2sql4j's same decision.
- **Name-based numeric-cast heuristic** (auto-cast of `score` / `value` / `count` / etc. to numeric) — pycel2sql never had it. Confirm with grep before porting any "removal" commits.
- **Comprehension pattern-matching tightening** — pycel2sql's Lark visitor matches comprehensions structurally and doesn't have the same false-positive surface as the Go AST walker.

## Workflow per upstream commit

1. **Read** the upstream diff: `git -C /Users/richardwooding/Code/SPAN/cel2sql show <sha>`.
2. **Verify** whether the change is needed (Pre-port section).
3. **Cross-reference cel2sql4j** if the change is non-trivial: `git -C /Users/richardwooding/Code/SPAN/cel2sql4j log --grep=<keyword>`. If cel2sql4j already ported it, the Java diff is usually a closer template than the Go diff.
4. **Map idioms** from Go (and Java) to Python — see [references/go-to-python-idioms.md](references/go-to-python-idioms.md) and [references/java-to-python-cross-refs.md](references/java-to-python-cross-refs.md).
5. **Port the test** — copy the upstream test case, assert the same generated SQL. Use `tests/conftest.py::ALL_DIALECTS` to parametrize when applicable.
6. **Document** — if behaviour differs from upstream by design (e.g. typed-exception model vs sentinel errors), update `CLAUDE.md` "Important Conventions" so the next porter sees the difference.

## When to split into multiple PRs

Big upstream syncs often bundle multiple themes. The v3.7.0+v3.7.1 sync (which became cel2sql Go's PRs #117 and #113 and pycel2sql's PR #8) was a candidate for splitting:

- **Bug fixes + new options** (BigQuery COALESCE, observe-fork options, format() dispatch). ~500 LOC.
- **New dialect** (Spark). ~1000 LOC.

PR #8 chose to bundle these into a single PR — that's also a valid choice. The trade-off:

| Single PR | Multiple PRs |
|---|---|
| Faster to author. Easier to write a coherent PR description. Reviewer sees the full picture. | Smaller diffs, easier to review individually. Each can land independently. Better for partial reverts. |

The user's preference governs. Default to whatever they've done before (single PR for #8).

## Verification

```bash
uv run ruff check src/ tests/
uv run pytest tests/ --ignore=tests/integration -v
python .claude/skills/skill-authoring/scripts/lint_skill.py .claude/skills/port-from-upstream/
```

## Scripts

- **Run** `bash .claude/skills/port-from-upstream/scripts/list_upstream_changes.sh [<since-sha>]` — lists upstream cel2sql commits and tags, plus cel2sql4j commits in the same time window. Default `<since>` is auto-detected from the most recent "Port" commit on pycel2sql `main`. Falls clearly when either sibling repo isn't checked out at the expected path. Override with `UPSTREAM_REPO=/path` and `CEL2SQL4J_REPO=/path` env vars.

## References

- [references/go-to-python-idioms.md](references/go-to-python-idioms.md) — Go-to-Python mapping table (closures, errors, struct-and-interface, generics).
- [references/java-to-python-cross-refs.md](references/java-to-python-cross-refs.md) — places where cel2sql4j's Java solution was a closer template for pycel2sql than the Go original (e.g. per-dialect `writeFormat`, `ConvertOptions` shape).
