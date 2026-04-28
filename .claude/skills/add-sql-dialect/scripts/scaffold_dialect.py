#!/usr/bin/env python3
"""Scaffold a new pycel2sql dialect by copying an existing one and stubbing it.

Usage:
    python scaffold_dialect.py <template> <new-name> <NewClassPrefix>

Example:
    python scaffold_dialect.py duckdb cockroach Cockroach

What it does:
    1. Reads src/pycel2sql/dialect/<template>.py (the analogue dialect).
    2. Renames the class to <NewClassPrefix>Dialect.
    3. Replaces every method body with `raise NotImplementedError(...)`.
    4. Writes to src/pycel2sql/dialect/<new-name>.py (refuses to overwrite).
    5. Prints the next manual steps (DialectName, _REGISTRY, tests, docs).

What it does NOT do:
    - Register the dialect anywhere (you do that consciously).
    - Touch tests/ or README/CLAUDE.md.
    - Create scripts in scripts/ directories — only the dialect file.

This script is intended to be run from the pycel2sql repo root.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT_HINT = "src/pycel2sql/dialect"


def find_repo_root(start: Path) -> Path:
    """Walk up to find a directory that contains src/pycel2sql/dialect."""
    cur = start.resolve()
    while cur != cur.parent:
        if (cur / REPO_ROOT_HINT).is_dir():
            return cur
        cur = cur.parent
    raise SystemExit(
        f"could not locate {REPO_ROOT_HINT}/ above {start}; "
        "run this script from inside the pycel2sql repo"
    )


def stub_method_bodies(src: str, template_class: str, new_class: str) -> str:
    """Rewrite every method body to `raise NotImplementedError(...)`.

    Module-level helpers (the regex / validation / type-map blocks above the
    class) are kept verbatim — the engineer will adapt them by hand.
    """
    # Split into module-level prefix and the class block.
    class_re = re.compile(rf"^class {re.escape(template_class)}\(Dialect\):", re.M)
    m = class_re.search(src)
    if not m:
        raise SystemExit(
            f"template class `{template_class}` not found in source — "
            "did you pass the right template name?"
        )

    prefix = src[: m.start()]
    body = src[m.start() :]

    # Rename the class header.
    body = body.replace(
        f"class {template_class}(Dialect):",
        f"class {new_class}(Dialect):",
        1,
    )

    # Walk methods and replace their bodies. A method starts at a line of the
    # form `    def name(...)` and continues until the next 4-space-indented
    # `def ` or end-of-file.
    method_header_re = re.compile(r"^(    def [^(]+\([^)]*\)(?:\s*->\s*[^:]+)?:\s*$)", re.M)
    headers = list(method_header_re.finditer(body))

    out: list[str] = []
    last_end = 0
    for i, hm in enumerate(headers):
        out.append(body[last_end : hm.end()])
        # body of this method ends at the next method header or EOF
        next_start = headers[i + 1].start() if i + 1 < len(headers) else len(body)
        # Extract the method name for the NotImplementedError message.
        name_m = re.search(r"def\s+(\w+)\s*\(", hm.group(1))
        method_name = name_m.group(1) if name_m else "?"
        # Build the stubbed body. Preserve any leading docstring if it's already present.
        block = body[hm.end() : next_start]
        docstring_m = re.match(r'^\s*("""[^"]*?"""|\'\'\'[^\']*?\'\'\')\s*\n', block)
        stub_indent = "        "
        new_block_lines: list[str] = []
        new_block_lines.append("\n")
        if docstring_m:
            new_block_lines.append(stub_indent + docstring_m.group(1) + "\n")
        new_block_lines.append(
            stub_indent
            + f"raise NotImplementedError("
            f'"{new_class}.{method_name}() not implemented yet")\n'
        )
        new_block_lines.append("\n")
        out.append("".join(new_block_lines))
        last_end = next_start

    if not headers:
        # No methods found — class body is unusual; bail out so we don't over-write.
        raise SystemExit(
            f"no methods found in class `{template_class}`; "
            "scaffold script is confused — copy by hand instead"
        )

    return prefix + "".join(out)


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(__doc__)
        return 2

    template, new_name, new_prefix = argv[1], argv[2], argv[3]
    if not re.fullmatch(r"[a-z][a-z0-9_]*", new_name):
        print(f"new-name must be lowercase identifier, got {new_name!r}")
        return 2
    if not re.fullmatch(r"[A-Z][A-Za-z0-9]*", new_prefix):
        print(f"NewClassPrefix must be CamelCase starting with uppercase, got {new_prefix!r}")
        return 2

    here = Path(__file__).resolve().parent
    repo_root = find_repo_root(here)
    dialect_dir = repo_root / REPO_ROOT_HINT

    template_path = dialect_dir / f"{template}.py"
    new_path = dialect_dir / f"{new_name}.py"

    if not template_path.exists():
        print(f"template not found: {template_path}")
        print(f"available: {sorted(p.stem for p in dialect_dir.glob('*.py') if not p.stem.startswith('_'))}")
        return 1
    if new_path.exists():
        print(f"refusing to overwrite existing {new_path}")
        return 1

    template_class = f"{template[0].upper()}{template[1:]}Dialect"
    # special-cases that don't follow the naming convention
    template_class = {
        "postgres": "PostgresDialect",
        "duckdb": "DuckDBDialect",
        "bigquery": "BigQueryDialect",
        "mysql": "MySQLDialect",
        "sqlite": "SQLiteDialect",
        "spark": "SparkDialect",
    }.get(template, template_class)

    new_class = f"{new_prefix}Dialect"

    src = template_path.read_text(encoding="utf-8")
    out = stub_method_bodies(src, template_class, new_class)

    new_path.write_text(out, encoding="utf-8")

    print(f"created {new_path.relative_to(repo_root)}")
    print()
    print("Next manual steps (in order):")
    print(f"  1. Fill in SQL bodies in src/pycel2sql/dialect/{new_name}.py")
    print(f"     (every method currently raises NotImplementedError).")
    print(f"  2. Add `{new_name.upper()} = \"{new_name}\"` to DialectName in")
    print(f"     src/pycel2sql/dialect/_base.py.")
    print(f"  3. Register {new_class} in src/pycel2sql/dialect/__init__.py:")
    print(f"     - import + add to __all__")
    print(f"     - add DialectName.{new_name.upper()}: {new_class} to _REGISTRY")
    print(f"  4. Export {new_class} from src/pycel2sql/__init__.py.")
    print(f"  5. Update tests/conftest.py:")
    print(f"     - add {new_name}_dialect fixture")
    print(f"     - append {new_class}() to ALL_DIALECTS")
    print(f"  6. Update tests/test_dialect_parametrized.py ALL_DIALECTS.")
    print(f"  7. Create tests/test_{new_name}.py mirroring tests/test_duckdb.py.")
    print(f"  8. Update README.md (badge, dialect count, placeholder table) and")
    print(f"     CLAUDE.md (count, dialect-files list, Dialect Differences bullet).")
    print(f"  9. Run: uv run ruff check src/ tests/ && \\")
    print(f"            uv run pytest tests/ --ignore=tests/integration -v")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
