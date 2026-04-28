#!/usr/bin/env bash
# List candidate upstream commits + recent tags for porting from
# github.com/spandigital/cel2sql (Go, the canonical source) and
# github.com/spandigital/cel2sql4j (Java, the cross-reference port)
# into pycel2sql.
#
# Usage:
#   list_upstream_changes.sh [<since-sha>]
#
# If <since-sha> is omitted, the script auto-detects it from the most recent
# commit on pycel2sql whose subject mentions "Port" or "backport" — the
# script extracts an upstream SHA from the commit body if present, otherwise
# falls back to the last 30 commits.
#
# Override paths with:
#   UPSTREAM_REPO=/path/to/cel2sql
#   CEL2SQL4J_REPO=/path/to/cel2sql4j
#
# The script does NOT modify either repo; it only reads.
set -euo pipefail

UPSTREAM_REPO="${UPSTREAM_REPO:-/Users/richardwooding/Code/SPAN/cel2sql}"
CEL2SQL4J_REPO="${CEL2SQL4J_REPO:-/Users/richardwooding/Code/SPAN/cel2sql4j}"

if [ ! -d "$UPSTREAM_REPO/.git" ]; then
  echo "Error: upstream cel2sql repo not found at $UPSTREAM_REPO" >&2
  echo "Set UPSTREAM_REPO=<path> if it's checked out elsewhere." >&2
  echo "Or clone it: git clone https://github.com/spandigital/cel2sql $UPSTREAM_REPO" >&2
  exit 2
fi

# cel2sql4j is optional but recommended.
HAS_CEL2SQL4J=1
if [ ! -d "$CEL2SQL4J_REPO/.git" ]; then
  echo "Note: cel2sql4j repo not found at $CEL2SQL4J_REPO — cross-reference section will be skipped." >&2
  echo "      Set CEL2SQL4J_REPO=<path> or clone https://github.com/spandigital/cel2sql4j to enable." >&2
  HAS_CEL2SQL4J=0
fi

# Resolve since-sha: arg, then auto-detect, then fallback.
SINCE="${1:-}"
if [ -z "$SINCE" ]; then
  PYCEL2SQL_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [ -n "$PYCEL2SQL_ROOT" ]; then
    SINCE="$(git -C "$PYCEL2SQL_ROOT" log --grep='[Pp]ort\|[Bb]ackport' -1 --pretty=format:%B \
              | grep -oE '[0-9a-f]{7,40}' | head -1 || true)"
  fi
fi

echo "=== Upstream cel2sql Go: $UPSTREAM_REPO ==="
echo

echo "--- Recent tags (10 newest) ---"
git -C "$UPSTREAM_REPO" tag --sort=-creatordate | head -10
echo

if [ -n "$SINCE" ] && git -C "$UPSTREAM_REPO" cat-file -e "$SINCE" 2>/dev/null; then
  echo "--- Commits since $SINCE ---"
  git -C "$UPSTREAM_REPO" log --oneline "$SINCE..HEAD"
  RANGE_SINCE_DATE="$(git -C "$UPSTREAM_REPO" show -s --format=%ct "$SINCE" 2>/dev/null || echo "")"
else
  if [ -n "$SINCE" ]; then
    echo "Note: '$SINCE' not found in upstream; showing last 30 commits instead." >&2
  else
    echo "Note: no since-sha auto-detected from pycel2sql git log; showing last 30 commits." >&2
  fi
  echo
  echo "--- Last 30 upstream commits ---"
  git -C "$UPSTREAM_REPO" log --oneline -30
  RANGE_SINCE_DATE=""
fi
echo

if [ "$HAS_CEL2SQL4J" = "1" ]; then
  echo "=== cel2sql4j Java cross-reference: $CEL2SQL4J_REPO ==="
  echo
  echo "--- Recent commits (in the same time window) ---"
  if [ -n "$RANGE_SINCE_DATE" ]; then
    git -C "$CEL2SQL4J_REPO" log --oneline --since="@$RANGE_SINCE_DATE" -50
  else
    git -C "$CEL2SQL4J_REPO" log --oneline -30
  fi
  echo
fi

echo "=== Hints ==="
echo "- Read an upstream commit:    git -C $UPSTREAM_REPO show <sha>"
echo "- Search by keyword:          git -C $UPSTREAM_REPO log --grep=<keyword>"
echo "- Many upstream fixes are already done in pycel2sql — grep first."
echo "  See SKILL.md 'Pre-port verification' section."
echo
echo "- For non-trivial features, cross-reference cel2sql4j:"
echo "    git -C $CEL2SQL4J_REPO log --grep=<keyword>"
echo "  The Java diff is often a closer template than the Go diff."
