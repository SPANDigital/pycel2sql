#!/usr/bin/env bash
# Pre-flight checks for cutting a pycel2sql release.
#
# Usage:
#   release_preflight.sh [<version>]
#
# Validates the working tree, checks CI on main, lists open Dependabot PRs,
# previews the commits that will be in the release, and — if <version> is
# supplied — validates its format and prints the tag commands.
#
# Exits non-zero if any hard check fails (wrong branch, dirty tree, out of sync,
# CI red on main, tag collision). Soft warnings (open Dependabot PRs) print
# but don't fail.
set -euo pipefail

VERSION="${1:-}"

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
  echo "Error: not inside a git repository" >&2
  exit 2
fi
cd "$REPO_ROOT"

ERRORS=0
WARNINGS=0

err()  { echo "  ERROR: $*"; ERRORS=$((ERRORS+1)); }
warn() { echo "  WARN:  $*"; WARNINGS=$((WARNINGS+1)); }
ok()   { echo "  OK:    $*"; }

echo "=== Working tree ==="

# 1. On main?
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$BRANCH" != "main" ]; then
  err "current branch is '$BRANCH', expected 'main' — switch with: git checkout main"
else
  ok "on main"
fi

# 2. Working tree clean?
if [ -n "$(git status --porcelain)" ]; then
  err "working tree has uncommitted changes — commit or stash first"
  git status --short | sed 's/^/         /'
else
  ok "working tree clean"
fi

# 3. In sync with origin/main?
git fetch --quiet origin main || warn "could not fetch origin/main"
LOCAL_HEAD="$(git rev-parse HEAD)"
ORIGIN_HEAD="$(git rev-parse origin/main 2>/dev/null || echo unknown)"
if [ "$LOCAL_HEAD" != "$ORIGIN_HEAD" ]; then
  AHEAD=$(git rev-list --count "origin/main..HEAD" 2>/dev/null || echo "?")
  BEHIND=$(git rev-list --count "HEAD..origin/main" 2>/dev/null || echo "?")
  err "local main is ahead $AHEAD / behind $BEHIND of origin/main — pull or push"
else
  ok "in sync with origin/main ($LOCAL_HEAD)"
fi

# 4. Last tag.
LAST_TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"
echo
if [ -n "$LAST_TAG" ]; then
  echo "=== Commits since $LAST_TAG (release-notes preview) ==="
  COMMITS_SINCE="$(git log "$LAST_TAG..HEAD" --oneline)"
  if [ -z "$COMMITS_SINCE" ]; then
    warn "no commits since $LAST_TAG — releasing now would be an empty release"
  else
    echo "$COMMITS_SINCE" | sed 's/^/  /'
  fi
else
  echo "=== Commit log (no prior tags) ==="
  git log --oneline -20 | sed 's/^/  /'
fi

# 5. CI status for the current main HEAD.
echo
echo "=== CI on origin/main HEAD ==="
if command -v gh >/dev/null 2>&1; then
  HEAD_SHA="$(git rev-parse origin/main 2>/dev/null || echo "")"
  ALL_RUNS="$(gh run list --limit 30 --json conclusion,name,status,headSha 2>/dev/null || true)"
  if [ -z "$ALL_RUNS" ] || [ "$ALL_RUNS" = "[]" ]; then
    warn "could not query GitHub Actions (gh not authed?) — verify CI manually"
  else
    if ! echo "$ALL_RUNS" | HEAD_SHA="$HEAD_SHA" python3 -c "
import json, os, sys
runs = [r for r in json.load(sys.stdin) if r.get('headSha') == os.environ['HEAD_SHA']]
if not runs:
    print('  (no CI runs found yet for this HEAD)')
    sys.exit(0)
latest = {}
for r in runs:
    latest.setdefault(r.get('name','?'), r)
ci_failed = False
for name, r in latest.items():
    status = r.get('conclusion') or r.get('status') or '?'
    advisory = '' if name == 'CI' else '  (advisory)'
    print(f'  {status:12} {name}{advisory}')
    if name == 'CI' and r.get('conclusion') in ('failure', 'cancelled', 'timed_out'):
        ci_failed = True
sys.exit(1 if ci_failed else 0)
"; then
      err "CI workflow failing on origin/main HEAD — investigate before releasing"
    fi
  fi
else
  warn "gh CLI not installed — verify CI manually at https://github.com/SPANDigital/pycel2sql/actions"
fi

# 6. Open Dependabot PRs.
echo
echo "=== Open Dependabot PRs ==="
if command -v gh >/dev/null 2>&1; then
  DEP_PRS="$(gh pr list --author "app/dependabot" --state open --json number,title 2>/dev/null || true)"
  if [ -n "$DEP_PRS" ] && [ "$DEP_PRS" != "[]" ]; then
    echo "$DEP_PRS" | python3 -c "
import json, sys
for p in json.load(sys.stdin):
    print(f\"  #{p['number']:<4} {p['title']}\")"
    warn "open Dependabot PRs above — consider merging security ones before tagging"
  else
    ok "no open Dependabot PRs"
  fi
fi

# 7. Validate version (if supplied) and print the tag commands.
if [ -n "$VERSION" ]; then
  echo
  echo "=== Version check ==="
  STRIPPED="${VERSION#v}"
  if [[ ! "$STRIPPED" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    err "version '$VERSION' does not match vX.Y.Z or vX.Y.Z-qualifier"
  else
    if [[ "$VERSION" != v* ]]; then
      VERSION="v$VERSION"
    fi
    ok "version '$VERSION' format valid"
    if git rev-parse "$VERSION" >/dev/null 2>&1; then
      err "tag $VERSION already exists locally — pick a different version"
    fi
    if git ls-remote --tags origin "refs/tags/$VERSION" 2>/dev/null | grep -q "$VERSION"; then
      err "tag $VERSION already exists on origin — pick a different version"
    fi
    echo
    echo "=== Tag commands (run after preflight is clean) ==="
    echo "  git tag -a $VERSION -m \"Release $VERSION\""
    echo "  git push origin $VERSION"
    echo "  gh run list --workflow Release --limit 1"
    echo "  gh run watch"
  fi
fi

echo
echo "=== Summary: $ERRORS error(s), $WARNINGS warning(s) ==="
[ "$ERRORS" -eq 0 ]
