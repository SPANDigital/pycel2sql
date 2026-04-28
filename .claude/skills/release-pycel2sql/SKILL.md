---
name: release-pycel2sql
description: Cuts a new pycel2sql release by validating the working tree, picking the semver bump, tagging vX.Y.Z, and pushing the tag to trigger .github/workflows/release.yml which runs CI, builds an sdist + wheel via hatch-vcs, and publishes to PyPI via OIDC trusted publishing. Use when shipping a new patch, minor, or major version of pycel2sql to PyPI.
---

# Release pycel2sql

pycel2sql ships to PyPI whenever a `v*` tag lands on `main`. The release flow is shorter than upstream cel2sql Go's because there's no manual CHANGELOG to maintain — `release.yml` builds release notes from commit history via `softprops/action-gh-release@v2` (without `generate_release_notes: true` — it produces its own `release_notes.md` from `git log $PREV_TAG..$TAG`). It's also simpler than cel2sql4j's Maven Central flow: no GPG signing (PyPI uses OIDC), no Sonatype staging, no `gradle.properties` version drift (hatch-vcs reads the tag — there's no version file to forget).

## Quick start

```bash
git checkout main && git pull --ff-only origin main

# 1. Run preflight (working tree clean, on main, in sync, CI green, list candidate commits).
bash .claude/skills/release-pycel2sql/scripts/release_preflight.sh

# 2. Decide the version (see "Picking the version").
VERSION=v0.3.0

# 3. Re-run preflight with the version to validate format + check for collisions.
bash .claude/skills/release-pycel2sql/scripts/release_preflight.sh "$VERSION"

# 4. Tag annotated and push — the tag push is what triggers release.yml.
git tag -a "$VERSION" -m "Release $VERSION"
git push origin "$VERSION"

# 5. Watch the release workflow.
gh run list --workflow Release --limit 1
gh run watch
```

The workflow then:

1. Reuses the `ci.yml` workflow (`workflow_call`) — runs ruff, mypy, unit tests on Python 3.12 + 3.13, integration tests in containers.
2. Builds sdist + wheel via `uv build` (hatch-vcs reads the tag for the version).
3. Publishes to PyPI via OIDC trusted publishing — the `publish-pypi` job uses the `pypi` GitHub Environment.
4. Creates a GitHub release with notes from `git log $PREV_TAG..$TAG`.

The artifact lands at `https://pypi.org/project/pycel2sql/` once the workflow succeeds (sync usually under 2 minutes).

## Picking the version

pycel2sql follows plain semver. Quick rules:

- **Patch (`v0.2.2`)** — Dependabot bumps without behaviour change, doc-only fixes, lock-file-only changes, internal refactors with identical generated SQL.
- **Minor (`v0.3.0`)** — new public kwarg on `convert()` / `convert_parameterized()` / `analyze()`, new dialect, new CEL feature, new `Dialect` ABC `@abstractmethod`. The most common bump.
- **Major (`v1.0.0`)** — removal of an exported function/class/kwarg, change to default behaviour that breaks callers, dropping a Python version. **Reserve for genuinely user-disruptive breakage.**

When unsure between patch and minor: any new public API surface bumps minor.

### Pre-1.0 caveat

Today the project is in the `0.x` line. While in 0.x, breaking changes can land in a minor bump (`0.2` → `0.3`) per semver convention. Once `v1.0.0` ships, breaking changes require a major bump.

## Common slip-ups

- **Lightweight tag instead of annotated.** Use `git tag -a vX.Y.Z -m "Release vX.Y.Z"`, not `git tag vX.Y.Z`. The release workflow reads the tag's commit log for release notes.
- **Tagging from a feature branch.** The tag must point at a commit reachable from `main`. The preflight script checks this implicitly — it requires you to be on `main` and in sync with `origin/main`.
- **Forgetting open Dependabot security PRs.** The release otherwise ships known-vulnerable transitive deps. The preflight script flags any open Dependabot PRs as a soft warning — review them before tagging.
- **Pre-release qualifier shape.** Use `v0.3.0-rc1`, `v0.3.0-beta.1`, `v0.3.0-rc.2`. The validator (in `release.yml`'s tag-event regex implicitly via Python packaging conventions) accepts `^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$` after stripping the leading `v`. Underscores are rejected.
- **Forgetting that hatch-vcs uses the tag, not a version file.** There's no `pyproject.toml` version to bump and no `_version.py` to edit — `hatch-vcs` injects the version at build time from the git tag. **Don't try to edit a version anywhere before tagging.** `pyproject.toml` line 7 says `dynamic = ["version"]`; that's intentional.

## PyPI OIDC trusted publishing

The `publish-pypi` job uses the `pypi` GitHub Environment + Trusted Publishing. There is *no API token* anywhere — no `PYPI_API_TOKEN` secret, nothing to rotate. If a publish fails with HTTP 403, the cause is almost always:

- The GitHub Environment isn't authorised on PyPI's side, OR
- The repo / workflow / environment combination doesn't match the trusted-publisher record on PyPI.

That's an org-admin task at https://pypi.org/manage/project/pycel2sql/settings/publishing/ — not a release-skill task.

## After the tag pushes

Once the workflow goes green:

1. The GitHub release at `https://github.com/SPANDigital/pycel2sql/releases/tag/<tag>` is created with notes from `git log $PREV_TAG..$TAG`.
2. The artifact appears on PyPI within a couple of minutes — sometimes longer if PyPI's CDN is slow.
3. The PyPI badge in `README.md` updates within ~5 minutes.
4. If the auto-generated notes need polishing (e.g. group changes by category), use `gh release edit <tag> --notes-file <path>` — post-edit polish is optional.

## Verification checklist

After pushing the tag:

- [ ] Release workflow succeeded — `gh run list --workflow Release --limit 1`.
- [ ] GitHub release exists at the expected URL with auto-generated notes.
- [ ] `pip index versions pycel2sql` shows the new version.
- [ ] PyPI badge in README updates to the new version.
- [ ] Smoke install: `pip install pycel2sql==<version>` in a fresh venv.

If the workflow fails mid-publish, **don't simply re-tag with the same version**: PyPI rejects re-uploads of the same version filename. Bump the patch (`v0.3.0` → `v0.3.1`), fix the underlying cause, and tag the new version.

## Scripts

- **Run** `bash .claude/skills/release-pycel2sql/scripts/release_preflight.sh [<version>]` — validates the working tree (clean, on `main`, in sync with `origin/main`); checks the `CI` workflow is green on `origin/main` HEAD; lists open Dependabot PRs (soft warning); prints the commit log since the previous tag (release-notes preview); if `<version>` is supplied, validates the version-string format, checks the tag doesn't already exist locally or on origin, and prints the exact `git tag -a` / `git push` commands.

## References

The relevant configuration files are small enough to read directly when needed (no separate references):

- `.github/workflows/release.yml` — the workflow definition.
- `.github/workflows/ci.yml` — reused by `release.yml` via `workflow_call` (added `permissions: contents: read` in PR #9).
- `pyproject.toml` lines 1–32 — hatch-vcs configuration; `dynamic = ["version"]` line 7; `[tool.hatch.version] source = "vcs"` line 28; `[tool.hatch.build.hooks.vcs] version-file = "src/pycel2sql/_version.py"` line 31.
