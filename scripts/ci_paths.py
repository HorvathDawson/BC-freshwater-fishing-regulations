#!/usr/bin/env python3
"""THE BRANCHES AND CI'S AREAS, typed once (T995). Standard library only: GitHub's `changes` jobs run it on a bare
runner (no venv), and `scripts/before_push.py --changed`, `scripts/ci_status.py` and `pipeline/release.py` import it.

    python3 scripts/ci_paths.py github <area>      # CI's `changes` job: run=true|false into $GITHUB_OUTPUT
    python3 scripts/ci_paths.py verdict            # CI's `ok` job: NEEDS (toJSON(needs)) all passed or skipped
    python3 scripts/ci_paths.py areas PATH...      # which areas these paths touch (a check by hand)
    python3 scripts/ci_paths.py trunk              # `main`: the shell scripts read the name here
    python3 scripts/ci_paths.py repo               # owner/name on GitHub (deploy/staging/upload.sh)
    python3 scripts/ci_paths.py release-branch     # the branch a release ships from: release/<app.json's version>, or
                                                   # (T998) the data-only branch checked out (release/data-<date>)

THE TRUNK is `main` (owner 2026-11-21, T995): protected, PRs with required checks, merge commits only. A release is cut
from it as `release/<version>` at scope lock (`python -m pipeline release --cut <version>`).

THE AREAS are what each CI workflow watches. A push to `main` / `release/**` runs a workflow only when its area changed
(the `on: push: paths:` lists, which must equal these: `pipeline/tests/test_workflow_pins.py`). A pull request runs
every workflow (and, T1041, every merge queue group); its first job, `changes`, asks this file whether the PR touched
the area, and the real jobs run only then (`if:`), so a required check never waits on a workflow that did not run: each workflow's last job (`REQUIRED`)
always runs and passes when every job before it passed or was skipped. `before_push.py --changed` reads the same areas,
so the hook and CI agree on what a change has to pass.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
#: the app's version lives here (expo.version); the release branch and the `v<version>` tag are named from it
APP_JSON = ROOT / "app" / "apps" / "mobile" / "app.json"

#: the repository on GitHub (owner/name: the workflow starts, the licence links); TypeScript: core/src/repo.ts
REPO = "canifishthis/canifishthis"
#: the protected trunk; the remote every script fetches it from
TRUNK, REMOTE = "main", "origin"
#: a release branch is RELEASE_PREFIX + the app version (app/apps/mobile/app.json expo.version)
RELEASE_PREFIX = "release/"
#: (T998) a DATA-ONLY release's branch: release/data-<YYYY-MM-DD>, cut from main by `release --cut --data-only` (app.json
#: still names the shipped version, whose own release branch is finished)
DATA_BRANCH = re.compile(re.escape(RELEASE_PREFIX) + r"data-\d{4}-\d{2}-\d{2}")

#: the shared setup every workflow's `changes` job uses: a change to it runs every workflow
_SHARED = [".github/actions/changes/**", "scripts/ci_paths.py"]

#: area -> the paths it watches, in GitHub's glob syntax (`*` stops at `/`, `**` crosses it)
AREAS: dict[str, list[str]] = {
    "docs": ["handoff/**", "AGENTS.md", ".github/copilot-instructions.md",
             "app/tools/check-tickets.mjs", "app/tools/check-tickets.test.ts", "app/tools/start-here.test.ts",
             # (T1041) the guard reads the rows and owner items through the ledger tool
             "app/tools/ledger.mjs", "app/tools/ledger.test.ts",
             # (T1081) the size guard: it reads every markdown file's length and each package's README / manifest
             "app/tools/check-size.mjs", "app/tools/check-size.test.ts", "*.md", "**/*.md", "pipeline/*/__init__.py",
             "app/apps/*/package.json", "app/packages/*/package.json", "app/workers/*/package.json",
             ".github/workflows/docs-ci.yml", ".github/actions/app-setup/**", *_SHARED],
    "app": ["app/**", ".github/workflows/app-ci.yml", ".github/actions/app-setup/**",
            "pipeline/deliver/bundle/schema.sql", "pipeline/deliver/bundle/indexes.sql",
            # (T995 review) what app/tools tests read outside app/: derived by test_workflow_pins.py, which fails when
            # one reads a path this list misses
            ".github/workflows/feeds-worker-ci.yml", "data/LICENSE-ODbL.md",
            "deploy/staging/build-apk.sh", "deploy/staging/build-ios-sim.sh", "deploy/staging/build-ios-testflight.sh",
            "deploy/staging/feeds.sh", "deploy/lib/build-links.sh", "scripts/fast-fail.sh", "scripts/heavy.sh",
            "handoff/PLAN/runbooks/release.md",
            "handoff/PLAN/ios/APP-STORE-SUBMISSION.md",   # (T1099) app/tools/privacy-manifest.test.ts compares the manifest with its labels
            "pipeline/deliver/tiles/basemap.py", "pipeline/deliver/tiles/ladder.py", "pipeline/deliver/tiles/layers.py",
            "pipeline/deliver/tiles/tile-contract.json", "pipeline/gauges/consume/panel.py",
            "pipeline/gauges/feed/climatology.py", "pipeline/gauges/feed/publish.py", "scripts/test-full.sh",
            ".githooks/commit-msg",
            *_SHARED],
    "feeds-worker": ["app/workers/feeds/**", "app/packages/core/**", "app/tools/test-tmp.ts", "app/package.json",
                     "app/pnpm-lock.yaml",
                     "pipeline/regs/dfo_salmon/**", "pipeline/gauges/**", "pipeline/tests/fixtures/**",
                     # (T993 review) what the worker's tests import or read outside the worker and core: derived by
                     # pipeline/tests/test_workflow_pins.py, which fails when a test reaches a path this list misses
                     "app/conformance/**", "app/tools/gauge-feed-contract*", "app/tools/seo/**",
                     "app/packages/map/style/**",
                     "data/curated/dfo_salmon/**", "data/source/bc_hydrometric_stations.json", "scripts/update-hydat.sh",
                     "deploy/**",
                     "requirements.txt", "constraints.txt",
                     ".github/workflows/feeds-worker-ci.yml", ".github/actions/app-setup/**", *_SHARED],
    "pipeline": ["pipeline/**", "data/**", "config.yaml", "project_config.py",
                 "requirements.txt", "constraints.txt",
                 # (T535) the review app's backend tests and the path gate's own dirs live here; fast-fail runs both too
                 "curation-review/**", "scripts/**",
                 # (T995 review) what the pipeline's tests read outside those: every workflow and action
                 # (test_workflow_pins), the hooks, the deploy scripts (test_deploy_gates, the fail-safe tests), the
                 # app (generated copies, fixtures, goldens, the tree scanners), the README, the pytest setup, two
                 # handoff folders. Derived by test_workflow_pins.py, which fails when a test reads a path this misses.
                 ".github/**", ".githooks/**", "deploy/**", "app/**", "README.md", "pytest.ini", "conftest.py",
                 ".gitignore", "handoff/PLAN/runbooks/**",
                  "handoff/PLAN/app-integration/store/graphics/store/light/**",   # (T1127) test_store_push_pictures reads the committed set
                  *_SHARED],
}

#: (T998) area -> the jobs of its workflow that run on EVERY pull request, whatever it touched (no `changes` gate):
#: docs-ci's Conventional Commits check of the PR's commits and title (app/tools/check-commits.mjs)
EVERY_PR = {"docs": ["commits"]}
#: (T998) the pull_request event types docs-ci runs on: the defaults, and `edited` (a re-run reads the old title)
PR_TYPES = {"docs": ["opened", "synchronize", "reopened", "edited"]}

#: area -> its workflow file
WORKFLOWS = {"docs": "docs-ci.yml", "app": "app-ci.yml", "feeds-worker": "feeds-worker-ci.yml",
             "pipeline": "pipeline-ci.yml"}
#: area -> the name of its workflow's last job: THE REQUIRED CHECKS on `main` (branch protection names these)
REQUIRED = {"docs": "docs ok", "app": "app ok", "feeds-worker": "feeds worker ok", "pipeline": "pipeline ok"}
#: the job results an `ok` job passes on (a job skipped by its `if:` counts as passed)
PASSING = ("success", "skipped")


def release_branch(version: str) -> str:
    return RELEASE_PREFIX + version


def data_branch(day: str) -> str:
    """(T998) the data-only release branch of that day (YYYY-MM-DD)."""
    return f"{RELEASE_PREFIX}data-{day}"


def shipping_branch(current: str, version: str) -> str:
    """(T998) the branch a release ships from: the data-only branch when one is checked out, else release/<version>."""
    return current if DATA_BRANCH.fullmatch(current) else release_branch(version)


def current_branch() -> str:
    """The branch checked out ("HEAD" when detached); a git failure raises, naming it."""
    return _git("rev-parse", "--abbrev-ref", "HEAD").strip()


def app_version(app_json: Path = APP_JSON) -> str:
    """app.json's expo.version (x.y.z); a file without one raises, naming it."""
    try:
        v = json.loads(app_json.read_text(encoding="utf-8"))["expo"]["version"]
    except (OSError, ValueError, KeyError) as e:
        raise SystemExit(f"ci-paths: cannot read expo.version from {app_json}: {e}") from e
    if not re.fullmatch(r"\d+\.\d+\.\d+", str(v)):
        raise SystemExit(f"ci-paths: {app_json} expo.version {v!r} is not x.y.z")
    return str(v)


def _glob(pattern: str) -> re.Pattern[str]:
    out, i = [], 0
    while i < len(pattern):
        if pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def matches(path: str, patterns: list[str]) -> bool:
    """GitHub's path-filter match: `*` stops at `/`, `**` crosses it."""
    return any(_glob(p).match(path) for p in patterns)


def areas_of(paths) -> set[str]:
    """Every area one of these paths is in."""
    return {a for a, pats in AREAS.items() for p in paths if matches(p, pats)}


def _git(*args: str) -> str:
    got = subprocess.run(["git", *args], capture_output=True, text=True)
    if got.returncode:
        raise SystemExit(f"ci-paths: git {' '.join(args)} failed (rc={got.returncode}): {got.stderr.strip()[-300:]}")
    return got.stdout


def pr_paths(base: str, head: str) -> list[str]:
    """The paths a pull request changes: its head against the merge base with its base (`base...head`). `--no-renames`:
    a rename lists BOTH paths (git's default names only the new one: a file moved out of an area skipped its jobs)."""
    return [x for x in _git("diff", "--name-only", "--no-renames", f"{base}...{head}").splitlines() if x]


#: (T1041) the events whose jobs run only when the change touched the area: event -> what the change is called. A
#: MERGE QUEUE's group (`merge_group`: the trunk + the queued pull requests, base_sha = the trunk's tip) is filtered as
#: its pull requests were, so a required `ok` check reports on it as on the PR. The event exists only once a ruleset
#: turns a merge queue on (post-0.9.0, after the move to an organisation): until then nothing sends it.
FILTERED = {"pull_request": "pull request", "merge_group": "merge group"}


def github(area: str, env=os.environ, paths_of=pr_paths) -> tuple[bool, str]:
    """(does the area's workflow run its jobs, why). A push (or a manual run) was already chosen by the workflow's own
    path filter: everything runs. A pull request (or (T1041) a merge queue's group) runs its jobs only when it touched
    the area."""
    if area not in AREAS:
        raise SystemExit(f"ci-paths: no area {area!r} (one of {', '.join(AREAS)})")
    event = env.get("EVENT_NAME", "")
    if event not in FILTERED:
        return True, f"{event or 'no event'}: the workflow's own trigger chose this run, every job runs"
    what = FILTERED[event]
    base, head = env.get("BASE_SHA", ""), env.get("HEAD_SHA", "")
    if not base or not head:
        raise SystemExit(f"ci-paths: a {event} run needs BASE_SHA and HEAD_SHA (the {what}'s base and head commits)")
    paths = paths_of(base, head)
    hit = [p for p in paths if matches(p, AREAS[area])]
    if not hit:
        return False, f"the {what} changes {len(paths)} path(s), none in {area}: its jobs are skipped (a pass)"
    return True, f"{len(hit)} of the {what}'s {len(paths)} path(s) are in {area}: " + ", ".join(hit[:12]) \
        + (" …" if len(hit) > 12 else "")


def verdict(needs: dict) -> tuple[bool, list[str]]:
    """(the `ok` job passes, one line per job). Passes only when every needed job passed or was skipped; a failed or
    cancelled one (or none at all) fails it."""
    lines = [f"{job}: {(d or {}).get('result')}" for job, d in sorted(needs.items())]
    ok = bool(needs) and all((d or {}).get("result") in PASSING for d in needs.values())
    return ok, lines


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["github"] and len(argv) == 2:
        run, why = github(argv[1])
        print(f"ci-paths: {argv[1]}: run={'true' if run else 'false'} — {why}")
        out = os.environ.get("GITHUB_OUTPUT")
        if out:
            with open(out, "a", encoding="utf-8") as f:
                f.write(f"run={'true' if run else 'false'}\n")
        return 0
    if argv == ["verdict"]:
        raw = os.environ.get("NEEDS")
        if not raw:
            print("ci-paths: verdict needs NEEDS (the workflow's toJSON(needs))", file=sys.stderr)
            return 2
        ok, lines = verdict(json.loads(raw))
        for ln in lines:
            print(f"  {ln}")
        print("ci-paths: every job passed or was skipped" if ok else
              "ci-paths: FAILED: a job above failed or was cancelled (or none ran)")
        return 0 if ok else 1
    if argv == ["trunk"]:
        print(TRUNK)
        return 0
    if argv == ["repo"]:
        print(REPO)
        return 0
    if argv == ["release-branch"]:
        print(shipping_branch(current_branch(), app_version()))
        return 0
    if argv[:1] == ["areas"]:
        print(" ".join(sorted(areas_of(argv[1:]))) or "(none)")
        return 0
    print(__doc__.split("\n\n")[1], file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
