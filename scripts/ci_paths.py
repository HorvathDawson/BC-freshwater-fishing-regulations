#!/usr/bin/env python3
"""THE BRANCHES AND CI'S AREAS, typed once (T995). Standard library only: GitHub's `changes` jobs run it on a bare
runner (no venv), and `scripts/before_push.py --changed`, `scripts/ci_status.py` and `pipeline/release.py` import it.

    python3 scripts/ci_paths.py github <area>      # CI's `changes` job: run=true|false into $GITHUB_OUTPUT
    python3 scripts/ci_paths.py verdict            # CI's `ok` job: NEEDS (toJSON(needs)) all passed or skipped
    python3 scripts/ci_paths.py areas PATH...      # which areas these paths touch (a check by hand)
    python3 scripts/ci_paths.py trunk              # `main`: the shell scripts read the name here
    python3 scripts/ci_paths.py release-branch     # release/<app.json's version>: the branch a release ships from

THE TRUNK is `main` (owner 2026-11-21, T995): protected, PRs with required checks, merge commits only. A release is cut
from it as `release/<version>` at scope lock (`python -m pipeline release --cut <version>`).

THE AREAS are what each CI workflow watches. A push to `main` / `release/**` runs a workflow only when its area changed
(the `on: push: paths:` lists, which must equal these: `pipeline/tests/test_workflow_pins.py`). A pull request runs
every workflow; its first job, `changes`, asks this file whether the PR touched the area, and the real jobs run only
then (`if:`), so a required check never waits on a workflow that did not run: each workflow's last job (`REQUIRED`)
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

#: the protected trunk; the remote every script fetches it from
TRUNK, REMOTE = "main", "origin"
#: a release branch is RELEASE_PREFIX + the app version (app/apps/mobile/app.json expo.version)
RELEASE_PREFIX = "release/"

#: the shared setup every workflow's `changes` job uses: a change to it runs every workflow
_SHARED = [".github/actions/changes/**", "scripts/ci_paths.py"]

#: area -> the paths it watches, in GitHub's glob syntax (`*` stops at `/`, `**` crosses it)
AREAS: dict[str, list[str]] = {
    "docs": ["handoff/**", "AGENTS.md", ".github/copilot-instructions.md",
             "app/tools/check-tickets.mjs", "app/tools/check-tickets.test.ts", "app/tools/start-here.test.ts",
             ".github/workflows/docs-ci.yml", ".github/actions/app-setup/**", *_SHARED],
    "app": ["app/**", ".github/workflows/app-ci.yml", ".github/actions/app-setup/**",
            "pipeline/deliver/bundle/schema.sql", "pipeline/deliver/bundle/indexes.sql", *_SHARED],
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
                 "requirements.txt", "constraints.txt", ".github/workflows/pipeline-ci.yml",
                 "app/packages/core/src/*.generated.*", "app/packages/data/dev/mini-release/**",
                 # (T535) the review app's backend tests and the path gate's own dirs live here; fast-fail runs both too
                 "curation-review/**", "scripts/**", *_SHARED],
}

#: area -> its workflow file
WORKFLOWS = {"docs": "docs-ci.yml", "app": "app-ci.yml", "feeds-worker": "feeds-worker-ci.yml",
             "pipeline": "pipeline-ci.yml"}
#: area -> the name of its workflow's last job: THE REQUIRED CHECKS on `main` (branch protection names these)
REQUIRED = {"docs": "docs ok", "app": "app ok", "feeds-worker": "feeds worker ok", "pipeline": "pipeline ok"}
#: the job results an `ok` job passes on (a job skipped by its `if:` counts as passed)
PASSING = ("success", "skipped")


def release_branch(version: str) -> str:
    return RELEASE_PREFIX + version


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


def github(area: str, env=os.environ, paths_of=pr_paths) -> tuple[bool, str]:
    """(does the area's workflow run its jobs, why). A push (or a manual run) was already chosen by the workflow's own
    path filter: everything runs. A pull request runs its jobs only when it touched the area."""
    if area not in AREAS:
        raise SystemExit(f"ci-paths: no area {area!r} (one of {', '.join(AREAS)})")
    event = env.get("EVENT_NAME", "")
    if event != "pull_request":
        return True, f"{event or 'no event'}: the workflow's own trigger chose this run, every job runs"
    base, head = env.get("BASE_SHA", ""), env.get("HEAD_SHA", "")
    if not base or not head:
        raise SystemExit("ci-paths: a pull_request run needs BASE_SHA and HEAD_SHA (the PR's base and head commits)")
    paths = paths_of(base, head)
    hit = [p for p in paths if matches(p, AREAS[area])]
    if not hit:
        return False, f"the pull request changes {len(paths)} path(s), none in {area}: its jobs are skipped (a pass)"
    return True, f"{len(hit)} of the pull request's {len(paths)} path(s) are in {area}: " + ", ".join(hit[:12]) \
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
    if argv == ["release-branch"]:
        print(release_branch(app_version()))
        return 0
    if argv[:1] == ["areas"]:
        print(" ".join(sorted(areas_of(argv[1:]))) or "(none)")
        return 0
    print(__doc__.split("\n\n")[1], file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
