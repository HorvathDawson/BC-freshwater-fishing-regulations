#!/usr/bin/env python3
"""ONE COMMAND BEFORE A PUSH (T993 item 7, T791 #2): what GitHub's runs ask, asked here first, every failure listed.

    .venv/bin/python scripts/before_push.py            # every step; exit 1 with the list of each one that failed
    .venv/bin/python scripts/before_push.py --list     # the steps, nothing run
    .venv/bin/python scripts/before_push.py --only REGEX       # the steps whose name matches (says PARTIAL)
    .venv/bin/python scripts/before_push.py --no-tier  # leave out the pipeline's no-data tier (minutes): the
                                                       # pipeline's source scanners run in its place
    .venv/bin/python scripts/before_push.py --app-suite        # + app-ci's whole vitest suite (through heavy.sh)
    .venv/bin/python scripts/before_push.py --hook     # how to make it a pre-push hook: printed, never installed

Why: 0.8.4 and 0.8.5 stopped on failures GitHub had already found (a stale mini release, a worker test, a ticket
ledger, tree-scanning inventory pins: 79 red runs in 30 days, `handoff/PLAN/audit-2026-11-21/t993-inventory.md`), each
seen only after the push. The steps:

  1. EVERY named `run` step of .github/workflows/pipeline-ci.yml, read from the file at run time as fast-fail's
     `ci_step` does (a step added there joins by itself; `python` is the venv's) — but the tool install (`SKIP`, with
     its reason) and "Tests", which is
  2. the pipeline's no-data tier with that step's own marker expression, in chunks through heavy.sh
     (scripts/pytest-chunked.sh: one process for the suite grows to ~111 GB) — or, with --no-tier, the pipeline's
     source scanners (every test file that walks the sources, found by fast-fail's grep), CI's markers;
  3. `pnpm -C app run -s check:fast` (every `pnpm check` gate but vitest: tickets, inventories, typecheck, …);
  4. the app's TREE SCANNERS: every app/tools/*.test.ts that reads the tree off disk (the class that failed app-ci 53
     times: failures.test.ts, back-layers, one-water-screen …), found by the same grep each run;
  5. the feeds worker suite (app-ci's "Feeds worker tests"; not in the pnpm workspace, so `pnpm check` skips it).

Not here, said at the end: app-ci's whole vitest suite (--app-suite adds it) and its web / native bundle exports.

A pre-push hook is the OWNER's call (T791 #2): `--hook` prints the two lines that install one and installs nothing.
Output is unfiltered (never through rtk: it has reported false passes).
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pipeline-ci.yml"
#: pipeline-ci steps not run here, each with why (a step added to the workflow runs unless it is named here)
SKIP = {"Tile tools (tippecanoe, pmtiles)": "installs the tile tools on the runner (here: brew install tippecanoe pmtiles)"}
#: the step whose marker expression is the no-data tier (`scripts/test-env.sh` `no_data_markers` reads the same one)
TESTS = "Tests"
#: what makes an app tools test a tree scanner (fast-fail's `vitest_changed` grep for the ui-native ones)
SCANS = re.compile(r"readFileSync|readdirSync")
#: what makes a pipeline test a source scanner (fast-fail's `source_scanners` grep)
PY_SCANS = re.compile(r'rglob\("\*\.py"\)|glob\("\*\*/\*\.py"\)|_py_sources\(|walk\(.*pipeline')
HOOK = """# a pre-push hook is the owner's call (T791 #2); to install it in this clone (every worktree shares it):
printf '#!/bin/sh\\nexec .venv/bin/python scripts/before_push.py\\n' > "$(git rev-parse --git-common-dir)/hooks/pre-push"
chmod +x "$(git rev-parse --git-common-dir)/hooks/pre-push"
# skip it once: git push --no-verify; remove it: rm "$(git rev-parse --git-common-dir)/hooks/pre-push\""""


@dataclass(frozen=True)
class Step:
    name: str
    cmd: tuple[str, ...]
    script: str | None = None       # a workflow step's `run`, executed with `bash -e` (as fast-fail's ci_step)


def py() -> str:
    """The pipeline's interpreter (`scripts/test-env.sh`: PIPELINE_PY, else the repo's .venv)."""
    return os.environ.get("PIPELINE_PY") or str(ROOT / ".venv" / "bin" / "python")


def workflow_steps(path: Path = WORKFLOW) -> list[dict]:
    import yaml
    return [s for j in yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"].values() for s in j["steps"]]


def tier_markers(steps: list[dict]) -> str:
    got = [s for s in steps if s.get("name") == TESTS]
    if len(got) != 1:
        raise SystemExit(f"before-push: pipeline-ci.yml has {len(got)} step(s) named {TESTS!r}")
    m = re.search(r'-m "([^"]+)"', got[0]["run"])
    if not m:
        raise SystemExit(f"before-push: pipeline-ci.yml's {TESTS!r} step names no -m marker expression")
    return m.group(1)


def app_scanners(root: Path = ROOT) -> list[str]:
    """app/tools/*.test.ts files that read the tree off disk, relative to app/."""
    return sorted(f"tools/{p.name}" for p in (root / "app" / "tools").glob("*.test.ts")
                  if SCANS.search(p.read_text(encoding="utf-8")))


def py_scanners(root: Path = ROOT) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in (root / "pipeline" / "tests").glob("test_*.py")
                  if PY_SCANS.search(p.read_text(encoding="utf-8")))


def plan(tier: bool = True, app_suite: bool = False, steps: list[dict] | None = None) -> list[Step]:
    steps = workflow_steps() if steps is None else steps
    out: list[Step] = []
    for s in steps:
        name = s.get("name")
        if not name or "run" not in s or name in SKIP or name == TESTS:
            continue
        out.append(Step(f"CI: {name}", ("bash", "-e"), s["run"]))
    markers = tier_markers(steps)
    heavy = ("bash", "scripts/heavy.sh", "before-push", "30", "--")
    if tier:
        out.append(Step("CI: Tests (the no-data tier, in chunks)", (*heavy, "bash", "scripts/pytest-chunked.sh", "--",
                                                                    "-q", "-m", markers)))
    else:
        out.append(Step("the pipeline's source scanners (--no-tier)", (py(), "-m", "pytest", "-q", "-p",
                                                                       "no:cacheprovider", "-m", markers,
                                                                       *py_scanners())))
    out.append(Step("app gates (pnpm check:fast)", ("pnpm", "-C", "app", "run", "-s", "check:fast")))
    out.append(Step("app tree scanners (app/tools tests that read the tree)",
                    ("pnpm", "-C", "app", "exec", "vitest", "run", *app_scanners())))
    out.append(Step("feeds worker tests", ("pnpm", "-C", "app", "exec", "vitest", "run", "--root", "workers/feeds")))
    if app_suite:
        out.append(Step("app-ci: the whole app vitest suite", (*heavy, "pnpm", "-C", "app", "exec", "vitest", "run")))
    return out


def run_step(step: Step) -> int:
    env = {**os.environ, "PATH": f"{ROOT / '.venv' / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"}
    if step.script is None:
        return subprocess.run(list(step.cmd), cwd=ROOT, env=env).returncode
    with tempfile.NamedTemporaryFile("w", suffix=".sh", prefix="before-push-", delete=False) as f:
        f.write(step.script)
    try:
        return subprocess.run([*step.cmd, f.name], cwd=ROOT, env=env).returncode
    finally:
        os.unlink(f.name)


def main(argv=None, runner=run_step) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", default=None, metavar="REGEX")
    ap.add_argument("--no-tier", action="store_true")
    ap.add_argument("--app-suite", action="store_true")
    ap.add_argument("--hook", action="store_true")
    a = ap.parse_args(argv)
    if a.hook:
        print(HOOK)
        return 0
    steps = plan(tier=not a.no_tier, app_suite=a.app_suite)
    if a.only:
        steps = [s for s in steps if re.search(a.only, s.name)]
        if not steps:
            print(f"before-push: --only {a.only!r} matches no step (--list names them): nothing ran", file=sys.stderr)
            return 2
    if a.list:
        for s in steps:
            print(f"  {s.name}" + ("" if s.script is not None else f"\n      $ {' '.join(s.cmd)}"))
        for name, why in SKIP.items():
            print(f"  (not run) CI: {name}: {why}")
        return 0
    failed, t0 = [], time.time()
    for s in steps:
        print(f"── {s.name}", flush=True)
        t = time.time()
        rc = runner(s)
        print(f"   {'ok' if rc == 0 else f'FAILED (rc={rc})'} ({round(time.time() - t)} s)", flush=True)
        if rc:
            failed.append(s.name)
    secs = round(time.time() - t0)
    tail = ("\nnot run here: app-ci's web and native bundle exports"
            + ("" if a.app_suite else ", app-ci's whole vitest suite (--app-suite)")
            + ("; the pipeline no-data tier (--no-tier)" if a.no_tier else "") + "."
            + "\na pre-push hook is the owner's call: scripts/before_push.py --hook prints how")
    if failed:
        print(f"\nbefore-push: {len(failed)} of {len(steps)} step(s) FAILED ({secs} s) — each one's output is above:"
              + "".join(f"\n  - {x}" for x in failed) + tail, file=sys.stderr)
        return 1
    print(f"\nbefore-push: {'PARTIAL (--only): ' if a.only else ''}{len(steps)} step(s) passed ({secs} s)" + tail)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
