#!/usr/bin/env python3
"""ONE COMMAND BEFORE A PUSH (T993 item 7, T791 #2): what GitHub's runs ask, asked here first, every failure listed.

    .venv/bin/python scripts/before_push.py            # every step; exit 1 with the list of each one that failed
    .venv/bin/python scripts/before_push.py --list     # the steps, nothing run
    .venv/bin/python scripts/before_push.py --only REGEX       # the steps whose name matches (says PARTIAL)
    .venv/bin/python scripts/before_push.py --no-tier  # leave out the pipeline's no-data tier (minutes): the
                                                       # pipeline's source scanners run in its place
    .venv/bin/python scripts/before_push.py --app-suite        # + app-ci's whole vitest suite (through heavy.sh)
    .venv/bin/python scripts/before_push.py --changed  # (T995) only the areas this branch changed (the pre-push hook)
    .venv/bin/python scripts/before_push.py --hook     # how to turn on the versioned hooks: printed, never installed

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
  5. the feeds worker suite (app-ci's "Feeds worker tests"; not in the pnpm workspace, so `pnpm check` skips it);
  6. docs-ci's vitest step: the ticket guard's own tests and the start-here page (T995 review).

Not here, said at the end: app-ci's whole vitest suite (--app-suite adds it) and its web / native bundle exports.

(T995) `--changed`: only the steps whose inputs changed since the branch left the trunk (`git merge-base HEAD
origin/main`) plus the working tree and untracked files, by the SAME areas CI's `changes` jobs read
(`scripts/ci_paths.py` AREAS, so the hook and CI agree): pipeline -> steps 1-2, app -> 3-4, feeds-worker -> 5,
docs -> 6. The
ticket guard (`node app/tools/check-tickets.mjs`) always runs — inside step 3 when the app changed, else on its own —
so a docs / handoff-only push takes seconds. `BEFORE_PUSH_SKIP=<reason>` skips every step and prints the reason (the
release command's own pushes: its preflight ran these checks); an empty reason is refused.

THE HOOKS ARE VERSIONED (T995, owner 2026-11-21): `.githooks/pre-push` runs `--changed`, `.githooks/commit-msg` is T998's.
They are on once a clone says `git config core.hooksPath .githooks` (`--hook` prints it; nothing is installed here).
`git push --no-verify` is the emergency skip, said in the commit or PR. Output is unfiltered (never through rtk: it has
reported false passes).
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
sys.path.insert(0, str(Path(__file__).resolve().parent))      # beside it, however it is loaded (the tests: by path)
import ci_paths as CP  # noqa: E402  (T995) the trunk and CI's areas, typed once
WORKFLOW = ROOT / ".github" / "workflows" / "pipeline-ci.yml"
#: pipeline-ci steps not run here, each with why (a step added to the workflow runs unless it is named here)
SKIP = {"Tile tools (tippecanoe, pmtiles)": "installs the tile tools on the runner (here: brew install tippecanoe pmtiles)"}
#: the step whose marker expression is the no-data tier (`scripts/test-env.sh` `no_data_markers` reads the same one)
TESTS = "Tests"
#: what makes an app tools test a tree scanner (fast-fail's `vitest_changed` grep for the ui-native ones)
SCANS = re.compile(r"readFileSync|readdirSync")
#: what makes a pipeline test a source scanner (fast-fail's `source_scanners` grep)
PY_SCANS = re.compile(r'rglob\("\*\.py"\)|glob\("\*\*/\*\.py"\)|_py_sources\(|walk\(.*pipeline')
HOOK = """# (T995) the hooks are versioned in .githooks/: pre-push runs `scripts/before_push.py --changed`, commit-msg is T998's.
# Turn them on once per clone (every worktree of it shares the setting):
git config core.hooksPath .githooks
# skip once, in an emergency only (say so in the commit or the PR): git push --no-verify
# turn them off: git config --unset core.hooksPath"""
#: (T995) the ticket guard on its own: what a push that changes no app file still runs (`pnpm check:fast` holds it)
TICKETS = ("node", "app/tools/check-tickets.mjs")
#: (T995 review) docs-ci.yml's vitest step, word for word (test_before_push.py pins it to the workflow)
DOCS_TESTS = ("tools/check-tickets.test.ts", "tools/start-here.test.ts")
#: (T995) skips every step, its reason printed: the release command's own pushes (its preflight ran these checks)
SKIP_ENV = "BEFORE_PUSH_SKIP"


@dataclass(frozen=True)
class Step:
    name: str
    cmd: tuple[str, ...]
    script: str | None = None       # a workflow step's `run`, executed with `bash -e` (as fast-fail's ci_step)


def py() -> str:
    """The pipeline's interpreter (`scripts/test-env.sh`: PIPELINE_PY, else the repo's .venv)."""
    return os.environ.get("PIPELINE_PY") or str(ROOT / ".venv" / "bin" / "python")


#: (T995) the jobs every CI workflow has that check nothing: `changes` (which jobs a PR needs) and `ok` (the required check)
PLUMBING = ("changes", "ok")


def workflow_steps(path: Path = WORKFLOW) -> list[dict]:
    import yaml
    jobs = yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"]
    return [s for name, j in jobs.items() if name not in PLUMBING for s in j["steps"]]


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


def _git(*args: str, trunk: str = CP.TRUNK) -> str:
    got = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    if got.returncode:
        raise SystemExit(f"before-push --changed: git {' '.join(args)} failed (rc={got.returncode}): "
                         f"{got.stderr.strip()[-300:]} — git fetch {CP.REMOTE} {trunk}")
    return got.stdout


ZERO = "0" * 40


def pushed_paths(lines: list[str], trunk: str = CP.TRUNK) -> tuple[list[str], list[str]]:
    """(T995 review) THE PRE-PUSH HOOK'S CHANGE: git hands the hook one line per ref, `<local ref> <local sha> <remote
    ref> <remote sha>`. A delete (local sha zero) and a tag push check nothing; a branch checks the commits it sends —
    `<remote sha>..<local sha>`, or, for a new branch (or a remote sha this clone does not hold), everything since its
    merge base with origin/main. (what was pushed, one line per ref; every path those commits change)"""
    said, paths = [], set()
    for ln in lines:
        parts = ln.split()
        if len(parts) != 4:
            continue
        lref, lsha, rref, rsha = parts
        if lsha == ZERO:
            said.append(f"{rref}: a delete, nothing to check")
            continue
        if lref.startswith("refs/tags/") or rref.startswith("refs/tags/"):
            said.append(f"{rref}: a tag, nothing to check")
            continue
        known = rsha != ZERO and subprocess.run(["git", "cat-file", "-e", f"{rsha}^{{commit}}"], cwd=ROOT,
                                                capture_output=True).returncode == 0
        base = rsha if known else _git("merge-base", lsha, f"{CP.REMOTE}/{trunk}", trunk=trunk).strip()
        got = [x for x in _git("diff", "--name-only", "--no-renames", base, lsha, trunk=trunk).splitlines() if x]
        said.append(f"{rref}: {len(got)} path(s) in {base[:12]}..{lsha[:12]}"
                    + ("" if known else f" (since the merge base with {CP.REMOTE}/{trunk})"))
        paths |= set(got)
    return said, sorted(paths)


def changed_paths(trunk: str = CP.TRUNK) -> tuple[str, list[str]]:
    """(T995) (the base, every path changed since the branch left the trunk): `git diff --name-only <merge-base>` (the
    commits and the working tree) plus untracked files. A git failure stops, naming the fix — never an empty change."""
    def git(*args: str) -> str:
        return _git(*args, trunk=trunk)
    base = git("merge-base", "HEAD", f"{CP.REMOTE}/{trunk}").strip()
    # `--no-renames`: a rename lists both paths, as CI's `changes` job reads them (ci_paths.pr_paths)
    paths = set(git("diff", "--name-only", "--no-renames", base).splitlines())
    paths |= set(git("ls-files", "--others", "--exclude-standard").splitlines())
    return base, sorted(x for x in paths if x)


def plan(tier: bool = True, app_suite: bool = False, steps: list[dict] | None = None,
         areas: set[str] | None = None) -> list[Step]:
    """Every step, or (T995, `areas` given) only those of the areas changed — the ticket guard always."""
    if areas is not None:
        out = [] if "app" in areas else [Step("ticket guard (node app/tools/check-tickets.mjs)", TICKETS)]
        full = plan(tier=tier, app_suite=app_suite, steps=steps)
        return out + [s for s in full if step_area(s) in areas]
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
    # (T995 review) docs-ci's second step: the guard's own tests and the start-here page (a handoff-only push runs it)
    out.append(Step("docs: the ticket guard's tests and the start-here page (docs-ci)",
                    ("pnpm", "-C", "app", "exec", "vitest", "run", *DOCS_TESTS)))
    if app_suite:
        out.append(Step("app-ci: the whole app vitest suite", (*heavy, "pnpm", "-C", "app", "exec", "vitest", "run")))
    return out


def step_area(step: Step) -> str:
    """(T995) the CI area a step stands for (scripts/ci_paths.py AREAS): what `--changed` keeps it by."""
    if step.name.startswith(("CI: ", "the pipeline's source scanners")):
        return "pipeline"
    if step.name.startswith("docs: "):
        return "docs"
    if step.name.startswith("feeds worker"):
        return "feeds-worker"
    if step.name.startswith("app"):
        return "app"
    raise ValueError(f"before-push: no area for step {step.name!r}")


def _git_local_vars() -> set[str]:
    """git's own list of the variables that tie a process to ONE repository (`git rev-parse --local-env-vars`)."""
    out = subprocess.run(["git", "rev-parse", "--local-env-vars"], capture_output=True, text=True, check=True).stdout
    return set(out.split()) | {"GIT_QUARANTINE_PATH"}


def step_env(environ: dict[str, str] | None = None) -> dict[str, str]:
    """A step's environment: the caller's, minus git's repository variables, plus the pipeline's interpreter on PATH.

    (T995, 2026-11-22) A hook runs with GIT_DIR (and friends) set. A test that ran `git init` in its own temp folder
    inherited it and re-initialised THE REAL REPOSITORY instead (core.bare = true: every worktree stopped working). The
    pre-commit framework strips the same set; so does .githooks/pre-push before it starts this."""
    environ = dict(os.environ if environ is None else environ)
    for k in _git_local_vars():
        environ.pop(k, None)
    environ["PATH"] = f"{Path(py()).parent}{os.pathsep}{environ.get('PATH', '')}"
    return environ


def run_step(step: Step) -> int:
    # the pipeline's interpreter first on PATH (a workflow step's `python`): this checkout's .venv or PIPELINE_PY's
    env = step_env()
    if step.script is None:
        return subprocess.run(list(step.cmd), cwd=ROOT, env=env).returncode
    with tempfile.NamedTemporaryFile("w", suffix=".sh", prefix="before-push-", delete=False) as f:
        f.write(step.script)
    try:
        return subprocess.run([*step.cmd, f.name], cwd=ROOT, env=env).returncode
    finally:
        os.unlink(f.name)


def main(argv=None, runner=run_step, stdin=sys.stdin) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", default=None, metavar="REGEX")
    ap.add_argument("--no-tier", action="store_true")
    ap.add_argument("--app-suite", action="store_true")
    ap.add_argument("--hook", action="store_true")
    ap.add_argument("--changed", action="store_true")
    ap.add_argument("--pre-push", action="store_true",
                    help="(T995 review) the hook: --changed over the refs git sends on stdin, not the working tree")
    a = ap.parse_args(argv)
    if a.hook:
        print(HOOK)
        return 0
    skip = os.environ.get(SKIP_ENV)
    if skip is not None and not a.list:
        if not skip.strip():
            print(f"before-push: {SKIP_ENV} needs a reason, which is printed", file=sys.stderr)
            return 2
        print(f"before-push: SKIPPED (nothing run): {skip}")
        return 0
    areas = None
    if a.pre_push:
        said, paths = pushed_paths(stdin.read().splitlines())
        for ln in said:
            print(f"before-push --pre-push: {ln}")
        if not paths and all(("a delete" in x or "a tag" in x) for x in said):
            print("before-push --pre-push: only tags / deletes pushed: nothing to check")
            return 0
        a.changed = True
        areas = CP.areas_of(paths)
        print(f"before-push --pre-push: {len(paths)} path(s) pushed; areas: "
              f"{', '.join(sorted(areas)) or 'none (the ticket guard only)'} (the checks run on this checkout)")
    elif a.changed:
        base, paths = changed_paths()
        areas = CP.areas_of(paths)
        print(f"before-push --changed: {len(paths)} path(s) since {base[:12]} (the merge base with "
              f"{CP.REMOTE}/{CP.TRUNK}); areas: {', '.join(sorted(areas)) or 'none (the ticket guard only)'}")
    steps = plan(tier=not a.no_tier, app_suite=a.app_suite, areas=areas)
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
            + ("; the pipeline no-data tier (--no-tier)" if a.no_tier else "")
            + ("; the areas this branch did not change (--changed)" if a.changed else "") + "."
            + "\nthe versioned hooks (.githooks, T995): scripts/before_push.py --hook prints how to turn them on")
    if failed:
        print(f"\nbefore-push: {len(failed)} of {len(steps)} step(s) FAILED ({secs} s) — each one's output is above:"
              + "".join(f"\n  - {x}" for x in failed) + tail, file=sys.stderr)
        return 1
    print(f"\nbefore-push: {'PARTIAL (--only): ' if a.only else ''}{'CHANGED AREAS (--changed): ' if a.changed else ''}"
          f"{len(steps)} step(s) passed ({secs} s)" + tail)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
