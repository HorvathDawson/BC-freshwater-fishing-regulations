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
ledger, tree-scanning inventory pins: 79 red runs in 30 days, the T993 inventory, cut in T1081), each
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
  6. docs-ci's vitest step: the ticket guard's own tests and the start-here page (T995 review);
  0. (T998) FIRST, ALWAYS: every commit about to be pushed follows Conventional Commits (`node
     app/tools/check-commits.mjs --range …`, the commit-msg hook's check and docs-ci's `commits` job): the commits on
     HEAD that origin/main does not have, or, as the pre-push hook, the commits of each pushed ref.

Not here, said at the end: app-ci's whole vitest suite (--app-suite adds it) and its web / native bundle exports.

(T995) `--changed`: only the steps whose inputs changed since the branch left the trunk (`git merge-base HEAD
origin/main`) plus the working tree and untracked files, by the SAME areas CI's `changes` jobs read
(`scripts/ci_paths.py` AREAS, so the hook and CI agree): pipeline -> steps 1-2, app -> 3-4, feeds-worker -> 5,
docs -> 6. The
ticket guard (`node app/tools/check-tickets.mjs`) always runs — inside step 3 when the app changed, else on its own —
so a docs / handoff-only push takes seconds. `BEFORE_PUSH_SKIP=<reason>` skips every step and prints the reason (the
release command's own pushes: its preflight ran these checks); an empty reason is refused.

(T1041) `--changed` ALSO RUNS THE TESTS THE CHANGE REACHES (`scripts/related_tests.py`), because both red CI runs of
2026-11-23 were checks this hook skipped (#63: a core change broke ui-native tests; #67: a test importing a changed
pipeline module failed in the no-data tier):
  - app: step 4 becomes `vitest related --run <the changed app code files> <the tree scanners>` — vitest's import
    graph, so a core change runs every package's tests that import it (most of the suite: ~2 min here);
  - pipeline: the test files that import a changed module directly (+ the changed tests), then one level up (the
    tests of a module that imports a changed one, at most related_tests.PY_UP_CAP), as CI runs them (its marker
    expression, in chunks), capped at related_tests.PY_CAP in all with the rest named.
And the steps run in TWO LANES AT ONCE (`--serial`: one after another): the pipeline's (Python) and the rest (node),
each step's output printed whole when it ends, so the hook takes the longer lane, not the sum. A full run (no
--changed) runs no related step: its whole set is the suites. `scripts/before_pr.sh` (T1041) is the once-before-the-PR
half: the whole app suite and the no-data tier through heavy.sh.

(T1144) `--pre-push` (the hook) QUEUES: its steps run under one machine-wide lock (`~/.claude/before-push.lock`,
fcntl.flock: a dead holder frees it), a second push waits saying who holds it, and fails after BEFORE_PUSH_LOCK_WAIT
minutes (20); `BEFORE_PUSH_NO_LOCK="<reason>"` runs beside it, the reason printed. Its vitest runs take the free cores
(`vitest_workers`: cores minus the load, 2 to the config's own), through vitest's VITEST_MAX_FORKS / _THREADS.

THE HOOKS ARE VERSIONED (T995, owner 2026-11-21): `.githooks/pre-push` runs `--changed`, `.githooks/commit-msg` checks the
message (T998: Conventional Commits, `app/tools/check-commits.mjs --edit`).
They are on once a clone says `git config core.hooksPath .githooks` (`--hook` prints it; nothing is installed here).
`git push --no-verify` is the emergency skip, said in the commit or PR. Output is unfiltered (never through rtk: it has
reported false passes).
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))      # beside it, however it is loaded (the tests: by path)
import ci_paths as CP  # noqa: E402  (T995) the trunk and CI's areas, typed once
import related_tests as RT  # noqa: E402  (T1041) the tests a change reaches
WORKFLOW = ROOT / ".github" / "workflows" / "pipeline-ci.yml"
#: pipeline-ci steps not run here, each with why (a step added to the workflow runs unless it is named here)
SKIP = {"Tile tools (tippecanoe, pmtiles)": "installs the tile tools on the runner (here: brew install tippecanoe pmtiles)"}
#: the step whose marker expression is the no-data tier (`scripts/test-env.sh` `no_data_markers` reads the same one)
TESTS = "Tests"
#: what makes an app tools test a tree scanner (fast-fail's `vitest_changed` grep for the ui-native ones)
SCANS = re.compile(r"readFileSync|readdirSync")
#: what makes a pipeline test a source scanner (fast-fail's `source_scanners` grep)
PY_SCANS = re.compile(r'rglob\("\*\.py"\)|glob\("\*\*/\*\.py"\)|_py_sources\(|walk\(.*pipeline')
HOOK = """# (T995) the hooks are versioned in .githooks/: pre-push runs `scripts/before_push.py --changed`, commit-msg checks
# the message follows Conventional Commits (T998: node app/tools/check-commits.mjs --edit).
# Turn them on once per clone (every worktree of it shares the setting):
git config core.hooksPath .githooks
# (T1041) keep git's ssh connection up while the hook runs (GitHub closed it under a 420 s hook):
git config core.sshCommand "$(git config --get core.sshCommand || echo ssh) -o ServerAliveInterval=15 -o ServerAliveCountMax=8"
# skip once, in an emergency only (say so in the commit or the PR): git push --no-verify
# turn them off: git config --unset core.hooksPath"""
#: (T995) the ticket guard on its own: what a push that changes no app file still runs (`pnpm check:fast` holds it)
TICKETS = ("node", "app/tools/check-tickets.mjs")
#: (T995 review) docs-ci.yml's vitest step, word for word (test_before_push.py pins it to the workflow)
DOCS_TESTS = ("tools/check-tickets.test.ts", "tools/start-here.test.ts", "tools/ledger.test.ts", "tools/check-size.test.ts")
#: (T995) skips every step, its reason printed: the release command's own pushes (its preflight ran these checks)
SKIP_ENV = "BEFORE_PUSH_SKIP"
#: (T998) the commit-message step's name: it runs first, whatever the change
COMMITS = "commit messages (Conventional Commits: node app/tools/check-commits.mjs)"
#: (T1041) the --changed steps that run the tests the change reaches
APP_RELATED = "app: related tests + tree scanners (vitest related)"
PY_RELATED = "the pipeline's related tests (importers, one level up, as CI runs them)"
SCANNERS = "app tree scanners (app/tools tests that read the tree)"
NO_TIER = "the pipeline's source scanners (--no-tier)"
#: (T1041) the steps of the Python lane (the pipeline's: step_area "pipeline"); every other step is the node lane's
PY_LANE = ("CI: ", "the pipeline's ")


def commits_step(ranges: list[str] | None = None) -> "Step":
    """(T998) every commit of these ranges follows Conventional Commits; default: the branch's commits that the trunk
    does not have (`origin/main..HEAD`)."""
    ranges = [f"{CP.REMOTE}/{CP.TRUNK}..HEAD"] if ranges is None else ranges
    return Step(COMMITS, ("node", "app/tools/check-commits.mjs", *(x for r in ranges for x in ("--range", r))))


@dataclass(frozen=True)
class Step:
    name: str
    cmd: tuple[str, ...]
    script: str | None = None       # a workflow step's `run`, executed with `bash -e` (as fast-fail's ci_step)
    note: tuple[str, ...] = ()      # (T1041) what the step chose and why, printed above its output


def title_limit(number: int | str) -> int:
    """(T1144) the longest PR title for this PR number: the merge commit `<title> (#N)` must fit `headerMax`."""
    import json
    header_max = json.loads((ROOT / "app" / "tools" / "versioning.json").read_text())["headerMax"]
    return header_max - len(f" (#{number})")


class TitleNotChecked(Exception):
    """(T1144) the title check could not run (no node, a timeout): said as a warning, the push goes on."""


def title_verdict(title: str, number: int | str) -> str | None:
    """(T1144) None when the PR title, as its merge commit `<title> (#N)`, follows Conventional Commits and fits;
    otherwise what to fix. CI's check, run through the same command (`check-commits.mjs --title --number`)."""
    try:
        got = subprocess.run(["node", "app/tools/check-commits.mjs", "--title", title, "--number", str(number)],
                             cwd=ROOT, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as e:
        raise TitleNotChecked(f"node could not run the check ({e})") from e
    if got.returncode == 2:                                   # check-commits: the check could not run
        raise TitleNotChecked(f"the check could not run ({(got.stderr or got.stdout).strip()[:200]})")
    if got.returncode == 0:
        return None
    limit = title_limit(number)
    over = len(title) - limit
    head = (f"the title is {len(title)} characters, {over} over the limit of {limit} for a {len(str(number))}-digit "
            f"number (its merge commit is \"{title} (#{number})\", at most 100)" if over > 0
            else f"the title ({len(title)}/{limit}) does not pass the commit check")
    return f"{head}\n{got.stdout}{got.stderr}".rstrip()


def _gh(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], cwd=ROOT, capture_output=True, text=True, timeout=60)


def pushed_title_check(lines: list[str], gh=None) -> tuple[int, list[str]]:
    """(T1144) THE PR TITLE, before the push: CI checks the merge commit `<title> (#N)`, and nothing local did (#266
    failed five pushes at 96 + 7 = 103). For each pushed branch with an open PR, check its title with its real
    number. (rc, messages): rc 1 a title to fix (with the `gh pr edit` command); a CONFLICTING PR and an unreachable
    `gh` are warnings, said, never silent."""
    import json
    gh = gh or _gh
    rc, out = 0, []
    for ln in lines:
        parts = ln.split()
        if len(parts) != 4 or not parts[0].startswith("refs/heads/") or parts[1] == ZERO:
            continue
        if not parts[2].startswith("refs/heads/"):
            continue
        branch = parts[2][len("refs/heads/"):]                # the REMOTE ref: `git push origin local:other` pushes `other`
        try:
            got = gh(["pr", "view", branch, "--json", "number,title,mergeable,state"])
        except (OSError, subprocess.SubprocessError) as e:
            out.append(f"WARNING: the PR title was not checked: gh could not run ({e})")
            continue
        if got.returncode != 0:
            if "no pull requests found" in (got.stderr or ""):
                continue                                      # no PR yet: before_pr.sh --title checks it at opening
            why = ((got.stderr or got.stdout).strip().splitlines() or [f"rc {got.returncode}"])[0]
            out.append(f"WARNING: the PR title was not checked: gh could not answer ({why})")
            continue
        pr = json.loads(got.stdout)
        if pr.get("state") != "OPEN":                         # gh pr view also answers a CLOSED or MERGED PR
            continue
        if pr.get("mergeable") == "CONFLICTING":
            out.append(f"WARNING: PR #{pr['number']} conflicts with main (mergeable: CONFLICTING); CI will not run on it")
        try:
            bad = title_verdict(pr["title"], pr["number"])
        except TitleNotChecked as e:
            out.append(f"WARNING: the PR title was not checked: {e}")
            continue
        if bad:
            rc = 1
            out.append(f"the title of PR #{pr['number']} fails CI's merge-commit check: {bad}\n  fix it: gh pr edit "
                       f"{pr['number']} --title \"<a shorter title>\"")
    return rc, out


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
    `<remote sha>..<local sha>` (T1144: those of its paths the PR itself changes), or, for a new branch (or a remote
    sha this clone does not hold), everything since its merge base with origin/main. (what was pushed, one line per
    ref; every path to test)"""
    said, paths, _ = pushed(lines, trunk)
    return said, paths


def pushed(lines: list[str], trunk: str = CP.TRUNK) -> tuple[list[str], list[str], list[str]]:
    """`pushed_paths`, and (T998) the commit range of each pushed branch: the commits whose messages the hook checks.
    (T1019) That range is `<merge base with origin/main>..<local sha>` — the branch's own commits, the range docs-ci's
    `commits` job checks on the PR — never `<remote sha>..<local sha>`: after a merge of (or a rebase onto) a newer
    trunk the remote's old tip is not the branch's base, and that range held the trunk's own commits (merge subjects,
    history before the T998 cutover) the PR never checks.

    (T1144) THE PATHS are the pushed change that is ALSO the PR's own: changed in `<remote sha>..<local sha>` AND in
    `<merge base>..<local sha>` (what CI's `changes` job reads for the PR, ci_paths.pr_paths). A plain follow-up push
    tests what it changed, as before; after `git merge origin/main` the first range holds every path main changed
    (165 for a docs-only branch, 2026-11-26: a 5 min hook re-testing main), and only the PR's own paths the merge
    touched stay (a conflict resolution is in both). The rest are as main has them (each came with main, or the push
    put it back to main's), so the PR and the merge queue test them, not this push: said, with their count. A stale
    local origin/main makes the merge base older and the PR's own set WIDER (main's newer paths count as the PR's):
    more is tested, never less. A new branch (remote sha unknown) is everything since the merge base, as before."""
    said, paths, ranges = [], set(), []
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
        mb = _git("merge-base", lsha, f"{CP.REMOTE}/{trunk}", trunk=trunk).strip()
        def diff(a: str) -> set[str]:
            return {x for x in _git("diff", "--name-only", "--no-renames", a, lsha, trunk=trunk).splitlines() if x}
        own = diff(mb)
        if known:
            sent = diff(rsha)
            got, came = sent & own, sent - own
            said.append((f"{rref}: {len(came)} path(s) came with main and are the merge queue's: not re-tested here; "
                         if came else f"{rref}: ")
                        + f"{len(got)} path(s) in {rsha[:12]}..{lsha[:12]}"
                        + (f" that the PR itself changes (since {mb[:12]}, the merge base with {CP.REMOTE}/{trunk})"
                           if came else ""))
        else:
            got = own
            said.append(f"{rref}: {len(got)} path(s) in {mb[:12]}..{lsha[:12]} (since the merge base with "
                        f"{CP.REMOTE}/{trunk})")
        paths |= got
        ranges.append(f"{mb}..{lsha}")
    return said, sorted(paths), ranges


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


def related_steps(paths: list[str], markers: str, app: bool = True, py: bool = True) -> dict[str, Step]:
    """(T1041) the steps that run the tests these changed paths reach: the app's (`vitest related` over the changed app
    code, with the tree scanners, which import nothing they scan) and the pipeline's (the direct importers)."""
    out: dict[str, Step] = {}
    if app:
        files, app_said = RT.app_related(paths, ROOT)
        scan = app_scanners()
        out["app"] = Step(APP_RELATED, ("pnpm", "-C", "app", "exec", "vitest", "related", "--run", "--passWithNoTests",
                                        *files, *scan),
                          note=(f"{len(files)} changed app code file(s) + {len(scan)} tree scanner(s)", *app_said))
    if not py:
        return out
    tests, said = RT.pytest_related(paths, ROOT)
    if tests:
        out["pipeline"] = Step(PY_RELATED, ("bash", "scripts/pytest-chunked.sh", "--only", RT.only_regex(tests), "--",
                                            "-q", "-p", "no:cacheprovider", "-m", markers), note=tuple(said))
    else:
        out["pipeline"] = Step(PY_RELATED, (), note=(*said, "no pipeline test file changed or imports a changed module"))
    return out


def plan(tier: bool = True, app_suite: bool = False, steps: list[dict] | None = None,
         areas: set[str] | None = None, ranges: list[str] | None = None,
         paths: list[str] | None = None) -> list[Step]:
    """Every step, or (T995, `areas` given) only those of the areas changed — the commit messages (T998, over `ranges`)
    and the ticket guard always. (T1041) With the changed `paths` too, the tests they reach: the pipeline's direct
    importers after its scanners, and the app's tree scanners step becomes `vitest related` over the change + them."""
    if areas is not None:
        out = [commits_step(ranges)]
        out += [] if "app" in areas else [Step("ticket guard (node app/tools/check-tickets.mjs)", TICKETS)]
        full = plan(tier=tier, app_suite=app_suite, steps=steps)
        kept = [s for s in full if s.name != COMMITS and step_area(s) in areas]
        names = {s.name for s in kept}
        # the whole app suite (--app-suite) and the whole tier already hold every related test
        app_rel, py_rel = SCANNERS in names and not app_suite, NO_TIER in names
        if paths is None or not (app_rel or py_rel):
            return out + kept
        rel = related_steps(paths, tier_markers(workflow_steps() if steps is None else steps), app=app_rel, py=py_rel)
        for s in kept:
            out.append(rel["app"] if s.name == SCANNERS and app_rel else s)
            if s.name == NO_TIER:
                out.append(rel["pipeline"])
        return out
    steps = workflow_steps() if steps is None else steps
    out: list[Step] = [commits_step(ranges)]
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
        out.append(Step(NO_TIER, (py(), "-m", "pytest", "-q", "-p",
                                                                       "no:cacheprovider", "-m", markers,
                                                                       *py_scanners())))
    out.append(Step("app gates (pnpm check:fast)", ("pnpm", "-C", "app", "run", "-s", "check:fast")))
    out.append(Step(SCANNERS, ("pnpm", "-C", "app", "exec", "vitest", "run", *app_scanners())))
    out.append(Step("feeds worker tests", ("pnpm", "-C", "app", "exec", "vitest", "run", "--root", "workers/feeds")))
    # (T995 review) docs-ci's second step: the guard's own tests and the start-here page (a handoff-only push runs it)
    out.append(Step("docs: the ticket guard's tests and the start-here page (docs-ci)",
                    ("pnpm", "-C", "app", "exec", "vitest", "run", *DOCS_TESTS)))
    if app_suite:
        out.append(Step("app-ci: the whole app vitest suite", (*heavy, "pnpm", "-C", "app", "exec", "vitest", "run")))
    return out


def step_area(step: Step) -> str:
    """(T995) the CI area a step stands for (scripts/ci_paths.py AREAS): what `--changed` keeps it by."""
    if step.name.startswith(PY_LANE):
        return "pipeline"
    if step.name.startswith("docs: ") or step.name == COMMITS:      # (T998) docs-ci's `commits` job runs it on CI
        return "docs"
    if step.name.startswith("feeds worker"):
        return "feeds-worker"
    if step.name.startswith("app"):
        return "app"
    raise ValueError(f"before-push: no area for step {step.name!r}")


#: (T1041) what keeps git's SSH connection to GitHub up while the hook runs: measured 2026-11-23 with a hook that slept
#: 420 s on `git push --dry-run` — plain ssh: "Connection to github.com closed by remote host", rc 141 (SIGPIPE, the
#: 2026-11-22 death); with these options the same push went through
KEEPALIVE = "-o ServerAliveInterval=15 -o ServerAliveCountMax=8"


def ssh_host(url: str) -> str | None:
    """The ssh host (or Host alias) of a remote URL, None for a non-ssh one: git@host:path, ssh://[user@]host[:port]/."""
    if url.startswith("ssh://"):
        m = re.match(r"ssh://(?:[^@/]+@)?([^:/]+)", url)
    elif "://" in url:
        return None                          # https, file, git: no ssh connection to keep
    else:
        m = re.match(r"(?:[^@/:]+@)?([^:/]+):", url)
    return m.group(1) if m else None


def keepalive_said(url: str, ssh_command: str | None, ssh_g=None) -> str | None:
    """(T1041) None when the push connection is kept alive (the ssh command git uses, or ssh's own settings for the
    host, name a ServerAliveInterval above 0) or the remote is not ssh; else the warning, with the fix. Never fails."""
    host = ssh_host(url)
    if host is None:
        return None
    if ssh_command and re.search(r"ServerAliveInterval[= ]+[1-9]", ssh_command):
        return None
    if ssh_g is None:
        def ssh_g(h):
            got = subprocess.run(["ssh", "-G", h], capture_output=True, text=True)
            return got.stdout if got.returncode == 0 else None
    out = ssh_g(host)
    if out is None:
        return f"before-push: could not read ssh's settings for {host} (ssh -G): keepalive unknown"
    m = re.search(r"^serveraliveinterval (\d+)$", out, re.M)
    if m and int(m.group(1)) > 0:
        return None
    cmd = f"{ssh_command or 'ssh'} {KEEPALIVE}"
    return (f"before-push: WARNING: git's ssh connection to {host} has no keepalive; GitHub closes it while a long hook "
            f"runs (a 420 s hook: the push died, SIGPIPE). Once per clone: git config core.sshCommand \"{cmd}\" "
            f"(or ServerAliveInterval 15 under `Host {host}` in ~/.ssh/config)")


# ── (T1144) ONE HOOK'S STEPS AT A TIME PER MACHINE, AND VITEST FROM THE FREE CORES ───────────────────────────────────────
# 2026-11-26: with several chats pushing at once the hook failed on LOAD, not on tests (vitest "Timeout calling
# onTaskUpdate" with every test passed; WaterScreen.sources.test.tsx 2 x 40 s): six pushes that night. Now the hook's
# steps queue on one machine-wide lock, and its vitest runs take the cores the machine has free.

#: the machine's push lock: fcntl.flock, so the kernel drops it when its holder dies (a crashed hook never leaves it
#: held); the file holds the holder's details (pid, checkout, since), emptied on release. Not a heavy-job lock. It is
#: in the home folder, so it is one lock per user account (every chat on this Mac pushes as the same user).
LOCK_FILE = Path.home() / ".claude" / "before-push.lock"
#: the longest wait for the lock, in minutes (default LOCK_WAIT_MIN); past it the hook FAILS, never runs unlocked
LOCK_WAIT_ENV = "BEFORE_PUSH_LOCK_WAIT"
LOCK_WAIT_MIN = 20.0
#: the logged escape: its reason is printed and the steps run without the lock (an empty reason is refused)
NO_LOCK_ENV = "BEFORE_PUSH_NO_LOCK"


class LockTimeout(Exception):
    """The push lock was not free within the wait: the hook fails, saying who holds it."""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:                  # another user's live process
        return True
    return True


def _holder(fd: int) -> dict | None:
    raw = os.pread(fd, 4096, 0).decode("utf-8", "replace").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return {"raw": raw[:200]}


def _who(h: dict | None) -> str:
    if h is None:
        return "a holder that has not written its details yet"
    if "pid" not in h:
        return f"an unreadable holder ({h.get('raw')!r})"
    return f"pid {h['pid']} ({h.get('checkout')}, since {h.get('since')})"


@contextlib.contextmanager
def machine_lock(path: Path, wait_s: float, poll_s: float = 1.0, say_every: float = 30.0, say=None, ppid=os.getppid):
    """(T1144) Hold the machine's push lock while the body runs. Free: taken at once (a last holder that died holding
    it — its details still in the file — is said, taken over). Held: says once who holds it, then every `say_every`
    seconds how long so far; after `wait_s` raises LockTimeout naming the holder. The fd is not inherited by the
    steps (PEP 446; subprocess closes fds too), so only this process holds the lock and its death frees it. A waiter
    whose parent ended (git gave the push up: its ssh connection dropped) stops waiting and raises too: it never goes
    on to hold the lock and run every step for a push that is already dead."""
    say = say or (lambda s: print(s, flush=True))
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        t0 = last = time.monotonic()
        told = False
        parent = ppid()
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                pass
            now = time.monotonic()
            if not told:
                say(f"before-push: waiting for the machine's push lock ({path}): held by {_who(_holder(fd))}; at most "
                    f"{round(wait_s)} s ({LOCK_WAIT_ENV}, minutes); {NO_LOCK_ENV}=\"<reason>\" runs beside it. git "
                    "holds its ssh connection open meanwhile (AGENTS W47, the keepalive)")
                told = True
            elif now - last >= say_every:
                say(f"before-push: still waiting for the push lock: {round(now - t0)} s so far")
                last = now
            if ppid() != parent:
                raise LockTimeout(
                    f"before-push: STOPPED after {round(now - t0)} s of waiting for the push lock ({path}): the git "
                    "that started this hook has ended (its connection dropped), so the push is gone; nothing was "
                    "checked. Push again")
            if now - t0 >= wait_s:
                raise LockTimeout(
                    f"before-push: FAILED: the machine's push lock ({path}) was held by {_who(_holder(fd))} for the "
                    f"whole {round(wait_s)} s wait ({LOCK_WAIT_ENV} minutes); nothing was checked. Push again when it "
                    f"is free, or {NO_LOCK_ENV}=\"<reason>\" git push to run beside it (git holds its ssh connection "
                    "open while the hook waits: AGENTS W47, the keepalive)")
            time.sleep(poll_s)
        waited = time.monotonic() - t0
        prev = _holder(fd)
        if prev is not None:
            pid = prev.get("pid")
            state = "gone" if isinstance(pid, int) and not _alive(pid) else "not holding it"
            say(f"before-push: the push lock's last holder, {_who(prev)}, ended without releasing it (pid {state}): "
                "taken over")
        if told:
            say(f"before-push: the push lock is ours after {round(waited)} s")
        os.ftruncate(fd, 0)
        os.pwrite(fd, json.dumps({"pid": os.getpid(), "checkout": str(ROOT),
                                  "since": time.strftime("%Y-%m-%d %H:%M:%S %Z")}).encode(), 0)
        try:
            yield waited
        finally:
            os.ftruncate(fd, 0)
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def lock_wait_s(environ=os.environ) -> float:
    """The lock's wait, in seconds, from LOCK_WAIT_ENV (minutes). A value that is not a number above 0 is refused
    (ValueError), never read as the default."""
    raw = environ.get(LOCK_WAIT_ENV)
    if raw is None:
        return LOCK_WAIT_MIN * 60
    try:
        m = float(raw)
    except ValueError:
        m = -1.0
    if not m > 0:
        raise ValueError(f"before-push: {LOCK_WAIT_ENV}={raw!r} is not a number of minutes above 0")
    return m * 60


def config_workers(cores: int, ci: bool) -> int:
    """What app/vitest.config.ts's `maxWorkers` comes to (test_before_push.py pins the config's line to this)."""
    return 2 if ci else max(2, cores // 2)


def vitest_workers(cores: int, load: float, config: int) -> int:
    """(T1144) the hook's vitest workers: the cores the machine has free (cores minus the 1-minute load average), at
    least 2, at most what the config would use."""
    return min(config, max(2, math.floor(cores - load)))


#: (T1144) the variables the hook gives its steps: vitest's own worker caps (it reads them over the config's
#: maxWorkers, for the forks and the threads pools), so `pnpm check:fast`'s vitest and the hook's vitest steps all take
#: them; CI and a hand `pnpm test` never see them
VITEST_CAPS = ("VITEST_MAX_FORKS", "VITEST_MAX_THREADS")
HOOK_ENV: dict[str, str] = {}


def vitest_unread_caps(dist: Path) -> list[str]:
    """The VITEST_CAPS the installed vitest (its dist) does not read: an upgrade that dropped them would make the cap
    a no-op, so the hook says so instead of printing a number vitest ignores."""
    files = list(dist.rglob("*.js")) if dist.is_dir() else []
    text = "".join(p.read_text(encoding="utf-8", errors="replace") for p in files)
    return [k for k in VITEST_CAPS if f"process.env.{k}" not in text]


def hook_vitest_env(environ=os.environ, cores: int | None = None, load: float | None = None,
                    dist: Path | None = None) -> tuple[dict, str]:
    """(the variables for the hook's steps, the line saying the number and why)."""
    mine = [k for k in VITEST_CAPS if k in environ]
    if mine:
        return {}, f"before-push: vitest workers: yours ({', '.join(f'{k}={environ[k]}' for k in mine)})"
    dist = ROOT / "app" / "node_modules" / "vitest" / "dist" if dist is None else dist
    unread = vitest_unread_caps(dist)
    if unread:
        return {}, (f"before-push: WARNING: vitest workers NOT capped: the vitest in {dist} does not read "
                    f"{', '.join(unread)} (the config's own count runs)")
    if cores is None:
        cores = os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()
    if load is None:
        try:
            load = os.getloadavg()[0]
        except OSError as e:                 # said, and vitest keeps the config's own count
            return {}, f"before-push: vitest workers: the config's own (the load average could not be read: {e})"
    if not cores:
        return {}, "before-push: vitest workers: the config's own (the core count could not be read)"
    cfg = config_workers(cores, bool(environ.get("CI")))
    n = vitest_workers(cores, load, cfg)
    return ({k: str(n) for k in VITEST_CAPS},
            f"before-push: vitest workers {n}: {cores} cores, load {load:.1f} (the config's own: {cfg})")


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
    environ.update(HOOK_ENV)                 # (T1144) the hook's vitest worker caps; empty outside the hook
    return environ


#: (T1041) where a step's output goes while the lanes run at once: the lane's own file (printed whole at the step's end)
_OUT = threading.local()


def run_step(step: Step) -> int:
    # the pipeline's interpreter first on PATH (a workflow step's `python`): this checkout's .venv or PIPELINE_PY's
    env = step_env()
    out = getattr(_OUT, "f", None)
    io = {"stdout": out, "stderr": subprocess.STDOUT} if out is not None else {}
    if not step.cmd:                         # (T1041) a step with nothing to run: its note says why
        return 0
    if step.script is None:
        return subprocess.run(list(step.cmd), cwd=ROOT, env=env, **io).returncode
    with tempfile.NamedTemporaryFile("w", suffix=".sh", prefix="before-push-", delete=False) as f:
        f.write(step.script)
    try:
        return subprocess.run([*step.cmd, f.name], cwd=ROOT, env=env, **io).returncode
    finally:
        os.unlink(f.name)


def lanes(steps: list[Step]) -> list[list[Step]]:
    """(T1041) the two lanes that run at once: the pipeline's steps (Python) and the rest (node); empty lanes dropped.
    Each lane keeps the plan's order. The commit messages and the ticket guard lead the node lane (seconds)."""
    py = [s for s in steps if s.name.startswith(PY_LANE)]
    rest = [s for s in steps if not s.name.startswith(PY_LANE)]
    return [x for x in (rest, py) if x]


def run_all(steps: list[Step], runner, parallel: bool) -> dict[str, int]:
    """Every step's exit code, by name. In lanes (`parallel`), each step's output is held in a file and printed whole,
    its header, note and timing with it, when it ends; one after another otherwise (the output as it comes)."""
    rcs: dict[str, int] = {}
    lock = threading.Lock()

    def one(s: Step, held: bool) -> None:
        t = time.time()
        if not held:
            print(f"── {s.name}", flush=True)
            for ln in s.note:
                print(f"   {ln}", flush=True)
            rc = runner(s)
            print(f"   {'ok' if rc == 0 else f'FAILED (rc={rc})'} ({round(time.time() - t)} s)", flush=True)
            rcs[s.name] = rc
            return
        with tempfile.TemporaryFile("w+", prefix="before-push-lane-") as f:
            _OUT.f = f
            try:
                rc = runner(s)
            except BaseException as e:      # a runner that raised: its step failed, said, and the lane goes on
                print(f"before-push: {s.name} raised {e!r}", file=f)
                rc = 1
            finally:
                _OUT.f = None
            f.seek(0)
            text = f.read()
        with lock:
            print(f"── {s.name}", flush=True)
            for ln in s.note:
                print(f"   {ln}")
            sys.stdout.write(text)
            print(f"   {'ok' if rc == 0 else f'FAILED (rc={rc})'} ({round(time.time() - t)} s)", flush=True)
            rcs[s.name] = rc

    groups = lanes(steps)
    if not parallel or len(groups) < 2:
        for s in steps:
            one(s, held=False)
        return rcs
    print(f"before-push: {len(groups)} lanes at once ({', '.join(f'{len(g)} step(s)' for g in groups)}; --serial: "
          "one after another); each step's output is printed whole when it ends", flush=True)
    threads = [threading.Thread(target=lambda g=g: [one(s, held=True) for s in g]) for g in groups]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    return rcs


def main(argv=None, runner=run_step, stdin=sys.stdin, gh=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", default=None, metavar="REGEX")
    ap.add_argument("--no-tier", action="store_true")
    ap.add_argument("--app-suite", action="store_true")
    ap.add_argument("--hook", action="store_true")
    ap.add_argument("--changed", action="store_true")
    ap.add_argument("--remote-url", default=None,
                    help="(T1041) the hook's $2: the push URL, checked for an ssh keepalive (a warning, never a failure)")
    ap.add_argument("--serial", action="store_true",
                    help="(T1041) run the steps one after another, not in the two lanes at once")
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
    areas, ranges, paths = None, None, None
    if a.remote_url:
        ssh_cmd = os.environ.get("GIT_SSH_COMMAND") or subprocess.run(
            ["git", "config", "--get", "core.sshCommand"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
        warn = keepalive_said(a.remote_url, ssh_cmd or None)
        if warn:
            print(warn, file=sys.stderr)
    if a.pre_push:
        stdin_lines = stdin.read().splitlines()
        said, paths, ranges = pushed(stdin_lines)
        for ln in said:
            print(f"before-push --pre-push: {ln}")
        if not paths and all(("a delete" in x or "a tag" in x) for x in said):
            print("before-push --pre-push: only tags / deletes pushed: nothing to check")
            return 0
        trc, tsaid = pushed_title_check(stdin_lines, gh)
        for ln in tsaid:
            print(f"before-push --pre-push: {ln}", file=sys.stderr)
        if trc:
            return 1
        a.changed = True
        areas = CP.areas_of(paths)
        print(f"before-push --pre-push: {len(paths)} path(s) pushed; areas: "
              f"{', '.join(sorted(areas)) or 'none (the ticket guard only)'} (the checks run on this checkout)")
    elif a.changed:
        base, paths = changed_paths()
        areas = CP.areas_of(paths)
        print(f"before-push --changed: {len(paths)} path(s) since {base[:12]} (the merge base with "
              f"{CP.REMOTE}/{CP.TRUNK}); areas: {', '.join(sorted(areas)) or 'none (the ticket guard only)'}")
    steps = plan(tier=not a.no_tier, app_suite=a.app_suite, areas=areas, ranges=ranges, paths=paths)
    if a.only:
        steps = [s for s in steps if re.search(a.only, s.name)]
        if not steps:
            print(f"before-push: --only {a.only!r} matches no step (--list names them): nothing ran", file=sys.stderr)
            return 2
    if a.list:
        for s in steps:
            print(f"  {s.name}" + ("" if s.script is not None else f"\n      $ {' '.join(s.cmd) or '(nothing to run)'}")
                  + "".join(f"\n      # {ln}" for ln in s.note))
        for name, why in SKIP.items():
            print(f"  (not run) CI: {name}: {why}")
        return 0
    # (T1144) the hook only: its steps queue on the machine's lock (a hand run is the caller's own choice, unchanged)
    lock = contextlib.nullcontext()
    if a.pre_push:
        nolock = os.environ.get(NO_LOCK_ENV)
        if nolock is not None:
            if not nolock.strip():
                print(f"before-push: {NO_LOCK_ENV} needs a reason, which is printed", file=sys.stderr)
                return 2
            print(f"before-push: WITHOUT the machine's push lock ({NO_LOCK_ENV}): {nolock}", flush=True)
        else:
            try:
                lock = machine_lock(LOCK_FILE, lock_wait_s())
            except ValueError as e:
                print(e, file=sys.stderr)
                return 2
    HOOK_ENV.clear()
    try:
        with lock:
            if a.pre_push and any("vitest" in s.cmd or s.cmd[-1:] == ("check:fast",) for s in steps):
                env, line = hook_vitest_env()     # measured once the lock is ours: the load the steps meet
                HOOK_ENV.update(env)
                print(line, flush=True)
            t0 = time.time()
            rcs = run_all(steps, runner, parallel=not a.serial)
    except LockTimeout as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        HOOK_ENV.clear()
    failed = [s.name for s in steps if rcs.get(s.name, 1)]
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
