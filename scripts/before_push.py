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

THE HOOKS ARE VERSIONED (T995, owner 2026-11-21): `.githooks/pre-push` runs `--changed`, `.githooks/commit-msg` checks the
message (T998: Conventional Commits, `app/tools/check-commits.mjs --edit`).
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


def title_verdict(title: str, number: int | str) -> str | None:
    """(T1144) None when the PR title, as its merge commit `<title> (#N)`, follows Conventional Commits and fits;
    otherwise what to fix. CI's check, run through the same command (`check-commits.mjs --title --number`)."""
    got = subprocess.run(["node", "app/tools/check-commits.mjs", "--title", title, "--number", str(number)],
                         cwd=ROOT, capture_output=True, text=True)
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
        branch = parts[0][len("refs/heads/"):]
        try:
            got = gh(["pr", "view", branch, "--json", "number,title,mergeable"])
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
        if pr.get("mergeable") == "CONFLICTING":
            out.append(f"WARNING: PR #{pr['number']} conflicts with main (mergeable: CONFLICTING); CI will not run on it")
        bad = title_verdict(pr["title"], pr["number"])
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
    `<remote sha>..<local sha>`, or, for a new branch (or a remote sha this clone does not hold), everything since its
    merge base with origin/main. (what was pushed, one line per ref; every path those commits change)"""
    said, paths, _ = pushed(lines, trunk)
    return said, paths


def pushed(lines: list[str], trunk: str = CP.TRUNK) -> tuple[list[str], list[str], list[str]]:
    """`pushed_paths`, and (T998) the commit range of each pushed branch: the commits whose messages the hook checks.
    (T1019) That range is `<merge base with origin/main>..<local sha>` — the branch's own commits, the range docs-ci's
    `commits` job checks on the PR — never `<remote sha>..<local sha>`: after a merge of (or a rebase onto) a newer
    trunk the remote's old tip is not the branch's base, and that range held the trunk's own commits (merge subjects,
    history before the T998 cutover) the PR never checks. The PATHS stay the pushed change (`<remote sha>..`)."""
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
        base = rsha if known else mb
        got = [x for x in _git("diff", "--name-only", "--no-renames", base, lsha, trunk=trunk).splitlines() if x]
        said.append(f"{rref}: {len(got)} path(s) in {base[:12]}..{lsha[:12]}"
                    + ("" if known else f" (since the merge base with {CP.REMOTE}/{trunk})"))
        paths |= set(got)
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
    t0 = time.time()
    rcs = run_all(steps, runner, parallel=not a.serial)
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
