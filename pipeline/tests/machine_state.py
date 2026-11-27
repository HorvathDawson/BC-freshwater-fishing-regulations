"""TESTS NEVER READ MACHINE STATE (T777; T993 plan item 6). A no-data test (one with no `needs_*` or `slow` marker, the
tier CI runs) sees what a fresh clone of the commit sees, whatever this machine holds; a data test reaches this
machine's trees only through its marker. A pytest plugin, registered by the repo-root `conftest.py`, so every run has
it whoever starts pytest (CI, fast-fail, the chunked runner, the fresh copy, a bare `pytest`).

THE CURATED SANDBOX. At configure, before any test module is imported, `CURATED_ROOT` (`pipeline.common.curated`, T422)
is pointed at a copy made for the session: the curated files git tracks, as this checkout has them (an untracked file is
left out: CI does not have it), with an EMPTY REVIEW — no marks, no closed list, no closed-areas file
(`fresh_review`). So the owner's review saves never move a test (T777: five tests went red each time he saved, and the
anchors scanner could not see them because they read through app code), and a test that writes without its own
`conftest.curated_root(monkeypatch, tmp_path)` writes the copy, never the real tree. Child processes inherit it. A
data test gets the live tree back for its run (`CURATED_ROOT` is removed while it runs, restored after).

THE GUARD. An audit hook (PEP 578) sees every open, sqlite connect and file-system change the test process makes.
While a no-data test runs (setup, call and teardown), touching
  * the REAL curated tree (`data/curated`; the copy above holds every curated file such a test may read), or
  * a path under the generated trees (`data/generated`, `data/release`) that git does not track (CI has only the
    tracked ones: the review queue, the committed gauge candidates, ...)
FAILS the test, naming the path, what read it and the marker to add (`MARKER_FOR`). A read is refused at the call (so
the traceback points at the line) and recorded, so code that swallows exceptions still fails the test; a WRITE to
either tree is refused before it happens. A data test reads the live trees by design (`need()`, `under_test`; the
no-data tier deselects it), but it too may never WRITE the real curated tree (human-owned, not recreatable). That
replaces the before/after digests of the live tree some files took around their routes (`_stamps(load_repo().base)`),
which a no-data test can no longer read.

THE RUN'S OWN TEMP FOLDER (T777, the pipeline twin of the app's T764). Tests make temp folders freely and a killed run
never removes its own (T777: 3,265 `fixture-release-*`, 2.1 GB of `pytest-of-*`; the disk filled twice). At configure
`TMPDIR` (and `tempfile.tempdir`) point at `<system temp>/pyt-<pid>`, which every child process inherits and which is
removed when the run ends; each start removes the folders of runs whose process is gone. pytest's own `tmp_path` root
stays where it was (`PYTEST_DEBUG_TEMPROOT`), kept for FAILED tests only (`tmp_path_retention_policy` in pytest.ini).
The curated sandbox lives in the run's folder.

A directory LISTING is not refused: it reads no content, and two kinds of code list the real trees by design (the code
scanners that walk the repo, the mini build's before/after listing of the live folders it must not write); what a test
then opens is refused as above.

KNOWN LIMITS (what it cannot see, said plainly):
  * CHILD PROCESSES: the hook lives in this interpreter only. A `cat`, `git show`, `node` or `python -c` the test starts
    reads and writes unseen (the child inherits the sandbox's `CURATED_ROOT`, nothing more).
  * PATH ALIASES: a path is matched as written (`os.path.abspath`, no `realpath` per event: that is a stat per open, too
    dear on a hook every open passes through). The trees' own names are matched both as written and resolved, so the
    checkout's path and the target of a linked `data/generated` are caught; a path that reaches them through ANOTHER
    alias (`/tmp` vs `/private/tmp`, a symlink placed elsewhere that points into the trees) is not.
  * native code (GDAL, PROJ, sqlite's own file handles after connect) that does not go through Python's `open`; a
    `stat` / `exists()` probe (no audit event); work done while no test runs (collection, module imports: a module that
    writes a folder on import is caught only when a test imports it first).

`test_machine_state.py` plants each case it does see (a read of `data/curated`, an untracked generated read, a write by
a no-data and by a data test, a swallowed read, the marked twin and the listing that pass) and holds the guard to them.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import pytest

from pipeline.tests import smoke

ROOT = Path(__file__).resolve().parents[2]

#: the marker a no-data test adds when it truly reads this machine's data (first matching prefix wins; repo-relative)
MARKER_FOR = (
    ("data/generated/atlas", "needs_atlas"),
    ("data/generated/bundle", "needs_bundle"),
    ("data/generated/reaches", "needs_reach_run"),
    ("data/generated/tiles", "needs_tiles"),
    ("data/generated/regs/extraction", "needs_source"),
    ("data/release", "needs_bundle"),
    ("data/generated", "needs_bundle"),
    ("data/curated", "needs_bundle"),
)

_READ_EVENTS = frozenset({"open", "sqlite3.connect"})
_WRITE_EVENTS = frozenset({"os.remove", "os.rename", "os.mkdir", "os.rmdir", "shutil.rmtree", "os.truncate",
                           "os.symlink", "os.link", "os.chmod", "os.utime", "shutil.copyfile", "shutil.copytree"})
_EVENTS = _READ_EVENTS | _WRITE_EVENTS
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC


class _State:
    def __init__(self) -> None:
        self.item = None             # the test running now (None: between tests)
        self.data = False            # it is a data test (a needs_* or slow marker): only its curated writes are refused
        self.found: list[str] = []   # what it touched, as failure lines
        self.curated: tuple[str, ...] = ()
        self.generated: tuple[str, ...] = ()
        self.tracked: frozenset[str] = frozenset()
        self.sandbox: Path | None = None
        self.run_tmp: Path | None = None
        self.saved_env: dict[str, str | None] = {}
        self.live_env: str | None = None
        self.local = threading.local()   # re-entry: the hook's own work never re-enters it


STATE = _State()


# ------------------------------------------------------------------------------------------------------- the sandbox
def fresh_review(root: Path) -> Path:
    """Empty the review store of the curated tree at `root`: the marks store empty, no closed list (absent = nothing
    closed), no closed-areas file (`review.closed_areas.path()`, beside the DFO places files: written from the marks,
    born with the first confirm, so with no marks it is absent). Returns `root`."""
    from pipeline.common import curated as C
    live = C.load_repo()
    (root / Path(live.review.marks).relative_to(live.base)).write_text("", encoding="utf-8")
    (root / Path(live.review.closed).relative_to(live.base)).unlink(missing_ok=True)
    (root / Path(live.dfo_salmon.closed_areas).relative_to(live.base)).unlink(missing_ok=True)
    return root


def _git_files(*paths: str) -> list[str]:
    """The files git tracks under `paths` (repo-relative). No git, no answer: the run stops naming why (a guess at
    which files a fresh clone has would be the machine state this module exists to keep out)."""
    try:
        out = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z", "--", *paths], capture_output=True,
                             check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        raise pytest.UsageError(f"machine_state (T777): cannot list the files git tracks under {paths} ({e}); the "
                                "suite needs a git checkout to know what a fresh clone has") from e
    return [p for p in out.stdout.decode("utf-8").split("\0") if p]


def make_sandbox(dst: Path) -> Path:
    """The session's curated tree at `dst`: every tracked curated file as this checkout has it, then `fresh_review`."""
    from pipeline.common import curated as C
    base = Path(os.path.abspath(C.load_repo().base))   # as named, never resolved (a linked tree, pytest_configure)
    rel_base = base.relative_to(ROOT).as_posix()
    for rel in _git_files(rel_base):
        src = ROOT / rel
        if src.is_file():                       # tracked but deleted here: this checkout's state has no such file
            out = dst / Path(rel).relative_to(rel_base)
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, out)
    return fresh_review(dst)


RUN_PREFIX = "pyt-"


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:                      # another user's process: not ours to judge
        return True
    return True


def run_temp(system: Path) -> Path:
    """`<system>/pyt-<pid>`, made, after removing the folders of runs whose process is gone (a killed run cannot clean
    up after itself). Short on purpose: a unix socket path in it must stay under macOS's 104 bytes."""
    for d in system.glob(RUN_PREFIX + "*"):
        pid = d.name[len(RUN_PREFIX):]
        if d.is_dir() and pid.isdigit() and int(pid) != os.getpid() and not _alive(int(pid)):
            shutil.rmtree(d, ignore_errors=True)
    run = system / f"{RUN_PREFIX}{os.getpid()}"
    run.mkdir(exist_ok=True)
    return run


def _setenv(name: str, value: str) -> None:
    STATE.saved_env.setdefault(name, os.environ.get(name))
    os.environ[name] = value


# --------------------------------------------------------------------------------------------------------- the guard
def _prefixes(p: Path) -> tuple[str, ...]:
    return tuple(sorted({os.path.abspath(p), os.path.realpath(p)}))


def _under(path: str, bases: tuple[str, ...]) -> bool:
    return any(path == b or path.startswith(b + os.sep) for b in bases)


def marker_for(path: str) -> str:
    rel = os.path.relpath(path, ROOT)
    return next((m for pre, m in MARKER_FOR if rel == pre or rel.startswith(pre + "/")), "needs_bundle")


def _is_write(event: str, args: tuple) -> bool:
    if event in _WRITE_EVENTS:
        return True
    if event == "open":
        mode, flags = (args[1] if len(args) > 1 else None), (args[2] if len(args) > 2 else 0)
        if isinstance(mode, str):
            return any(c in mode for c in "wax+")
        return bool((flags or 0) & _WRITE_FLAGS)
    return False


#: events with a (source, destination) pair: the source is only read, except by a rename, which removes it
_PAIRS = frozenset({"os.rename", "os.symlink", "os.link", "shutil.copyfile", "shutil.copytree"})


def _paths(event: str, args: tuple) -> list[tuple[str, bool]]:
    """`(absolute path, written?)` for each path the event names."""
    write = _is_write(event, args)
    raw = [(args[0] if args else None, write if event not in _PAIRS or event == "os.rename" else False)]
    if event in _PAIRS:
        raw.append((args[1] if len(args) > 1 else None, True))
    out = []
    for a, w in raw:
        if a is None or isinstance(a, int):
            continue
        try:
            s = os.fsdecode(os.fspath(a))
        except TypeError:
            continue
        if event == "sqlite3.connect":
            if s.startswith("file:"):
                s = s[5:].split("?", 1)[0]
            if s in ("", ":memory:"):
                continue
        out.append((os.path.abspath(s), w))
    return out


def _offence(path: str, write: bool) -> str | None:
    """Why the running test may not touch `path`, or None. A data test is refused only a write to the curated tree."""
    if STATE.data:
        return "the real curated tree" if write and _under(path, STATE.curated) else None
    if _under(path, STATE.curated):
        return "the real curated tree"
    if _under(path, STATE.generated) and path not in STATE.tracked:
        return "an untracked generated path (CI and a fresh clone do not have it)"
    return None


def _message(item, path: str, why: str, write: bool, event: str) -> str:
    rel = os.path.relpath(path, ROOT)
    verb = "writes" if write else "reads"
    if why.startswith("the real curated"):
        fix = ("a no-data test reads curated data through `CURATED` / `under_test.curated()`, which this run points "
               "at the session sandbox (the tracked tree, review empty); one that writes makes its own with "
               "`curated_root(monkeypatch, tmp_path)`. If it checks the live tree against a build, it is a data test: "
               f"add `@pytest.mark.{marker_for(path)}` (or that build's needs_* marker)")
    else:
        fix = (f"build the data in the test (tmp_path, a committed fixture, the mini release), or, if it checks this "
               f"machine's build, add `@pytest.mark.{marker_for(path)}`")
    if write:
        fix = "a test never writes the real trees: write under tmp_path (`curated_root(monkeypatch, tmp_path)` for " \
              "curated data)"
    marker = "" if STATE.data else " without a data marker"
    return f"{item.nodeid} {verb} {why}: {rel} ({event}){marker}. Fix: {fix}. (T777, pipeline/tests/machine_state.py)"


def _hook(event: str, args: tuple) -> None:
    if event not in _EVENTS or STATE.item is None:
        return
    local = STATE.local
    if getattr(local, "busy", False):
        return
    local.busy = True
    try:
        for path, write in _paths(event, args):
            why = _offence(path, write)
            if why is None:
                continue
            item = STATE.item
            if item is None:
                return
            msg = _message(item, path, why, write, event)
            if msg not in STATE.found:
                STATE.found.append(msg)
            raise pytest.fail.Exception(msg, pytrace=True)
    finally:
        local.busy = False


# -------------------------------------------------------------------------------------------------------- the plugin
@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    if STATE.sandbox is not None:            # configured once per process (a pytester run in-process re-enters)
        return
    from pipeline.common import curated as C
    roots = C.load_roots()
    STATE.curated = _prefixes(Path(C.load_repo().base))
    STATE.generated = _prefixes(Path(roots.generated)) + _prefixes(Path(roots.release))
    # the repo paths as named, never resolved: a worktree that links data/release (or data/generated) to the main
    # checkout's resolves outside this repo, and configure stopped the run with an INTERNALERROR (T777 review)
    rels = [Path(os.path.abspath(p)).relative_to(ROOT).as_posix() for p in (roots.generated, roots.release)]
    tracked = [os.path.abspath(ROOT / f) for f in _git_files(*rels)]
    STATE.tracked = frozenset(tracked)
    STATE.live_env = os.environ.get(C.ROOT_ENV)
    system = Path(tempfile.gettempdir())
    _setenv("PYTEST_DEBUG_TEMPROOT", os.environ.get("PYTEST_DEBUG_TEMPROOT") or str(system))
    STATE.run_tmp = run_temp(system)
    _setenv("TMPDIR", str(STATE.run_tmp))
    STATE.saved_env["tempfile.tempdir"] = tempfile.tempdir
    tempfile.tempdir = str(STATE.run_tmp)
    STATE.sandbox = make_sandbox(STATE.run_tmp / "curated")
    _setenv(C.ROOT_ENV, str(STATE.sandbox))
    sys.addaudithook(_hook)


def pytest_unconfigure(config):
    if STATE.sandbox is None:
        return
    STATE.item = None
    tempfile.tempdir = STATE.saved_env.pop("tempfile.tempdir", None)
    for name, value in STATE.saved_env.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    STATE.saved_env = {}
    shutil.rmtree(STATE.run_tmp, ignore_errors=True)
    STATE.sandbox = STATE.run_tmp = None


def pytest_report_header(config):
    if STATE.sandbox is not None:
        return (f"curated tree: the session sandbox {STATE.sandbox} (tracked files, review empty); temp folder "
                f"{STATE.run_tmp}, removed at the end (pipeline/tests/machine_state.py, T777)")


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(item, nextitem):
    from pipeline.common.curated import ROOT_ENV
    data = smoke.needs_data(item)
    # The run's own values (the sandbox; the caller's root or none, which a data test reads). Any OTHER value is a
    # fixture's root (`curated_root` in a module-scoped fixture) and is left alone, here and after the test: restoring
    # the value saved at the start undid it after the module's first test, so the rest of the module ran on the
    # session sandbox (no-data) or the LIVE tree (data) instead of its own copy (T777 review).
    ours = {str(STATE.sandbox), STATE.live_env, None}
    if os.environ.get(ROOT_ENV) in ours:
        _put_root(ROOT_ENV, STATE.live_env if data else str(STATE.sandbox))
    STATE.found = []
    STATE.item, STATE.data = item, data
    try:
        return (yield)
    finally:
        STATE.item, STATE.data = None, False
        if os.environ.get(ROOT_ENV) in ours:  # between tests (and after a fixture's teardown) the run's sandbox
            _put_root(ROOT_ENV, str(STATE.sandbox))


def _put_root(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    if STATE.item is not item or not STATE.found:
        return report
    msg = "\n".join(STATE.found)
    STATE.found = []
    if report.passed:                         # the refusal was swallowed: the test still fails, saying why
        report.outcome = "failed"
        report.longrepr = msg
    elif any(m not in str(report.longrepr) for m in msg.splitlines()):
        report.sections.append(("machine state (T777)", msg))
    return report
