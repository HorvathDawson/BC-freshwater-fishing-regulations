"""WHAT A PROCESS READ (T522): the recorder behind the build's input-keyed caches. Standard library only.

Installed by `pipeline/__init__.py` when `PIPELINE_TRACE=<dir>` is set (never otherwise), so every process that runs
pipeline code under it — the stage, the `python -m pipeline.deliver <step>` children, their multiprocessing workers
(they inherit the environment and import `pipeline` first) — appends to `<dir>/<pid>.trace`, one line per first event,
each STAMPED WHEN IT HAPPENED (the consumer, `pipeline.common.stepcache`, refuses to key a run whose inputs moved
between that moment and the end of the run — the output came from what was read, not from what is there now):

    M <file> <sha256>   a module imported from this repo (any package under the repo root, outside the venv), hashed
                        AS IT WAS IMPORTED
    R <file> <stamp>    a file opened for reading (`open`, `os.open` read-only, `sqlite3.connect`); stamp = `stamp()`
    L <dir>  <listing>  a directory listed (`os.listdir` / `os.scandir`: glob, iterdir); listing = `listing()`
    E <path> <kind>     a path probed (`os.stat`, `os.lstat`, `os.access`: exists / lexists / is_file / is_dir /
                        access) — an OPTIONAL input: its appearing or going away must change the key; kind = `kind()`
    X <what>            an event that names no path this recorder can key (a directory fd, a `dir_fd=` relative path):
                        the run is NOT keyable and the consumer keeps no record — never a silent drop
    P <name>            (T1171) a THIRD-PARTY top-level package imported (not this repo, not the standard library): the
                        consumer keys the installed distribution that owns it, by version (`stepcache.package_versions`)
                        — so only what a stage imports moves its key, never pytest or vulture; one no distribution
                        owns is a raise, never an empty set. Also swept from `sys.modules` at install and at exit.
    C <key> <digest>    (T1171) a config.yaml value read (`config_read`, called by the two config loaders): its dotted
                        key (`*` = the whole file) and the digest of its value — so a stage is keyed on the config
                        values it reads, not on the whole file

Each line is written straight to the file descriptor (no buffer), so a worker killed by its pool still left its lines.

NOT RECORDED, by design, and covered elsewhere: paths under the interpreter (`sys.prefix`: the venv — the consumer keys
the interpreter and the distributions of the P lines), the temp dir (a step's own scratch;
no build input lives there), `/dev` and the system — EXCEPT the repo itself: a skipped root that holds the repo (a
lane worktree under `/private/tmp` or the temp dir) skips only what is outside the repo, so a checkout anywhere is
traced (T799/T835: a worktree under /tmp recorded 0 modules and 0 reads, and the cache reused a stale bundle); `shutil.rmtree`'s fd-relative scans (a deletion, not an input);
`os.stat` of an open file descriptor (the open was recorded); and every read or scan the IMPORT SYSTEM makes —
a module is keyed by its M line, a package's version by the env. (Recording the import system's reads made the read
set depend on whether a `.pyc` existed: a fresh checkout's first run read each module's source, a later run its
`.pyc` — 198 / 142 / 82 reads for one bundle on the same inputs, T522 review.) What it CANNOT see is declared by the caller
(`pipeline.deliver.__main__.step_declared`): a read made in C by a library that does not go through Python's `open`
(GDAL `read_file`, a SQLite `ATTACH`), and a subprocess that is not Python.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat as _st
import sys
import tempfile
import threading

#: (T1171) THIS MODULE IS THE RECORDER'S OWN MACHINERY: what decides what is recorded, never what a build makes. The
#: atlas's code record leaves out every module that declares this (`stepcache.is_machinery`), so a change to the cache
#: tooling never makes the atlas "behind".
RECORD_MACHINERY = True

#: the repo root: the directory holding the `pipeline` package this file is in
_REPO = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "")
#: the repo root as written and as resolved (`/tmp` → `/private/tmp`, `/var` → `/private/var`); `install` adds the
#: spelling through every skipped root that resolves to one holding it (`split_roots`)
_REPOS = tuple(sorted({_REPO, os.path.join(os.path.realpath(_REPO), "")}))
_state = threading.local()
_seen: set = set()
_fd = -1
_dir: str | None = None    # where this process's trace goes (`directory`)
#: (T1171) {config key: digest} read in this process, kept even before a recorder is installed (a config loader that ran
#: at import time, before `install`, is still written out by it: never lost to the order of imports)
_config_seen: dict[str, str] = {}
_site: tuple = ()          # the installed-package roots (site-packages): a module there is a P line
_stdlib: tuple = ()        # the interpreter's own library roots
_skip: tuple = ()          # roots skipped wherever the path is (the venv, /dev, a system dir that does not hold the repo)
_skip_outside: tuple = ()  # roots that HOLD the repo (the temp dir, /private/tmp): skipped only outside the repo
_real_stat = os.stat
_real_lstat = os.lstat
_real_listdir = os.listdir


# THE STAMPS — the one definition, used here when the event happens and by `stepcache` at the end of the run.
def kind(path: str, *, _stat=None, _lstat=None) -> str:
    """dir / file / dangling (a symlink to nothing) / missing."""
    try:
        m = (_stat or _real_stat)(path).st_mode
        return "dir" if _st.S_ISDIR(m) else "file"
    except OSError:
        try:
            (_lstat or _real_lstat)(path)
            return "dangling"
        except OSError:
            return "missing"


def stamp(path: str, *, _stat=None) -> str:
    """A read file's identity at a moment: size and mtime (ns), or its kind when it is no regular file."""
    try:
        st = (_stat or _real_stat)(path)
    except OSError:
        return kind(path)
    if _st.S_ISDIR(st.st_mode):
        return "dir"
    return f"{st.st_size}:{st.st_mtime_ns}"


def listing(path: str, *, _listdir=None) -> str:
    """A directory's sorted names, as a digest; its kind when it is no directory."""
    try:
        names = (_listdir or _real_listdir)(path)
    except NotADirectoryError:
        return "file"
    except OSError:
        return kind(path)
    return hashlib.sha256("\n".join(sorted(names)).encode("utf-8", "surrogateescape")).hexdigest()[:16]


def _emit(k: str, path: str, extra: str = "") -> None:
    key = (k, path)
    if key in _seen:
        return
    _seen.add(key)
    os.write(_fd, f"{k}\t{path}\t{extra}\n".encode("utf-8", "surrogateescape"))


def _unkeyable(what: str) -> None:
    _emit("X", what.replace("\t", " ").replace("\n", " "))


def _norm(p) -> str | None:
    """An absolute path to record, or None for one under a skipped root. A non-path (an fd) is the caller's to handle."""
    p = os.fsdecode(os.fspath(p))
    p = os.path.abspath(p)
    if _skipped(p) or "__pycache__" in p:
        return None
    return p


def _skipped(p: str) -> bool:
    """Whether an absolute path is under a skipped root: always for `_skip`; for a root that holds the repo, only when
    the path is outside the repo."""
    return p.startswith(_skip) or (p.startswith(_skip_outside) and not p.startswith(_REPOS))


def _is_path(p) -> bool:
    return isinstance(p, (str, bytes, os.PathLike))


def _called_from(*needles: str) -> bool:
    """Whether the Python frame that made the audited call (the one under `_audit`) is in a file matching a needle.
    ONLY that frame: a module's own top-level code reading a file while it is being imported runs UNDER the import
    system's frames, and its reads are inputs."""
    f = sys._getframe(3)                    # 0 this, 1 _record, 2 _audit, 3 the caller of open/listdir/scandir
    return f is not None and any(n in f.f_code.co_filename for n in needles)


_EVENTS = frozenset(("open", "sqlite3.connect", "os.listdir", "os.scandir"))


def _audit(event: str, args) -> None:
    if event in _EVENTS and not getattr(_state, "busy", False):
        _state.busy = True
        try:
            _record(event, args)
        finally:
            _state.busy = False


def _record(event: str, args) -> None:
    if event == "open":
        path, mode, flags = args
        if isinstance(mode, str):
            if any(c in mode for c in "wax+"):
                return
        elif flags is not None and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            return
        if isinstance(path, int):
            return                          # `open(fd)`: a descriptor already opened (and recorded) by path
        if _called_from("<frozen importlib", "importlib/metadata"):
            return                          # the import system reading a module's source / .pyc (M keys it)
        if not _is_path(path):
            return _unkeyable(f"open of {path!r}")
        p = _norm(path)
        if p:
            _emit("R", p, stamp(p))
    elif event == "sqlite3.connect":
        db = args[0]
        if not _is_path(db):
            return _unkeyable(f"sqlite3.connect of {db!r}")
        s = os.fsdecode(os.fspath(db))
        if s.startswith("file:"):
            s = s[5:].split("?", 1)[0]
        if s and s != ":memory:":
            p = _norm(s)
            if p:
                _emit("R", p, stamp(p))
    elif event in ("os.listdir", "os.scandir"):
        # the import system's own scans and package-version lookups are not inputs (M lines are; the env is keyed)
        if _called_from("<frozen importlib", "importlib/metadata"):
            return
        target = args[0] if args else None
        if isinstance(target, int):
            if _called_from(os.sep + "shutil.py"):
                return                      # `shutil.rmtree` walks by descriptor to DELETE: not an input
            return _unkeyable(f"{event} of a directory descriptor")
        p = _norm("." if target is None else target)
        if p:
            _emit("L", p, listing(p))


def _library_roots() -> tuple[tuple, tuple]:
    """(site-packages roots, standard-library roots), each spelled as written and resolved."""
    import site
    import sysconfig
    paths = sysconfig.get_paths()
    sites = set(site.getsitepackages()) | {paths.get("purelib"), paths.get("platlib")}
    try:
        sites.add(site.getusersitepackages())
    except (AttributeError, OSError):
        pass
    libs = {paths.get("stdlib"), paths.get("platstdlib")}

    def spell(roots):
        return tuple(sorted({os.path.join(x, "") for r in roots if r for x in (r, os.path.realpath(r))}))
    return spell(sites), spell(libs)


def _third_party(name: str, origin, locations) -> str | None:
    """The top-level package to name in a P line, or None for a module of this repo or of the standard library."""
    top = name.partition(".")[0]
    if top in sys.stdlib_module_names or top in sys.builtin_module_names:
        return None
    paths = [os.path.abspath(p) for p in ([origin] if origin and origin not in ("built-in", "frozen") else [])
             + list(locations or [])]
    if not paths:
        return None if origin in ("built-in", "frozen") else top
    if all(p.startswith(_REPOS) and not p.startswith(_site) for p in paths):
        return None                                   # this repo (its modules are M lines)
    if all(p.startswith(_stdlib) and not p.startswith(_site) for p in paths):
        return None                                   # the interpreter's own (`_sysconfigdata_*`)
    return top


def _sweep() -> None:
    """M for every repo module in `sys.modules` not yet named, P for every third-party package (once each)."""
    if _fd < 0:
        return
    busy = getattr(_state, "busy", False)
    _state.busy = True
    try:
        for name, mod in sorted(sys.modules.items(), key=lambda kv: kv[0]):
            f = getattr(mod, "__file__", None)
            if f and _is_repo_module(f):
                if ("M", os.path.abspath(f)) not in _seen:
                    with open(f, "rb") as fh:
                        _emit("M", os.path.abspath(f), hashlib.sha256(fh.read()).hexdigest())
                continue
            spec = getattr(mod, "__spec__", None)
            top = _third_party(name, f, getattr(spec, "submodule_search_locations", None) if spec else None)
            if top is not None and (f is not None or spec is not None):
                _emit("P", top)
    finally:
        _state.busy = busy


class _ModuleRecorder:
    """A finder that finds nothing: it records where the real finders will load a repo module from, and its bytes."""

    @staticmethod
    def find_spec(name, path=None, target=None):
        if getattr(_state, "busy", False):
            return None
        _state.busy = True
        try:
            from importlib.machinery import PathFinder
            spec = PathFinder.find_spec(name, path)
            origin = getattr(spec, "origin", None) if spec else None
            if origin and _is_repo_module(origin):
                with open(origin, "rb") as fh:
                    _emit("M", os.path.abspath(origin), hashlib.sha256(fh.read()).hexdigest())
            elif spec is not None:          # (T1171) found: a third-party package is a P line (a failed optional
                top = _third_party(name, origin, spec.submodule_search_locations)   # import names nothing)
                if top is not None:
                    _emit("P", top)
        finally:
            _state.busy = False
        return None


def _is_repo_module(origin: str) -> bool:
    """A module file of this repo: under the repo root, not under a skipped root (the venv is `sys.prefix`)."""
    p = os.path.abspath(origin)
    return p.startswith(_REPOS) and not _skipped(p) and p.endswith(".py")


def _probe_recorder(real, name: str):
    """`os.stat` / `os.lstat` / `os.access`, recording the path probed (E) — or, for a path relative to a `dir_fd`,
    the run as not keyable (X)."""
    def probe(path, *a, **kw):
        if not getattr(_state, "busy", False):
            _state.busy = True
            try:
                if isinstance(path, int):
                    pass                    # fstat of an open descriptor: the open was recorded
                elif not _is_path(path):
                    _unkeyable(f"os.{name} of {path!r}")
                elif kw.get("dir_fd") is not None and not os.path.isabs(os.fsdecode(os.fspath(path))):
                    _unkeyable(f"os.{name}({os.fsdecode(os.fspath(path))!r}, dir_fd=…)")
                else:
                    p = _norm(path)
                    if p:
                        _emit("E", p, kind(p))
            finally:
                _state.busy = False
        return real(path, *a, **kw)
    probe.__wrapped__ = real
    return probe


def split_roots(roots, repo: str = _REPO) -> tuple[tuple, tuple, tuple]:
    """(skipped everywhere, skipped outside the repo, the repo's spellings). A root that holds the repo — as written
    or through a symlink (`/var/folders/…/T` is `/private/var/folders/…/T`) — goes in the second, and the repo as
    spelled under that root joins the spellings, so a path opened either way is known as the repo's."""
    real_repo = os.path.join(os.path.realpath(repo), "")
    repos = {os.path.join(repo, ""), real_repo}
    roots = sorted({os.path.join(r, "") for r in roots if r})
    for r in roots:
        rr = os.path.join(os.path.realpath(r), "")
        if real_repo.startswith(rr):
            repos.add(os.path.join(r, real_repo[len(rr):]))
    holds = lambda r: any(x.startswith(r) for x in repos)              # noqa: E731
    return (tuple(r for r in roots if not holds(r)), tuple(r for r in roots if holds(r)), tuple(sorted(repos)))


def config_digest(value) -> str:
    """A config value's digest: canonical JSON (sorted keys), 16 hex. The one definition: the loaders' C lines and the
    consumer's value NOW (`stepcache.config_now`) both use it."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
                          .encode("utf-8")).hexdigest()[:16]


#: the digest of a config key that is not in the file (`config_read(missing=True)`, `stepcache.config_now`)
CONFIG_MISSING = "missing"


def config_read(key: str, value, missing: bool = False) -> None:
    """(T1171) A config loader read `key` (dotted; `*` the whole file) with `value` (`missing`: the key is not there): a
    C line when recording; kept for a recorder installed later. The loaders call it (`pipeline.common.curated`,
    `project_config`); nothing else reads config.yaml (`test_build_key_holes` pins that)."""
    d = CONFIG_MISSING if missing else config_digest(value)
    _config_seen[key] = d
    if _fd >= 0:
        _emit("C", key, d)


def start_own(prefix: str, caller: str) -> tuple[str, bool]:
    """(T1171) Record THIS process from here on, as early as its caller can call this (the atlas build: the first
    statement of its module, before any import that reads a file); `caller` is that module's `__file__`, named as a
    module of the run whichever way it was started (`python -m`, runpy). Returns (the trace directory, whether this call
    made it — the caller removes it): an installed recorder is used as it is. PIPELINE_TRACE is set too, so a Python
    process it starts records into the same directory."""
    own = _fd < 0
    if own:
        d = tempfile.mkdtemp(prefix=prefix)
        os.environ["PIPELINE_TRACE"] = d
        install(d)
    if _is_repo_module(caller):
        with open(caller, "rb") as fh:
            _emit("M", os.path.abspath(caller), hashlib.sha256(fh.read()).hexdigest())
    return _dir, own


def directory() -> str | None:
    """(T1171) The directory this process's recorder writes to, or None when none is installed: a process that records
    its own code (the atlas build, `start_own`) reads the trace back from here."""
    return _dir if _fd >= 0 else None


def install(directory: str) -> None:
    global _fd, _dir, _skip, _skip_outside, _REPOS, _site, _stdlib
    if _fd >= 0:
        return
    _state.busy = True
    try:
        os.makedirs(directory, exist_ok=True)
        _dir = os.path.abspath(directory)
        _fd = os.open(os.path.join(directory, f"{os.getpid()}.trace"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        roots = {sys.prefix, sys.base_prefix, sys.exec_prefix, tempfile.gettempdir(), os.path.realpath(tempfile.gettempdir()),
                 "/dev", "/System", "/usr", "/Library", "/private/var/folders", "/private/tmp", "/tmp", "/etc", "/private/etc"}
        _skip, _skip_outside, _REPOS = split_roots(roots)
        _emit("V", sys.version.replace("\n", " "), os.path.realpath(sys.executable))
        _site, _stdlib = _library_roots()
        # the modules loaded before this hook: `pipeline/__init__.py` (and any repo module a caller imported first), and
        # (T1171) the third-party packages already imported; the config values already read
        _sweep()
        for key, d in sorted(_config_seen.items()):
            _emit("C", key, d)
    finally:
        _state.busy = False
    import atexit
    atexit.register(_sweep)                 # a package imported by a finder this hook does not see is still named
    sys.meta_path.insert(0, _ModuleRecorder)
    sys.addaudithook(_audit)
    os.stat = _probe_recorder(os.stat, "stat")
    os.lstat = _probe_recorder(os.lstat, "lstat")
    os.access = _probe_recorder(os.access, "access")
