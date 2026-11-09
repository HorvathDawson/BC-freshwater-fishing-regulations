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

Each line is written straight to the file descriptor (no buffer), so a worker killed by its pool still left its lines.

NOT RECORDED, by design, and covered elsewhere: paths under the interpreter (`sys.prefix`: the venv — the consumer keys
the interpreter and every installed distribution, `stepcache.env_fingerprint`), the temp dir (a step's own scratch;
no build input lives there), `/dev` and the system; `shutil.rmtree`'s fd-relative scans (a deletion, not an input);
`os.stat` of an open file descriptor (the open was recorded); and every read or scan the IMPORT SYSTEM makes —
a module is keyed by its M line, a package's version by the env. (Recording the import system's reads made the read
set depend on whether a `.pyc` existed: a fresh checkout's first run read each module's source, a later run its
`.pyc` — 198 / 142 / 82 reads for one bundle on the same inputs, T522 review.) What it CANNOT see is declared by the caller
(`pipeline.deliver.__main__.step_declared`): a read made in C by a library that does not go through Python's `open`
(GDAL `read_file`, a SQLite `ATTACH`), and a subprocess that is not Python.
"""
from __future__ import annotations

import hashlib
import os
import stat as _st
import sys
import tempfile
import threading

#: the repo root: the directory holding the `pipeline` package this file is in
_REPO = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "")
_state = threading.local()
_seen: set = set()
_fd = -1
_skip: tuple = ()
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
    if p.startswith(_skip) or "__pycache__" in p:
        return None
    return p


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
        finally:
            _state.busy = False
        return None


def _is_repo_module(origin: str) -> bool:
    """A module file of this repo: under the repo root, not under a skipped root (the venv is `sys.prefix`)."""
    p = os.path.abspath(origin)
    return p.startswith(_REPO) and not p.startswith(_skip) and p.endswith(".py")


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


def install(directory: str) -> None:
    global _fd, _skip
    if _fd >= 0:
        return
    _state.busy = True
    try:
        os.makedirs(directory, exist_ok=True)
        _fd = os.open(os.path.join(directory, f"{os.getpid()}.trace"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        roots = {sys.prefix, sys.base_prefix, sys.exec_prefix, tempfile.gettempdir(), os.path.realpath(tempfile.gettempdir()),
                 "/dev", "/System", "/usr", "/Library", "/private/var/folders", "/private/tmp", "/tmp", "/etc", "/private/etc"}
        _skip = tuple(sorted(os.path.join(r, "") for r in roots if r))
        _emit("V", sys.version.replace("\n", " "), os.path.realpath(sys.executable))
        # the modules loaded before this hook: `pipeline/__init__.py` (and any repo module a caller imported first)
        for name, mod in list(sys.modules.items()):
            f = getattr(mod, "__file__", None)
            if f and _is_repo_module(f):
                with open(f, "rb") as fh:
                    _emit("M", os.path.abspath(f), hashlib.sha256(fh.read()).hexdigest())
    finally:
        _state.busy = False
    sys.meta_path.insert(0, _ModuleRecorder)
    sys.addaudithook(_audit)
    os.stat = _probe_recorder(os.stat, "stat")
    os.lstat = _probe_recorder(os.lstat, "lstat")
    os.access = _probe_recorder(os.access, "access")
