"""WHAT A PROCESS READ (T522): the recorder behind the build's input-keyed caches. Standard library only.

Installed by `pipeline/__init__.py` when `PIPELINE_TRACE=<dir>` is set (never otherwise), so every process that runs
pipeline code under it — the stage, the `python -m pipeline.deliver <step>` children, their multiprocessing workers
(they inherit the environment and import `pipeline` first) — appends to `<dir>/<pid>.trace`, one line per first event:

    M <file> <sha256>   a repo module imported (`pipeline.*`, `project_config`), hashed AS IT WAS IMPORTED
    R <file>            a file opened for reading (`open`, `os.open` read-only, `sqlite3.connect`)
    L <dir>             a directory listed (`os.listdir` / `os.scandir`: glob, iterdir)
    E <path>            a path probed (`os.stat`: exists / is_file / is_dir) — an OPTIONAL input: its appearing or
                        going away must change the key, though nothing was read

Each line is written straight to the file descriptor (no buffer), so a worker killed by its pool still left its
lines. Paths under the interpreter (`sys.prefix`, the venv), the temp dir, `/dev` and the system are not recorded.

What it cannot see is named, not guessed (the consumer, `pipeline.common.inputs`, adds them as declared inputs): a read
made in C by a library that does not go through Python's `open` (GDAL `read_file`, a SQLite `ATTACH`), and a
subprocess that is not Python.
"""
from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import threading

_LOCAL = ("pipeline", "project_config")
_state = threading.local()
_seen: set = set()
_fd = -1
_skip: tuple = ()


def _emit(kind: str, path: str, extra: str = "") -> None:
    key = (kind, path)
    if key in _seen:
        return
    _seen.add(key)
    os.write(_fd, f"{kind}\t{path}\t{extra}\n".encode("utf-8", "surrogateescape"))


def _norm(p) -> str | None:
    if isinstance(p, int) or p is None:
        return None
    try:
        p = os.fsdecode(os.fspath(p))
    except TypeError:
        return None
    p = os.path.abspath(p)
    if p.startswith(_skip) or "__pycache__" in p:
        return None
    return p


def _guarded(fn):
    def run(*a):
        if getattr(_state, "busy", False):
            return
        _state.busy = True
        try:
            fn(*a)
        finally:
            _state.busy = False
    return run


_EVENTS = frozenset(("open", "sqlite3.connect", "os.listdir", "os.scandir"))


def _audit(event: str, args) -> None:
    if event in _EVENTS:
        _record(event, args)


@_guarded
def _record(event: str, args) -> None:
    if event == "open":
        path, mode, flags = args
        if isinstance(mode, str):
            if any(c in mode for c in "wax+"):
                return
        elif flags is not None and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            return
        p = _norm(path)
        if p:
            _emit("R", p)
    elif event == "sqlite3.connect":
        db = args[0]
        if isinstance(db, (str, bytes, os.PathLike)):
            s = os.fsdecode(os.fspath(db))
            if s.startswith("file:"):
                s = s[5:].split("?", 1)[0]
            if s and s != ":memory:":
                p = _norm(s)
                if p:
                    _emit("R", p)
    elif event in ("os.listdir", "os.scandir"):
        f = sys._getframe(1)
        for _ in range(8):                  # the import system's own directory scans are not inputs (M lines are)
            if f is None:
                break
            fn = f.f_code.co_filename
            if fn.startswith("<frozen importlib") or "importlib/metadata" in fn:     # + package-version lookups
                return
            f = f.f_back
        p = _norm(args[0] if args and args[0] is not None else ".")
        if p:
            _emit("L", p)


class _ModuleRecorder:
    """A finder that finds nothing: it records where the real finders will load a repo module from, and its bytes."""

    @staticmethod
    def find_spec(name, path=None, target=None):
        if name.split(".")[0] not in _LOCAL or getattr(_state, "busy", False):
            return None
        _state.busy = True
        try:
            from importlib.machinery import PathFinder
            spec = PathFinder.find_spec(name, path)
            origin = getattr(spec, "origin", None) if spec else None
            if origin and origin.endswith(".py"):
                with open(origin, "rb") as fh:
                    _emit("M", os.path.abspath(origin), hashlib.sha256(fh.read()).hexdigest())
        finally:
            _state.busy = False
        return None


def _stat_recorder(real):
    def stat(path, *a, **kw):
        if not getattr(_state, "busy", False):
            _state.busy = True
            try:
                p = _norm(path)
                if p:
                    _emit("E", p)
            finally:
                _state.busy = False
        return real(path, *a, **kw)
    stat.__wrapped__ = real
    return stat


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
        # the modules loaded before this hook: `pipeline/__init__.py` (and `project_config` if a caller imported it)
        for name, mod in list(sys.modules.items()):
            f = getattr(mod, "__file__", None)
            if name.split(".")[0] in _LOCAL and f and f.endswith(".py"):
                with open(f, "rb") as fh:
                    _emit("M", os.path.abspath(f), hashlib.sha256(fh.read()).hexdigest())
    finally:
        _state.busy = False
    sys.meta_path.insert(0, _ModuleRecorder)
    sys.addaudithook(_audit)
    os.stat = _stat_recorder(os.stat)
