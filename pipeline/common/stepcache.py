"""INPUT-KEYED REUSE (T522): a step reruns only when something it READ changed — by content, never by time.

The recorder (`pipeline/_trace.py`, on with `PIPELINE_TRACE=<dir>`) lists what a step's processes imported, read,
listed and probed, each STAMPED when it happened. This module turns that into a FINGERPRINT — each repo module's bytes
as imported, each file read by content, each directory listed by its names, each probed path by its kind, the
interpreter and every installed distribution (`env_fingerprint`) — and answers "is it still the same?" by taking the
same fingerprint from disk now. Same code + same inputs is the same output (AGENTS 19: every step is deterministic), so
a matching step's recorded outputs are its outputs.

Two users:

  * `pipeline build` keys the reach and deliver STAGES on the modules they really imported (`build.TRACED`), instead
    of every .py under broad roots: an edit to the review app, an audit tool or the DFO suggester reruns neither.
  * `pipeline.deliver all` reuses a STEP (bundle, verdicts, answers, …) whose fingerprint, staging files and declared
    inputs all match its last run (`StepCache`).

A RECORD IS KEPT ONLY WHEN IT IS HONEST. Not kept (and the step runs next time, saying why) when: an input MOVED between
the moment it was read and the end of the run (its stamp then ≠ now — the output came from what was read, not from what
is on disk now), a module was edited while the step ran, the recorder met an event it cannot key (`X`: a directory
descriptor, a `dir_fd=` path), or the step ran under another interpreter. A kept output whose bytes changed since is
never served; a restored output is re-hashed in place.

WHAT THE RECORDER CANNOT SEE is declared by the caller, never guessed: GDAL reads (the source gpkg, the boundary, the
atlas's area catalogue), SQLite ATTACH, and a choice made on file TIMES (the bundle's newest-reach-run pick):
`declared_stamps` stats every file under the given roots (size + mtime: cheap, and stricter than content).

CONCURRENCY: one lock file per step (`<root>/<step>.lock`, `fcntl.flock`) is held across lookup + restore and across
record, so two runs sharing a cache never serve a half-written record.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import sys
from functools import lru_cache
from pathlib import Path

from pipeline import _trace
from pipeline.common.curated import REPO_ROOT

_TRANSIENT = ("-journal", "-wal", "-shm")
_PARTS = ("modules", "reads", "lists", "probes")


def sha_file(p: Path, memo: dict | None = None) -> str:
    """A file's sha256; `memo` caches it by (path, size, mtime_ns) for one process."""
    st = p.stat()
    k = (str(p), st.st_size, st.st_mtime_ns)
    if memo is not None and k in memo:
        return memo[k]
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    d = h.hexdigest()
    if memo is not None:
        memo[k] = d
    return d


def _rel(p: str) -> str:
    try:
        return str(Path(p).relative_to(REPO_ROOT))
    except ValueError:
        return p


def _abs(r: str) -> Path:
    p = Path(r)
    return p if p.is_absolute() else REPO_ROOT / p


@lru_cache(maxsize=1)
def env_fingerprint() -> dict:
    """The interpreter and every installed distribution (name==version): what the recorder leaves out (`sys.prefix`)
    and what a step's third-party code is. A package upgrade, a new interpreter or a different venv reruns every step."""
    from importlib import metadata
    dists = sorted({f"{(d.metadata['Name'] or '?').lower()}=={d.version}" for d in metadata.distributions()})
    return {"python": sys.version, "executable": os.path.realpath(sys.executable),
            "distributions": hashlib.sha256("\n".join(dists).encode()).hexdigest()[:16], "count": len(dists)}


class Trace:
    """Everything the recorder wrote under one directory (every process, every sub-directory)."""

    def __init__(self, directory: Path):
        self.modules: dict[str, str] = {}
        self.reads: dict[str, str] = {}
        self.lists: dict[str, str] = {}
        self.probes: dict[str, str] = {}
        self.conflicts: list[str] = []        # a module imported with two contents
        self.moved: list[str] = []            # an input stamped differently by two processes
        self.unkeyable: list[str] = []        # X lines
        self.interpreters: set[tuple[str, str]] = set()
        files = sorted(Path(directory).rglob("*.trace")) if Path(directory).is_dir() else []
        if not files:
            raise FileNotFoundError(f"stepcache: no trace under {directory} — was PIPELINE_TRACE passed to the step?")
        into = {"R": self.reads, "L": self.lists, "E": self.probes}
        for f in files:
            for line in f.read_text(encoding="utf-8", errors="surrogateescape").splitlines():
                kind, path, extra = (line.split("\t") + ["", ""])[:3]
                if kind == "V":
                    self.interpreters.add((path, extra))
                    continue
                if kind == "X":
                    self.unkeyable.append(path)
                    continue
                rel = _rel(path)
                if kind == "M":
                    if self.modules.get(rel, extra) != extra:
                        self.conflicts.append(rel)
                    self.modules[rel] = extra
                elif kind in into:
                    if into[kind].get(rel, extra) != extra:
                        self.moved.append(f"{rel} (two {kind} stamps)")
                    into[kind][rel] = extra


def code_now(files) -> dict[str, str]:
    """{module: sha256 now} for recorded module files ("missing" for one that is gone)."""
    out = {}
    for rel in sorted(files):
        p = _abs(rel)
        out[rel] = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "missing"
    return out


def code_digest(files: dict[str, str]) -> str:
    h = hashlib.sha256()
    for rel, d in sorted(files.items()):
        h.update(rel.encode())
        h.update(d.encode())
    return h.hexdigest()[:16]


def fingerprint(paths: dict, memo: dict | None = None) -> dict:
    """The fingerprint of recorded paths ({"modules", "reads", "lists", "probes"}: iterables), taken from disk NOW."""
    reads = {}
    for r in sorted(paths["reads"]):
        p = _abs(r)
        k = _trace.kind(str(p))
        reads[r] = sha_file(p, memo) if k == "file" else k
    return {"modules": code_now(paths["modules"]),
            "reads": reads,
            "lists": {r: _trace.listing(str(_abs(r))) for r in sorted(paths["lists"])},
            "probes": {r: _trace.kind(str(_abs(r))) for r in sorted(paths["probes"])}}


def moved_since(trace: Trace, keep) -> list[str]:
    """The inputs (those `keep(rel)` accepts) whose stamp NOW differs from the stamp taken when they were read,
    listed or probed — the step's output was made from the earlier state."""
    out = list(trace.moved)
    for rel, was in sorted(trace.reads.items()):
        if keep(rel) and _trace.stamp(str(_abs(rel))) != was:
            out.append(f"{rel} (read)")
    for rel, was in sorted(trace.lists.items()):
        if keep(rel) and _trace.listing(str(_abs(rel))) != was:
            out.append(f"{rel} (listed)")
    for rel, was in sorted(trace.probes.items()):
        if keep(rel) and _trace.kind(str(_abs(rel))) != was:
            out.append(f"{rel} (probed)")
    return out


def declared_stamps(roots) -> dict[str, list]:
    """Every file under each root (a file or a directory) as [size, mtime_ns]: the inputs read in C (GDAL, ATTACH)
    or chosen by time, which the recorder cannot key by content. A root that does not exist raises."""
    out: dict[str, list] = {}
    for root in roots:
        root = Path(root)
        if not root.exists():
            raise FileNotFoundError(f"stepcache: declared input {root} is missing")
        for p in ([root] if root.is_file() else sorted(q for q in root.rglob("*") if q.is_file())):
            if p.name == ".DS_Store":
                continue
            st = p.stat()
            out[_rel(str(p))] = [st.st_size, st.st_mtime_ns]
    return out


def snapshot(d: Path, memo: dict | None = None) -> dict[str, str]:
    """{name relative to d: sha256} for every file in the staging directory d (SQLite's transient files excluded)."""
    out = {}
    for p in sorted(q for q in Path(d).rglob("*") if q.is_file() and not q.is_symlink()):
        if not p.name.endswith(_TRANSIENT):
            out[str(p.relative_to(d))] = sha_file(p, memo)
    return out


def _under(rel: str, d: Path) -> bool:
    try:
        _abs(rel).resolve().relative_to(Path(d).resolve())
        return True
    except ValueError:
        return False


class StepCache:
    """One record per step under `root/<step>/`: record.json (its key parts and its outputs' sha256) and out/ (a copy
    of its outputs). One record per step: the last run."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.memo: dict = {}

    def _rec_path(self, step: str) -> Path:
        return self.root / step / "record.json"

    @contextlib.contextmanager
    def _lock(self, step: str):
        self.root.mkdir(parents=True, exist_ok=True)
        with open(self.root / f"{step}.lock", "a") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)

    def reuse(self, step: str, argv: list[str], d: Path, declared: dict) -> tuple[dict | None, str]:
        """Under the step's lock: (the record, with its outputs placed in `d` and re-verified there) when the step can
        be reused; else (None, why)."""
        with self._lock(step):
            rec, why = self._lookup(step, argv, d, declared)
            if rec is None:
                return None, why
            placed = []
            for name, digest in sorted(rec["outputs"].items()):
                dst = Path(d) / name
                dst.parent.mkdir(parents=True, exist_ok=True)
                _place(self.root / step / "out" / name, dst)
                placed.append(dst)
                if sha_file(dst) != digest:
                    for p in placed:
                        p.unlink(missing_ok=True)
                    return None, f"its kept output {name} did not copy back intact"
            return rec, why

    def _lookup(self, step: str, argv: list[str], d: Path, declared: dict) -> tuple[dict | None, str]:
        rp = self._rec_path(step)
        if not rp.is_file():
            return None, "no record"
        rec = json.loads(rp.read_text(encoding="utf-8"))
        if rec.get("argv") != argv:
            return None, "its arguments changed"
        if rec.get("env") != env_fingerprint():
            return None, "the interpreter or an installed package changed"
        if rec.get("declared") != declared:
            was = rec.get("declared") or {}
            moved = sorted(k for k in set(declared) | set(was) if declared.get(k) != was.get(k))
            return None, f"declared inputs changed: {', '.join(moved[:3])}{' …' if len(moved) > 3 else ''}"
        now_d = snapshot(d, self.memo)
        if rec.get("staging") != now_d:
            moved = sorted(k for k in set(now_d) | set(rec["staging"]) if now_d.get(k) != rec["staging"].get(k))
            return None, f"its staging inputs changed: {', '.join(moved[:3])}"
        fp = fingerprint(rec["paths"], self.memo)
        for part in _PARTS:
            if fp[part] != rec["fingerprint"][part]:
                moved = sorted(k for k in fp[part] if fp[part][k] != rec["fingerprint"][part].get(k))
                return None, f"{part} changed: {', '.join(moved[:3])}{' …' if len(moved) > 3 else ''}"
        for name, digest in rec["outputs"].items():
            p = self.root / step / "out" / name
            if not p.is_file() or sha_file(p, self.memo) != digest:
                return None, f"its kept output {name} is gone or was changed since"
        return rec, "inputs unchanged"

    def record(self, step: str, argv: list[str], d: Path, declared: dict, before: dict[str, str],
               trace: Trace) -> str:
        """Keep the step just run, under its lock. Returns what was done, in words; a step that cannot be keyed
        honestly is not kept (and any older record is dropped, so it reruns)."""
        with self._lock(step):
            rp = self._rec_path(step)
            why = self._refuse(trace, d)
            if why is None:
                outside = lambda r: not _under(r, d)       # noqa: E731
                paths = {"modules": sorted(trace.modules),
                         "reads": sorted(filter(outside, trace.reads)),
                         "lists": sorted(filter(outside, trace.lists)),
                         "probes": sorted(filter(outside, trace.probes))}
                fp = fingerprint(paths, self.memo)
                # AFTER the content is hashed: every input still as it was when the step read it (else the hash may
                # be of bytes the step never saw), every module as imported
                moved = moved_since(trace, outside)
                if moved:
                    why = f"{moved[0]} changed while the step ran" + (f" (+{len(moved) - 1} more)" if len(moved) > 1 else "")
                elif fp["modules"] != trace.modules:
                    why = "a module changed on disk after the step imported it"
            if why is not None:
                rp.unlink(missing_ok=True)
                return f"not kept: {why}"
            after = snapshot(d, self.memo)
            outputs = {n: s for n, s in after.items() if before.get(n) != s}
            out_dir = self.root / step / "out"
            if out_dir.exists():
                shutil.rmtree(out_dir)
            rp.unlink(missing_ok=True)                    # no record names outputs that are half copied
            for name, digest in sorted(outputs.items()):
                dst = out_dir / name
                dst.parent.mkdir(parents=True, exist_ok=True)
                _place(Path(d) / name, dst)
                if sha_file(dst) != digest:
                    shutil.rmtree(out_dir, ignore_errors=True)
                    return f"not kept: {name} changed while it was copied"
            rec = {"step": step, "argv": argv, "env": env_fingerprint(), "declared": declared, "staging": before,
                   "paths": paths, "fingerprint": fp, "outputs": outputs}
            tmp = rp.with_suffix(".tmp")
            tmp.write_text(json.dumps(rec, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(tmp, rp)
            return f"kept ({len(outputs)} output(s), {len(paths['modules'])} modules, {len(paths['reads'])} reads)"

    @staticmethod
    def _refuse(trace: Trace, d: Path) -> str | None:
        if trace.unkeyable:
            return f"the recorder could not key {trace.unkeyable[0]}"
        if trace.conflicts:
            return f"{trace.conflicts[0]} changed while the step ran"
        me = {(sys.version.replace("\n", " "), os.path.realpath(sys.executable))}
        if trace.interpreters != me:
            return f"it ran under another interpreter ({sorted(trace.interpreters)})"
        return None


def _place(src: Path, dst: Path) -> None:
    """A COPY, never a hard link: the staging file is promoted to the live home, and a later step or reader that
    wrote to a shared inode would change the cache, the live file and the next run's staging at once."""
    if dst.exists():
        dst.unlink()
    shutil.copyfile(src, dst)


def write_modules_trace(directory: Path, modules: dict[str, str], name: str) -> None:
    """A reused step's modules, written as a trace so the enclosing stage's code key still holds them (the stage
    key must name the code of every step, run or reused)."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    lines = "".join(f"M\t{_abs(rel)}\t{d}\n" for rel, d in sorted(modules.items()))
    (Path(directory) / f"reused-{name}.trace").write_text(lines, encoding="utf-8")
