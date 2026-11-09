"""INPUT-KEYED REUSE (T522): a step reruns only when something it READ changed — by content, never by time.

The recorder (`pipeline/_trace.py`, on with `PIPELINE_TRACE=<dir>`) lists what a step's processes imported, read,
listed and probed. This module turns that into a FINGERPRINT — each repo module's bytes as imported, each file read by
content, each directory listed by its names, each probed path by its kind — and answers "is it still the same?" by
taking the same fingerprint from disk now. Same code + same inputs is the same output (AGENTS 19: every step is
deterministic), so a matching step's recorded outputs are its outputs.

Two users:

  * `pipeline build` keys the reach and deliver STAGES on the modules they really imported (`traced_code`), instead
    of every .py under broad roots: an edit to the review app, an audit tool or the DFO suggester reruns neither.
  * `pipeline.deliver all` reuses a STEP (bundle, verdicts, answers, …) whose fingerprint, staging files and declared
    inputs all match its last run (`StepCache`).

WHAT THE RECORDER CANNOT SEE is declared by the caller, never guessed: GDAL reads (the source gpkg, the boundary, the
atlas's area catalogue) and SQLite ATTACH. `declared_stamps` stats every file under the source tree and the atlas
(size + mtime: cheap, and stricter than content). A missing trace, a module that changed WHILE the step ran (two
hashes for one file), or a cached output whose bytes moved since it was recorded: each one RUNS the step, and says why.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

_TRANSIENT = ("-journal", "-wal", "-shm")


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


class Trace:
    """Everything the recorder wrote under one directory (every process, every sub-directory)."""

    def __init__(self, directory: Path):
        self.modules: dict[str, str] = {}
        self.reads: set[str] = set()
        self.lists: set[str] = set()
        self.probes: set[str] = set()
        self.conflicts: list[str] = []
        files = sorted(Path(directory).rglob("*.trace")) if Path(directory).is_dir() else []
        if not files:
            raise FileNotFoundError(f"stepcache: no trace under {directory} — was PIPELINE_TRACE passed to the step?")
        for f in files:
            for line in f.read_text(encoding="utf-8", errors="surrogateescape").splitlines():
                kind, path, extra = (line.split("\t") + ["", ""])[:3]
                rel = _rel(path)
                if kind == "M":
                    if self.modules.get(rel, extra) != extra:
                        self.conflicts.append(rel)
                    self.modules[rel] = extra
                elif kind == "R":
                    self.reads.add(rel)
                elif kind == "L":
                    self.lists.add(rel)
                elif kind == "E":
                    self.probes.add(rel)


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


def _kind(p: Path) -> str:
    return "dir" if p.is_dir() else "file" if p.exists() else "missing"


def _listing(p: Path) -> str:
    if not p.is_dir():
        return _kind(p)
    return hashlib.sha256("\n".join(sorted(os.listdir(p))).encode()).hexdigest()[:16]


def fingerprint(paths: dict, memo: dict | None = None) -> dict:
    """The fingerprint of recorded paths ({"modules", "reads", "lists", "probes"}: iterables), taken from disk NOW."""
    reads = {}
    for r in sorted(paths["reads"]):
        p = _abs(r)
        reads[r] = sha_file(p, memo) if p.is_file() else _kind(p)
    return {"modules": code_now(paths["modules"]),
            "reads": reads,
            "lists": {r: _listing(_abs(r)) for r in sorted(paths["lists"])},
            "probes": {r: _kind(_abs(r)) for r in sorted(paths["probes"])}}


def declared_stamps(roots) -> dict[str, list]:
    """Every file under each root (a file or a directory) as [size, mtime_ns]: the inputs read in C (GDAL, ATTACH)
    that the recorder cannot see. A root that does not exist raises."""
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

    def lookup(self, step: str, argv: list[str], d: Path, declared: dict) -> tuple[dict | None, str]:
        """(the record when the step can be reused, else None; the reason in words)."""
        rp = self._rec_path(step)
        if not rp.is_file():
            return None, "no record"
        rec = json.loads(rp.read_text(encoding="utf-8"))
        if rec.get("argv") != argv:
            return None, "its arguments changed"
        if rec.get("declared") != declared:
            moved = sorted(k for k in set(declared) | set(rec.get("declared") or {})
                           if declared.get(k) != (rec.get("declared") or {}).get(k))
            return None, f"declared inputs changed: {', '.join(moved[:3])}{' …' if len(moved) > 3 else ''}"
        now_d = snapshot(d, self.memo)
        if rec.get("staging") != now_d:
            moved = sorted(k for k in set(now_d) | set(rec["staging"]) if now_d.get(k) != rec["staging"].get(k))
            return None, f"its staging inputs changed: {', '.join(moved[:3])}"
        fp = fingerprint(rec["paths"], self.memo)
        for part in ("modules", "reads", "lists", "probes"):
            if fp[part] != rec["fingerprint"][part]:
                moved = sorted(k for k in fp[part] if fp[part][k] != rec["fingerprint"][part].get(k))
                return None, f"{part} changed: {', '.join(moved[:3])}{' …' if len(moved) > 3 else ''}"
        for name, digest in rec["outputs"].items():
            p = self.root / step / "out" / name
            if not p.is_file() or sha_file(p, self.memo) != digest:
                return None, f"its kept output {name} is gone or was changed since"
        return rec, "inputs unchanged"

    def restore(self, step: str, rec: dict, d: Path) -> None:
        for name in rec["outputs"]:
            src, dst = self.root / step / "out" / name, Path(d) / name
            dst.parent.mkdir(parents=True, exist_ok=True)
            _place(src, dst)

    def record(self, step: str, argv: list[str], d: Path, declared: dict, before: dict[str, str],
               trace: Trace) -> str:
        """Keep the step just run. Returns what was done, in words; a step that cannot be keyed honestly is not kept."""
        rp = self._rec_path(step)
        if trace.conflicts:
            rp.unlink(missing_ok=True)
            return f"not kept: {trace.conflicts[0]} changed while the step ran"
        after = snapshot(d, self.memo)
        outputs = {n: s for n, s in after.items() if before.get(n) != s}
        paths = {"modules": sorted(trace.modules),
                 "reads": sorted(r for r in trace.reads if not _under(r, d)),
                 "lists": sorted(r for r in trace.lists if not _under(r, d)),
                 "probes": sorted(r for r in trace.probes if not _under(r, d))}
        fp = fingerprint(paths, self.memo)
        if fp["modules"] != trace.modules:
            rp.unlink(missing_ok=True)
            return "not kept: a module changed on disk after the step imported it"
        out_dir = self.root / step / "out"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        for name in outputs:
            dst = out_dir / name
            dst.parent.mkdir(parents=True, exist_ok=True)
            _place(Path(d) / name, dst)
        rec = {"step": step, "argv": argv, "declared": declared, "staging": before, "paths": paths,
               "fingerprint": fp, "outputs": outputs}
        tmp = rp.with_suffix(".tmp")
        tmp.write_text(json.dumps(rec, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, rp)
        return f"kept ({len(outputs)} output(s), {len(paths['modules'])} modules, {len(paths['reads'])} reads)"

    def modules_of(self, step: str) -> dict[str, str]:
        rec = json.loads(self._rec_path(step).read_text(encoding="utf-8"))
        return rec["fingerprint"]["modules"]


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
