"""INPUT-KEYED REUSE (T522): a step reruns only when something it READ changed — by content, never by time.

The recorder (`pipeline/_trace.py`, on with `PIPELINE_TRACE=<dir>`) lists what a step's processes imported, read,
listed and probed, each STAMPED when it happened. This module turns that into a FINGERPRINT — each repo module's bytes
as imported, each file read by content, each directory listed by its names, each probed path by its kind, the
interpreter and every installed distribution (`env_fingerprint`) — and answers "is it still the same?" by taking the
same fingerprint from disk now. Same code + same inputs is the same output (AGENTS 19: every step is deterministic), so
a matching step's recorded outputs are its outputs.

Two users:

  * `pipeline build` keys every STAGE on the modules it really imported and (T1171) the code files it read
    (`build.TRACED`, `is_code_read`), instead of every .py under broad roots: an edit to the review app, an audit tool
    or the DFO suggester reruns none, and an edit to `schema.sql` reruns deliver. The atlas build records the same
    stamp in its meta (`atlas.inputs.record_code`).
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
import re
import hashlib
import json
import os
import shutil
import sys
from functools import lru_cache
from pathlib import Path

from pipeline import _trace
from pipeline.common.curated import REPO_ROOT

#: (T1171) the build-record machinery (`_trace.RECORD_MACHINERY`): left out of the atlas's code record
RECORD_MACHINERY = True

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
    """The interpreter, the SQLite library and every installed distribution (name==version): what the recorder leaves
    out (`sys.prefix`) and what a step's third-party code is. A package upgrade, a new interpreter or a new SQLite reruns
    every step.

    (T1171) THE SQLITE LIBRARY BY ITS OWN VERSION: `sys.version` does not name it, it can be upgraded under the same
    interpreter (a conda / brew `sqlite`), and the bundle's bytes are SQLite's pages. NO INSTALL PATH: the interpreter's
    location is not what it computes (the same version and packages in another venv make the same bytes), and a key
    holding a path could never match on another machine."""
    import sqlite3
    from importlib import metadata
    dists = sorted({f"{(d.metadata['Name'] or '?').lower()}=={d.version}" for d in metadata.distributions()})
    return {"python": sys.version, "sqlite": sqlite3.sqlite_version,
            "distributions": hashlib.sha256("\n".join(dists).encode()).hexdigest()[:16], "count": len(dists)}


#: (T1171) CODE THAT IS NOT A MODULE: a file a stage reads under these is part of its code key, by content (the bundle's
#: `schema.sql` / `indexes.sql`, the tiles' `tile-contract.json`). The recorder's M lines are `.py` files only; without
#: this a commit that changed only `schema.sql` left deliver "up to date". The data a stage reads (curated, source,
#: generated) is keyed by the stage's own key parts, and config.yaml by the VALUES it reads (C lines, `config_now`).
#: (Third review of #286) The map style is code the tiles stage reads too: `tiles.assets.wanted` takes every font the
#: style names (`style_fonts`, through `version.to_release` -> `assets.write`), so a font added to it reruns the tiles
#: stage. That alone does not refresh the shipped assets: an existing tile version folder whose tile bytes are unchanged
#: is reused, assets and all (a follow-up ticket: the assets need their own key).
CODE_READ_ROOTS = ("pipeline/", "app/packages/map/style/")
#: the settings file: keyed by the values read from it, never by its bytes
CONFIG_FILE = "config.yaml"


def is_code_read(rel: str) -> bool:
    """Whether a recorded read (repo-relative, as `Trace` keys it) is code: under `CODE_READ_ROOTS`. An absolute path
    (outside the repo) never is."""
    return rel.startswith(CODE_READ_ROOTS)


class Untraceable(RuntimeError):
    """(T1171) A run whose config reads cannot be keyed: config.yaml read with no value recorded, or a run of repo code
    that recorded no package or no config value at all (`EMPTY_IS_A_MISS`). Never an empty set: the caller raises,
    naming the stage. (Third review of #286) A package no distribution owns is NOT one: it is keyed by its file
    (`distributions_of`)."""


def stage_env() -> dict:
    """(T1171) What a stage key holds of the interpreter beside its packages: the Python build and the SQLite library
    (`sys.version` does not name it; a conda / brew `sqlite` upgrades it under the same interpreter). No install path."""
    import sqlite3
    return {"python": sys.version, "sqlite": sqlite3.sqlite_version}


#: (T1171, third review of #286) the prefix of a package entry that NO installed distribution owns: `module:<top>=<where>`
#: (`distributions_of`), keyed by the bytes at `where` (`file_identity`) instead of a version. What it covers, each found
#: in a real setup: a uv / virtualenv venv's `_virtualenv.py` (installed by the venv tool, owned by no distribution), an
#: entry script outside the repo (`__main__`, and `__mp_main__` in every spawn worker), a PEP 660 editable install with no
#: `top_level.txt`, a conda package with no RECORD. Before, each STOPPED the build after the stage had run, before the
#: manifest was saved — so every build repeated the stage and the stop.
UNOWNED = "module:"


def unowned_entry(top: str, where: str) -> str:
    return f"{UNOWNED}{top}={where}"


def unowned_parts(entry: str) -> tuple[str, str]:
    """(top, where) of a `module:<top>=<where>` entry."""
    top, _, where = entry[len(UNOWNED):].partition("=")
    return top, where


def file_identity(where: str) -> str:
    """(T1171) A file or package directory as a key holds it: `sha256:<16 hex>` of its bytes (a directory: every file
    under it by relative name and bytes, caches left out), "missing" for one gone, "no file" when the import named none
    (then nothing can move it: said by `unowned_note`)."""
    if not where:
        return "no file"
    p = Path(where)
    if p.is_file():
        return "sha256:" + hashlib.sha256(p.read_bytes()).hexdigest()[:16]
    if not p.is_dir():
        return "missing"
    h = hashlib.sha256()
    for q in sorted(x for x in p.rglob("*") if x.is_file() and "__pycache__" not in x.parts and x.name != ".DS_Store"):
        h.update(str(q.relative_to(p)).encode("utf-8", "surrogateescape"))
        h.update(b"\0")
        h.update(q.read_bytes())
    return "sha256:" + h.hexdigest()[:16]


def distributions_of(tops, stage: str = "the run", where: dict | None = None) -> list[str]:
    """The installed distributions that own each imported top-level package (`P` lines), sorted, lower-case; (third
    review of #286) one NO distribution owns is the entry `module:<top>=<where>` (`UNOWNED`: keyed by its file's bytes,
    `file_identity`), never a stop. `where`: {top: the file or package directory it was loaded from} (the P lines')."""
    from importlib import metadata
    owners = metadata.packages_distributions()
    out: set[str] = set()
    for top in sorted(tops):
        dists = owners.get(top)
        if not dists:
            out.add(unowned_entry(top, (where or {}).get(top, "")))
            continue
        out |= {re.sub(r"[-_.]+", "-", d).lower() for d in dists}      # PEP 503: one spelling everywhere
    return sorted(out)


def package_versions(dists) -> dict[str, str]:
    """{distribution: its installed version NOW} ("missing" for one no longer installed: the key moves); an `UNOWNED`
    entry: its file's identity now (`file_identity`)."""
    from importlib import metadata
    out = {}
    for d in sorted(dists):
        if d.startswith(UNOWNED):
            out[d] = file_identity(unowned_parts(d)[1])
            continue
        try:
            out[d] = metadata.version(d)
        except metadata.PackageNotFoundError:
            out[d] = "missing"
    return out


#: (T1187) how a record SAYS where an unowned module was: relative to the interpreter's prefix or to the repo, never an
#: absolute path (the record is read on other checkouts and venvs); a file under neither is named by its file name only.
#: THE PREFIX FIRST (review of the T1187 PR): a venv inside the repo and a worktree with a linked .venv say one place
PLACES = (("<prefix>", lambda: Path(sys.prefix)), ("<base_prefix>", lambda: Path(sys.base_prefix)),
          ("<repo>", lambda: REPO_ROOT))
ELSEWHERE = "<elsewhere>"
#: entry scripts: their NAME finds no file (every process has its own `__main__`), so the recorded place finds it
ENTRY_NAMES = ("__main__", "__mp_main__")


def portable_where(where: str) -> str:
    """(T1187) `where` (an absolute file or package directory) as a record says it: `<repo>/rel`, `<prefix>/rel`,
    `<base_prefix>/rel`, else `<elsewhere>/<name>`; "" stays "" (the import named no file)."""
    if not where:
        return ""
    p = Path(where).resolve()
    for tag, base in PLACES:
        try:
            return f"{tag}/{p.relative_to(base().resolve())}"
        except ValueError:
            continue
    return f"{ELSEWHERE}/{p.name}"


def unowned_identity_now(top: str, where: str) -> str:
    """(T1187) The bytes NOW of a module no distribution owns (`file_identity`), FOUND BY ITS NAME
    (`importlib.util.find_spec`: the place this interpreter would import it from), so the same bytes in another venv or
    checkout are the same identity. An entry script (`ENTRY_NAMES`) is found by its recorded place (`portable_where`,
    or an absolute path in a record from before T1187). "missing" only when no spec is found (the record then says so);
    a spec with no file is "no file", as `file_identity` records it."""
    import importlib.util
    if top in ENTRY_NAMES:
        tag, _, rel = where.partition("/")
        bases = dict(PLACES)
        if tag in bases:
            return file_identity(str(bases[tag]() / rel))
        return file_identity(where) if where and Path(where).is_absolute() else "missing"
    try:
        spec = importlib.util.find_spec(top)
    except (ImportError, ValueError):               # ValueError: in sys.modules with no spec
        spec = None
    if spec is None:
        return "missing"
    return file_identity(_trace._where(spec.origin, spec.submodule_search_locations))


def unowned_note(packages) -> str | None:
    """The words a plan or a run prints for the packages it keys by their file (`UNOWNED`), or None for none."""
    tops = sorted(unowned_parts(d)[0] for d in packages or () if d.startswith(UNOWNED))
    if not tops:
        return None
    return (f"{len(tops)} imported module(s) no installed distribution owns, keyed by their file's bytes instead of a "
            f"version: {', '.join(tops)}")


def config_path() -> Path:
    return REPO_ROOT / CONFIG_FILE


def config_now(keys) -> dict[str, str]:
    """{config key: digest of its value in config.yaml NOW} for recorded keys (`*` = the whole file; "missing" for a
    key that is gone)."""
    from pipeline.common.curated import read_yaml     # (T1192) the loaders' rules: a duplicate key is refused
    blob = read_yaml(config_path()) or {}
    out = {}
    for key in sorted(keys):
        v = blob
        if key != "*":
            for part in key.split("."):
                if not isinstance(v, dict) or part not in v:
                    v = _MISSING
                    break
                v = v[part]
        out[key] = _trace.CONFIG_MISSING if v is _MISSING else _trace.config_digest(v)
    return out


_MISSING = object()


def is_machinery(rel: str) -> bool:
    """(T1171) Whether a repo module is the build-record machinery: it assigns `RECORD_MACHINERY = True` at top level
    (`_trace`, this module, `atlas.inputs`). A rule read from the file, so a new machinery module says so where it is."""
    p = _abs(rel)
    return p.is_file() and re.search(r"^RECORD_MACHINERY = True\b", p.read_text(encoding="utf-8"), re.M) is not None


#: (T1171, second review of #286) WHY AN EMPTY PACKAGE OR CONFIG SET IS REFUSED, never read as "none": every stage (and
#: the atlas build) imports `pipeline.common.curated`, which imports PyYAML, and takes its paths from config.yaml through
#: it (or `project_config`) — so a run that recorded no package or no config value is a recorder that missed them, and a
#: key or an atlas record holding the empty set would leave every package upgrade or config edit out of it ("up to
#: date"). Measured on the real imports (2026-11-26): clim 6 distributions and 3 config blocks, the others more.
EMPTY_IS_A_MISS = ("every stage imports PyYAML and takes its paths from config.yaml (`pipeline.common.curated`), so "
                   "an empty set is a recorder miss, never \"none\" — it would key no package upgrade or config edit")


def empty_parts(packages, config) -> list[str]:
    """The parts of a code record that are EMPTY where a real run always has some (`EMPTY_IS_A_MISS`), in words."""
    return [w for w, v in (("no package", packages), ("no config value", config)) if not v]


def code_reads_now(files) -> dict[str, str]:
    """{file: sha256 now} for recorded code reads (its kind, "missing" / "dir", for one that is no file now)."""
    out = {}
    for rel in sorted(files):
        p = _abs(rel)
        k = _trace.kind(str(p))
        out[rel] = hashlib.sha256(p.read_bytes()).hexdigest() if k == "file" else k
    return out


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
        self.packages: set[str] = set()       # (T1171) P lines: third-party top-level packages imported
        self.package_files: dict[str, str] = {}   # (T1171) {top: the file / directory it was loaded from}
        self.config: dict[str, str] = {}      # (T1171) C lines: config key -> digest of the value read
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
                if kind == "P":
                    self.packages.add(path)
                    if extra and not self.package_files.get(path):
                        self.package_files[path] = extra
                    continue
                if kind == "C":
                    if self.config.get(path, extra) != extra:
                        self.moved.append(f"config {path} (two values)")
                    self.config[path] = extra
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


def traced_code(trace: "Trace", stage: str = "the run") -> dict:
    """(T1171) The code a traced run ran on: {"modules": {module: sha256 as imported}, "reads": {code file it read
    (`is_code_read`): sha256}, "packages": [the distributions owning what it imported], "config": {config key read:
    digest}, "moved": [...]}. `moved` names each module, code file or config value that changed while it ran — imported
    with two contents, read with two stamps or values, or different now — and a run with any cannot be keyed honestly
    (its caller keys nothing). Raises `Untraceable` (never an empty set) for a package no distribution owns,
    config.yaml read with no value recorded, or a run of repo code that recorded no package or no config value at all
    (`EMPTY_IS_A_MISS`)."""
    reads = {r: s for r, s in sorted(trace.reads.items()) if is_code_read(r)}
    now = code_reads_now(reads)
    if CONFIG_FILE in trace.reads and not trace.config:
        raise Untraceable(f"{stage} read {CONFIG_FILE} but recorded no value of it: a reader outside the two config "
                          "loaders (`curated`, `project_config`), whose reads cannot be keyed")
    dists = distributions_of(trace.packages, stage, trace.package_files)
    if trace.modules and (gone := empty_parts(dists, trace.config)):
        raise Untraceable(f"{stage} recorded {' and '.join(gone)}: {EMPTY_IS_A_MISS}")
    # AFTER the content is hashed: each code file still as it was when it was read (else the hash may be of bytes the
    # run never saw), each module as imported, each config value as read
    cfg = config_now(trace.config) if trace.config else {}
    moved = list(trace.conflicts) + sorted(k for k, v in code_now(trace.modules).items() if trace.modules[k] != v) \
        + [x for x in trace.moved if is_code_read(x.split(" (")[0]) or x.startswith("config ")] \
        + [r for r, s in reads.items() if _trace.stamp(str(_abs(r))) != s] \
        + [f"config {k}" for k, d in trace.config.items() if cfg.get(k) != d]
    return {"modules": dict(sorted(trace.modules.items())), "reads": now, "packages": dists,
            "config": dict(sorted(trace.config.items())), "moved": moved}


def code_key(modules, reads, packages, config) -> dict:
    """(T1171) A stage's code key from what its record names, all taken NOW: the modules' and code files' bytes, the
    versions of the distributions it imported, the config values it read, and `stage_env`. The same function keys a
    run that just finished and the next plan, so an unchanged tree gives the same key."""
    # an UNOWNED entry by its name, not its path: the key holds the file's bytes, never where this machine keeps it
    pk = {(UNOWNED + unowned_parts(d)[0] if d.startswith(UNOWNED) else d): v for d, v in package_versions(packages).items()}
    return {"traced": code_digest(code_now(modules)), "reads": code_digest(code_reads_now(reads)),
            "packages": pk, "config": config_now(config) if config else {}, **stage_env()}


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
        if "packages" not in (rec.get("paths") or {}):
            return None, "its record predates T1171 (it names no packages for the stage key)"
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
                         "probes": sorted(filter(outside, trace.probes)),
                         # (T1171) what a reuse hands the enclosing stage's key (`write_modules_trace`)
                         "packages": sorted(trace.packages), "config": dict(sorted(trace.config.items())),
                         "package_files": dict(sorted(trace.package_files.items()))}
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
        if not trace.modules:
            # every step is pipeline code: a trace with no repo module is a recorder that saw nothing (T835: a
            # checkout under /tmp), and a record keyed on nothing would be reused whatever the code says
            return "the recorder saw no module of this repo (is the checkout under a skipped root?)"
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


def write_modules_trace(directory: Path, modules: dict[str, str], name: str, reads=(), packages=(),
                        config: dict | None = None, package_files: dict | None = None) -> None:
    """A reused step's code, written as a trace so the enclosing stage's code key still holds it (the stage key must
    name the code of every step, run or reused): its modules, and (T1171) the code files it read (`is_code_read`;
    `reads`: the record's read paths), stamped now — `reuse` has just matched them by content — the packages it imported
    and the config values it read (`package_files`: where each package was loaded from, for one no distribution owns)."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    lines = "".join(f"M\t{_abs(rel)}\t{d}\n" for rel, d in sorted(modules.items()))
    lines += "".join(f"R\t{_abs(rel)}\t{_trace.stamp(str(_abs(rel)))}\n" for rel in sorted(reads) if is_code_read(rel))
    lines += "".join(f"P\t{top}\t{(package_files or {}).get(top, '')}\n" for top in sorted(packages))
    lines += "".join(f"C\t{k}\t{d}\n" for k, d in sorted((config or {}).items()))
    (Path(directory) / f"reused-{name}.trace").write_text(lines, encoding="utf-8")
