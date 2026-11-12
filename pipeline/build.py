"""ONE REBUILD COMMAND (P2): curated + source data -> atlas -> clim -> reach -> tiles -> deliver.

    python -m pipeline build                    # preflight, then every stage that is out of date
    python -m pipeline build --dry-run          # the plan: each stage, its key, run or up to date
    python -m pipeline build --atlas            # also build a side atlas (<build>_next) and print
                                                #   its parity report; stops there
    python -m pipeline build --atlas --promote  # ... and promote it, then run the rest
    python -m pipeline build --force reach      # rerun a stage (and everything after it)
    python -m pipeline build --graph            # the stage graph: what each stage reads, runs and writes
    python -m pipeline build --side DIR [--atlas]   # EVERY stage into DIR, nothing live touched (a proof run)
    python -m pipeline build --hold-latest      # the new release waits in data/release/CANDIDATE (LATEST untouched)

THE FAST PATH (T522) is this same command: reach and deliver key on the modules they really imported (`TRACED`), and
`pipeline.deliver all` reuses every step whose inputs did not move (`pipeline.common.stepcache`); pipeline/README.md
"The fast path" says which edit reruns what.

A RELEASE is `python -m pipeline release --staging` (`pipeline/release.py`, T408): this build with --hold-latest, then
the checks, then LATEST, then the staging upload. Run that, not this, to ship.

THE STAGE GRAPH (`STAGE_GRAPH`, T297): clim, reach and tiles read only the atlas and the curated / source
data, so they are independent; deliver reads the reach run and the tiles (and clim.json by content):

    [atlas --promote]  ->  clim ─────────────(content)──┐
                           reach ───────────────────────┼─> deliver -> vintage check
                           tiles ───────────────────────┘

Every stage runs as its own process. `--jobs N` (default 2, at most 4: memory `heavy-jobs-one-at-a-time`)
runs independent stages side by side — clim and reach start together, tiles as soon as a slot frees —
each logging to `build-logs/<run>/<stage>.log` beside the manifest; `--jobs 1` is one after another, on
the console. A stage is UP TO DATE when its key — a digest of everything it reads:
the atlas's handles and registry, the corpus, the curated tree, the source stamps, its own code —
equals the key of its last successful run in the manifest and its outputs exist. Any stage that
runs makes every later stage run. The atlas is never rebuilt to check it (it is deterministic for the same
inputs — T65 / N12 — but a rebuild takes hours and a promoted atlas is immutable: every derivative is keyed
by its handle digest): only `--atlas` builds one, and only `--promote` makes it the atlas everything else
reads.

THE GAUGE ENVELOPE IS A STAGE (N9, plan 2.11): `clim` makes `clim.json` from HYDAT
(`pipeline.gauges.feed.climatology`), keyed on the HYDAT stamp, the roster and its own code. The deliver
key holds `clim.json` by CONTENT, so after `clim` runs the deliver key is taken again: the bundle reruns
only when the envelope's bytes moved (`READS_CONTENT`). Fetching HYDAT stays outside (a 266 MB
download: `scripts/update-hydat.sh` / `data/fetch_data.py --layers hydat`); the preflight refuses
without it.

The manifest (`data/generated/build-manifest.json`) records, per run, each stage's command, key,
status, seconds and peak memory (its process tree's summed RSS, sampled); per stage, its last
successful key and the digest of each INPUT, so the plan says WHY a stage runs (`--dry-run`: "inputs
changed: code, curated"). Durations are never part of a key. The run ends with a timings table and
the strict vintage check: tiles and bundle from one atlas and one registry.

THE MARKS ARE NOT A BUILD INPUT (T266): `marks.jsonl` is the review log; it feeds the apply paths
(`pipeline.regs.review.apply`, which compute its decisions in memory), and they land the decided edits in the
curated files the stages read. It is in no key (`NOT_BUILD_INPUTS`), and a build runs with marks.jsonl absent.

SIDE RUNS (`--side DIR`, T297): every stage writes under DIR (atlas/<build>_next, reaches/, tiles/,
clim.json, deliver/ with its release/) with its own manifest; reach, tiles and deliver read the side
atlas DIR/atlas/<build>_next when there is one (`--atlas` builds it), else the promoted atlas (a side run never promotes); the live tiles,
bundle, release and LATEST are not touched. A side clim.json that differs from the live one is refused (deliver reads the live one).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT, SOURCE
from pipeline.gauges.consume.bundle_tables import CLIM_PATH

MANIFEST = GENERATED.bundle.parent / "build-manifest.json"
#: `--side DIR` (T297): every stage's outputs go under it; None = the live homes
SIDE: Path | None = None
#: (T408) `--hold-latest`: deliver names its new release in CANDIDATE, not LATEST (`pipeline release` promotes it)
HOLD_LATEST = False
#: at most this many stages at once (memory `heavy-jobs-one-at-a-time`: <= 4 workers)
MAX_JOBS = 4


@dataclass(frozen=True)
class Stage:
    """One node of the stage graph. `depends`: stages whose RUN reruns this one; `reads_content`: stages
    whose output this one keys by content (it is re-keyed after they run, and runs only if that moved);
    `code`: the .py roots whose change reruns it (tests excluded); `does`: one line for `--graph`."""
    name: str
    depends: tuple[str, ...]
    reads_content: tuple[str, ...]
    code: tuple[str, ...]
    does: str


#: THE STAGE GRAPH, in run order (T297: the one table; STAGES / CODE / DEPENDS / READS_CONTENT are views of it).
#: TILES BEFORE DELIVER: deliver reads the tiles' polygons and section geometry for `section_access` (T54), and
#: `pipeline.deliver all` ends with the vintage check against the shipped tiles.
STAGE_GRAPH: tuple[Stage, ...] = (
    Stage("clim", (), (), ("pipeline/gauges/feed/climatology.py", "pipeline/gauges/feed/record.py"),
          "the gauge envelope clim.json from HYDAT (N9)"),
    Stage("reach", (), (), ("pipeline/atlas/reach", "pipeline/atlas/graph", "pipeline/atlas/registry", "pipeline/regs",
                            "pipeline/common"),     # graph/: the tributary walk the reach run uses
          "every rule bound to its sections (deterministic)"),
    # P18: GEOMETRY ONLY. The tile code and the area cutter; names and registry changes are a data release, and the
    # tile labels catch up with `--labels` (MINIMAL-PACKAGING §6.1).
    Stage("tiles", (), (), ("pipeline/deliver/tiles", "pipeline/atlas/splits",
                            # T166: the tidal pieces the tiles leave off (`tiles/tidal.py`): T678, the atlas's
                            # record; an atlas cut before it, the reach's resolvers
                            "pipeline/atlas/reach/outside.py", "pipeline/atlas/reach/extent.py"),
          "atlas.pmtiles + layers/ + the released tile folder"),
    Stage("deliver", ("reach", "tiles"), ("clim",), ("pipeline",),
          "bundle -> verdicts -> status index -> export -> answers -> card gate -> store -> release"),
)
STAGES = tuple(st.name for st in STAGE_GRAPH)
# What each stage's CODE is: a change to any .py file under these (tests excluded) reruns it.
CODE = {st.name: st.code for st in STAGE_GRAPH}
#: (T522) THE STAGES KEYED ON THE CODE THEY IMPORTED: each runs under the recorder (`pipeline/_trace.py`); the manifest
#: keeps the modules its last run imported (`code_files`), and its code key is those files' bytes NOW — so an edit
#: to the review app, an audit tool or the DFO suggester reruns neither. Before a stage's first recorded run it keys on
#: `CODE` (its broad roots) as before. tiles and clim keep their hand-picked roots: they are NARROWER than what the
#: process imports (P18: tiles are geometry only, names catch up with --labels).
TRACED = ("reach", "deliver")
#: (T522) `--force deliver`: the delivery also runs every step afresh (`deliver all --no-step-cache`)
FRESH_STEPS = False
#: (T266) curated paths that are REVIEW STATE or its export, read by the apply paths and never by a stage: in no key
#: (T325) + the closed-domains list beside the marks: review state too, read by the exporter / apply / review app only
NOT_BUILD_INPUTS = ("review.marks", "review.closed")


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                     default=str).encode("utf-8")).hexdigest()[:16]


def code_digest(roots) -> str:
    """The stage's code, by content (uncommitted edits count): every .py file under `roots` (a root may
    be one file), tests and caches excluded."""
    h = hashlib.sha256()
    for root in roots:
        base = REPO_ROOT / root
        for p in ([base] if base.is_file() else sorted(base.rglob("*.py"))):
            if "tests" in p.parts or "__pycache__" in p.parts:
                continue
            h.update(str(p.relative_to(REPO_ROOT)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()[:16]


#: (T331) THE DELIVER KEY HASHES WHAT DELIVER CAN READ: every path the `CURATED` model declares (stage code reads
#: curated data only through it, AGENTS 36) — not `base`, the whole tree, so an undeclared file never reruns deliver —
#: minus these, which no stage reads: review inputs and worklists (the slough page calls, the split confidence
#: overrides, the owner's found-name calls — `tools.mined_names` turns those into `found_names.json`, which IS hashed —
#: the closed-domains list) and the hand worklist with no build reader (`unmapped_waters`).
NOT_DELIVER_INPUTS = ("base", "review.proposal_overrides", "review.closed",
                      "waters.slough_page_calls", "waters.found_name_calls", "waters.unmapped_waters")


def curated_digest() -> str:
    """Every curated input deliver can read, by CONTENT: each path the `CURATED` model names (a file, or every file
    under a directory) except the review state (`NOT_BUILD_INPUTS`) and `NOT_DELIVER_INPUTS`. A path the model names
    that is missing raises (P2).

    (T324) NOT BY PATH: each file contributes its path RELATIVE TO THE DECLARED ROOT (empty for a file key, the name
    inside a directory key) and its bytes, and the pairs are sorted — so moving or renaming a curated file or folder
    (the T324 naming pass) leaves the key alone, and only a change of bytes, or of a file's name inside a declared
    folder, reruns deliver. The one thing this cannot see is two declared FILES swapping their bytes; no tool does that."""
    paths: set[Path] = set()

    def walk(v):
        if isinstance(v, dict):
            for x in v.values():
                walk(x)
        elif isinstance(v, (list, tuple)):
            for x in v:
                walk(x)
        elif isinstance(v, Path):
            paths.add(v)
    walk(CURATED.model_dump())
    skip = not_build_inputs()           # T266: the review log and its export (also under `base`)
    paths -= skip | not_build_inputs(NOT_DELIVER_INPUTS)
    parts: list[tuple[str, str]] = []
    for root in sorted(paths):
        if not root.exists():
            raise FileNotFoundError(f"curated path {root} is missing")
        files = [root] if root.is_file() else _files_under(root, skip)
        for q in files:
            if q.name != ".DS_Store" and q not in skip:
                rel = "" if q == root else str(q.relative_to(root))
                parts.append((rel, hashlib.sha256(q.read_bytes()).hexdigest()))
    h = hashlib.sha256()
    for rel, digest in sorted(parts):
        h.update(rel.encode())
        h.update(digest.encode())
    return h.hexdigest()[:16]


def _files_under(root: Path, skip: set[Path]) -> list[Path]:
    """Every file under `root`, sorted, never descending into a `skip` directory (it is not even listed)."""
    out = []
    for d, dirs, files in os.walk(root):
        dirs[:] = [x for x in dirs if Path(d, x) not in skip]
        out += [Path(d, f) for f in files if Path(d, f) not in skip]
    return sorted(out)


def not_build_inputs(dotted_keys: tuple[str, ...] | None = None) -> set[Path]:
    """The curated paths in no stage key (`NOT_BUILD_INPUTS`): the marks store and the closed list (T266).
    `dotted_keys`: resolve another list instead (`NOT_DELIVER_INPUTS`, T331)."""
    out = set()
    for dotted in (NOT_BUILD_INPUTS if dotted_keys is None else dotted_keys):
        v = CURATED
        for part in dotted.split("."):
            v = getattr(v, part)
        out.add(Path(v))
    return out


def stamp(path: Path) -> list:
    """A big source file's identity without reading it: size and mtime. A missing one raises."""
    st = Path(path).stat()
    return [str(path), st.st_size, int(st.st_mtime)]


def sources() -> dict:
    from project_config import get_config
    return {"gpkg": Path(get_config().fwa_data_gpkg),
            "stations": SOURCE / "bc_hydrometric_stations.json",
            "places": SOURCE / "bc_places.json",
            "hydat": SOURCE / "hydat.sqlite3"}


def deliver_sources(missing_ok: bool = False) -> dict:
    """What the deliver step reads from OUTSIDE the atlas and the corpus (N7, F06): every source
    stamp — the raw gpkg too (the bundle fits its catchment-area model and drainage groups from it)
    — and the gauge envelope `clim.json` by CONTENT: the `clim` stage writes it, and a rewrite need
    not change its size or time. A missing one raises; only the PLAN passes `missing_ok` (the `clim`
    stage is about to make it, and deliver is keyed again after it runs — `READS_CONTENT`)."""
    out = {k: stamp(p) for k, p in sources().items()}
    if not Path(CLIM_PATH).exists():
        if missing_ok:
            out["clim"] = None
            return out
        raise SystemExit(f"build: no {CLIM_PATH} — the bundle snapshots the gauge envelope; make it:\n"
                         f"    python -m pipeline.gauges.feed.climatology --hydat "
                         f"{SOURCE / 'hydat.sqlite3'} --out {CLIM_PATH}")
    out["clim"] = hashlib.sha256(Path(CLIM_PATH).read_bytes()).hexdigest()[:16]
    # (T211) the DFO salmon rules the bundle snapshots (the typed feed), by content
    out["dfo"] = dfo_feed_digest()
    return out


def dfo_feed_digest() -> str | None:
    """The typed DFO feed (`GENERATED.regs.dfo_salmon/typed`) by content; None when there is none (the bundle then
    stops, naming the command)."""
    root = Path(GENERATED.regs.dfo_salmon) / "typed"
    files = sorted(root.glob("region-*.json")) if root.is_dir() else []
    if not files:
        return None
    h = hashlib.sha256()
    for p in files:
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def clim_key() -> dict:
    """N9: what the envelope is made from — the HYDAT archive (stamp) and its release marker, the station roster that
    restricts it, its code — and the YEAR: `climatology` keeps the record up to last year (`date.today()`), so a new
    year is a new envelope."""
    hydat = SOURCE / "hydat.sqlite3"
    marker = hydat.with_suffix(".release")
    return {"hydat": stamp(hydat), "release": marker.read_text().strip() if marker.exists() else None,
            "stations": stamp(SOURCE / "bc_hydrometric_stations.json"), "year": date.today().year,
            "code": code_digest(CODE["clim"])}


def stage_code(stage: str, m: dict | None = None):
    """(T522) A stage's code input: for a TRACED stage with a recorded run, {"traced": the bytes now of the modules that
    run imported, "env": the interpreter and installed distributions}; else the digest of its `CODE` roots."""
    from pipeline.common import stepcache
    rec = ((m if m is not None else load_manifest())["stages"].get(stage) or {})
    if stage in TRACED and rec.get("code_files"):
        return {"traced": stepcache.code_digest(stepcache.code_now(rec["code_files"])), "env": stepcache.env_fingerprint()}
    return code_digest(CODE[stage])


def keys(build_dir: Path, missing_ok: bool = False) -> dict[str, dict]:
    """Every stage's inputs, as digests (the key is `_sha` of these). `missing_ok`: the plan, before `clim`
    has made the envelope (`deliver_sources`)."""
    from pipeline.atlas.registry import load_registry
    from pipeline.atlas.reach.steelhead import fingerprint, load_list
    from pipeline.atlas.reach.dfo_salmon import entries_digest as dfo_entries_digest
    from pipeline.common.section_handles import digest_for, registry_digest_for
    from pipeline.regs.parsing.io import corpus_digest, read_all_entries

    atlas = {"handles": digest_for(build_dir), "registry": registry_digest_for(build_dir)}
    m = load_manifest()
    corpus = corpus_digest(read_all_entries(registry=load_registry(str(build_dir / "registry.json"))))
    return {
        "clim": clim_key(),
        "reach": dict(atlas, corpus=corpus, steelhead=fingerprint(load_list()),
                      # (T211) the DFO salmon locations the run places: a curator placing one reruns the reach step
                      dfo=dfo_entries_digest(),
                      # the tidal water the run takes out: T678, the atlas's one tidal record; before it, the
                      # atlas's tidal sidecars (T166/T326 limits, T212 whole waters, T665 inside lines), so an older
                      # atlas's stage keys stay what they were
                      **_tidal_key(build_dir),
                      # (T632) the run refuses a dead override id and an unplaced piece the owner did not accept:
                      # editing either file must rerun it, or a build would pass on the old answer
                      overrides=hashlib.sha256(Path(CURATED.regulations.book_name_links).read_bytes()).hexdigest()[:16],
                      unplaced=hashlib.sha256(Path(CURATED.waters.unplaced_piece_calls).read_bytes()).hexdigest()[:16],
                      # (T679 D4) the accepted printed-word reads the run is guarded by: accepting a list reruns it
                      word_reads=hashlib.sha256(Path(CURATED.regulations.word_reads).read_bytes()).hexdigest()[:16],
                      code=stage_code("reach", m)),
        "deliver": dict(atlas, corpus=corpus, curated=curated_digest(),
                        sources=deliver_sources(missing_ok),
                        code=stage_code("deliver", m)),
        "tiles": tiles_key(build_dir),
    }


def _tidal_key(build_dir: Path) -> dict:
    """The reach stage's tidal inputs: `{"tidal_record": digest}` of an atlas cut by the coast rule (T678), else the
    pre-T678 sidecar keys exactly as they were."""
    from pipeline.atlas.reach.outside import tidal_splits, tidal_waters
    from pipeline.atlas.splits import coast as _coast
    if _coast.built_by_coast_rule(build_dir):
        return {"tidal_record": _sha(sorted(_coast.read_record(build_dir).items()))}
    return {"tidal": _sha(tidal_splits(build_dir)), "tidal_waters": _sha(tidal_waters(build_dir)),
            **_inside_key(build_dir)}


def _inside_key(build_dir: Path) -> dict:
    """(T665) `{"inside_tidal": digest}` of the atlas's lines wholly in tidal water, or {} when it lists none (an atlas
    cut before the polygon rule), so an older atlas's stage keys stay what they were."""
    from pipeline.atlas.reach.outside import inside_lines
    got = inside_lines(build_dir)
    return {"inside_tidal": _sha(got)} if got else {}


def tiles_key(build_dir: Path) -> dict:
    """P18: the tiles stage reruns only when GEOMETRY can change: the atlas's sid space (handles), the FWA source, the
    curated areas and the tile code. Not the registry, the corpus, place names or anything a data release changes, so a
    data-only `pipeline build` never rebuilds (or re-ships) the tiles."""
    from pipeline.common.section_handles import digest_for
    areas = Path(CURATED.waters.areas)
    # (T173) the fish obstacles' own fetched file; missing, the key says so and the stage runs (and names the fetch)
    ob = obstacles_source()
    from pipeline.atlas.reach.outside import tidal_splits, tidal_waters
    return {"handles": digest_for(build_dir), "gpkg": stamp(sources()["gpkg"]),
            # T166: the tidal boundaries — the pieces below them come off the map
            "tidal": _sha(tidal_splits(build_dir)),
            # T212: whole tidal waters come off the map too
            "tidal_waters": _sha(tidal_waters(build_dir)),
            # T665: lines wholly in tidal water come off the map too (absent on an older atlas)
            **_inside_key(build_dir),
            "areas": hashlib.sha256(areas.read_bytes()).hexdigest()[:16], "code": code_digest(CODE["tiles"]),
            "obstacles": stamp(ob) if ob.exists() else None}


def obstacles_source() -> Path:
    """(T173) The fish obstacles the tiles stage reads (`pipeline.deliver.tiles.obstacles.GPKG`)."""
    from pipeline.deliver.tiles.obstacles import GPKG
    return Path(SOURCE / GPKG)


@dataclass(frozen=True)
class Homes:
    """Where the stages write: the live homes, or everything under a side directory (`--side`)."""
    clim: Path
    reaches: Path              # the reach run's directory
    tiles: Path
    deliver: Path | None       # `deliver all --out-dir`; None = the canonical homes
    release: Path              # the release root (r-* folders, LATEST, tiles/)

    @classmethod
    def of(cls, build_dir: Path, side: Path | None = None) -> "Homes":
        if side is None:
            return cls(Path(CLIM_PATH), GENERATED.reaches / build_dir.name, Path(GENERATED.tiles), None,
                       Path(GENERATED.release))
        side = Path(side)
        return cls(side / "clim.json", side / "reaches" / build_dir.name, side / "tiles", side / "deliver",
                   side / "deliver" / "release")

    def bundle_dir(self) -> Path:
        return self.deliver if self.deliver is not None else Path(GENERATED.bundle)


def outputs(build_dir: Path) -> dict[str, list[Path]]:
    h = Homes.of(build_dir, SIDE)
    return {"clim": [h.clim],
            "reach": [h.reaches / "report.json", h.reaches / "rule_section.jsonl.gz",  # T73: the gzip form
                      h.reaches / "dfo_salmon.jsonl.gz"],                              # T211
            "deliver": [h.bundle_dir() / "bundle.sqlite", h.bundle_dir() / "verdicts.sqlite",
                        h.bundle_dir() / "status_index.bin", h.release / "LATEST"],
            "tiles": [h.tiles / "atlas.pmtiles", h.tiles / "atlas.meta.json"]}


def commands(build_dir: Path) -> dict[str, list[str]]:
    py = [sys.executable, "-m"]
    h = Homes.of(build_dir, SIDE)
    side = SIDE is not None
    return {
        "clim": py + ["pipeline.gauges.feed.climatology", "--hydat", str(SOURCE / "hydat.sqlite3"),
                      "--out", str(h.clim)],
        "reach": py + ["pipeline.atlas.reach.cli", "--build", str(build_dir), "--out", str(h.reaches)],
        "deliver": py + ["pipeline.deliver", "all", "--build", str(build_dir)]
        + (["--reaches", str(h.reaches), "--tiles", str(h.tiles), "--out-dir", str(h.deliver)] if side else [])
        + (["--hold-latest"] if HOLD_LATEST else []) + (["--no-step-cache"] if FRESH_STEPS else []),
        "tiles": py + ["pipeline.deliver.tiles", "--build", str(build_dir), "--out", str(h.tiles)]
        + (["--release-root", str(h.release)] if side else []),
    }


def prepare_side(build_dir: Path) -> None:
    """A side run's tree: the basemap the release names is the live one (its sidecar beside the side tiles, its
    released folder linked under the side release root) — a side run builds no basemap."""
    h = Homes.of(build_dir, SIDE)
    for d in (h.tiles, h.release, h.reaches.parent):
        d.mkdir(parents=True, exist_ok=True)
    live_meta = Path(GENERATED.tiles) / "basemap.meta.json"
    if live_meta.exists() and not (h.tiles / "basemap.meta.json").exists():
        (h.tiles / "basemap.meta.json").write_bytes(live_meta.read_bytes())
    bm = Path(GENERATED.release) / "basemap"
    if bm.is_dir() and not (h.release / "basemap").exists():
        (h.release / "basemap").symlink_to(bm.resolve(), target_is_directory=True)


def load_manifest() -> dict:
    if not MANIFEST.exists():               # the first run: every stage is out of date
        return {"stages": {}, "runs": []}
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def save_manifest(m: dict) -> None:
    m["runs"] = m["runs"][-20:]
    MANIFEST.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def preflight(want_tiles: bool) -> None:
    """Refuse to start on a bad hand edit, a stale curated artifact or a missing source file — never regenerate.

    FIRST (T324, owner 2026-11-05): the fast check of the hand-edited waters files (`check_curated --waters`, seconds) —
    a bad edit fails here, not an hour into the atlas."""
    rc = subprocess.run([sys.executable, "-m", "pipeline.tools.audit.check_curated", "--waters"]).returncode
    if rc:
        raise SystemExit("build: a hand-edited waters file is wrong (check_curated --waters above); fix it, then rebuild")
    missing = [f"{k}: {p}" for k, p in sources().items() if not Path(p).exists()]
    if missing:
        raise SystemExit("build: source data missing — python data/fetch_data.py\n  "
                         + "\n  ".join(missing))
    rc = subprocess.run([sys.executable, "-m", "pipeline.tools.audit.check_curated", "--ci"]).returncode
    if rc:
        raise SystemExit("build: a curated artifact is stale (check_curated --ci above); "
                         "regenerate it with the command it prints, then rebuild")
    if want_tiles:
        from pipeline.deliver.tiles import tippe
        tippe.check()


# What each stage READS from an earlier stage: a stage reruns when one of THESE reran, not every later
# one. Tiles read only the atlas (no reach input), so a reach-only change no longer rebuilds them. Deliver reads the
# tiles' polygons and section geometry for `section_access` (T54), so a tile rebuild re-runs it.
DEPENDS = {st.name: st.depends for st in STAGE_GRAPH}
#: What a stage reads from an earlier stage BY CONTENT, in its own key: after that stage runs, the key is taken again
#: and the stage runs only if it moved. Deliver keys `clim.json`'s bytes (N7), so a `clim` run that rewrote the same
#: envelope does not rebuild the bundle.
READS_CONTENT = {st.name: st.reads_content for st in STAGE_GRAPH if st.reads_content}


def input_digests(k: dict) -> dict[str, str]:
    """A stage key's inputs, each as its own digest: what the manifest keeps so the plan can say WHICH changed."""
    return {name: _sha(v) for name, v in sorted(k.items())}


def plan(build_dir: Path, force: set[str], why: dict | None = None) -> list[tuple[str, str, bool]]:
    """[(stage, key, run?)] in order; a stage runs when its key changed, its outputs are missing, it is
    forced, or a stage it DEPENDS on runs. `why`, when given, gets {stage: reason} and {"inputs": {stage:
    input_digests}} — the reason a stage runs, printed up front."""
    m, k, out = load_manifest(), keys(build_dir, missing_ok=True), outputs(build_dir)
    rows, runs = [], set()
    for s in STAGES:
        key = _sha(k[s])
        rec = m["stages"].get(s) or {}
        last = rec.get("key")
        missing = [p for p in out[s] if not p.exists()]
        after = [d for d in DEPENDS[s] if d in runs]
        go = s in force or key != last or bool(missing) or bool(after)
        if go:
            runs.add(s)
        rows.append((s, key, go))
        if why is not None:
            now = input_digests(k[s])
            why.setdefault("inputs", {})[s] = now
            why.setdefault("keys", {})[s] = k[s]
            if s in force:
                why[s] = "forced"
            elif last is None:
                why[s] = "never built"
            elif key != last:
                was = rec.get("inputs") or {}
                moved = sorted(n for n in now if was.get(n) != now[n]) if was else []
                why[s] = "inputs changed: " + (", ".join(moved) if moved else "(no per-input record yet)")
            elif missing:
                why[s] = f"output missing: {missing[0].name}"
            elif after:
                why[s] = f"after {', '.join(after)}"
    return rows


_LOCK = threading.RLock()


def execute(build_dir: Path, rows, cmds: dict, run: dict, m: dict, skip=(), jobs: int = 1,
            inputs: dict | None = None, logs: Path | None = None, keyd: dict | None = None) -> set[str]:
    """Run the plan's stages, at most `jobs` at once, each as soon as every stage it reads (`DEPENDS`,
    `READS_CONTENT`) is done; a stage that reads an earlier one BY CONTENT is keyed again once that one ran, and
    runs only if its key moved. With `jobs` > 1 each stage logs to `logs/<stage>.log`. A failed stage stops new
    launches; the ones running finish, then the build stops. Returns the stages that ran."""
    jobs = max(1, min(int(jobs), MAX_JOBS))
    plan_go = {s: g for s, _k, g in rows}
    plan_key = {s: k for s, k, _g in rows}
    todo = [s for s, _k, _g in rows if s not in skip]
    done: set[str] = set(skip)
    ran: set[str] = set()
    failures: list[BaseException] = []
    running: dict = {}
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        while todo or running:
            for s in list(todo):
                if len(running) >= jobs:
                    break
                ups = set(DEPENDS.get(s, ())) | set(READS_CONTENT.get(s, ()))
                if not ups <= done:
                    continue
                todo.remove(s)
                if failures:
                    done.add(s)                     # not started: a stage failed
                    continue
                key, go = plan_key[s], plan_go[s] or any(d in ran for d in DEPENDS.get(s, ()))
                if any(d in ran for d in READS_CONTENT.get(s, ())):
                    k_now = keys(build_dir)[s]      # taken again: it reads that stage's output by content
                    key = _sha(k_now)
                    if keyd is not None:
                        keyd[s] = k_now
                    if inputs is not None:
                        inputs[s] = input_digests(k_now)
                    go = go or key != (m["stages"].get(s) or {}).get("key")
                    print(f"  {s:<8} {key}  {'RUN' if go else 'up to date'} "
                          f"(re-keyed after {', '.join(READS_CONTENT[s])})", flush=True)
                if not go:
                    done.add(s)
                    continue
                log = (logs / f"{s}.log") if (logs is not None and jobs > 1) else None
                kw = {}
                if inputs is not None and s in inputs:
                    kw["inputs"] = inputs[s]
                if log is not None:
                    kw["log"] = log
                if keyd is not None and s in keyd:
                    kw["keyd"] = keyd[s]
                running[pool.submit(run_stage, s, cmds[s], key, run, m, **kw)] = s
            if not running:
                if todo and not failures:
                    raise RuntimeError(f"build: no stage can start ({todo}) — the stage graph has a cycle")
                continue
            finished, _ = wait(list(running), return_when=FIRST_COMPLETED)
            for f in finished:
                s = running.pop(f)
                done.add(s)
                try:
                    f.result()
                    ran.add(s)
                except BaseException as ex:  # noqa: BLE001 — SystemExit from run_stage; stop launching
                    failures.append(ex)
    if failures:
        raise failures[0]
    return ran


def _tree_rss_kb(root: int) -> int:
    """The summed RSS (KB) of `root` and every process under it (`ps`; 0 when it cannot be read)."""
    try:
        out = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,rss="], capture_output=True, text=True,
                             timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return 0
    kids: dict[int, list[int]] = {}
    rss: dict[int, int] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        pid, ppid, r = (int(x) for x in parts)
        kids.setdefault(ppid, []).append(pid)
        rss[pid] = r
    total, stack = 0, [root]
    while stack:
        p = stack.pop()
        total += rss.get(p, 0)
        stack.extend(kids.get(p, ()))
    return total


def _run(cmd: list[str], log: Path | None = None, every: float = 5.0, env: dict | None = None) -> tuple[int, int]:
    """Run `cmd` from the repo root (console, or `log`); returns (returncode, peak tree RSS in MB, sampled)."""
    fh = open(log, "w", encoding="utf-8") if log is not None else None
    try:
        p = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=fh, stderr=subprocess.STDOUT if fh else None,
                             env={**os.environ, **env} if env else None)
        peak = 0
        while True:
            try:
                rc = p.wait(timeout=every)
                break
            except subprocess.TimeoutExpired:
                peak = max(peak, _tree_rss_kb(p.pid))
        return rc, round(peak / 1024)
    finally:
        if fh is not None:
            fh.close()


def run_stage(stage: str, cmd: list[str], key: str, run: dict, m: dict, *, inputs: dict | None = None,
              log: Path | None = None, keyd: dict | None = None) -> None:
    import shutil
    import tempfile
    where = f" (log: {log})" if log is not None else ""
    print(f"\n=== {stage}: {' '.join(cmd)}{where}", flush=True)
    if log is not None:
        log.parent.mkdir(parents=True, exist_ok=True)
    # (T522) a TRACED stage runs under the recorder; its key is then re-taken on the modules it really imported
    tdir = Path(tempfile.mkdtemp(prefix=f".trace-{stage}-", dir=MANIFEST.parent)) \
        if stage in TRACED and keyd is not None else None
    t0 = time.time()
    try:
        rc, peak = _run(cmd, log, env={"PIPELINE_TRACE": str(tdir)} if tdir else None)
        traced = _traced_key(stage, tdir, keyd) if (tdir is not None and rc == 0) else None
    finally:
        if tdir is not None:
            shutil.rmtree(tdir, ignore_errors=True)
    secs = round(time.time() - t0)
    rec = {"stage": stage, "cmd": cmd, "key": key, "status": "ok" if rc == 0 else f"rc={rc}",
           "seconds": secs, "peak_mb": peak}
    print(f"=== {stage}: {'ok' if rc == 0 else f'rc={rc}'} in {secs // 60}m{secs % 60:02d}s, peak {peak / 1024:.1f} GB",
          flush=True)
    with _LOCK:
        run["stages"].append(rec)
        if rc:
            run["status"] = f"failed at {stage}"
            save_manifest(m)
            tail = ("\n" + "".join(log.read_text(encoding="utf-8", errors="replace").splitlines(True)[-30:])
                    if log is not None and log.exists() else "")
            raise SystemExit(f"build: {stage} failed (rc={rc}); the manifest says so: {MANIFEST}{tail}")
        if traced is not None:
            key, inputs, code_files, note = traced
            print(f"=== {stage}: {note}", flush=True)
        m["stages"][stage] = {"key": key, "at": run["started"], **({"inputs": inputs} if inputs else {}),
                              **({"code_files": code_files} if traced is not None and code_files else {})}
        save_manifest(m)


def _traced_key(stage: str, tdir: Path, keyd: dict):
    """(T522) (key, inputs, code_files, note) for a traced stage that just ran: its key with the code part re-taken on
    the modules its processes imported, as they were imported. A module edited while it ran (imported twice with two
    contents, or changed on disk since) cannot key the run honestly: the key is None, so the next build runs it again."""
    from pipeline.common import stepcache
    tr = stepcache.Trace(tdir)
    if tr.conflicts or stepcache.code_now(tr.modules) != tr.modules:
        moved = tr.conflicts or sorted(k for k, v in stepcache.code_now(tr.modules).items() if tr.modules[k] != v)
        return None, None, None, f"NOT keyed — {moved[0]} changed while it ran; the next build runs it again"
    k2 = dict(keyd, code={"traced": stepcache.code_digest(tr.modules), "env": stepcache.env_fingerprint()})
    return _sha(k2), input_digests(k2), dict(sorted(tr.modules.items())), \
        f"keyed on the {len(tr.modules)} modules it imported"


def timings(run: dict) -> str:
    """The run's per-stage table: seconds and peak memory (T295)."""
    rows = [f"  {r['stage']:<8} {r['seconds'] // 60:>3}m{r['seconds'] % 60:02d}s  "
            f"{(r.get('peak_mb') or 0) / 1024:5.1f} GB  {r['status']}" for r in run["stages"]]
    wall = run.get("wall_seconds")
    return "\n".join(["build: stage timings (wall clock per stage; peak = summed RSS of its process tree)"] + rows
                     + ([f"  total    {wall // 60:>3}m{wall % 60:02d}s wall"] if wall is not None else []))


def graph_text() -> str:
    lines = ["the stage graph (python -m pipeline build), in run order:"]
    for st in STAGE_GRAPH:
        reads = ", ".join([*st.depends, *(f"{d} (content)" for d in st.reads_content)]) or "the atlas + curated/source"
        lines.append(f"  {st.name:<8} reads {reads}\n           does  {st.does}\n"
                     f"           code  {', '.join(st.code)}")
    lines.append("  (atlas)  only with --atlas: a side atlas <build>_next + its parity; --promote adopts it")
    return "\n".join(lines)


def atlas_stage(build_dir: Path, promote: bool, run: dict, m: dict) -> bool:
    """Build `<build>_next`, print its parity, and promote it only when asked. Returns whether
    the rest of the build should go on (it reads the PROMOTED atlas)."""
    nxt = (SIDE / "atlas" / (build_dir.name + "_next")) if SIDE is not None else \
        build_dir.with_name(build_dir.name + "_next")
    if nxt.is_dir():
        # (T547 §4) never build into an older run's files: a stale registry beside a new graph promoted as one atlas
        print(f"build: clearing {nxt} (an earlier atlas build's files) before building into it", flush=True)
        shutil.rmtree(nxt)
    run_stage("atlas", [sys.executable, "-m", "pipeline.atlas.build", "--full", "--out", str(nxt)],
              "-", run, m)
    run_stage("parity", [sys.executable, "-m", "pipeline.atlas.promote", str(nxt), "--dry-run"],
              "-", run, m)
    if SIDE is not None:
        print(f"\nbuild (side): {nxt} is built and its parity printed above; it is NOT promoted — the rest of "
              f"the side run reads it (a side rebuild proves the whole chain on the new atlas).")
        return True
    if not promote:
        print(f"\nbuild: {nxt} is built and its parity printed above. Promote it with\n"
              f"    python -m pipeline build --promote   (or: python -m pipeline.atlas.promote {nxt})\n"
              "and the rest of the build runs against it. Stopping here.")
        return False
    run_stage("promote", [sys.executable, "-m", "pipeline.atlas.promote", str(nxt)], "-", run, m)
    return True


def atlas_inputs_warning(build_dir: Path) -> str | None:
    """(T547) A warning naming each curated input the atlas is behind (`atlas.inputs.behind`), or None when it is
    current. A warning, not a stop: a reach rerun on the old atlas is how a curator checks a placement; `pipeline
    release` is what refuses to SHIP it."""
    from pipeline.atlas.inputs import behind
    try:
        bad = behind(build_dir)
    except (OSError, ValueError) as e:
        return f"build: WARNING — cannot tell whether the atlas is current with its curated inputs: {e}"
    if not bad:
        return None
    return (f"build: WARNING — the atlas is behind {len(bad)} curated input(s) it was built from; the stages below read "
            "the OLD atlas and `pipeline release` refuses it (T547):\n  " + "\n  ".join(bad))


def keep_live_baseline() -> str:
    """(T483 fast-fail; caught 2026-11-09) a live build about to rerun reach keeps LATEST's reach run first — the copy
    `pipeline release` diffs against (data/generated/release/baseline/<LATEST>/, `release.keep_baseline`). Lanes had run
    `pipeline build` outside a release, the reach run was overwritten, and `release --staging` had no "before" (it was
    rebuilt by hand from the old commit). When the run on disk is already another one, nothing can be kept: said loudly,
    with the release's way out, and the build goes on (refusing would not bring the old run back)."""
    from pipeline import release as R
    live = R.read_pointer("LATEST")
    if live is None or not (R.release_root() / live / "manifest.json").exists():
        return "build: no live release (LATEST) — no reach baseline to keep"
    if not (R.manifest_of(live).get("bundle") or {}).get("reach_digest"):
        return f"build: WARNING — the live release {live} names no reach digest; no reach baseline kept"
    kept, msg = R.keep_baseline(live)
    if kept:
        return f"build: reach baseline for {live}: {msg}"
    return (f"build: WARNING — {msg}. `python -m pipeline release --staging` will need --baseline DIR (a reach run of "
            f"that digest) for its binding diff")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline build", description=__doc__.split("\n")[0])
    ap.add_argument("--atlas", action="store_true", help="build a side atlas <build>_next first")
    ap.add_argument("--promote", action="store_true",
                    help="promote <build>_next (built now with --atlas, or already on disk)")
    ap.add_argument("--force", nargs="*", default=[], choices=STAGES, metavar="STAGE",
                    help=f"rerun these stages and what depends on them ({', '.join(STAGES)})")
    ap.add_argument("--no-tiles", action="store_true", help="stop after deliver (tiles take ~40 min)")
    ap.add_argument("--labels", action="store_true",
                    help="rebuild the tiles to refresh their labels (names are not in the tiles stage key, P18)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and stop")
    ap.add_argument("--jobs", type=int, default=2,
                    help=f"independent stages at once (default 2, at most {MAX_JOBS}; 1 = one after another)")
    ap.add_argument("--side", type=Path, default=None, metavar="DIR",
                    help="every stage into DIR with its own manifest; the live outputs and LATEST are not touched")
    ap.add_argument("--hold-latest", action="store_true",
                    help="(T408) deliver names its new release in data/release/CANDIDATE and leaves LATEST alone; "
                         "`python -m pipeline release` runs this, checks, then promotes")
    ap.add_argument("--graph", action="store_true", help="print the stage graph and stop")
    ap.add_argument("--status", action="store_true",
                    help="the last run: each stage's time, memory and status, and its logs (with --side: that run's)")
    a = ap.parse_args(argv)
    global SIDE, MANIFEST, HOLD_LATEST, FRESH_STEPS
    HOLD_LATEST = a.hold_latest
    FRESH_STEPS = "deliver" in a.force
    if a.graph:
        print(graph_text())
        return 0
    if a.status:
        mf = (a.side.resolve() / "build-manifest.json") if a.side else MANIFEST
        if not mf.exists():
            print(f"build: no manifest at {mf}")
            return 1
        last = json.loads(mf.read_text(encoding="utf-8"))["runs"][-1]
        print(f"build: last run {last['started']} {' '.join(last.get('argv') or [])} — {last['status']}")
        print(timings(last))
        logs = mf.parent / "build-logs" / last["started"].replace(":", "")
        for log in sorted(logs.glob("*.log")) if logs.is_dir() else []:
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-1:] or [""]
            print(f"  {log}: {tail[0][:140]}")
        return 0

    build_dir = GENERATED.build()
    if a.side is not None:
        if a.promote:
            raise SystemExit("build: a side run never promotes (--side with --promote)")
        SIDE = a.side.resolve()
        MANIFEST = SIDE / "build-manifest.json"
        SIDE.mkdir(parents=True, exist_ok=True)
        prepare_side(build_dir)
    m = load_manifest()
    run = {"started": time.strftime("%Y-%m-%dT%H:%M:%S"), "stages": [], "status": "running",
           "argv": sys.argv[1:]}
    t_start = time.time()
    if not a.dry_run:
        preflight(want_tiles=not a.no_tiles)
        m["runs"].append(run)
        if a.atlas:
            if not atlas_stage(build_dir, a.promote, run, m):
                run["status"] = "atlas built, not promoted"
                save_manifest(m)
                return 0
        elif a.promote:
            nxt = build_dir.with_name(build_dir.name + "_next")
            run_stage("promote", [sys.executable, "-m", "pipeline.atlas.promote", str(nxt)], "-",
                      run, m)

    if SIDE is not None and (SIDE / "atlas" / (build_dir.name + "_next")).is_dir():
        # A SIDE RUN READS ITS OWN ATLAS when it has one (built now with --atlas, or by an earlier run into the same
        # DIR) (2026-10-30, Sheldens): a change that lives in the atlas (a registry name) is proven end to end
        # without promoting it.
        build_dir = SIDE / "atlas" / (build_dir.name + "_next")
        prepare_side(build_dir)
    if a.dry_run and not (build_dir / "registry.json").exists():
        # (T416) a fresh clone has no atlas: the stages after it are keyed by it, so there is nothing more to plan
        print(f"build: no atlas at {build_dir} — " + ("--atlas builds it, then every stage RUNs" if a.atlas else
                                                      "run with --atlas --promote first"))
        return 0
    why: dict = {}
    rows = plan(build_dir, set(a.force) | ({"tiles"} if a.labels else set()), why)
    cmds = commands(build_dir)
    print(f"build: atlas {build_dir}" + (f" — SIDE run into {SIDE}" if SIDE is not None else ""))
    warn = atlas_inputs_warning(build_dir)
    if warn:
        print(warn, flush=True)
    for s, key, go in rows:
        skip = a.no_tiles and s == "tiles"
        print(f"  {s:<8} {key}  {'skip (--no-tiles)' if skip else 'RUN' if go else 'up to date'}"
              + (f"  ({why[s]})" if go and not skip and s in why else ""))
    if a.dry_run:
        return 0
    if SIDE is None and dict((s, go) for s, _k, go in rows).get("reach"):
        print(keep_live_baseline(), flush=True)
    logs = MANIFEST.parent / "build-logs" / run["started"].replace(":", "")
    try:
        execute(build_dir, rows, cmds, run, m, skip={"tiles"} if a.no_tiles else set(), jobs=a.jobs,
                inputs=why.get("inputs"), logs=logs, keyd=why.get("keys"))
    finally:
        run["wall_seconds"] = round(time.time() - t_start)
        save_manifest(m)
        print("\n" + timings(run))
    h = Homes.of(build_dir, SIDE)
    if SIDE is not None and h.clim.exists() and Path(CLIM_PATH).exists() \
            and h.clim.read_bytes() != Path(CLIM_PATH).read_bytes():
        run["status"] = "side clim.json differs from the live one"
        save_manifest(m)
        raise SystemExit(f"build (side): {h.clim} differs from {CLIM_PATH}, which the side deliver read — "
                         "the envelope moved; run the live clim stage first")

    # P07: every artifact of the delivery, not only tiles <-> bundle
    from pipeline.common.vintage import Artifacts, report_all
    arts = Artifacts.canonical() if SIDE is None else Artifacts(
        h.deliver / "bundle.sqlite", h.deliver / "status_index.bin", h.deliver / "ui-rules-export.json")
    ok, msg = report_all(h.tiles, arts, strict=not a.no_tiles)
    print(msg)
    run["status"] = "ok" if ok else "vintage mismatch"
    save_manifest(m)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
