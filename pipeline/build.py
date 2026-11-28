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

THE FAST PATH (T522) is this same command: every stage keys on the modules it really imported and the code files it
read (`TRACED`, T1171), and
`pipeline.deliver all` reuses every step whose inputs did not move (`pipeline.common.stepcache`); pipeline/README.md
"The fast path" says which edit reruns what.

A RELEASE is `python -m pipeline release --staging` (`pipeline/release.py`, T408): this build with --hold-latest, then
the checks, then LATEST, then the staging upload. Run that, not this, to ship.

THE STAGE GRAPH (`STAGE_GRAPH`, T297): clim, reach and tiles read only the atlas and the curated / source
data, so they are independent; deliver reads the reach run and the tiles (and clim.json by content):

    [atlas --promote]  ->  clim ─────────────(content)──┐
                           reach ───────────────────────┼─> deliver -> vintage check
                           tiles ───────────────────────┤
                           (after reach) inseason (content)┘

(T1120) `inseason` makes the in-season matches' generated resolution only where there is none (a fresh machine; or a
first build's made on another atlas): on the live release when one is live, else `resolve --first-build` on the atlas
+ its reach run. Where a release exists the resolution is the release's, and the build never re-makes it.

Every stage runs as its own process. `--jobs N` (default 2, at most 4: memory `heavy-jobs-one-at-a-time`)
runs independent stages side by side — clim and reach start together, tiles as soon as a slot frees —
each logging to `build-logs/<run>/<stage>.log` beside the manifest; `--jobs 1` is one after another, on
the console. A stage is UP TO DATE when its key — a digest of everything it reads:
the atlas's handles and registry, the corpus, the curated tree, the source files' sha256 (T1220), its own code —
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

from pipeline.common import source_hash
from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT, ROOTS, SOURCE
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
    `code`: the roots whose change reruns it (tests excluded) before its first recorded run (`TRACED`); `does`: one
    line for `--graph`."""
    name: str
    depends: tuple[str, ...]
    reads_content: tuple[str, ...]
    code: tuple[str, ...]
    does: str
    #: (T1120) stages this one waits for WITHOUT their run rerunning it (an order, not a key input)
    after: tuple[str, ...] = ()


#: THE STAGE GRAPH, in run order (T297: the one table; STAGES / CODE / DEPENDS / READS_CONTENT are views of it).
#: TILES BEFORE DELIVER: deliver reads the tiles' polygons and section geometry for `section_access` (T54), and
#: `pipeline.deliver all` ends with the vintage check against the shipped tiles.
#: (T1120) INSEASON BEFORE DELIVER: deliver reads the in-season matches' generated resolution (their end tokens:
#: `display.ends`, the export's named cuts) and keys it by content. It is made where it is missing — a fresh machine
#: — and nowhere else (`MADE_WHEN_MISSING`): the release's follows and the review app own it once a release exists.
STAGE_GRAPH: tuple[Stage, ...] = (
    Stage("clim", (), (), ("pipeline/gauges/feed/climatology.py", "pipeline/gauges/feed/record.py",
                           "pipeline/gauges/feed/datum.py"),
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
    # after reach: a FIRST BUILD resolves on the atlas + its reach run (`resolve --first-build`); a reach rerun does
    # not rerun it (`after`, not `depends`): an existing resolution belongs to the release that made it
    Stage("inseason", (), (), ("pipeline/regs/inseason",),
          "the in-season matches' resolution, only when there is none (or a first build's for another atlas)",
          after=("reach",)),
    Stage("deliver", ("reach", "tiles"), ("clim", "inseason"), ("pipeline",),
          "bundle -> verdicts -> status index -> export -> answers -> card gate -> store -> release"),
)
STAGES = tuple(st.name for st in STAGE_GRAPH)
#: (T1120) stages run only when their output is missing or stale (`inseason_stale`), or forced — never because their
#: key moved or they have no record: on a machine with a release the resolution is the release's (its follows
#: re-resolve it on every candidate), and a build re-resolving it on an older LATEST would refuse what deliver needs
MADE_WHEN_MISSING = ("inseason",)
AFTER = {st.name: st.after for st in STAGE_GRAPH}
# What each stage's CODE is before its first recorded run: a change to any file under these (tests excluded) reruns it.
CODE = {st.name: st.code for st in STAGE_GRAPH}
#: (T522) THE STAGES KEYED ON THE CODE THEY IMPORTED: each runs under the recorder (`pipeline/_trace.py`); the manifest
#: keeps the modules its last run imported (`code_files`) and (T1171, owner pick A 2026-11-26 "only what a stage
#: imports") the code files it read (`code_reads`: schema.sql, tile-contract.json — `stepcache.is_code_read`), the
#: distributions owning the packages it imported (`code_packages`) and the config.yaml values it read (`code_config`);
#: its code key is all of them NOW (`stepcache.code_key`) — so an edit to the review app, an audit tool or the DFO
#: suggester reruns none, an upgrade of a package a stage imports reruns it (pytest, vulture or playwright reruns none),
#: and an edit to a config value only the stages that read it. Before a stage's first recorded run it keys on `CODE`
#: (its broad roots, every file), the whole environment and the whole config.yaml.
#: (T1171) tiles and clim are traced too: their hand-picked .py roots left out `pipeline/common`, every non-.py file and
#: every installed package (a shapely / pyogrio upgrade changes the geometry and left the tiles "up to date"). P18 is
#: unchanged: it is about DATA (the registry, the corpus and place names are in no tiles key part); the tiles' code is
#: whatever the tiles process imports, and an edit to it may change the tiles.
TRACED = ("clim", "reach", "tiles", "deliver")
#: (T522) `--force deliver`: the delivery also runs every step afresh (`deliver all --no-step-cache`)
FRESH_STEPS = False
#: (T266) curated paths that are REVIEW STATE or its export, read by the apply paths and never by a stage: in no key
#: (T325) + the closed-domains list beside the marks: review state too, read by the exporter / apply / review app only
NOT_BUILD_INPUTS = ("review.marks", "review.closed")


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                     default=str).encode("utf-8")).hexdigest()[:16]


def code_digest(roots) -> str:
    """The stage's code, by content (uncommitted edits count): every file under `roots` (a root may be one file) —
    (T1171) not only the .py files: a stage reads schema.sql and tile-contract.json too — tests and caches excluded."""
    h = hashlib.sha256()
    for root in roots:
        base = REPO_ROOT / root
        for p in ([base] if base.is_file() else sorted(q for q in base.rglob("*") if q.is_file())):
            if "tests" in p.parts or "__pycache__" in p.parts or p.name == ".DS_Store":
                continue
            h.update(str(p.relative_to(REPO_ROOT)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()[:16]


#: (T331) THE DELIVER KEY HASHES WHAT DELIVER CAN READ: every path the `CURATED` model declares (stage code reads
#: curated data only through it, AGENTS 36) — not `base`, the whole tree, so an undeclared file never reruns deliver —
#: minus these, which no stage reads: review inputs and worklists (the slough page calls, the split confidence
#: overrides, the owner's found-name calls — `tools.mined_names` turns those into `found_names.json`, which IS hashed —
#: the closed-domains list) and the hand worklist with no build reader (`unmapped_waters`).
NOT_DELIVER_INPUTS = ("review.proposal_overrides", "review.closed",
                      "waters.slough_page_calls", "waters.found_name_calls", "waters.unmapped_waters",
                      # T679 #13: read by the added-streams generator only (its output, added_streams.json, is read)
                      "waters.added_stream_calls",
                      # declared by T1192 and kept out as they were before it: review state read by the review app and
                      # the proposals only; the accepted bathymetry / stocking matches, which no stage reads yet; and
                      # the closed areas, which the reach run reads (deliver is covered through its reach dependency)
                      "review.owner_carries", "review.recheck", "bathymetry.matches", "bathymetry.contour_matches",
                      "stocking.matches", "dfo_salmon.closed_areas")


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


#: (T1220) THE SOURCE HASHES CACHE, beside the manifest (so a `--side` run and a test keep their own); its format, hit
#: rule and checks are `pipeline.common.source_hash` (T1223: shared with the atlas record, which only reads it)
HASHES_NAME = source_hash.NAME
HASHES_FORMAT = source_hash.FORMAT
#: (T1220 review) `--dry-run` writes nothing: the plan hashes what the cache does not hold and keeps it in memory only
PERSIST_HASHES = True


def hashes_path() -> Path:
    return MANIFEST.with_name(HASHES_NAME)


def stamp(path: Path) -> str:
    """(T1220) A big source file's identity: `sha256:<hex>` of its BYTES — no path and no time, so a moved checkout, a
    fresh clone, a copy or a re-download of the same bytes keys the same, and a one-byte change moves the key. (Before
    T1220 it was [absolute path, size, mtime]: moving the checkout reran clim, tiles and deliver.)

    THE HIT RULE (`source_hash`): the cache in `hashes_path()` answers only when the file's size, mtime_ns, ctime_ns AND
    inode all equal its entry; anything else rehashes once (a touch, a copy, a move, a re-download), and every new
    `--side` directory pays the first hash again (about 11.5 GB read). A missing file raises (FileNotFoundError, naming
    it); a damaged cache raises, naming it. On a `--dry-run` (`PERSIST_HASHES` off) nothing is written."""
    return "sha256:" + source_hash.sha256(path, cache=hashes_path(), root=REPO_ROOT, persist=PERSIST_HASHES)


def sources() -> dict:
    return {"gpkg": SOURCE.fwa_gpkg,
            "stations": SOURCE / "bc_hydrometric_stations.json",
            "places": SOURCE / "bc_places.json",
            "hydat": SOURCE / "hydat.sqlite3"}


def deliver_sources(missing_ok: bool = False) -> dict:
    """What the deliver step reads from OUTSIDE the atlas and the corpus (N7, F06): every source
    file's sha256 (`stamp`, T1220) — the raw gpkg too (the bundle fits its catchment-area model and drainage groups
    from it)
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
    # (T679 #11) the in-season notices' end tokens `display.ends` holds words for: read from the matches' GENERATED
    # resolution (no longer in the curated digest). (T1120) Missing, only the PLAN gets a placeholder: the `inseason`
    # stage is about to make it, and deliver is keyed again after it runs (`READS_CONTENT`)
    from pipeline.regs.inseason import matches as M
    out["inseason_ends"] = None if (missing_ok and not M.resolved_path().exists()) else inseason_ends_digest()
    return out


def inseason_ends_digest() -> str:
    """The end tokens of the resolved in-season matches (`matches.end_tokens`, all deliver reads of them). A missing
    resolution raises, naming the command (`matches.load`). (T1120) Only the tokens: a first build's resolution and the
    release's later one on the same atlas key deliver alike when their ends agree (their `release` stamps differ)."""
    from pipeline.regs.inseason import matches as M
    return _sha(M.end_tokens(M.load()))


def inseason_state() -> dict:
    """(T1120) What the in-season resolution on this machine is: `{"release": <what it names>}`, or
    `{"release": None}` when there is none. The `inseason` stage's key (shown by the plan; the stage runs by
    `inseason_stale`, never by its key)."""
    from pipeline.regs.inseason import matches as M
    p = M.resolved_path()
    if not p.exists():
        return {"release": None}
    return {"release": str(json.loads(p.read_text(encoding="utf-8")).get("release") or "")}


def inseason_stale(build_dir: Path) -> str | None:
    """(T1120) Why the `inseason` stage must run, or None: there is no resolution, or it is a FIRST BUILD's
    (`first-build:<handles>`) made on another atlas — its ranges name that atlas's sections. A resolution that names a
    release is never re-made here: the release's follows and the review app own it (its vintage is theirs to check)."""
    from pipeline.common.section_handles import digest_for
    from pipeline.regs.inseason import matches as M
    on = inseason_state()["release"]
    if on is None:
        return f"no resolution ({M.resolved_path()})"
    if on.startswith(M.FIRST_BUILD) and on != M.FIRST_BUILD + digest_for(build_dir):
        # (T1120 review) a release made by that first build's deliver, from that same atlas, is live: the file is that
        # release's resolution (the two loaders give one `Release`), so it stands exactly as a release's own would
        # after an atlas promote — kept, the release's follows re-resolve it on the next candidate. Re-resolving here
        # on that older release would be refused (`resolve_match`: another atlas) and stop the build for nothing
        if on == M.FIRST_BUILD + str(live_release_handles() or ""):
            return None
        return f"the resolution is a first build's on another atlas ({on}; this atlas {digest_for(build_dir)})"
    return None


def live_release_handles() -> str | None:
    """(T1120) The atlas handles the live release was built from (its manifest's `files.core.section_handles`), or
    None when no release is live."""
    from pipeline.regs.inseason.resolve import release_dir, release_is_live
    if not release_is_live():
        return None
    m = json.loads((release_dir(None) / "manifest.json").read_text(encoding="utf-8"))
    return str(((m.get("files") or {}).get("core") or {}).get("section_handles") or "") or None


def inseason_cmd(build_dir: Path, h: "Homes") -> list[str]:
    """(T1120) The `inseason` stage's command: on the live release when one is live, else a FIRST BUILD
    (`--first-build`, on the atlas + its reach run) — chosen here, said by the plan and by the resolve itself."""
    from pipeline.regs.inseason.resolve import release_is_live
    cmd = [sys.executable, "-m", "pipeline.regs.inseason.resolve"]
    return cmd if release_is_live(h.release) else cmd + ["--first-build", "--reaches", str(h.reaches)]


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


def clim_key(m: dict | None = None) -> dict:
    """N9: what the envelope is made from — the HYDAT archive (stamp) and its release marker, the station roster that
    restricts it, its code, (T1047) the owner's ACCEPTED datum changes by content (a proposed row reruns nothing) — and
    the YEAR: `climatology` keeps the record up to last year (`date.today()`), so a new year is a new envelope."""
    hydat = SOURCE / "hydat.sqlite3"
    marker = hydat.with_suffix(".release")
    return {"hydat": stamp(hydat), "release": marker.read_text().strip() if marker.exists() else None,
            "stations": stamp(SOURCE / "bc_hydrometric_stations.json"), "year": date.today().year,
            **curated_parts("clim"), "code": stage_code("clim", m)}


#: (T1229) THE REACH RUN'S CURATED FILES KEYED BY THEIR BYTES: {key part: curated leaf} (`curated_parts`)
REACH_FILES = {
    # (T632) the run refuses a dead override id and an unplaced piece the owner did not accept: editing either file must
    # rerun it, or a build would pass on the old answer
    "overrides": "regulations.book_name_links", "unplaced": "waters.unplaced_piece_calls",
    # (T679 D4) the accepted printed-word reads the run is guarded by: accepting a list reruns it
    "word_reads": "regulations.word_reads",
    # (T1229) a confirmed closed area's new end binds once it has LANDED as a cut in the curated cuts file
    # (`dfo_salmon.landed_resolver` -> `review.proposals.landed_cut`, which reads the file itself)
    "cuts": "waters.cuts",
    # (T1250) the steelhead barrier list holds water out of the Pacific streams (RULINGS S14): a row added, moved or
    # removed changes the presence set, so editing the list must rerun the reach step
    "steelhead_barriers": "waters.steelhead_barriers"}

#: (T1229) CURATED FILES A STAGE READS WHOSE CONTENT NEEDS NO PART OF ITS KEY: {stage: {leaf: why}}. The guard
#: (`test_stage_content_keys.py`) fails on a curated leaf a stage reads that is in none of its content parts
#: (`curated_parts`; a stage it depends on; the atlas's `atlas.inputs.ATLAS_INPUTS`) and not here (the atlas's own list
#: is `NOT_ATLAS_INPUTS`), and on an entry here that names no leaf or one the key holds anyway.
_MATCH_PATH = "a path constant at import (`gauges.matches.MATCH_FILE`, through `pipeline.gauges`): never opened here"
_ATLAS_ONLY = "read by the atlas build only ({}; in `ATLAS_INPUTS`): this stage reads the atlas, never the file"
NOT_CONTENT_KEYED: dict[str, dict[str, str]] = {
    "clim": {"gauges.matches": _MATCH_PATH + " (the roster is the source `bc_hydrometric_stations.json`)"},
    "reach": {"waters.pond_calls": _ATLAS_ONLY.format("`registry.flowing.load_pond_calls`"),
              "waters.tidal_split_authority": _ATLAS_ONLY.format("`splits.coast.load_authority`"),
              "dfo_salmon.scope_places": "read only by the curation commands of `regs.dfo_salmon.entries` (`main`): the "
                                         "run reads the places they write (`dfo_salmon.places`, the `dfo` part)",
              "review.proposal_overrides": "loaded by `review.proposals.Sources.live` beside the cuts; `landed_cut` never "
                                           "reads it (confidence calls only)",
              "review.marks": "named only by `review.proposals.main` (the `--marks` default), never run by the stage"},
    "tiles": {"gauges.matches": _MATCH_PATH + " (no tile holds a gauge, P18)",
              "waters.tidal_split_authority": _ATLAS_ONLY.format("`splits.coast.load_authority`"),
              "regulations.synopsis": "imported with the `deliver.bundle` package (`place_names`): only the bundle's "
                                      "`build` reads it"},
    "deliver": {},
}


def _bytes_digest(leaf: str) -> str:
    """The sha256 (16 hex) of the file one curated leaf names; a missing file raises, naming it (AGENTS 37)."""
    v = CURATED
    for part in leaf.split("."):
        v = getattr(v, part)
    return hashlib.sha256(Path(v).read_bytes()).hexdigest()[:16]


def curated_parts(stage: str, corpus: str | None = None) -> dict:
    """(T1229) THE CURATED CONTENT A STAGE'S KEY HOLDS: {key part: digest}, the parts of `keys()` that say what is IN a
    curated file (a stage's config record, T1192, says only where the file is). The one place per stage: the guard
    (`test_stage_content_keys.py`) runs this under the recorder and takes the files it OPENS as the files the key holds
    by content, so no second list can drift from it. The atlas's are `atlas.inputs.ATLAS_INPUTS`. `corpus`: the corpus
    digest `keys()` read through the atlas's registry; else it is read here without one (the same files)."""
    if stage == "clim":
        from pipeline.gauges.feed import datum
        # (T1047) the owner's ACCEPTED datum changes by content (a proposed row reruns nothing)
        return {"datum": datum.digest(datum.load())}
    if stage in ("reach", "deliver") and corpus is None:
        from pipeline.regs.parsing.io import corpus_digest, read_all_entries
        corpus = corpus_digest(read_all_entries())
    if stage == "reach":
        from pipeline.atlas.reach.dfo_salmon import entries_digest as dfo_entries_digest
        from pipeline.atlas.reach.steelhead import fingerprint, load_list
        # (T211) the DFO salmon locations the run places: a curator placing one reruns the reach step
        return {"corpus": corpus, "steelhead": fingerprint(load_list()), "dfo": dfo_entries_digest(),
                **{part: _bytes_digest(leaf) for part, leaf in REACH_FILES.items()}}
    if stage == "tiles":
        from pipeline.atlas.splits.area_splits import area_files
        # the curated areas, and (T1229) every file a def of them names by `file` (the hand-drawn sign zones): a moved
        # vertex redraws the `sign_zone` layer
        return {"areas": _bytes_digest("waters.area_closures"),
                "area_files": {name: hashlib.sha256(p.read_bytes()).hexdigest()[:16]
                               for name, p in sorted(area_files().items())}}
    if stage == "deliver":
        return {"corpus": corpus, "curated": curated_digest()}
    raise KeyError(f"build: no curated parts for stage {stage!r} (the atlas's are atlas.inputs.ATLAS_INPUTS)")


def stage_code(stage: str, m: dict | None = None) -> dict:
    """(T522) A stage's code input: for a TRACED stage with a recorded run, `stepcache.code_key` of what that run used
    (T1171: its modules, code files, packages and config values, now, and the interpreter + SQLite); else (no record
    yet: the stage runs) {"roots": the digest of its `CODE` roots, "env": every installed distribution, "config": the
    whole config.yaml}. A record from before T1171 (no `code_packages`) is keyed as no record: the stage runs once.
    (Second review of #286) So is a record whose package or config set is EMPTY (`refused_record`): never "none"."""
    from pipeline.common import stepcache
    rec = ((m if m is not None else load_manifest())["stages"].get(stage) or {})
    if stage in TRACED and rec.get("code_files") and "code_packages" in rec and refused_record(stage, rec) is None:
        return stepcache.code_key(rec["code_files"], rec.get("code_reads") or {}, rec["code_packages"],
                                  rec["code_config"])
    return {"roots": code_digest(CODE[stage]), "env": stepcache.env_fingerprint(), "config": stepcache.config_now(["*"])}


def _unowned_of(m: dict, stage: str) -> str | None:
    from pipeline.common import stepcache
    return stepcache.unowned_note((m["stages"].get(stage) or {}).get("code_packages"))


def refused_record(stage: str, rec: dict) -> str | None:
    """(T1171, second review of #286) Why a traced stage's manifest record cannot key it, or None: one with modules but
    an EMPTY package or config set (or none at all) is a recorder miss (`stepcache.EMPTY_IS_A_MISS`) — taken as "no
    packages", every upgrade and config edit would leave the stage "up to date". The record is REFUSED: the stage is
    keyed as never recorded, so it runs, and the plan prints this reason (`plan`); a run that records an empty set again
    stops the build (`_traced_key`). A record from before T1171 (no `code_packages` at all) is not refused here: it is
    the planned rerun-once."""
    from pipeline.common import stepcache
    if stage not in TRACED or not rec.get("code_files") or "code_packages" not in rec:
        return None
    gone = stepcache.empty_parts(rec.get("code_packages"), rec.get("code_config"))
    return (f"its record names {' and '.join(gone)} (a recorder miss): refused as a record — it runs, and a run that "
            "records an empty set again stops the build") if gone else None


def keys(build_dir: Path, missing_ok: bool = False) -> dict[str, dict]:
    """Every stage's inputs, as digests (the key is `_sha` of these). `missing_ok`: the plan, before `clim`
    has made the envelope (`deliver_sources`)."""
    from pipeline.atlas.registry import load_registry
    from pipeline.common.section_handles import digest_for, registry_digest_for
    from pipeline.regs.parsing.io import corpus_digest, read_all_entries

    atlas = {"handles": digest_for(build_dir), "registry": registry_digest_for(build_dir)}
    m = load_manifest()
    corpus = corpus_digest(read_all_entries(registry=load_registry(str(build_dir / "registry.json"))))
    return {
        "clim": clim_key(m),
        "reach": dict(atlas, **curated_parts("reach", corpus),
                      # the tidal water the run takes out: T678, the atlas's one tidal record; before it, the
                      # atlas's tidal sidecars (T166/T326 limits, T212 whole waters, T665 inside lines), so an older
                      # atlas's stage keys stay what they were
                      **_tidal_key(build_dir),
                      code=stage_code("reach", m)),
        "deliver": dict(atlas, **curated_parts("deliver", corpus),
                        sources=deliver_sources(missing_ok),
                        code=stage_code("deliver", m)),
        "tiles": tiles_key(build_dir, m),
        "inseason": inseason_state(),
    }


def _tidal_key(build_dir: Path) -> dict:
    """The tidal stage input of the reach and the tiles: `{"tidal_record": digest}` of the atlas's one tidal record
    (T678). An atlas cut before the coast rule is refused (`coast.require_coast_cut`): its sidecars are no longer read."""
    from pipeline.atlas.splits import coast as _coast
    _coast.require_coast_cut(build_dir)
    return {"tidal_record": _sha(sorted(_coast.read_record(build_dir).items()))}


def tiles_key(build_dir: Path, m: dict | None = None) -> dict:
    """P18: the tiles stage reruns only when GEOMETRY can change: the atlas's sid space (handles), the FWA source, the
    curated areas, the tile code and (T1171) the tools that draw them (`tool_versions`). Not the registry, the corpus,
    place names or anything a data release changes, so a data-only `pipeline build` never rebuilds (or re-ships) the
    tiles."""
    from pipeline.common.section_handles import digest_for
    # (T173) the fish obstacles' own fetched file; missing, the key says so and the stage runs (and names the fetch)
    ob = obstacles_source()
    return {"handles": digest_for(build_dir), "gpkg": stamp(sources()["gpkg"]),
            # T166 -> T678: the atlas's tidal water comes off the map (its one record; the pre-T678 sidecar keys
            # `tidal` / `tidal_waters` / `inside_tidal` are retired, 2026-11-20)
            **_tidal_key(build_dir),
            **curated_parts("tiles"), "code": stage_code("tiles", m),
            "tools": tool_versions(),
            "obstacles": stamp(ob) if ob.exists() else None}


#: (T1171) THE OUTSIDE PROGRAMS THE TILES STAGE RUNS (`tiles.tippe`: tippecanoe draws every pass, tile-join merges them;
#: `tiles.version.stamp`: pmtiles reads and rewrites the archive's metadata, `pmtiles show` / `pmtiles edit`) and how
#: each is named in the key. The recorder sees only Python, so a `brew upgrade tippecanoe` must move the key:
#:   * tippecanoe by what `--version` prints (v2.79.0 on the build Mac): readable, and the same on another machine with
#:     the same release;
#:   * tile-join by the sha256 of its resolved binary: 2.79.0 has NO version flag (`--version`: rc 101, "unrecognized
#:     option", review of #286), and the binary's bytes change with every install of another build;
#:   * (third review of #286) pmtiles by the sha256 of its resolved binary too: it has no `--version` flag ("unknown flag
#:     --version", rc 1) and its `version` subcommand says nothing usable on the build Mac (Homebrew 1.31.2 prints
#:     "pmtiles dev, commit none, built at unknown").
TILE_TOOLS = (("tippecanoe", "--version"), ("tile-join", None), ("pmtiles", None))


def tool_versions() -> dict[str, str]:
    """{tool: its identity} for each of `TILE_TOOLS`: the first line of its version output, or `sha256:<16 hex>` of its
    resolved binary for a tool with no version flag. A tool not on $PATH is "missing" in the key — the stage then runs,
    and the preflight (`tiles.tippe.check`, which names all three, pmtiles since the re-check of #286) refuses it by
    name before the tiles stage starts — never a guess. A tool on $PATH that cannot say its
    version stops the build, naming the command."""
    out: dict[str, str] = {}
    for tool, flag in TILE_TOOLS:
        exe = shutil.which(tool)
        if exe is None:
            out[tool] = "missing"
            continue
        if flag is None:
            out[tool] = "sha256:" + hashlib.sha256(Path(os.path.realpath(exe)).read_bytes()).hexdigest()[:16]
            continue
        r = subprocess.run([exe, flag], capture_output=True, text=True, timeout=60)
        said = [ln.strip() for ln in (r.stdout + "\n" + r.stderr).splitlines() if ln.strip()]
        if r.returncode != 0 or not said:
            raise SystemExit(f"build: `{tool} {flag}` failed (rc={r.returncode}): the tiles key holds each tool's "
                             f"version — fix the install ({exe})\n{(r.stdout + r.stderr).strip()[:400]}")
        out[tool] = said[0]
    return out


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
                       Path(ROOTS.release))
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
            "tiles": [h.tiles / "atlas.pmtiles", h.tiles / "atlas.meta.json"],
            "inseason": [_resolved_path()]}


def _resolved_path() -> Path:
    from pipeline.regs.inseason import matches as M
    return M.resolved_path()


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
        "inseason": inseason_cmd(build_dir, h),
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
    bm = Path(ROOTS.release) / "basemap"
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
        stale = inseason_stale(build_dir) if s in MADE_WHEN_MISSING else None
        go = (s in force or bool(stale)) if s in MADE_WHEN_MISSING else \
            s in force or key != last or bool(missing) or bool(after) or refused_record(s, rec) is not None
        if go and s in MADE_WHEN_MISSING and SIDE is not None:
            # a side run touches nothing live, and deliver reads the live resolution: make it there first
            raise SystemExit("build (side): the in-season resolution must be made in the live tree first — "
                             f"{stale or 'forced'}:\n    {' '.join(inseason_cmd(build_dir, Homes.of(build_dir)))}")
        if go:
            runs.add(s)
        rows.append((s, key, go))
        if why is not None:
            now = input_digests(k[s])
            why.setdefault("inputs", {})[s] = now
            why.setdefault("keys", {})[s] = k[s]
            if s in MADE_WHEN_MISSING and go:
                why[s] = "forced" if s in force else stale + (
                    "" if "--first-build" not in inseason_cmd(build_dir, Homes.of(build_dir, SIDE)) else
                    " — FIRST BUILD: no release is live, so it is resolved on the atlas + its reach run "
                    "(named first-build:<atlas handles>; publish refuses it, the release re-resolves on its candidate)")
            elif s in force:
                why[s] = "forced"
            elif (refused := refused_record(s, rec)) is not None:
                why[s] = refused
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
                ups = set(DEPENDS.get(s, ())) | set(READS_CONTENT.get(s, ())) | set(AFTER.get(s, ()))
                if not ups <= done:
                    continue
                todo.remove(s)
                if failures:
                    done.add(s)                     # not started: a stage failed
                    continue
                key, go = plan_key[s], plan_go[s] or any(d in ran for d in DEPENDS.get(s, ()))
                if any(d in ran for d in READS_CONTENT.get(s, ())):
                    try:
                        k_now = keys(build_dir)[s]  # taken again: it reads that stage's output by content
                    except BaseException as ex:     # noqa: BLE001 — said in the manifest, then raised
                        # (T1120) in this process, not a stage's: the manifest names the stage it was keying
                        run["status"] = f"failed at {s} (taking its key): {_said(ex)}"
                        raise
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
                    with _LOCK:              # (T1120) raised, not a failed process: the run still names the stage
                        if run.get("status") == "running":
                            run["status"] = f"failed at {s}: {_said(ex)}"
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
        code = None
        if traced is not None:
            key, inputs, code, note = traced
            print(f"=== {stage}: {note}", flush=True)
        m["stages"][stage] = {"key": key, "at": run["started"], **({"inputs": inputs} if inputs else {}),
                              **({"code_files": code["modules"], "code_reads": code["reads"],
                                  "code_packages": code["packages"], "code_config": code["config"]} if code else {})}
        save_manifest(m)


def _traced_key(stage: str, tdir: Path, keyd: dict):
    """(T522) (key, inputs, code, note) for a traced stage that just ran: its key with the code part re-taken on what its
    processes used (`stepcache.traced_code`: the modules as imported and, T1171, the code files, packages and config
    values), `code` the record the manifest keeps. Something edited while it ran cannot key the run honestly: the key is
    None, so the next build runs it again. (T1171) Imports or config reads that cannot be keyed STOP the build, naming
    the stage — never an empty set."""
    from pipeline.common import stepcache
    try:
        code = stepcache.traced_code(stepcache.Trace(tdir), stage)
    except stepcache.Untraceable as e:
        raise SystemExit(f"build: {stage}: {e}") from e
    if code["moved"]:
        return None, None, None, f"NOT keyed — {code['moved'][0]} changed while it ran; the next build runs it again"
    del code["moved"]
    k2 = dict(keyd, code=stepcache.code_key(code["modules"], code["reads"], code["packages"], code["config"]))
    unowned = stepcache.unowned_note(code["packages"])
    return _sha(k2), input_digests(k2), code, \
        (f"keyed on the {len(code['modules'])} modules, {len(code['reads'])} code files, {len(code['packages'])} "
         f"packages and {len(code['config'])} config values it used" + (f" ({unowned})" if unowned else ""))


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
        reads = ", ".join([*st.depends, *(f"{d} (content)" for d in st.reads_content),
                           *(f"{d} (after it, order only)" for d in st.after)]) or "the atlas + curated/source"
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
    from pipeline.atlas.inputs import REBUILD, behind
    try:
        bad = behind(build_dir)
    except Exception as e:                          # noqa: BLE001 — (re-check of #286) any failure: "cannot tell"
        return f"build: WARNING — cannot tell whether the atlas needs rebuilding: {type(e).__name__}: {e}"
    if not bad:
        return None
    return (f"build: WARNING — the atlas NEEDS REBUILDING ({len(bad)} reason(s): an input or its code moved since it was "
            "built, T547 / T1171); the stages below read the OLD atlas and `pipeline release` refuses it. Rebuild: "
            f"{REBUILD}\n  " + "\n  ".join(bad))



def atlas_status_line(build_dir: Path) -> str:
    """(T1171, owner 2026-11-26: "a way to know if the atlas needs rebuilding programatically") `--status`'s atlas line:
    current, or how many reasons it needs rebuilding and the command that lists them (`atlas.inputs.CHECK`)."""
    from pipeline.atlas import inputs as I
    try:
        bad = I.behind(build_dir)
    except Exception as e:                          # noqa: BLE001 — (re-check of #286) any failure: "cannot tell"
        return f"atlas {build_dir}: cannot tell whether it needs rebuilding: {type(e).__name__}: {e}"
    if not bad:
        return f"atlas {build_dir}: current (its inputs and its code as built)"
    return (f"atlas {build_dir}: NEEDS REBUILDING — {len(bad)} reason(s), each listed by `{I.CHECK}` (rc 1); "
            f"rebuild: {I.REBUILD}")


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
    global SIDE, MANIFEST, HOLD_LATEST, FRESH_STEPS, PERSIST_HASHES
    HOLD_LATEST = a.hold_latest
    PERSIST_HASHES = not a.dry_run              # (T1220 review) a dry run writes nothing, the hash cache included
    FRESH_STEPS = "deliver" in a.force
    if a.graph:
        print(graph_text())
        return 0
    if a.status:
        mf = (a.side.resolve() / "build-manifest.json") if a.side else MANIFEST
        side_atlas = a.side.resolve() / "atlas" / (GENERATED.build().name + "_next") if a.side else None
        atlas_line = atlas_status_line(side_atlas if side_atlas is not None and side_atlas.is_dir() else GENERATED.build())
        if not mf.exists():
            print(f"build: no manifest at {mf}\n{atlas_line}")
            return 1
        last = json.loads(mf.read_text(encoding="utf-8"))["runs"][-1]
        print(f"build: last run {last['started']} {' '.join(last.get('argv') or [])} — {last['status']}")
        print(timings(last))
        logs = mf.parent / "build-logs" / last["started"].replace(":", "")
        for log in sorted(logs.glob("*.log")) if logs.is_dir() else []:
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-1:] or [""]
            print(f"  {log}: {tail[0][:140]}")
        print(atlas_line)           # (T1171, owner 2026-11-26) does the atlas need rebuilding: `atlas.inputs.CHECK`
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
    at = {"step": "plan"}
    if a.dry_run:
        return _build(a, build_dir, run, m, t_start, at)
    preflight(want_tiles=not a.no_tiles)
    m["runs"].append(run)
    try:
        return _build(a, build_dir, run, m, t_start, at)
    except BaseException as ex:  # noqa: BLE001 — recorded, then raised as it was
        close_failed(run, m, at["step"], ex, t_start)
        raise


def _said(ex: BaseException) -> str:
    """An exception in one line for the manifest: its type and the first line of what it says."""
    text = str(ex).strip().splitlines()
    return f"{type(ex).__name__}: {text[0][:200]}" if text else type(ex).__name__


def close_failed(run: dict, m: dict, step: str, ex: BaseException, t_start: float | None = None) -> None:
    """(T1120) A RUN THAT RAISED IS CLOSED, never left "running": `--status` reads `failed at <step>: <why>` — the
    stage whose process failed (`run_stage` has said so already, and that stands), else the step this process was
    in (atlas, promote, plan, a stage's key, the vintage check)."""
    if run.get("status") == "running":
        run["status"] = f"failed at {step}: {_said(ex)}"
    if t_start is not None:
        run.setdefault("wall_seconds", round(time.time() - t_start))
    save_manifest(m)


def _build(a, build_dir: Path, run: dict, m: dict, t_start: float, at: dict) -> int:
    """`main` past the preflight: `at["step"]` says where it is, for `close_failed`."""
    if not a.dry_run:
        if a.atlas:
            at["step"] = "atlas"
            if not atlas_stage(build_dir, a.promote, run, m):
                run["status"] = "atlas built, not promoted"
                save_manifest(m)
                return 0
        elif a.promote:
            at["step"] = "promote"
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
    at["step"] = "plan"
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
        # (third review of #286) a module no distribution owns is keyed by its file: the plan says which
        if s in TRACED and (note := _unowned_of(m, s)):
            print(f"           {s}: {note}")
    if a.dry_run:
        return 0
    if SIDE is None and dict((s, go) for s, _k, go in rows).get("reach"):
        print(keep_live_baseline(), flush=True)
    logs = MANIFEST.parent / "build-logs" / run["started"].replace(":", "")
    at["step"] = "stages"
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
    at["step"] = "vintage check"
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
