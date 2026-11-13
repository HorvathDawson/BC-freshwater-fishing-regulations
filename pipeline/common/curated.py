"""Where the data is — validated once, at import, by pydantic.

    from pipeline.common.curated import CURATED, SOURCE
    defs = load_split_defs(CURATED.waters.cuts)
    roster = SOURCE / "bc_hydrometric_stations.json"

WHY A MODEL AND NOT `get_path("curated", "splits")`.

`ProjectConfig.get_path` returns `Path()` — the current directory — for a key that does not
exist. No exception, no warning. Hand that to a loader and you get an empty list, and an
empty list of curated cuts looks exactly like a build with no cuts to make. That is not a
hypothetical: a `--splits` flag with no default silently dropped all 376 curated cuts from
three full builds, one of which was promoted, and nothing anywhere looked wrong.

pydantic's `FilePath` and `DirectoryPath` refuse to construct if the path is not there. So
the failure moves from "a quiet wrong answer, three builds later" to "the process will not
start, and it names the key". That is the entire argument for the dependency, and pydantic
is already in `requirements.txt` for the parsing models.

THREE KINDS OF DATA, AND THE DIFFERENCE IS WHAT IT COSTS TO LOSE:

    source      fetched from an authority. Re-downloadable.
    generated   computed. Re-runnable.
    curated     human-owned. Not recreatable at any price.

The third splits again, and the split is what this module exists to make visible:

    authored    a person typed it — cuts, name_variants, added_lakes
    promoted    a machine produced it and a person APPROVED it — the gauge matches,
                added_streams. It LOOKS generated, which is exactly the danger: someone
                re-runs the generator "just to check" and destroys the review.

A CURATED PATH IS NEVER OVERRIDABLE BY AN ENVIRONMENT VARIABLE. That is the
`review_build` failure with a new coat of paint — the app rebuilt one directory and read
another and neither said so. There is one answer to "where is the curated data" and it
lives in `config.yaml`. Generated paths live there too, under `generated:` — see `GENERATED`.

THE ONE EXCEPTION IS THE WHOLE TREE, NEVER A FILE (T422). `CURATED_ROOT=<dir>` moves EVERY curated path under
`<dir>` (the layout of `data/curated`; `make_root` copies the live tree there): the review app's tests, a side
rebuild's app (`proposals.side_commands`) and the parity / gate side loads run on such a copy. One root means a
process reads all real files or all copies — the per-file overrides it replaced (`CURATION_MARKS`,
`CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR`, …) let a test or a side build read a mix
without noticing, so they are REFUSED when set (`RETIRED_ENV`), never ignored.
"""

from __future__ import annotations

import functools
import os
import shutil
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, DirectoryPath, FilePath, field_validator

#: The repo root. `parents[2]` because this module sits at pipeline/common/ — it was
#: parents[1] when it lived at pipeline/, and the move silently repointed config.yaml at
#: pipeline/config.yaml, which does not exist. Exactly the class of bug this file prevents
#: everywhere else, so it gets an assertion rather than a comment.
ROOT = Path(__file__).resolve().parents[2]
assert (ROOT / "config.yaml").is_file(), f"repo root wrong: no config.yaml under {ROOT}"

#: THE repo root, for the handful of modules that legitimately need one. Import this instead
#: of counting `parents[N]` yourself: the count depends on how deep the module sits, so it
#: silently becomes wrong the moment a package moves — which is exactly what happened to
#: bundle/, tiles/ and parsing/ when they were grouped, and none of them raised.
REPO_ROOT = ROOT
CONFIG = ROOT / "config.yaml"


def _absolute(v: object) -> object:
    """Resolve against the repo root, so nothing depends on the current directory.

    `pipeline/regs/dfo_salmon/match.py` used a bare `Path("pipeline/atlas/splits.json")`, which worked
    only because everything happens to be run from the repo root. A path that is correct
    from one directory and silently wrong from another is the same class of bug as a
    missing one, and harder to see.
    """
    if isinstance(v, str):
        p = Path(v)
        return p if p.is_absolute() else ROOT / p
    return v


class Waters(BaseModel):
    """Curation that shapes the atlas itself, and the lists of waters (T324 naming: `*_calls` = the owner's per-item
    decisions, `*_waters` = a list of waters).

    `tidal` and `areas` keep their pre-T324 attribute names (files `tidal_limits.json`, `area_closures.json`): the
    tiles stage keys on its code's bytes and that code reads these two, so renaming them would rebuild and re-ship the
    tiles for nothing. Rename them when that code next changes anyway."""

    model_config = ConfigDict(frozen=True)

    #: AUTHORED: every curated cut on a water (`cuts.json`, was `splits.json`); the atlas sectionizer applies them.
    cuts: FilePath
    name_variants: FilePath
    #: PROMOTED (T224): name variants found in city layers, OSM, ECCC… that the owner accepted (auto rule or
    #: review mark); each name carries its real source. Written by `pipeline.tools.mined_names`.
    found_names: FilePath
    #: AUTHORED (T327): the owner's calls on single found-name candidates (accept / hold, a stored name, display),
    #: by candidate id — was `names.OWNER` in code. Read by `pipeline.regs.review.names`; an input to `mined_names`.
    found_name_calls: FilePath
    #: THE OWNER-DECIDED CONTINUATION LINKS (`water_continue`): `reviewed` ship, `refused` never, anything else the
    #: topology proposes is held for review (owner 2026-10-29, `bundle.continuation.gate`).
    continuations: FilePath
    #: T326: THE ONE TIDAL FILE (`tidal_limits.json`) — one row per ocean-entering water: its tidal limit at the
    #: `mouth` (an alias, no cut), on an existing `split`, a new `cut`, or the `whole` water. Written by
    #: `pipeline.tools.tidal_limits` (review write-through); read by the ATLAS only (`atlas.splits.tidal.load`), which
    #: writes `tidal.resolved.json`.
    tidal: FilePath
    #: T678/T685: the owner's rulings on single lines for the coast cut (cut at a measure / whole / fresh, each row
    #: `decided`), applied as written. Hand-written only; read by the ATLAS only (`build.cut_shores`).
    shore_held: FilePath
    #: T678 (owner 2026-11-11): one row per `split` row of `tidal`: is the boundary DFO-written (quoted from the cached
    #: DFO copies)? Only DFO-written splits have authority over the tidal cut; the rest are listed for the owner, never
    #: dropped. A sidecar because `tidal` is rewritten from the marks with its own fields only. No reader yet.
    tidal_split_authority: FilePath
    #: T322 (owner 2026-10-30): the owner-CONFIRMED slough / canal polygons, applied from the slough marks by
    #: `pipeline.tools.slough_calls`; the registry folds each into the one stream through it, else ignores it
    #: (`registry.flowing.fold_confirmed_sloughs`).
    slough_calls: FilePath
    #: (owner 2026-11-13, Gagnon Creek) unnamed polygons the owner keeps as LAKES of their own: the registry's widening
    #: fold leaves them (`registry.flowing.load_pond_calls`). Hand-written only.
    pond_calls: FilePath
    #: T322: the retired slough review page's database — the owner's verdicts and free-text notes
    #: (`pipeline.regs.review.slough.page_note`). Was `sloughs/page-marks-2026-10-28.json`.
    slough_page_calls: FilePath
    #: THE USER'S KNOWN-STEELHEAD LIST (authored; user ruling 2026-10-02) — rivers that are steelhead
    #: water though no row of the book says so. `pipeline.atlas.reach.steelhead.load_list`; a new generated list is
    #: accepted with `python -m pipeline.regs.steelhead.accept` (T324).
    steelhead_waters: FilePath
    #: AUTHORED: blanket area closures (`area_closures.json`) — cut every stream crossing the polygon.
    areas: FilePath
    #: Hand-drawn admin polygons — closure zones a regulation states as an AREA rather than a
    #: reach. Selected by an `area_closures.json` def that names it in `file`; see `load_area_polys`.
    added_areas: FilePath
    added_lakes: FilePath
    #: PROMOTED, not authored — 361 minted streams whose negative `blk` ids are a live ABI.
    added_streams: FilePath
    #: The worklist for minting more added lakes (was `ungazetted.json`). No build reader BY DESIGN; the minting is a
    #: hand process. Checked by `check_curated --waters`; declared so a dead-file sweep cannot delete it.
    unmapped_waters: FilePath
    #: AUTHORED (T632, owner 2026-11-11): the pieces the reach could not place that the owner ACCEPTED staying
    #: unplaced, one row per (entry or DFO place, rule, piece, reason). Any other unplaced piece stops the reach run
    #: (`pipeline.atlas.reach.build.unplaced_gate`); `proposed` rows are not accepted and stop it too.
    unplaced_piece_calls: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class DfoSalmon(BaseModel):
    """DFO salmon: the curated places DFO's rules bind to (`places/region-*.json`, was `regulations/entries/
    dfo_salmon/`) and T321's limits wording as a closed registry (`pipeline.regs.dfo_salmon.wording`), AUTHORED; the
    feeds worker reads synced copies (`pipeline.deliver.sync_inputs`)."""

    model_config = ConfigDict(frozen=True)

    places: DirectoryPath
    wording: FilePath
    #: AUTHORED (T632, owner 2026-11-11): the DFO places that KEEP their tidal water, each with its stated reason;
    #: every other place has the run's tidal sections subtracted (`pipeline.atlas.reach.dfo_salmon.load_tidal_calls`).
    tidal_calls: FilePath
    #: AUTHORED (T679 #2): the places the page defaults and cascade scopes bind (`entries.load_scope_places`), read
    #: by the atlas promote guards too (`promote.curated_boundary_ids`: a moved cut it names stops the promote).
    scope_places: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class Review(BaseModel):
    """The curation review's REVIEWED data (T97): `marks` is the append-only store of every
    reviewer's mark (`pipeline.regs.review.marks`), checked in empty before the first mark.

    T266: the marks are REVIEW STATE. No build reads them: what they decide lands in the curated files via the
    apply paths (`pipeline.regs.review.apply`, decisions in memory, T408), so the path is NOT required to exist here
    (a build runs with it absent); `marks.load` still raises on a missing store for its readers."""

    model_config = ConfigDict(frozen=True)

    marks: Path
    #: owner calls on PROPOSED splits the marks cannot carry (the store is append-only, never
    #: edited): a cut's confidence (`marks.ConfidenceOverride`, owner 2026-10-27). Folded by T326.
    proposal_overrides: FilePath
    #: T325: the CLOSED review domains (`pipeline.regs.review.closed`) — beside the marks, never required to exist
    #: (absent = nothing closed); the apply and the review app skip a domain listed here
    closed: Path

    _abs = field_validator("marks", "proposal_overrides", "closed", mode="before")(_absolute)


class Regulations(BaseModel):
    """The synopsis corpus and the book. `synopsis` is the per-region rule corpus (was `entries/catalogue`) — the
    format doc 18 describes: a rule is a TYPE plus named CONDITIONS and the label is generated."""

    model_config = ConfigDict(frozen=True)

    synopsis: DirectoryPath
    #: AUTHORED: a book water name -> the FWA water(s) it means (was `overrides.json`).
    book_name_links: FilePath
    #: THE BOOK'S TEXT, transcribed verbatim (`definitions.md` p.80, `water-specific-tables.md`
    #: p.4, `licensing.md` pp.6-7, `region-*.md`): source text, read by the answers glossary.
    reference: DirectoryPath
    #: ACCEPTED (T679 D4): every printed-word read the reach made that matched on the accepted run; a listed read that
    #: stops matching stops the reach run (`pipeline.atlas.reach.word_reads`).
    word_reads: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class Domain(BaseModel):
    """One matching domain: an external id -> FWA keys, plus the hand review of it.

    `match_calls` is an INPUT to matching and is never written by a program. `matches` is
    regenerated wholesale. Two files rather than one is what makes a regenerate PHYSICALLY
    unable to harm the review — the safety is in the filesystem, not in code remembering.

    `None` means "declared, not built yet". Stocking and bathymetry are named before they
    exist so the freshness gate is already waiting for them.
    """

    model_config = ConfigDict(frozen=True)

    matches: FilePath | None = None
    match_calls: FilePath | None = None
    #: (T331) ECCC's stated water-body type is scraped SOURCE data now: `SOURCE / "bc_station_waterbody_type.json"`
    #: (`pipeline.gauges.generate.waterbody_type.TYPES_FILE`), not a curated key. (T324) The gauge review QUEUE
    #: (`candidates.json`) is machine output: `GENERATED.gauges.candidates`.
    #: Gauges only: AUTHORED — level stations the tide or a tidal river's backwater moves, each with its reason, joined
    #: with HYDAT's own tidal remarks by `pipeline.gauges.consume.tidal` into the bundle's `gauge.tidal` (T78).
    tidal_stations: FilePath | None = None
    #: Gauges only: AUTHORED — level stations a LAKE below them backs up (a standing backwater), each naming the lake
    #: and quoting the HYDAT remark that says so, checked against the record by `pipeline.gauges.consume.tidal` into
    #: the bundle's `gauge.backwater` (T160).
    backwater_stations: FilePath | None = None
    #: Gauges only: AUTHORED — gauges on a DIVERSION canal that speak for the river above its intake instead of the
    #: gauges named (T696, RULINGS P54), checked against HYDAT by `pipeline.gauges.consume.diversion` into the bundle's
    #: `gauge.diversion` and the intake placement.
    diversion_stations: FilePath | None = None

    _abs = field_validator("*", mode="before")(_absolute)


class RunTiming(BaseModel):
    """Run timing: the authored curation (`run_calls.json`, was `review.json`). The parsed timings (`timing.json`, `pipeline/runtiming/
    timing.py`) are source data, `SOURCE / "runtiming/timing.json"` (T331)."""

    model_config = ConfigDict(frozen=True)

    run_calls: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class InSeason(BaseModel):
    """T226: the provincial in-season notices bound to waters (`notice_waters.json`, was `matches.json`;
    `pipeline.regs.inseason.matches`), AUTHORED — written by the review app's In-season tab or by hand, never by a
    generator."""

    model_config = ConfigDict(frozen=True)

    notice_waters: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class Curated(BaseModel):
    """Every path whose loss costs human time rather than CPU."""

    model_config = ConfigDict(frozen=True)

    base: DirectoryPath
    waters: Waters
    regulations: Regulations
    review: Review
    gauges: Domain
    stocking: Domain
    bathymetry: Domain
    runtiming: RunTiming
    inseason: InSeason
    dfo_salmon: DfoSalmon

    _abs = field_validator("base", mode="before")(_absolute)

    def domain(self, name: str) -> Domain:
        """`c.domain("gauges")` — for tools that loop over all three."""
        got = getattr(self, name, None)
        if not isinstance(got, Domain):
            raise KeyError(f"{name!r} is not a matching domain; "
                           f"try one of gauges, stocking, bathymetry")
        return got


class DataTree(BaseModel):
    """Where fetched and computed data live. Not curated; losing either costs machine time."""

    model_config = ConfigDict(frozen=True)

    source: DirectoryPath
    #: Created on demand — a fresh checkout has run nothing, so this must not require it.
    generated: Path

    _abs = field_validator("*", mode="before")(_absolute)


# ======================================================================================
# generated/ — everything a program writes
# ======================================================================================


class GeneratedAtlas(BaseModel):
    """Graph builds. One subdirectory per region, plus `full/`."""

    model_config = ConfigDict(frozen=True)

    builds: Path
    #: The build the review app serves AND rebuilds into. One name, read by both — these
    #: were two independent hard-codings of "output/v2/full", and the app could rebuild one
    #: directory and read another with nothing saying so.
    default_build: str = "full"

    _abs = field_validator("builds", mode="before")(_absolute)


class GeneratedRegs(BaseModel):
    model_config = ConfigDict(frozen=True)

    extraction: Path
    parse: Path
    dfo_salmon: Path
    entries_backup: Path

    _abs = field_validator("*", mode="before")(_absolute)


class GeneratedGauges(BaseModel):
    model_config = ConfigDict(frozen=True)

    feeds: Path
    #: The matcher's RAW output (`pipeline.gauges.generate.match`), never the curated file: only
    #: `promote` writes `CURATED.gauges.matches`, and only after it has shown what would be demoted (T94).
    match: Path
    #: (T324) the REVIEW QUEUE — matches `pipeline.gauges.generate.promote` did not promote (an independent check
    #: disagreed), with their evidence. Written by promote beside the curated `matches`; committed (data/.gitignore)
    #: so `check_curated --ci` sees the queue in CI too. Machine output, so not curated.
    candidates: Path

    _abs = field_validator("*", mode="before")(_absolute)


class Generated(BaseModel):
    """Where computed data goes. Losing any of it costs CPU, never human time.

    Every field is a plain `Path`, not a `DirectoryPath`: a fresh checkout has run nothing,
    so requiring these to exist would make the config unloadable before the first build.
    That permissiveness is exactly what `output/` got wrong — a missing output directory was
    created on demand, so a build could write somewhere new and a reader could go on reading
    the old place, and neither would say so.

    The rule that replaces it is asymmetric, and it is the whole point of this class:

        a WRITER may create its directory      -> `mkdir(...)`
        a READER may not                       -> `require_build(...)`, which raises

    `require_build` names the command that produces what is missing, so the failure is one
    line and actionable instead of a FileNotFoundError three frames down naming a pickle.
    """

    model_config = ConfigDict(frozen=True)

    base: Path
    atlas: GeneratedAtlas
    reaches: Path
    bundle: Path
    tiles: Path
    added_streams: Path
    scratch: Path
    regs: GeneratedRegs
    gauges: GeneratedGauges
    runtiming: Path
    #: T226: what `pipeline.regs.inseason.publish` writes for the app (`matches.json` beside the feed; served as
    #: `/feeds/inseason/` by the local tile server, R2 `feeds/inseason/` in production)
    inseason: Path
    #: T330: the curation review's machine-written inputs (candidates, pre-verify, suggestions, the Q154 sample, the
    #: mined name candidates, the T185 proposals) — moved out of handoff/; the review app and `regs/review/*` read them
    review: Path
    #: (T435) the slough finder's output (`slough-automark.json`, `candidates_v2.json.gz`) the slough review reads
    sloughs: Path
    #: The shipped set (`data/release`), NOT a build artifact: only what an app downloads (P08/P18).
    release: Path

    _abs = field_validator("base", "reaches", "bundle", "tiles", "added_streams", "scratch",
                           "runtiming", "inseason", "review", "sloughs", "release", mode="before")(_absolute)

    # --- builds -----------------------------------------------------------------------

    def build(self, name: str | None = None) -> Path:
        """The path of a build. Does NOT check it exists — for writers and for messages."""
        return self.atlas.builds / (name or self.atlas.default_build)

    def require_build(self, name: str | None = None) -> Path:
        """The path of a build that must already be there, or raise saying how to make it."""
        p = self.build(name)
        if not p.is_dir():
            raise FileNotFoundError(
                f"no build at {p} — build it:\n"
                f"    PYTHONPATH=\"$PWD\" .venv/bin/python -m pipeline.atlas.build "
                f"--full --out {p}"
            )
        return p

    def registry(self, name: str | None = None) -> Path:
        """`registry.json` of a build that must already be there.

        This replaces `default_registry_path()`, which returned
        `output/pipeline/graph/registry.json` — a directory that never existed on disk — and
        was the declared default of seven parser tools.
        """
        return self.require_build(name) / "registry.json"

    def mkdir(self, *parts: str) -> Path:
        """A directory under `generated/`, created. For WRITERS only."""
        p = self.base.joinpath(*parts)
        p.mkdir(parents=True, exist_ok=True)
        return p


#: T422: THE sandbox — the one variable that moves curated data, and it moves ALL of it (module docstring).
ROOT_ENV = "CURATED_ROOT"

#: The per-file overrides `CURATED_ROOT` replaced, and the key each one repointed. Set (even empty) = refused:
#: honouring one would read a mix of real and copied files; ignoring one would run a "sandboxed" test on the
#: real files with nothing saying so.
RETIRED_ENV = {
    "CURATION_MARKS": "review.marks",
    "CURATION_SPLITS_JSON": "waters.cuts",
    "CURATION_ENTRIES_DIR": "regulations.synopsis",
    "CURATION_DFO_DIR": "dfo_salmon.places",
    "CURATION_INSEASON_MATCHES": "inseason.notice_waters",
    "CURATION_SLOUGH_PAGE_MARKS": "waters.slough_page_calls",
}


def refuse_retired_env() -> None:
    """Raise when a retired per-file override is set (`RETIRED_ENV`)."""
    bad = sorted(k for k in RETIRED_ENV if k in os.environ)
    if bad:
        raise RuntimeError(
            "retired curated-path override(s) set: " + ", ".join(f"{k} (was {RETIRED_ENV[k]})" for k in bad)
            + f" — unset them; point {ROOT_ENV} at a whole copy of the curated tree instead "
              "(pipeline.common.curated.make_root, T422)")


def sandbox_root() -> Path | None:
    """The curated root `CURATED_ROOT` names, or None for the live tree. Refuses a retired override, an empty
    value and a root that is not a directory."""
    refuse_retired_env()
    v = os.environ.get(ROOT_ENV)
    if v is None:
        return None
    return _checked_root(v)


@functools.lru_cache(maxsize=16)
def _checked_root(v: str) -> Path | None:
    if not v.strip():
        raise RuntimeError(f"{ROOT_ENV} is set but empty — unset it for the live tree, or name a root")
    p = Path(v).resolve()
    if not p.is_dir():
        raise FileNotFoundError(f"{ROOT_ENV}={v}: no such directory (make one with curated.make_root)")
    return None if p == _load(None, None).base.resolve() else p


def _rebase(node: object, base: Path, root: Path, key: str = "curated") -> object:
    """Every path string of the `curated:` blob moved from under `base` to under `root`."""
    if isinstance(node, dict):
        return {k: _rebase(v, base, root, f"{key}.{k}") for k, v in node.items()}
    if isinstance(node, str):
        p = Path(_absolute(node))
        if p != base and base not in p.parents:
            raise ValueError(f"{key} = {node} is outside curated.base ({base}): {ROOT_ENV} cannot move it")
        return str(root / p.relative_to(base))
    return node


def load(config: Path | None = None) -> Curated:
    """The validated curated tree — the live one, or the whole copy `CURATED_ROOT` names (T422). Cached, because
    validation stats every file.

    Raises `pydantic.ValidationError` naming the offending key when a path is wrong — which
    is the whole point, and is why nothing here has a fallback default.
    """
    return _load(config, sandbox_root())


def load_repo(config: Path | None = None) -> Curated:
    """The LIVE tree whatever `CURATED_ROOT` says — for the guards that must name the real files (a side copy is
    refused under it; a test checks the real tree was not written)."""
    return _load(config, None)


@functools.lru_cache(maxsize=8)
def _load(config: Path | None, root: Path | None) -> Curated:
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "curated" not in blob:
        raise KeyError(f"{path} has no `curated:` tree — see the restructure plan")
    cur = blob["curated"]
    if root is not None:
        cur = _rebase(cur, Path(_absolute(cur["base"])), root)
    return Curated.model_validate(cur)


def make_root(dst: Path) -> Path:
    """A whole copy of the LIVE curated tree at `dst` (created; existing files there are overwritten), for
    `CURATED_ROOT`. A copy, never links: a write through the sandbox must not reach a real file. Refuses a `dst`
    inside the live tree (or the tree itself)."""
    live = load_repo().base.resolve()
    dst = Path(dst).resolve()
    if dst == live or live in dst.parents or dst in live.parents:
        raise ValueError(f"{dst} overlaps the live curated tree ({live}): a sandbox root goes elsewhere")
    shutil.copytree(live, dst, dirs_exist_ok=True)
    return dst


@functools.lru_cache(maxsize=2)
def load_data_tree(config: Path | None = None) -> DataTree:
    """Where `source/` and `generated/` are."""
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "data_tree" not in blob:
        raise KeyError(f"{path} has no `data_tree:` — see the restructure plan")
    return DataTree.model_validate(blob["data_tree"])


@functools.lru_cache(maxsize=2)
def load_generated(config: Path | None = None) -> Generated:
    """The generated tree. Cross-checked against `data_tree.generated` so the two roots
    cannot drift into naming different directories."""
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "generated" not in blob:
        raise KeyError(f"{path} has no `generated:` tree — `output:` was retired into it")
    got = Generated.model_validate(blob["generated"])
    root = DataTree.model_validate(blob["data_tree"]).generated
    if got.base != root:
        raise ValueError(
            f"config.yaml disagrees with itself: data_tree.generated={root} but "
            f"generated.base={got.base}. They must name the same directory."
        )
    return got


class _Lazy:
    """`CURATED.waters.cuts` without paying validation at import of an unrelated module.

    Importing `pipeline.common.curated` must not stat twenty files just because something wanted a
    type from this module. The first ATTRIBUTE access loads and validates; everything after
    is cached.
    """

    def __init__(self, loader):
        self._loader = loader

    def __getattr__(self, name: str):
        return getattr(self._loader(), name)

    def __repr__(self) -> str:
        return f"<lazy {self._loader.__name__} {CONFIG}>"

    # --- so a lazy DIRECTORY behaves like the Path it stands for -----------------------
    #
    # `SOURCE` is meant to be usable anywhere a Path is. Without these, `str(SOURCE)` gives
    # the repr above and `Path(str(SOURCE))` silently becomes a relative path named
    # "<lazy ...>" — which is how the bundler ended up looking for its roster in a directory
    # that could never exist, and reporting success with five tables missing.

    def _dir(self):
        got = self._loader()
        return got.source if hasattr(got, "source") else got

    def __truediv__(self, other):
        """So `SOURCE / "roster.json"` reads naturally where SOURCE is a directory."""
        return self._dir() / other

    def __fspath__(self) -> str:
        return str(self._dir())

    def __str__(self) -> str:
        return str(self._dir())


#: The curated tree. Import this, not the loader.
CURATED = _Lazy(load)

#: `SOURCE / "bc_hydrometric_stations.json"` — fetched data, addressed through config so a
#: fetcher and a reader cannot disagree about where a file went.
SOURCE = _Lazy(load_data_tree)


#: `GENERATED.tiles`, `GENERATED.require_build()` — computed data, addressed through config
#: so a writer and a reader cannot disagree about where a build went.
GENERATED = _Lazy(load_generated)


def generated(*parts: str) -> Path:
    """A path under `data/generated/`, creating the directory. Nothing here is precious."""
    p = load_data_tree().generated.joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p
