"""Where the data is — validated once, at import, by pydantic.

    from pipeline.common.curated import CURATED, SOURCE
    defs = load_split_defs(CURATED.waters.cuts)
    roster = SOURCE / "bc_hydrometric_stations.json"

WHY A MODEL AND NOT `get_path("curated", "splits")`.

`ProjectConfig.get_path` returned `Path()` — the current directory — for a key that did not
exist. No exception, no warning (it raises since T435). Hand that to a loader and you get an empty list, and an
empty list of curated cuts looks exactly like a build with no cuts to make. That is not a
hypothetical: a `--splits` flag with no default silently dropped all 376 curated cuts from
three full builds, one of which was promoted, and nothing anywhere looked wrong.

pydantic's `FilePath` and `DirectoryPath` refuse to construct if the path is not there. So
the failure moves from "a quiet wrong answer, three builds later" to "the process will not
start, and it names the key". That is the entire argument for the dependency, and pydantic
is already in `requirements.txt` for the parsing models.

ONE SCHEMA FOR THE WHOLE FILE (T1192). `Config` covers every block of config.yaml, refuses an unknown key at every
level and has no field default and no `null`: `parse_config` is the one load path, the loaders here and
`project_config.get_config()` are views on it, and a bad key fails as "config.yaml: unknown key curated.waters.cutz" /
"config.yaml: missing key source.fwa_gpkg" (`ConfigError`).

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
another and neither said so. There is one answer to "where is the curated data": its root is
`roots.curated` in `config.yaml`, and each file's name under it is declared HERE, on its field
(`At`). Generated paths work the same way, under `roots.generated` — see `GENERATED`.

WHAT LIVES WHERE. config.yaml holds the five roots, the one named source file that is a real
choice (`source.fwa_gpkg`) and the one setting (`atlas_build`); every other file's name, what it
is and who owns it are this module's (`At` on the field, the `#:` note above it). `parse_config`
puts the two together (`read_config`) before validating, so a name stated in both places, a key
in neither, or one missing is refused naming the key.

THE ONE EXCEPTION IS THE WHOLE TREE, NEVER A FILE (T422). `CURATED_ROOT=<dir>` moves EVERY curated path under
`<dir>` (the layout of `data/curated`; `make_root` copies the live tree there): the review app's tests, a side
rebuild's app (`proposals.side_commands`) and the parity / gate side loads run on such a copy. One root means a
process reads all real files or all copies — the per-file overrides it replaced (`CURATION_MARKS`,
`CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR`, …) let a test or a side build read a mix
without noticing, so they are REFUSED when set (`RETIRED_ENV`), never ignored.
"""

from __future__ import annotations

import copy
import functools
import os
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Annotated

import yaml
from pydantic import (BaseModel, ConfigDict, DirectoryPath, FilePath, PrivateAttr, ValidationError, ValidationInfo,
                      field_validator, model_validator)

from pipeline import _trace

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


#: (T1192) Every model of the file: immutable, and an UNKNOWN key refused (pydantic's default `extra="ignore"` dropped a
#: misspelt key without a word, and an optional field it was meant for silently became `None`). No field has a default
#: and none takes `null`: a key config.yaml does not state is a missing key, never "not built".
_STRICT = ConfigDict(frozen=True, extra="forbid")


@dataclass(frozen=True)
class At:
    """A file's or folder's NAME under the root of its tree (`TREES`), declared on its field:
    `cuts: Annotated[FilePath, At("waters/cuts.json")]`. Schema-side data, never a field default: `read_config` puts
    every name into the parsed file before validation, so the model still takes each value from its input, and a field
    with neither a name here nor a key in config.yaml is a missing key."""

    name: str


@dataclass(frozen=True)
class _Reads:
    """(T1192) What a read of one node of the `curated:` / `generated:` tree records (`_trace.config_read`): its dotted
    key `at` with its value as `read_config` states it (`raw`, what `stepcache.config_now` digests), and the root it is
    joined to (`root`: `("roots.curated", value)`; None under `CURATED_ROOT`, whose root is no config value, as
    `SOURCE.under`)."""

    at: str
    raw: object
    root: tuple | None

    def leaf(self, name: str) -> None:
        _trace.config_read(f"{self.at}.{name}", self.raw[name])
        self.root_read()

    def whole(self) -> None:
        """The whole node: a consumer that takes every path under it (`model_dump`, iteration)."""
        _trace.config_read(self.at, self.raw)
        self.root_read()

    def root_read(self) -> None:
        if self.root is not None:
            _trace.config_read(*self.root)


class _Tree(BaseModel):
    """(T1192) A node of the `curated:` / `generated:` tree that RECORDS EACH READ, so a build stage is keyed on the
    config values it reads (`CURATED.waters.cuts` records `curated.waters.cuts` + `roots.curated`), never on a whole
    block. The record is made per ACCESS, in the process that reads (the loaders are cached; validation at load is no
    read), and callers get the model itself: `isinstance`, pickling and equality are pydantic's. A sub-block is not a
    read (its leaves are); a method reads what it touches (`GENERATED.build()`: `atlas.builds`, and
    `atlas_build` when no name is given). Taking the whole node (`model_dump`, `model_dump_json`, iterating)
    records the whole node: `build.curated_digest` is the one whole-tree consumer on a stage's path. Bound by the loaders
    (`_bind`); an unbound model (another config file's, a test's) records nothing."""

    model_config = _STRICT

    _reads: _Reads | None = PrivateAttr(default=None)

    def __eq__(self, other: object) -> bool:
        """pydantic's equality, the record left out: a recording tree equals the same values unbound."""
        if not isinstance(other, _Tree):
            return NotImplemented

        def kept(m: _Tree) -> dict:
            return {k: v for k, v in (m.__pydantic_private__ or {}).items() if k != "_reads"}
        return type(self) is type(other) and self.__dict__ == other.__dict__ and kept(self) == kept(other)

    __hash__ = BaseModel.__hash__

    def __getattribute__(self, name: str):
        v = object.__getattribute__(self, name)
        if name[0] != "_" and name in type(self).__pydantic_fields__ and not isinstance(v, _Tree):
            private = object.__getattribute__(self, "__pydantic_private__")
            reads = private.get("_reads") if private else None
            if reads is not None:
                reads.leaf(name)
        return v

    def _whole(self) -> None:
        if self._reads is not None:
            self._reads.whole()

    def model_dump(self, *args, **kwargs):
        self._whole()
        return super().model_dump(*args, **kwargs)

    def model_dump_json(self, *args, **kwargs):
        self._whole()
        return super().model_dump_json(*args, **kwargs)

    def __iter__(self):
        self._whole()
        return super().__iter__()


def _bind(model: _Tree, at: str, raw: dict, root: tuple | None) -> _Tree:
    """`model` and every sub-block under it record their reads as `at.<key>` (`_Reads`)."""
    model._reads = _Reads(at, raw, root)
    for name in sorted(type(model).__pydantic_fields__):
        v = model.__dict__[name]
        if isinstance(v, _Tree):
            _bind(v, f"{at}.{name}", raw[name], root)
    return model


class ConfigError(ValueError):
    """config.yaml does not match `Config`: one line per offending key, named by its dotted path."""


#: THE JOIN RULE. Every path name (an `At`, or `source.fwa_gpkg` in config.yaml) is a name under the root of the block
#: it sits in: a `curated:` path under `roots.curated`, a `generated:` one under `roots.generated`, and so on for these
#: blocks; the roots themselves are relative to the repo root. `CURATED_ROOT` moves the curated root and nothing else.
TREES = ("source", "curated", "generated", "cache")


def _under(tree: str):
    """The validator of a `tree` block's path values: a name RELATIVE to `roots.<tree>` and inside it, joined to that
    root. An absolute name, or one that climbs out with `..`, would put a file outside the tree its writers and readers
    share; the root comes from the validation context (`roots`), which `parse_config` (or `CURATED_ROOT`) supplies."""
    def join(v: object, info: ValidationInfo) -> object:
        if not isinstance(v, str):
            return v                                # a sub-block's mapping: validated by its own model
        p = PurePosixPath(v)
        if not v.strip() or p.is_absolute() or ".." in p.parts or p == PurePosixPath("."):
            raise ValueError(f"a name under the {tree} root (roots.{tree}): relative to it and inside it")
        roots = (info.context or {}).get("roots")
        if roots is None:
            raise RuntimeError(f"a `{tree}:` path is joined to its root: validate config.yaml with `parse_config`")
        return Path(roots[tree]) / v
    join.__name__ = f"_under_{tree}"
    return join


class Waters(_Tree):
    """Curation that shapes the atlas itself, and the lists of waters (T324 naming: `*_calls` = the owner's per-item
    decisions, `*_waters` = a list of waters). Each key is its file's name."""

    model_config = _STRICT

    #: AUTHORED: every curated cut on a water (`cuts.json`, was `splits.json`); the atlas sectionizer applies them.
    cuts: Annotated[FilePath, At("waters/cuts.json")]
    #: AUTHORED: name variants on FWA waters, read by the atlas registry and the name matchers (`tools.mined_names`,
    #: the bathymetry and stocking proposals, `bathymetry.contours`).
    name_variants: Annotated[FilePath, At("waters/name_variants.json")]
    #: PROMOTED (T224): name variants found in city layers, OSM, ECCC… that the owner accepted (auto rule or
    #: review mark); each name carries its real source. Written by `pipeline.tools.mined_names`.
    found_names: Annotated[FilePath, At("waters/found_names.json")]
    #: AUTHORED (T327): the owner's calls on single found-name candidates (accept / hold, a stored name, display),
    #: by candidate id — was `names.OWNER` in code. Read by `pipeline.regs.review.names`; an input to `mined_names`.
    found_name_calls: Annotated[FilePath, At("waters/found_name_calls.json")]
    #: AUTHORED: THE OWNER-DECIDED CONTINUATION LINKS (`water_continue`): `reviewed` ship, `refused` never, anything
    #: else the topology proposes is held for review (owner 2026-10-29, `bundle.continuation.gate`).
    continuations: Annotated[FilePath, At("waters/continuations.json")]
    #: T326: THE ONE TIDAL FILE (`tidal_limits.json`) — one row per ocean-entering water: its tidal limit at the
    #: `mouth` (an alias, no cut), on an existing `split`, a new `cut`, or the `whole` water. Written by
    #: `pipeline.tools.tidal_limits` (review write-through); read by the ATLAS only (`atlas.splits.tidal.load`), which
    #: writes `tidal.resolved.json`.
    tidal_limits: Annotated[FilePath, At("waters/tidal_limits.json")]
    #: T678/T685: the owner's rulings on single lines for the coast cut (cut at a measure / whole / fresh, each row
    #: `decided`), applied as written. Hand-written only; read by the ATLAS only (`build.cut_shores`).
    shore_held: Annotated[FilePath, At("waters/shore_held.json")]
    #: T678 (owner 2026-11-11): one row per `split` row of `tidal_limits`: is the boundary DFO-written (quoted from the
    #: cached DFO copies)? Only DFO-written splits have authority over the tidal cut; the rest are listed for the owner,
    #: never dropped. A sidecar because `tidal_limits` is rewritten from the marks with its own fields only. Read by the
    #: atlas (`splits.coast.load_authority`).
    #: Classified by Claude, each row cited; the owner may correct it.
    tidal_split_authority: Annotated[FilePath, At("waters/tidal_split_authority.json")]
    #: T322 (owner 2026-10-30): APPLIED: the owner-CONFIRMED slough / canal polygons, applied from the slough marks by
    #: `pipeline.tools.slough_calls`; the registry folds each into the one stream through it, else ignores it
    #: (`registry.flowing.fold_confirmed_sloughs`).
    slough_calls: Annotated[FilePath, At("waters/slough_calls.json")]
    #: (owner 2026-11-13, Gagnon Creek) AUTHORED: unnamed polygons the owner keeps as LAKES of their own: the
    #: registry's widening fold leaves them (`registry.flowing.load_pond_calls`). Hand-written only.
    pond_calls: Annotated[FilePath, At("waters/pond_calls.json")]
    #: T322: the retired slough review page's database — the owner's verdicts and free-text notes
    #: (`pipeline.regs.review.slough.page_note`). Was `sloughs/page-marks-2026-10-28.json`.
    slough_page_calls: Annotated[FilePath, At("waters/slough_page_calls.json")]
    #: THE USER'S KNOWN-STEELHEAD LIST (authored; user ruling 2026-10-02) — rivers that are steelhead
    #: water though no row of the book says so. `pipeline.atlas.reach.steelhead.load_list`; a new generated list is
    #: accepted with `python -m pipeline.regs.steelhead.accept` (T324).
    steelhead_waters: Annotated[FilePath, At("waters/steelhead_waters.json")]
    #: AUTHORED: blanket area closures — cut every stream crossing the polygon.
    area_closures: Annotated[FilePath, At("waters/area_closures.json")]
    #: AUTHORED: hand-drawn admin polygons — closure zones a regulation states as an AREA rather than a
    #: reach. Selected by an `area_closures.json` def that names it in `file` (instead of a fetched gpkg `layer`);
    #: see `load_area_polys`.
    added_areas: Annotated[FilePath, At("waters/added_areas.geojson")]
    #: AUTHORED: hand-drawn lake polygons, read by the atlas build.
    added_lakes: Annotated[FilePath, At("waters/added_lakes.geojson")]
    #: PROMOTED, not authored — 361 minted streams whose negative `blk` ids are a live ABI.
    #: Re-running `added_streams/build_dataset.py` remints them and strands every cut bound to the old ids.
    added_streams: Annotated[FilePath, At("waters/added_streams.json")]
    #: AUTHORED (T679 #13): the added-streams generator's hand fixes (a feature dropped by name or src_id, a name typo,
    #: a receiver override), read by the generator only (`added_streams.clean.calls`), never by the atlas build.
    added_stream_calls: Annotated[FilePath, At("waters/added_stream_calls.json")]
    #: The worklist for minting more added lakes (was `ungazetted.json`). No build reader BY DESIGN; the minting is a
    #: hand process. Checked by `check_curated --waters`; declared so a dead-file sweep cannot delete it.
    unmapped_waters: Annotated[FilePath, At("waters/unmapped_waters.json")]
    #: AUTHORED (T632, owner 2026-11-11): the pieces the reach could not place that the owner ACCEPTED staying
    #: unplaced, one row per (entry or DFO place, rule, piece, reason). Any other unplaced piece stops the reach run
    #: (`pipeline.atlas.reach.build.unplaced_gate`); `proposed` rows are not accepted and stop it too.
    unplaced_piece_calls: Annotated[FilePath, At("waters/unplaced_piece_calls.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class DfoSalmon(_Tree):
    """DFO salmon: the curated places DFO's rules bind to (`places/region-*.json`, was `regulations/entries/
    dfo_salmon/`) and T321's limits wording as a closed registry (`pipeline.regs.dfo_salmon.wording`), AUTHORED; the
    feeds worker reads synced copies (`pipeline.deliver.sync_inputs`)."""

    model_config = _STRICT

    #: The curated DFO locations, one file per region (item ids, extents); synced to the feeds worker as R2
    #: `private/feeds/dfo/entries/` (`pipeline.deliver.sync_inputs`).
    places: Annotated[DirectoryPath, At("dfo_salmon/places")]
    #: AUTHORED (templates reviewed by hand; `pipeline.regs.dfo_salmon.wording`); synced to R2
    #: `private/feeds/dfo/wording.json` (`pipeline.deliver.sync_inputs`), the feeds worker's ONLY copy.
    wording: Annotated[FilePath, At("dfo_salmon/wording.json")]
    #: AUTHORED (T632, owner 2026-11-11): the DFO places that KEEP their tidal water, each with its stated reason;
    #: every other place has the run's tidal sections subtracted (`pipeline.atlas.reach.dfo_salmon.load_tidal_calls`).
    tidal_calls: Annotated[FilePath, At("dfo_salmon/tidal_calls.json")]
    #: AUTHORED (T679 #2): the places the page defaults and cascade scopes bind (`entries.load_scope_places`), read
    #: by the atlas promote guards too (`promote.curated_boundary_ids`: a moved cut it names stops the promote).
    scope_places: Annotated[FilePath, At("dfo_salmon/scope_places.json")]
    #: REVIEWED: the closed areas the owner confirmed, one record per closed area of a DFO row's entry B (a stretch
    #: between two cuts, or a circle), written only by the review apply (`pipeline.regs.review.closed_areas`); the reach
    #: run binds the live ones and the bundle ships them. A plain `Path` ON PURPOSE: the key must be stated, the file
    #: MAY BE ABSENT (it is born with the owner's first confirm; a review copy with no marks has none).
    closed_areas: Annotated[Path, At("dfo_salmon/closed_areas.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class Review(_Tree):
    """The curation review's REVIEWED data (T97): `marks` is the append-only store of every
    reviewer's mark (`pipeline.regs.review.marks`), checked in empty before the first mark.

    T266: the marks are REVIEW STATE. No build reads them: what they decide lands in the curated files via the
    apply paths (`pipeline.regs.review.apply`, decisions in memory, T408), so the path is NOT required to exist here
    (a build runs with it absent); `marks.load` still raises on a missing store for its readers."""

    model_config = _STRICT

    #: REVIEWED: the reviewers' marks, append-only. A plain `Path` ON PURPOSE (the one curated file besides `closed`
    #: that may be absent): the key must be stated, the file need not exist.
    marks: Annotated[Path, At("review/marks.jsonl")]
    #: owner calls on PROPOSED splits the marks cannot carry (the store is append-only, never
    #: edited): a cut's confidence (`marks.ConfidenceOverride`, owner 2026-10-27). Folded by T326.
    proposal_overrides: Annotated[FilePath, At("review/proposal_overrides.json")]
    #: T325: the CLOSED review domains (`pipeline.regs.review.closed`) — beside the marks, never required to exist
    #: (absent = nothing closed); the apply and the review app skip a domain listed here. A plain `Path` on purpose, as
    #: `marks`: the key must be stated, the file may be absent.
    closed: Annotated[Path, At("review/closed.json")]
    #: REVIEWED: the owner's carries, a DFO place's decision kept on while its binding is the one that decision made
    #: (`pipeline.regs.review.owner_carry`). A plain `Path`: the file MAY BE ABSENT (absent = nothing carried).
    owner_carries: Annotated[Path, At("review/owner_carries.json")]
    #: REVIEWED: the owner's check-again list, marks he wants to see again with why, who and when
    #: (`pipeline.regs.review.recheck`). A plain `Path`: the file MAY BE ABSENT (absent = nothing listed).
    recheck: Annotated[Path, At("review/recheck.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class Regulations(_Tree):
    """The synopsis corpus and the book. `synopsis` is the per-region rule corpus (was `entries/catalogue`) — the
    format doc 18 describes: a rule is a TYPE plus named CONDITIONS and the label is generated."""

    model_config = _STRICT

    #: The parsed synopsis rows, one file per region.
    synopsis: Annotated[DirectoryPath, At("regulations/synopsis")]
    #: AUTHORED: a book water name -> the FWA water(s) it means (was `overrides.json`).
    book_name_links: Annotated[FilePath, At("regulations/book_name_links.json")]
    #: THE BOOK'S TEXT, transcribed verbatim (`definitions.md` p.80, `water-specific-tables.md`
    #: p.4, `licensing.md` pp.6-7, `region-*.md`): source text, read by the answers glossary.
    reference: Annotated[DirectoryPath, At("regulations/reference")]
    #: ACCEPTED (T679 D4): every printed-word read the reach made that matched on the accepted run; a listed read that
    #: stops matching stops the reach run (`pipeline.atlas.reach.word_reads`).
    word_reads: Annotated[FilePath, At("regulations/word_reads.json")]
    #: AUTHORED (T679 #12): the owner's rulings on the export's two closure scans, a dated water release / a water
    #: row's quota under a blanket closure (`pipeline.deliver.export.closures.under_closure_calls`).
    under_closure_calls: Annotated[FilePath, At("regulations/under_closure_calls.json")]
    #: AUTHORED, ACCEPTED ROWS ONLY (T981): waters closed to fishing by a city, park board or other local authority, not
    #: by the synopsis (`region-*.json`, each entry a water's closure with its `authority` cited). A second ENTRY SOURCE
    #: beside `synopsis` (`parsing.io.entry_sources`); read and checked by `pipeline.regs.local_closures`.
    local_closures: Annotated[DirectoryPath, At("regulations/local_closures")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class Gauges(_Tree):
    """The gauge matching domain: a station id -> FWA keys, plus the hand review of it.

    `match_calls` is an INPUT to matching and is never written by a program. `matches` is
    regenerated wholesale. Two files rather than one is what makes a regenerate PHYSICALLY
    unable to harm the review — the safety is in the filesystem, not in code remembering."""

    model_config = _STRICT

    #: PROMOTED: a machine produced it and a person approved it; regenerated wholesale (only
    #: `pipeline.gauges.generate.promote` writes it).
    matches: Annotated[FilePath, At("gauges/matches.json")]
    #: AUTHORED: an INPUT to matching, never written by a program.
    match_calls: Annotated[FilePath, At("gauges/match_calls.json")]
    #: (T331) ECCC's stated water-body type is scraped SOURCE data now: `SOURCE / "bc_station_waterbody_type.json"`
    #: (`pipeline.gauges.generate.waterbody_type.TYPES_FILE`), not a curated key. (T324) The gauge review QUEUE
    #: (`candidates.json`) is machine output: `GENERATED.gauges.candidates`.
    #: AUTHORED — level stations the tide or a tidal river's backwater moves, each with its reason, joined
    #: with HYDAT's own tidal remarks by `pipeline.gauges.consume.tidal` into the bundle's `gauge.tidal` (T78).
    tidal_stations: Annotated[FilePath, At("gauges/tidal_stations.json")]
    #: AUTHORED — level stations a LAKE below them backs up (a standing backwater), each naming the lake
    #: and quoting the HYDAT remark that says so, checked against the record by `pipeline.gauges.consume.tidal` into
    #: the bundle's `gauge.backwater` (T160).
    backwater_stations: Annotated[FilePath, At("gauges/backwater_stations.json")]
    #: AUTHORED — gauges on a DIVERSION canal that speak for the river above its intake instead of the
    #: gauges named (T696, RULINGS P54), checked against HYDAT by `pipeline.gauges.consume.diversion` into the bundle's
    #: `gauge.diversion` and the intake placement.
    diversion_stations: Annotated[FilePath, At("gauges/diversion_stations.json")]
    #: (T1047) gauges whose live level is on a different zero (datum) than their HYDAT record — one row per
    #: change, each `proposed` until the owner accepts it; only an ACCEPTED row with a known offset puts the record on
    #: today's zero (`pipeline.gauges.feed.datum`, read by `climatology`).
    datum_change_stations: Annotated[FilePath, At("gauges/datum_change_stations.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class RunTiming(_Tree):
    """Run timing: the authored curation (`run_calls.json`, was `review.json`). The parsed timings (`timing.json`, `pipeline/runtiming/
    timing.py`) are source data, `SOURCE / "runtiming/timing.json"` (T331)."""

    model_config = _STRICT

    #: AUTHORED, and short by design: run labels only where the CU names do not already separate two runs (most
    #: state their own), and `coverage_gaps`, the waters an angler will look up and find nothing for.
    run_calls: Annotated[FilePath, At("runtiming/run_calls.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class InSeason(_Tree):
    """T226: the provincial in-season notices bound to waters (`notice_waters.json`, was `matches.json`;
    `pipeline.regs.inseason.matches`), AUTHORED — written by the review app's In-season tab or by hand, never by a
    generator. (T679 #11) The owner's matches only: their resolved ranges are generated (`matches.resolved_path()`)."""

    model_config = _STRICT

    #: AUTHORED: an in-season feed row id -> the waters (extents) it applies to.
    notice_waters: Annotated[FilePath, At("inseason/notice_waters.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class Bathymetry(_Tree):
    """The lake bathymetry charts matched to lakes: the owner's accepted matches, frozen. Written ONLY by
    `python -m pipeline.bathymetry.accept`, one owner decision per row, from the proposals under `GENERATED.bathymetry`;
    no build reads them yet."""

    model_config = _STRICT

    #: PROMOTED: chart -> lake matches the owner accepted (or refused, with the reason). A plain `Path`: the file MAY BE
    #: ABSENT until the first accept writes it.
    matches: Annotated[Path, At("bathymetry/matches.json")]
    #: PROMOTED: contour-map -> lake matches the owner accepted (`accept --set contours`). A plain `Path`: the file MAY
    #: BE ABSENT until the first accept writes it.
    contour_matches: Annotated[Path, At("bathymetry/contour_matches.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class Stocking(_Tree):
    """The stocked waterbodies matched to lakes: the owner's accepted matches, frozen. Written ONLY by
    `python -m pipeline.bathymetry.accept --set stocking`, from the proposals under `GENERATED.stocking`; no build reads
    them yet."""

    model_config = _STRICT

    #: PROMOTED: stocked waterbody -> lake matches the owner accepted (or refused). A plain `Path`: the file MAY BE
    #: ABSENT until the first accept writes it.
    matches: Annotated[Path, At("stocking/matches.json")]

    _abs = field_validator("*", mode="before")(_under("curated"))


class Curated(_Tree):
    """Every path whose loss costs human time rather than CPU. Each group is a folder of that name under the curated
    root, and each path a name under that root (`TREES`).

    `base` is the root this tree was joined to: `roots.curated`, or the whole copy `CURATED_ROOT` names. It is not a
    key of config.yaml (stating `curated.base` is an unknown key); the validation context supplies it."""

    model_config = _STRICT

    waters: Waters
    regulations: Regulations
    review: Review
    gauges: Gauges
    runtiming: RunTiming
    inseason: InSeason
    dfo_salmon: DfoSalmon
    bathymetry: Bathymetry
    stocking: Stocking

    _base: Path = PrivateAttr()

    @model_validator(mode="after")
    def _joined_to(self, info: ValidationInfo) -> "Curated":
        self._base = Path(info.context["roots"]["curated"])
        return self

    @property
    def base(self) -> Path:
        """The curated root this tree was joined to (`roots.curated`, or the `CURATED_ROOT` copy). Records the root (no
        leaf): `make_root` copies the directory, whatever its keys name."""
        if self._reads is not None:
            self._reads.root_read()
        return self._base


class Roots(BaseModel):
    """THE ROOTS (config.yaml `roots:`): where each tree of data lives, relative to the repo root (or absolute). Every
    other path is a name under the root of its block (`TREES`), so moving a tree is one line of config.yaml."""

    model_config = _STRICT

    #: Fetched from an authority, re-downloadable. Read through `SOURCE` (`SOURCE / "bc_places.json"`,
    #: `SOURCE.fwa_gpkg`).
    source: DirectoryPath
    #: Human-owned, not recreatable at any price. Read through `CURATED`.
    curated: DirectoryPath
    #: Computed, re-runnable. Created on demand: a fresh checkout has run nothing, so this must not require it. Read
    #: through `GENERATED`.
    generated: Path
    #: The shipped set, NOT a build artifact: only what an app downloads. Holds `<data_release>/`,
    #: `tiles/<tile_version>/` (the geometry-only atlas, the outside-BC mask, the map assets) and `basemap/`. Read as
    #: `ROOTS.release`.
    release: Path
    #: Gitignored machine state outside data/ (the DFO salmon page cache). Its paths are the `cache:` block.
    cache: Path

    _abs = field_validator("*", mode="before")(_absolute)


# ======================================================================================
# generated/ — everything a program writes
# ======================================================================================


class GeneratedAtlas(_Tree):
    """Graph builds: `full/`, plus any small named build (`--gnis` / `--bbox`, named by `--out`). Which build is the
    default is config.yaml's `atlas_build` (`Generated.build`)."""

    model_config = _STRICT

    #: Written by `pipeline.atlas.build --out`: `full/`, or a small named build (`--gnis` / `--bbox`, e.g. `adams/`);
    #: each holds graph.pkl, registry.json, geometries.pkl, item_points.json, splits.resolved.json.
    builds: Annotated[Path, At("atlas")]

    _abs = field_validator("*", mode="before")(_under("generated"))


class GeneratedRegs(_Tree):
    model_config = _STRICT

    #: `synopsis_raw_data.json` and `row_images/` (`pipeline.regs.extraction`).
    extraction: Annotated[Path, At("regs/extraction")]
    #: The LLM parse work dir: batches/, responses/, reviews/ (`pipeline.regs.parsing.io.default_work_dir`).
    parse: Annotated[Path, At("regs/parse")]
    #: The DFO salmon scrape: locations/, rules/, untangled/, and the typed feed (typed/).
    dfo_salmon: Annotated[Path, At("regs/dfo_salmon")]
    entries_backup: Annotated[Path, At("regs/entries_backup")]
    #: The export's data file (`pipeline.deliver.export`), `ui-rules-export.json`; its guide, answers and card check are
    #: written beside it.
    export: Annotated[Path, At("regs/ui-rules-export.json")]

    _abs = field_validator("*", mode="before")(_under("generated"))


class GeneratedGauges(_Tree):
    model_config = _STRICT

    #: The published feed: per-station JSON (`<station>.json`) plus `index.json`, `clim.json`, `stats.json`,
    #: `temp_history.json`.
    feeds: Annotated[Path, At("gauges/feeds")]
    #: The matcher's RAW output (`pipeline.gauges.generate.match`), never the curated file: only
    #: `promote` writes `CURATED.gauges.matches`, and only after it has shown what would be demoted (T94).
    match: Annotated[Path, At("gauges/match.json")]
    #: (T324) the REVIEW QUEUE — matches `pipeline.gauges.generate.promote` did not promote (an independent check
    #: disagreed), with their evidence. Written by promote beside the curated `matches`; committed (data/.gitignore)
    #: so `check_curated --ci` sees the queue in CI too. Machine output, so not curated.
    candidates: Annotated[Path, At("gauges/candidates.json")]
    #: (T1047) the datum / relocation remarks HYDAT holds for the roster, parsed: PROPOSALS
    #: (`pipeline.tools.audit.datum_remarks`), never in effect; the curated rows are `CURATED.gauges.datum_change_stations`.
    #: Committed (data/.gitignore) because those curated rows cite it.
    datum_proposals: Annotated[Path, At("gauges/datum_proposals.json")]
    #: HYDAT's daily means per station (`pipeline.gauges.feed.hydat_daily`, one `<station>.json` each; the deploy
    #: scripts upload them as `feeds/hydat`). Static between HYDAT releases; its stale files are pruned.
    hydat_daily: Annotated[Path, At("gauges/hydat_daily")]

    _abs = field_validator("*", mode="before")(_under("generated"))


class Generated(_Tree):
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

    model_config = _STRICT

    atlas: GeneratedAtlas
    #: The reach binding (`pipeline.atlas.reach`): rule -> sections, one directory per build.
    reaches: Annotated[Path, At("reaches")]
    #: A deliverable: the bundle stage's output (`pipeline.deliver.bundle`).
    bundle: Annotated[Path, At("bundle")]
    #: A deliverable: the tiles stage's output (`pipeline.deliver.tiles`).
    tiles: Annotated[Path, At("tiles")]
    #: The added-streams review artifacts (`pipeline.atlas.waters.added_streams`): the mapcheck / verify_map html, the
    #: demo gpkg, the OSM candidates.
    added_streams: Annotated[Path, At("added_streams")]
    #: Ad-hoc candidate dumps from one-off audits, looked at once and thrown away; nothing downstream reads them. Kept
    #: out of the domain folders on purpose.
    scratch: Annotated[Path, At("scratch")]
    regs: GeneratedRegs
    gauges: GeneratedGauges
    #: The CU polygons `pipeline.runtiming.parse` stitches from the cached Explorer tiles (`cu_polygons.geojson`, read
    #: by `pipeline.runtiming.feed.release`, which `pipeline.deliver runtiming` runs) and the run-timing feed (`feed/`);
    #: re-runnable from `data/source/runtiming/`.
    runtiming: Annotated[Path, At("runtiming")]
    #: T226: `feed/matches.json`: what `pipeline.regs.inseason.publish` writes for the app. The local tile server serves
    #: `feed/` as `/feeds/inseason/`; in production it is R2 `feeds/inseason/`, beside the feeds worker's `index.json`
    #: (in dev `--feed` copies a captured `index.json` beside it). `resolved.json`: the curated matches resolved on a
    #: release (`pipeline.regs.inseason.resolve`), never committed.
    inseason: Annotated[Path, At("inseason")]
    #: T330: the curation review's machine-written inputs (candidates, pre-verify, suggestions, the Q154 sample, the
    #: mined name candidates, the T185 proposals) — moved out of docs/; the review app and `regs/review/*` read them.
    #: Committed (data/.gitignore) because the review app reads them; nothing human-made lives here.
    review: Annotated[Path, At("review")]
    #: (T435) the slough finder's output (`slough-automark.json`, `candidates_v2.json.gz`) the slough review reads
    sloughs: Annotated[Path, At("sloughs")]
    #: (T1021) the T185 proposals' side rebuilds, one directory per proposal id + digest (`proposals.side_dir`): the
    #: patched curated copies, the side atlas, its reach run
    side: Annotated[Path, At("side")]
    #: The bathymetry PROPOSALS (`proposals.json`: charts, `contour_proposals.json`: contour maps), machine output
    #: rewritten freely by `pipeline.bathymetry.propose` / `contours`; the owner's accepts go to `CURATED.bathymetry`.
    bathymetry: Annotated[Path, At("bathymetry")]
    #: The stocking PROPOSALS (`proposals.json`, `pipeline.stocking.propose`); the owner's accepts go to
    #: `CURATED.stocking`.
    stocking: Annotated[Path, At("stocking")]
    #: A copy of the regulations store kept only on request (`deliver all --debug-json`, or `deliver store` run alone):
    #: `regs.sqlite` (`pipeline.deliver.store.DEBUG_OUT`). Nothing reads it.
    debug: Annotated[Path, At("debug")]
    #: The release's shrunk copies of the synopsis PDF, cached by the source's digest
    #: (`pipeline.deliver.release.synopsis`).
    synopsis: Annotated[Path, At("synopsis")]
    #: The steelhead generator's output (`pipeline.regs.steelhead.known_waters`): the generated list
    #: `steelhead_waters.json` that `pipeline.regs.steelhead.accept` compares with the curated one, and its review csv.
    steelhead: Annotated[Path, At("steelhead")]
    #: The curated writer's backups and its log of who wrote what (`pipeline.common.curated_write`); a `CURATED_ROOT`
    #: copy keeps its own beside the copy instead.
    curated_writes: Annotated[Path, At("curated_writes")]

    #: Every field (T1192: a list of names left a new Path field relative, silently); `_absolute` passes the sub-blocks'
    #: dicts through untouched.
    _abs = field_validator("*", mode="before")(_under("generated"))

    #: config.yaml's `atlas_build`, set by `Config` (a setting, not a path of this tree: `build()` reads it)
    _atlas_build: str = PrivateAttr()

    # --- builds -----------------------------------------------------------------------

    def build(self, name: str | None = None) -> Path:
        """The path of a build. Does NOT check it exists — for writers and for messages. With no name: the default
        build, config.yaml's `atlas_build` (recorded as that key)."""
        builds = self.atlas.builds
        if name:
            return builds / name
        if self._reads is not None:
            _trace.config_read("atlas_build", self._atlas_build)
        return builds / self._atlas_build

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

    def mkdir(self, key: str, *parts: str) -> Path:
        """The directory a declared key names (`parts` below it), created. For WRITERS only. A name that is no key of
        `generated:` is refused: every folder a program writes is declared."""
        p = getattr(self, key) if key in type(self).model_fields else None
        if not isinstance(p, Path):
            raise KeyError(f"`generated:` has no folder {key!r}: declare it (pipeline.common.curated, `At`)")
        p = p.joinpath(*parts)
        p.mkdir(parents=True, exist_ok=True)
        return p


# ======================================================================================
# source/ — the named source files
# ======================================================================================


class Source(BaseModel):
    """The source files that have a name of their own (`source:`), each RELATIVE to the source root (`roots.source`).
    Read them as `SOURCE.fwa_gpkg`, never as a literal: one name per file, so the fetcher that writes a file and the readers that open it cannot disagree about where it is. Everything `data/fetch_data.py` writes
    lands under the source root: a fetcher that writes where no reader looks is how a freshness gate goes quiet about a
    file nobody is reading. Every other source file is still `SOURCE / "<file>"`.

    config.yaml states the gpkg's name (the one a machine may choose); the other names are declared here (`At`). The
    validated model holds each one joined to the source root (`TREES`), so every path of the model is absolute and none
    resolves against the current directory. Fetched, so nothing here must exist: a CI
    runner has no gpkg, and a reader raises naming the file."""

    model_config = _STRICT

    #: THE FWA GeoPackage (about 9.5 GB): streams, lakes, wetlands, the management units and the other layers
    #: `data/fetch_data.py` writes. The atlas and the tiles read it, and its size and modified time are an input of every
    #: atlas (`pipeline.atlas.inputs`), so a layer added to it moves every atlas.
    fwa_gpkg: Path
    #: The FWA coast (`rivers`: the river polygons, `land`: the land mass) the atlas's coast-crossing tidal cut reads
    #: (`pipeline.atlas.splits.coast`). A file of its own so that fetching it never moves the main gpkg's stamp.
    coast_gpkg: Annotated[Path, At("fwa_coast.gpkg")]
    #: The repo copy of the synopsis PDF, tracked in git: the edition the entries were read from (gov.bc.ca's file is
    #: the same edition, but its bytes move day to day). The page numbers, the synopsis period and the extractor read it.
    synopsis_pdf: Annotated[Path, At("fishing_synopsis.pdf")]
    #: `data/fetch_data.py`'s download and unzip directory (the FWA zips and their .gdb). Scratch: safe to delete.
    fetch_temp: Annotated[Path, At("temp_bc_data")]

    _names = field_validator("*", mode="before")(_under("source"))


# ======================================================================================
# the block read through `project_config.get_config()` (what each path is: its accessor there)
# ======================================================================================


class Cache(BaseModel):
    """Gitignored machine state outside data/, under `roots.cache` (`get_config().dfo_salmon_cache`)."""

    model_config = _STRICT

    #: DFO's saved salmon region pages: raw/ (the newest per region), history/ (every page fetched), manifest.json.
    dfo_salmon: Annotated[Path, At("dfo_salmon")]

    _abs = field_validator("*", mode="before")(_under("cache"))


class Config(BaseModel):
    """THE schema of config.yaml, every block (T1192), its files' names included (`At`). Parsed once per loader by
    `parse_config`; `load` / `load_generated` / `load_roots` and `project_config.get_config()` are views on it, so a key
    unknown or missing ANYWHERE stops every process that reads config, naming the key. The roots are validated first
    (`_validate_config`): every other path is joined to one of them."""

    model_config = _STRICT

    roots: Roots
    source: Source
    curated: Curated
    generated: Generated
    cache: Cache
    #: The atlas build the review app serves AND rebuilds into, and every reader's default (`GENERATED.build()`). One
    #: name, read by both: these were two independent hard-codings of "output/v2/full", and the app could rebuild one
    #: directory and read another with nothing saying so. A build NAME under `GENERATED.atlas.builds`, never a path.
    atlas_build: str

    @model_validator(mode="after")
    def _default_build(self) -> "Config":
        self.generated._atlas_build = self.atlas_build
        return self


def _names(model: type[BaseModel]) -> dict:
    """{key: name} of every field of `model` that declares its name (`At`), {key: {...}} for a sub-block's."""
    out = {}
    for key, f in model.model_fields.items():
        at = [m for m in f.metadata if isinstance(m, At)]
        if at:
            out[key] = at[0].name
        elif isinstance(f.annotation, type) and issubclass(f.annotation, BaseModel) and (sub := _names(f.annotation)):
            out[key] = sub
    return out


#: Every name the schema declares (`At`), as the nested mapping config.yaml once stated: what `read_config` adds to the
#: parsed file. Taken from the fields themselves, so a name cannot exist without its field.
NAMES = _names(Config)


def _where(path: Path) -> str:
    return CONFIG.name if Path(path).resolve() == CONFIG.resolve() else str(path)


def _validate(model: type[BaseModel], blob: object, path: Path, prefix: tuple = (), note: str = "",
              roots: dict | None = None):
    """`model` validated from `blob`, or `ConfigError` with one line per offending key ("config.yaml: unknown key
    curated.waters.cutz", "... missing key source.fwa_gpkg", "... curated.gauges.matches is null"). `roots`: {tree:
    absolute root} each path is joined to (`TREES`)."""
    try:
        return model.model_validate(blob, context={"roots": roots})
    except ValidationError as e:
        lines = []
        for err in e.errors():
            key = ".".join(str(k) for k in (*prefix, *err["loc"])) or "(the file)"
            if err["type"] == "extra_forbidden":
                lines.append(f"unknown key {key}")
            elif err["type"] == "missing":
                lines.append(f"missing key {key}")
            elif err.get("input", ...) is None:
                lines.append(f"{key} is null: state its value (a key is never `null` for \"not built\")")
            else:
                lines.append(f"{key}: {err['msg']} (got {err.get('input')!r})")
        where = _where(path)
        raise ConfigError("\n".join(f"{where}{note}: {line}" for line in lines)) from e


class _Duplicate(Exception):
    pass


class UniqueKeyLoader(yaml.SafeLoader):
    """`yaml.SafeLoader` that REFUSES a key stated twice in one mapping (T1192 review): PyYAML keeps the last one without a
    word, so a second `cuts:` under `curated.waters` silently repointed the cuts. Used by `read_yaml`, for every reader of
    config.yaml (`parse_config` and the build record's `stepcache.config_now`), so both read by the same rules."""

    def __init__(self, stream):
        super().__init__(stream)
        self._at: dict = {}                       # a mapping node -> its dotted place (parents are built first)

    def construct_mapping(self, node, deep=False):
        at, seen = self._at.get(node, ()), set()
        for k, v in node.value:
            key = self.construct_object(k, deep=True)
            if key in seen:
                raise _Duplicate(f"duplicate key {'.'.join(map(str, (*at, key)))} (line {k.start_mark.line + 1})")
            seen.add(key)
            if isinstance(v, yaml.MappingNode):
                self._at[v] = (*at, key)
        return super().construct_mapping(node, deep=deep)


def read_yaml(path: Path):
    """`path` parsed by `UniqueKeyLoader`, or `ConfigError` naming a duplicate key."""
    try:
        return yaml.load(Path(path).read_text(encoding="utf-8"), Loader=UniqueKeyLoader)
    except _Duplicate as e:
        raise ConfigError(f"{_where(path)}: {e}") from None


def _with_names(blob: dict, names: dict, path: Path, at: tuple = ()) -> dict:
    """`blob` with `names` added; a key stated in both is refused, naming it (each file has one name, in one place)."""
    out = dict(blob)
    for k, v in names.items():
        if k not in out:
            out[k] = copy.deepcopy(v)               # a reader's copy: editing it never edits `NAMES`
        elif isinstance(v, dict) and isinstance(out[k], dict):
            out[k] = _with_names(out[k], v, path, (*at, k))
        else:
            raise ConfigError(f"{_where(path)}: unknown key {'.'.join((*at, k))} (its name is declared in "
                              f"pipeline/common/curated.py, not in config.yaml)")
    return out


def read_config(path: Path | None = None) -> dict:
    """config.yaml parsed (`read_yaml`) with the schema's names added (`NAMES`): THE values every reader sees. The one
    source for the loaders (`parse_config`) and for a build stage's record (`stepcache.config_now` resolves a recorded
    key here), so a recorded `curated.waters.cuts` resolves to the name the loader joined."""
    path = path or CONFIG
    blob = read_yaml(path)
    if not isinstance(blob, dict):
        raise ConfigError(f"{_where(path)}: not a mapping of blocks (got {type(blob).__name__})")
    return _with_names(blob, NAMES, path)


def _validate_config(blob: dict, path: Path) -> Config:
    """`Config` from the parsed file: the roots first, since every other path is a name under one of them; a file whose
    roots are missing or wrong is refused on those alone (each other path would fail only for want of its root)."""
    if "roots" not in blob:
        raise ConfigError(f"{_where(path)}: missing key roots")
    roots = _validate(Roots, blob["roots"], path, ("roots",))
    return _validate(Config, blob, path, roots=roots.model_dump())


def parse_config(config: Path | None = None) -> tuple[dict, Config]:
    """(the parsed values, the validated `Config`) of config.yaml — THE one load path. The values (`read_config`: the
    file with the schema's names) are what the build's stage keys digest (`_config_read`); the model is what every
    reader gets. Not cached itself: each loader caches its own view (and `load_generated.__wrapped__()` reads the file
    afresh). (T1222) No `.env` is read: a setting the pipeline needs at run time comes from the shell environment
    (docs/runbooks/fresh-machine.md section 10)."""
    path = config or CONFIG
    blob = read_config(path)
    return blob, _validate_config(blob, path)


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


def load(config: Path | None = None) -> Curated:
    """The validated curated tree — the live one, or the whole copy `CURATED_ROOT` names (T422). Cached, because
    validation stats every file.

    Raises `ConfigError` naming the offending key when a path is wrong, or a key is unknown, missing or `null`
    anywhere in config.yaml — which is the whole point, and is why nothing here has a fallback default.
    """
    return _load(config, sandbox_root())


def load_repo(config: Path | None = None) -> Curated:
    """The LIVE tree whatever `CURATED_ROOT` says — for the guards that must name the real files (a side copy is
    refused under it; a test checks the real tree was not written)."""
    return _load(config, None)


def _is_config(path: Path) -> bool:
    """Whether `path` is config.yaml: another config file (a test's) holds no config.yaml value, so its trees and reads
    record nothing."""
    return Path(path).resolve() == CONFIG.resolve()


def _config_read(path: Path, key: str, value) -> None:
    """(T1171) The build's stage keys hold the config.yaml VALUES a stage reads, not the file
    (`pipeline._trace.config_read`); (T1192) one value per read, never a block (`_Tree`, `SOURCE`, `ROOTS`)."""
    if _is_config(path):
        _trace.config_read(key, value)


@functools.lru_cache(maxsize=8)
def _load(config: Path | None, root: Path | None) -> Curated:
    """Loading records nothing (validation stats each file: no read by the stage); each read records itself (`_Tree`)."""
    path = config or CONFIG
    blob, cfg = parse_config(path)
    if root is None:
        tree, under = cfg.curated, ("roots.curated", blob["roots"]["curated"])
    else:
        # the sandbox moves ONE root: the same names, joined to the copy instead of `roots.curated` (no config value)
        tree, under = _validate(Curated, blob["curated"], path, ("curated",), note=f" (under {ROOT_ENV}={root})",
                                roots={"curated": root}), None
    return _bind(tree, "curated", blob["curated"], under) if _is_config(path) else tree


def make_root(dst: Path) -> Path:
    """A whole copy of THIS PROCESS'S curated tree at `dst` (created; existing files there are overwritten), for
    `CURATED_ROOT`: the live tree, or the copy `CURATED_ROOT` already names — a sandboxed process copies its own world,
    never reaches past it to the live files (one root, never a mix; T777: a test run's sandbox). A copy, never links: a
    write through the sandbox must not reach a real file. Refuses a `dst` that overlaps the live tree or the copied one."""
    live = load_repo().base.resolve()
    src = Path(load().base).resolve()
    dst = Path(dst).resolve()
    for tree, what in ((live, "the live curated tree"), (src, "the curated tree it copies")):
        if dst == tree or tree in dst.parents or dst in tree.parents:
            raise ValueError(f"{dst} overlaps {what} ({tree}): a sandbox root goes elsewhere")
    shutil.copytree(src, dst, dirs_exist_ok=True)
    return dst


@functools.lru_cache(maxsize=2)
def _roots(config: Path | None = None) -> tuple[dict, Roots]:
    """(the parsed YAML, the validated roots) — untraced: `ROOTS` and `SOURCE` record what each read names."""
    blob, cfg = parse_config(config or CONFIG)
    return blob, cfg.roots


def load_roots(config: Path | None = None) -> Roots:
    """The validated roots (`roots:`), untraced: read them as `ROOTS.<name>`, which records the root it read."""
    return _roots(config)[1]


@functools.lru_cache(maxsize=2)
def load_generated(config: Path | None = None) -> Generated:
    """The generated tree: every path a name under `roots.generated`."""
    path = config or CONFIG
    blob, cfg = parse_config(path)
    if not _is_config(path):
        return cfg.generated
    return _bind(cfg.generated, "generated", blob["generated"], ("roots.generated", blob["roots"]["generated"]))


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

@functools.lru_cache(maxsize=2)
def _source(config: Path | None = None) -> tuple[dict, Source]:
    """(the parsed YAML, the validated `source:` files) — untraced: `SOURCE` records what each read names."""
    blob, cfg = parse_config(config or CONFIG)
    return blob, cfg.source


def _root_read(name: str) -> Path:
    """`roots.<name>`, recorded (`_trace.config_read`) as the one config value it is."""
    blob, roots = _roots()
    _config_read(CONFIG, f"roots.{name}", blob["roots"][name])
    return getattr(roots, name)


class _SourceTree(_Lazy):
    """`SOURCE`: the source root (`SOURCE / "bc_places.json"`) and its named files (`SOURCE.fwa_gpkg`, `Source`).

    A named file is the source root joined with its name. Each read records the two config values it is made of,
    `source.<name>` and `roots.source` (`_trace.config_read`), never the whole block: a stage that reads only the
    gpkg is not keyed on the coast file. `SOURCE.under(d)` is the same names under another source tree (a scratch
    `source/` a test or the mini release writes, a `--source` flag): it records `source.<name>` alone."""

    def __init__(self, loader, root: Path | None = None):
        super().__init__(loader)
        self._root = root

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        blob, files = _source()
        if name not in Source.model_fields:
            raise AttributeError(f"`source:` names no {name!r} (it names {', '.join(Source.model_fields)}); "
                                 f"any other source file is SOURCE / \"<file>\"")
        _config_read(CONFIG, f"source.{name}", blob["source"][name])
        if self._root is not None:
            return self._root / blob["source"][name]          # the name, as `read_config` states it (validated)
        _root_read("source")
        return getattr(files, name)

    def under(self, root: Path) -> "_SourceTree":
        """The named source files under `root` instead of the source root."""
        return _SourceTree(self._loader, Path(root))

    def _dir(self):
        return self._root if self._root is not None else _root_read("source")


#: `SOURCE / "bc_hydrometric_stations.json"`, `SOURCE.fwa_gpkg` — fetched data, addressed through config so a
#: fetcher and a reader cannot disagree about where a file went.
SOURCE = _SourceTree(load_roots)


#: `GENERATED.tiles`, `GENERATED.require_build()` — computed data, addressed through config
#: so a writer and a reader cannot disagree about where a build went.
GENERATED = _Lazy(load_generated)


class _RootsView:
    """`ROOTS.release`: one root of `roots:`, absolute. Each read records that one value (`roots.<name>`), never the
    block. The curated tree is read through `CURATED` (its `base` honours `CURATED_ROOT`), the source tree through
    `SOURCE`, the generated tree through `GENERATED`; `ROOTS` is for a root with no tree model (`release`) and for the
    few that need a root itself (`ROOTS.generated`). `ROOTS.curated` is refused: `roots.curated` is the LIVE root even
    under `CURATED_ROOT`, a door to a mix of live and sandboxed files."""

    def __init__(self):
        self._loader = load_roots                     # the model it reads (as every lazy here holds its loader)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in Roots.model_fields:
            raise AttributeError(f"config.yaml `roots:` names no {name!r} (it names {', '.join(Roots.model_fields)})")
        if name == "curated":
            raise AttributeError(f"ROOTS.curated is the live root whatever {ROOT_ENV} says: read CURATED.base: it "
                                 f"honours {ROOT_ENV}")
        return _root_read(name)

    def __repr__(self) -> str:
        return f"<roots {CONFIG}>"


#: `ROOTS.release`, `ROOTS.generated` — the roots of config.yaml (`Roots`).
ROOTS = _RootsView()
