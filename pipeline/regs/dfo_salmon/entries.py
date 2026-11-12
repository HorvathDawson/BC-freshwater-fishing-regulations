"""entries — the curated half, and the reconciler that decides when a human is needed.

An entry file holds one record per **location** (a named water's reach, or a cascade
default like Region 6's section A / B(i) / "tidal water Area 5"). It carries the
expensive, slow-moving half: which registry items the location is, what extent ops cut
it, and whether it reaches tributaries. Rules are never stored here — they are
re-scraped wholesale every run and joined on `location_id`.

Identity rules, each paid for by something measured in the archives:

* ``location_id`` is assigned once and derived from **nothing**. Rules and geometry hang
  off it. Deriving it from text would orphan a binding on
  ``"Highway 37 Bridge"`` -> ``"Highway 37 bridge"``.
* ``section`` is an *attribute with history*, never part of the id. Section B became
  B(i)/B(ii) between 2018 and 2020; every Skeena binding must survive that.
* ``fingerprints`` is a list, not a value. DFO flipped the Kispiox sign count between
  "three white triangular" and "the 4 triangular" **and back** — the second flip must
  cost nothing.
* ``status: dormant`` instead of deletion. These pages list *openings*, so a location
  leaves when its fishery closes and returns later. The Kispiox River Resort reach has
  cycled out and back four times since 2024.

CLI
---
    .venv/bin/python -m pipeline.regs.dfo_salmon.entries seed          # create/extend files
    .venv/bin/python -m pipeline.regs.dfo_salmon.entries reconcile     # what changed, and how bad
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES, normalize_slug
from pipeline.regs.dfo_salmon.cascade import Scope, build_scopes, resolution_chain
from pipeline.regs.parsing.entry_models import Extent
from pipeline.regs.dfo_salmon.locations import Location, normalize
from pipeline.common.names import key
from pipeline.common.curated import CURATED, GENERATED, SOURCE

logger = logging.getLogger(__name__)

ENTRIES_DIR = CURATED.dfo_salmon.places

#: Fraction of a region's locations that may go unbound before the whole region is held.
_STRUCTURAL_UNBOUND = 0.25
#: Fractional change in location count that counts as the table being rebuilt.
_STRUCTURAL_COUNT = 0.20

SEVERITY = {"ok": 0, "dormant": 0, "revived": 0, "drift": 1, "new": 2,
            "section_moved": 3, "structural": 4}


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class Binding:
    """The curated EXTENT of one reach. Shaped to feed `pipeline.atlas.reach.build.build_reach`.

    `extents` and `tributary_excludes` hold the **provincial `Extent` model**, imported
    rather than re-declared, so there is exactly one definition of "upstream of split s"
    in the repo and the DFO side cannot drift from what the resolver implements. It also
    means the entry file cannot hold a malformed extent: `Extent` validates arity —
    `upstream_of` takes exactly one split, `between` exactly two, `whole` none.

    The registry item this cuts lives on the WATER (`WaterBinding.item_ids`), not here:
    Skeena River has 15 reaches and one answer to "which blue line is this".

    `sections_cached` is a convenience, never a source of truth — a resolved section
    list must not be reused across bundle versions (AGENTS rule 6), so it records which
    bundle produced it.
    """

    extents: List[Extent] = field(default_factory=list)
    #: Three-valued, reach-level. `None` inherits the water's own setting; this becomes
    #: the rule's `includes_tributaries` (AGENTS rule 9).
    tributaries: Optional[bool] = None
    tributaries_only: bool = False
    #: Carve-outs subtracted from the tributary set — "other than Morice River and
    #: tributaries, Suskwa River and tributaries, and Two Mile Creek".
    tributary_excludes: List[Extent] = field(default_factory=list)
    sections_cached: Optional[dict] = None
    #: Spatial qualifiers kept as text rather than geometry (radius closures, lake lines).
    notes: List[str] = field(default_factory=list)
    #: True when `notes` NARROW the extent. The app must show them and must not draw the
    #: rule as a plain fill, or a closed area reads as open.
    spatial_caveat: bool = False

    @property
    def resolved(self) -> bool:
        """Has a curator authored the extent? (The item lives on the water.)"""
        return bool(self.extents)

    def to_dict(self) -> dict:
        return {
            "extents": [e.model_dump(mode="json", exclude_defaults=True) for e in self.extents],
            "tributaries": self.tributaries,
            "tributaries_only": self.tributaries_only,
            "tributary_excludes": [e.model_dump(mode="json", exclude_defaults=True)
                                   for e in self.tributary_excludes],
            "sections_cached": self.sections_cached,
            "notes": list(self.notes),
            "spatial_caveat": self.spatial_caveat,
        }

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "Binding":
        d = dict(d or {})
        return cls(
            # a DFO `rest` may hold no siblings: the build takes every other placed extent on the water
            extents=[Extent.model_validate(x, context={"rest_at_build": True}) for x in d.get("extents") or []],
            tributaries=d.get("tributaries"),
            tributaries_only=bool(d.get("tributaries_only")),
            tributary_excludes=[Extent(**x) for x in d.get("tributary_excludes") or []],
            sections_cached=d.get("sections_cached"),
            notes=list(d.get("notes") or []),
            spatial_caveat=bool(d.get("spatial_caveat")),
        )


@dataclass
class WaterBinding:
    """One waterbody, bound once. **The only place a name becomes geometry.**

    Nothing heuristic may write `item_ids`: it is set either by an exact registry
    name/variant hit, or by a curator. A near-spelling or a re-worded retry is a
    *suggestion* recorded in `match.suggestions`, never a binding — that path turned
    Yakoun River into Yakoun Lake and Long Lake into Long Creek before it was removed.

    `item_ids` is a list because one DFO name really can be several registry items:
    "Chilliwack/Vedder River (including Sumas River)" is three, "Adam and Eve Rivers"
    is two. That is a curator override, expressed here rather than in a side file.
    """

    water_id: str                 # "6:babine-lake" — stable, region + normalised name
    region: str
    region_number: int
    name: str                     # verbatim, as DFO publishes it
    aliases: List[str] = field(default_factory=list)
    sections: List[str] = field(default_factory=list)
    #: THE BINDING. Empty means unbound — no rule on this water can resolve.
    item_ids: List[str] = field(default_factory=list)
    #: Entry-wide tributary scope, when the NAME says so ("… and tributaries").
    tributaries: Optional[bool] = None
    #: {status, via, reason, candidates} — what the matcher found. Informational.
    match: dict = field(default_factory=dict)
    #: A CURATOR decision, not the matcher's:
    #:   "pending"    nobody has looked yet
    #:   "bound"      item_ids is authoritative
    #:   "not_found"  looked, and this name has no registry item — record why
    #:   "n/a"        the row carries no bindable regulation (a note, a cross-reference)
    #: `skip` deliberately does NOT live in the overrides file: a name having no
    #: geometry is a fact about THIS table, not about the name.
    resolution: str = "pending"
    resolution_reason: str = ""
    locked: bool = False
    reviewed_by: str = ""
    reviewed_at: str = ""
    note: str = ""

    @property
    def bound(self) -> bool:
        return bool(self.item_ids)

    @property
    def needs_curator(self) -> bool:
        """Unbound and nobody has ruled it out yet."""
        return not self.bound and self.resolution == "pending"


@dataclass
class EntryLocation:
    location_id: str
    region: str
    region_number: int
    kind: str
    precedence: int
    water: str
    #: The water this reach sits on — `WaterBinding.water_id`, where the registry
    #: binding lives. None for the cascade scopes (a section default is not a water).
    water_id: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    section: Optional[str] = None
    section_history: List[str] = field(default_factory=list)
    inherits: List[str] = field(default_factory=list)
    #: Every source wording ever confirmed for this location, newest last.
    fingerprints: List[str] = field(default_factory=list)
    source_text: dict = field(default_factory=dict)
    #: The EXTENT of this reach — ops and split ids. The registry item it cuts lives
    #: on the water, not here, so one answer serves all of a river's reaches.
    binding: Binding = field(default_factory=Binding)
    status: str = "active"          # active | dormant
    #: This location is the same reach as `duplicate_of`, reworded. The superset seeds
    #: every wording ever published, and 9% of them are drift variants of another —
    #: Chilliwack/Vedder has four near-identical entries. Grouping means binding once.
    duplicate_of: Optional[str] = None
    duplicate_score: Optional[float] = None
    #: None = proposed automatically · True = curator agreed · **False = CONTESTED**,
    #: the curator says these are different reaches, so it is bound on its own and no
    #: future grouping pass may re-absorb it.
    duplicate_confirmed: Optional[bool] = None
    locked: bool = False
    reviewed_by: str = ""
    reviewed_at: str = ""
    review_note: str = ""
    #: T449 (owner 2026-11-06): THE PLACE LIES WHOLLY IN TIDAL WATER — below its water's tidal limit, off the freshwater
    #: map. Written only by the review write-through from an accepted DFO mark (call "tidal water"); never proposed
    #: here. Such a place binds nothing (no extent) and is not missing: the reach row says `tidal_water`, and the
    #: bundle does not paint its water "parts not mapped" for it. Absent from the file when False.
    tidal_water: bool = False

    @property
    def is_variant(self) -> bool:
        """Folded into another location — unless the curator contested it."""
        return self.duplicate_of is not None and self.duplicate_confirmed is not False

    def to_dict(self) -> dict:
        d = asdict(self, dict_factory=lambda kv: {k: v for k, v in kv if k != "binding"})
        d["binding"] = self.binding.to_dict()
        if not self.tidal_water:
            d.pop("tidal_water")
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "EntryLocation":
        d = dict(d)
        d["binding"] = Binding.from_dict(d.get("binding"))
        if d.get("tidal_water") and d["binding"].extents:
            raise ValueError(f"{d.get('location_id')}: a place in tidal water binds nothing, but it has an extent")
        return cls(**d)


@dataclass
class EntryFile:
    region: str
    region_number: int
    region_name: str
    #: The spatial cascade — Region 6's A/B/B(i)/C/D/E/F tree. Empty elsewhere.
    scopes: List[Scope] = field(default_factory=list)
    #: Name -> registry item, curated once per waterbody.
    waters: List[WaterBinding] = field(default_factory=list)
    locations: List[EntryLocation] = field(default_factory=list)
    #: Last accepted region shape; a change is structural.
    structure: dict = field(default_factory=dict)

    def chain_for(self, scope_id: Optional[str]) -> List[str]:
        """Narrowest-first resolution order for a scope, e.g. [E:areas-5, E, A]."""
        return resolution_chain(self.scopes, scope_id)

    def water(self, water_id: Optional[str]) -> Optional[WaterBinding]:
        return next((w for w in self.waters if w.water_id == water_id), None)

    def by_fingerprint(self) -> Dict[str, EntryLocation]:
        return {fp: loc for loc in self.locations for fp in loc.fingerprints}

    def to_dict(self) -> dict:
        return {
            "region": self.region, "region_number": self.region_number,
            "region_name": self.region_name, "structure": self.structure,
            "scopes": [sc.to_dict() for sc in self.scopes],
            "waters": [asdict(w) for w in self.waters],
            "locations": [l.to_dict() for l in self.locations],
        }


#: Two scopes on the same water at or above this similarity are the same reach
#: reworded. Deliberately high: a wrong grouping hides a real reach, and the curator
#: has to notice to contest it.
DUPLICATE_THRESHOLD = 0.87


#: THE ONLY SCOPE THAT IS UNAMBIGUOUSLY THE WHOLE WATER IS NO SCOPE AT ALL.
#:
#: The parsed `op` looked like it could stand in for this and it cannot. `described`,
#: `named_tributaries` and `tributary_set` are the classifier's leftover buckets, and what sits in
#: them is not whole water:
#:
#:     [described]         Babine Lake    "within a 400 m radius of the mouth of Pinkut Creek."
#:     [tributary_set]     Bulkley River  "all tributaries ... other than the Suskwa River."
#:     [named_tributaries] Osoyoos Lake   "the portion ... north of the bridge at Highway 3"
#:     [described]         Skeena River   "mainstem waters only."
#:
#: Binding the first whole applies a 400 m radius closure to the entire lake; the second adds a
#: mainstem the scope omits; the third opens a lake the page bounds at a bridge. So the op is not
#: consulted. A locator qualifies when DFO printed NOTHING in the specific-area column — the row
#: is the water and the whole of it — and every locator that says anything at all goes to a human.
_WHOLE_SCOPE_IS_EMPTY = True


def bind_whole(ef: EntryFile) -> tuple[list, list]:
    """Write `{op: whole}` where the scope column is empty. Returns (wrote, held).

    Each condition below is a way of being wrong:

    * the water must be bound to a registry item — with no item there is nothing to take the whole of;
    * the locator must have no extent — an existing binding is a curator's and is never overwritten;
    * the scope column must be **empty**, because any text may narrow the reach (see above);
    * **no spatial caveat** — `narrows_extent` marks a note that makes the reach smaller than the
      water. Binding those whole publishes a rule as reaching water the source closes, which is the
      one direction of error that matters.
    """
    wrote, held = [], []
    for loc in ef.locations:
        st = loc.source_text or {}
        scope = (st.get("specific_area") or "").strip()
        if loc.binding.extents:
            continue
        water = ef.water(loc.water_id) if loc.water_id else None
        if not (water and water.item_ids):
            held.append((loc.location_id, "water is not bound to a registry item"))
            continue
        if scope:
            held.append((loc.location_id, f"scope names something: {scope[:56]!r}"))
            continue
        if loc.binding.spatial_caveat:
            held.append((loc.location_id, "a note narrows the extent — binding whole reads too open"))
            continue
        loc.binding.extents = [Extent(op="whole")]
        wrote.append(loc.location_id)
    return wrote, held


#: THE SCOPE PLACES ARE CURATED DATA (T679 #2, owner 2026-11-11: a decision lives in the stage that owns it):
#: `data/curated/dfo_salmon/scope_places.json` (`CURATED.dfo_salmon.scope_places`) holds how each cascade default's
#: scope becomes an extent (`scope_extents`, keyed by `(region slug, section)`; `scope_held` the scopes deliberately
#: not bound, with why), the tidal-area scopes (`tidal_extents`, keyed by the Areas a scope names — three sit under E)
#: and the page defaults (`page_defaults`, T349 below). The atlas promote guards read the same file
#: (`promote.curated_boundary_ids`: the CNR bridge cut B(i)/B(ii) name; `orphaned_ids`: the Skeena item), so a rebuild
#: that moves either no longer passes promote unseen. The measurements that chose each extent (a watershed is its
#: basin, not a walk; DFO Region 6 holds Haida Gwaii, provincial Region 6 none of it; F is the Fraser basin WITHIN
#: Region 6; E is a residual computed by `residual_minus`) are the rows' `note`s there.


class ScopePlaces(NamedTuple):
    scope_extents: Dict[tuple, List[dict]]
    scope_held: Dict[tuple, str]
    tidal_extents: Dict[tuple, List[dict]]
    #: (slug, scope) -> (label, extents, sentence printed?), in the file's order (the order defaults are seeded in)
    page_defaults: Dict[tuple, tuple]


def load_scope_places(path: Optional[Path] = None) -> ScopePlaces:
    """Read the scope places file. A missing file or key raises (AGENTS 37), never an empty table."""
    p = Path(path) if path is not None else Path(CURATED.dfo_salmon.scope_places)
    doc = json.loads(p.read_text(encoding="utf-8"))
    for key in ("scope_extents", "scope_held", "tidal_extents", "page_defaults"):
        if key not in doc:
            raise KeyError(f"{p}: no {key!r} (T679 #2: the DFO scope places)")
    return ScopePlaces(
        scope_extents={(r["region"], r["section"]): r["extents"] for r in doc["scope_extents"]},
        scope_held={(r["region"], r["section"]): r["reason"] for r in doc["scope_held"]},
        tidal_extents={tuple(r["areas"]): r["extents"] for r in doc["tidal_extents"]},
        page_defaults={(r["region"], r["scope"]): (r["label"], r["extents"], r["printed"])
                       for r in doc["page_defaults"]})


# ---------------------------------------------------------------------------
# T349 — THE PAGE DEFAULTS (owner 2026-11-02)
# ---------------------------------------------------------------------------
#
# Every region's "not specified" answer is a first-class ENTRY: one location per region / scope, kind `page_default`,
# resolved by the normal cascade (`deliver.salmon.chain_order`: narrowest first), never a hard-coded fallback. Its one
# rule is the page's own sentence, carried by the typed feed under `cascade.page_fingerprint` ("There is no fishing for
# salmon in Region 3 except …", "Unless otherwise stated …, the daily limit … is zero (0)"). Owner rulings:
#
#   * "daily limit 0" = CLOSED, and salmon non-retention = closed to salmon — every page default answers closed;
#   * unlisted = closed: a water nothing gives a quota falls to its default, which is closed. Region 2's waters that
#     drain to no BC tidal Area (Skagit, Similkameen headwaters, Nooksack tributaries — the page names only Areas
#     13/15/16/28/29) get the "other" default, which DFO prints no sentence for: no fingerprint, no rule, so it answers
#     `deliver.salmon.NOT_LISTED` — closed, from an entry, cleanly;
#   * narrowest place wins.
#
# PRECEDENCE -1, BELOW A REGION DEFAULT. A page default is "unless otherwise stated in the table below": every table row
# of every region answers first — including a neighbour's at a shared boundary section, where the default must not
# out-rank a row (a straddling section of Region 6 keeps section A's answer). Among page defaults the narrower wins
# (Region 2's three are disjoint by construction).

PAGE_DEFAULT = "page_default"
PAGE_DEFAULT_PRECEDENCE = -1


#: The page defaults themselves are `scope_places.json` `page_defaults` (T679 #2).


#: ONE MODEL FOR A SCOPE DEFAULT (owner 2026-11-02): where the page's table already prints a row for the same scope
#: (Region 4's "All Region 4 waters", 5B's MU row, 6's section A), the page's sentence is attached to THAT location as
#: another fingerprint — never a parallel entry. Region 2's remainder (to the US) has no DFO sentence and no row: no
#: entry; it falls to `deliver.salmon.NOT_LISTED` (closed — nothing opens it).
PAGE_DEFAULT_ON = {("4", "region"): "4:region:section", ("5b", "region"): "5b:region:section",
                   ("6", "region"): "6:a:region"}


def page_default_id(slug: str, scope: str) -> str:
    return PAGE_DEFAULT_ON.get((slug, scope)) or f"{slug}:default:{scope}"


def seed_page_defaults(ef: EntryFile, statements: Optional[List[dict]] = None) -> List[str]:
    """Add this region's page-default locations (`scope_places.json` `page_defaults`) where missing; returns the ids added. Never edits an
    existing one (a curator's binding stands). `statements` (`cascade.statements` of the page) supply the sentence each
    default quotes; a sentence the page no longer prints keeps the location — it then answers nothing, i.e. closed."""
    from pipeline.regs.dfo_salmon.cascade import UNLISTED, page_fingerprint
    said = {tuple(st["areas"]): st for st in statements or []}
    have = {l.location_id for l in ef.locations}
    added = []
    for (slug, scope), (label, exts, printed) in load_scope_places().page_defaults.items():
        if slug != ef.region:
            continue
        lid = page_default_id(slug, scope)
        areas = [int(n) for n in scope.split("-")[1:]] if scope.startswith("areas-") else []
        if lid in have:
            loc = next(l for l in ef.locations if l.location_id == lid)
            fp = page_fingerprint(slug, areas)
            if printed and fp not in loc.fingerprints:     # the page's own row: its sentence joins it
                loc.fingerprints.append(fp)
                added.append(lid)
            continue
        st = said.get(tuple(areas)) if printed else None
        ef.locations.append(EntryLocation(
            location_id=lid, region=slug, region_number=ef.region_number, kind=PAGE_DEFAULT,
            precedence=PAGE_DEFAULT_PRECEDENCE, water=label,
            fingerprints=[page_fingerprint(slug, areas)] if printed else [],
            source_text={"waters": "", "specific_area": label, "areas": areas,
                         "statement": (st or {}).get("k") or ("unlisted" if not printed else ""),
                         "says": (st or {}).get("t") or ("" if printed else
                                                         "owner 2026-11-02: unlisted = closed (no DFO sentence)"),
                         "unlisted": (st or {}).get("unlisted") or UNLISTED.get((st or {}).get("k"), "closed")},
            binding=Binding(extents=[Extent(**e) for e in exts], tributaries=False),
            reviewed_by="owner", reviewed_at="2026-11-02",
            review_note="T349 owner decisions: region/scope defaults are entries; limit 0 = closed; unlisted = closed",
        ))
        added.append(lid)
    return added


def bind_scopes(ef: EntryFile) -> tuple[list, list]:
    """Write the cascade defaults' extents from `scope_places.json` `scope_extents`. Returns (wrote, held).

    Never overwrites an existing binding, exactly like `bind_whole`. A scope with no table entry
    is HELD with the reason from `scope_held`, so an unbound default is always explained rather
    than silently empty.
    """
    wrote, held = [], []
    places = load_scope_places()
    for loc in ef.locations:
        if loc.water_id or loc.binding.extents:
            continue                      # a named water, or already bound
        areas_named = tuple((loc.source_text or {}).get("areas") or ())
        if areas_named:
            exts = places.tidal_extents.get(areas_named)
            if not exts:
                held.append((loc.location_id, f"no tidal extent defined for Areas {list(areas_named)}"))
                continue
            loc.binding.extents = [Extent(**e) for e in exts]
            if loc.binding.tributaries is None:
                loc.binding.tributaries = False   # the drainage area already IS the whole basin
            wrote.append(loc.location_id)
            continue
        key = (ef.region, loc.section or "")
        exts = places.scope_extents.get(key)
        if not exts:
            held.append((loc.location_id, places.scope_held.get(key, f"no extent defined for {key}")))
            continue
        loc.binding.extents = [Extent(**e) for e in exts]
        if loc.binding.tributaries is None:
            # A scope on a named river reaches its tributaries; a basin or a watershed PART
            # already holds them, and walking a part would climb into the other side.
            loc.binding.tributaries = any(e.get("item_id") and not e.get("watershed")
                                          for e in exts)
        wrote.append(loc.location_id)
    return wrote, held


def propose_groups(ef: EntryFile, threshold: float = DUPLICATE_THRESHOLD) -> List[tuple]:
    """Fold near-identical scopes on the same water into one primary.

    The primary is the location that is live today (falling back to the longest scope,
    which is usually the most explicit wording). Never touches a location the curator
    contested, and never groups across waters or sections.

    **Text similarity alone is not enough, and using it alone is dangerous.** Measured:
    "upstream of the 112th Street bridge" and "downstream of the 112th Street bridge"
    score 0.92 — they differ by four characters and are the two opposite halves of the
    river. So `op` and tributary scope must match exactly before similarity is even
    consulted; those are the fields that say which *side* of the landmark is meant.

    Returns `(primary_id, [(variant_id, score)])` for what it changed.
    """
    by_water: Dict[tuple, List[EntryLocation]] = {}
    for loc in ef.locations:
        if loc.kind != "water" or loc.duplicate_confirmed is False:
            continue
        st = loc.source_text or {}
        # The bucket key carries everything that decides WHICH water this is: the name,
        # the section, the directional op, and whether tributaries are in or out.
        # Two scopes that differ on any of those are never the same reach.
        by_water.setdefault(
            (loc.water, loc.section, st.get("op"), loc.binding.tributaries), []
        ).append(loc)

    changed = []
    for _key, locs in by_water.items():
        if len(locs) < 2:
            continue
        # Live first, then the most explicit wording — that is the one worth binding.
        locs = sorted(locs, key=lambda l: (l.status != "active",
                                           -len(l.source_text.get("specific_area", "")),
                                           l.location_id))
        taken: set = set()
        for i, primary in enumerate(locs):
            if primary.location_id in taken or primary.duplicate_of:
                continue
            group = []
            a = normalize(primary.source_text.get("specific_area", ""))
            for other in locs[i + 1:]:
                if other.location_id in taken or other.duplicate_of:
                    continue
                b = normalize(other.source_text.get("specific_area", ""))
                if not a and not b:
                    score = 1.0
                elif not a or not b:
                    continue
                else:
                    score = difflib.SequenceMatcher(None, a, b).ratio()
                if score >= threshold:
                    other.duplicate_of = primary.location_id
                    other.duplicate_score = round(score, 3)
                    taken.add(other.location_id)
                    group.append((other.location_id, other.duplicate_score))
            if group:
                changed.append((primary.location_id, group))
    return changed


def contest(ef: EntryFile, location_id: str) -> bool:
    """Break a location back out of its group, permanently. Returns False if unknown."""
    for loc in ef.locations:
        if loc.location_id == location_id:
            loc.duplicate_confirmed = False
            loc.duplicate_of = None
            loc.duplicate_score = None
            return True
    return False


def path_for(slug: str, entries_dir: Path = ENTRIES_DIR) -> Path:
    return Path(entries_dir) / f"region-{slug}.json"


def load(slug: str, entries_dir: Path = ENTRIES_DIR, registry=None) -> EntryFile:
    """One region's DFO entry file. Given a `registry`, a water's `item_ids` naming an item the
    registry ABSORBED (`pipeline.atlas.registry.flowing`) are read as the item that holds it now —
    the DFO side's one read point for item ids, like `parsing.io.read_entryfile` for the
    catalogue. Without one (the tools that write the file back) the ids are read raw."""
    p = path_for(slug, entries_dir)
    if not p.exists():
        return EntryFile(region=slug, region_number=PAGES[slug].region,
                         region_name=PAGES[slug].name)
    d = json.loads(p.read_text(encoding="utf-8"))
    if registry is not None and d.get("waters"):
        from pipeline.atlas.registry.flowing import canonical_ids
        d = {**d, "waters": [canonical_ids(w, registry) for w in d["waters"]]}
    return _from_dict(d)


def _from_dict(d: dict) -> EntryFile:
    return residual_minus(EntryFile(
        region=d["region"], region_number=d["region_number"], region_name=d["region_name"],
        structure=d.get("structure") or {},
        scopes=[Scope(**x) for x in d.get("scopes") or []],
        waters=[WaterBinding(**x) for x in d.get("waters") or []],
        locations=[EntryLocation.from_dict(x) for x in d.get("locations") or []],
    ))


#: the defaults a residual scope's area is written on (a named water inside E keeps its own extent)
_RESIDUAL_DEFAULT_KINDS = ("section_default", "area_default")


def sibling_areas(ef: EntryFile, scope: Scope) -> List[str]:
    """(owner 2026-11-09) What a RESIDUAL scope subtracts, COMPUTED: the areas its siblings' section defaults bind
    (Region 6 E = the region minus B, C, D and F), in `Scope.minus` order (a section closed whole: its closure row). Never a hand list: a sibling whose default
    changes moves E with it. A sibling with no default, or one bound by anything but whole areas (`within`, no cuts or
    items), cannot be subtracted as an area and is refused by name."""
    out: List[str] = []
    for sid in scope.minus:
        # a section closed whole (Region 6 F: "no fishing for salmon") holds its area on its section-wide closure row
        defaults = ([l for l in ef.locations if l.kind == "section_default" and l.section == sid]
                    or [l for l in ef.locations if l.kind == "closure" and l.section == sid
                        and l.location_id == f"{ef.region}:{sid.lower()}:closure"])
        if len(defaults) != 1 or not defaults[0].binding.extents:
            raise ValueError(f"dfo region {ef.region}: residual {scope.scope_id} subtracts {sid}, which has "
                             f"{len(defaults)} section default(s) with an extent — need exactly one")
        for ex in defaults[0].binding.extents:
            if ex.op != "within" or not ex.area_id or ex.splits or ex.item_id or ex.item_ids or ex.outside_areas:
                raise ValueError(f"dfo region {ef.region}: residual {scope.scope_id} subtracts {sid}, whose default "
                                 f"{defaults[0].location_id} is not a whole area ({ex.op} {ex.area_id or ''}): "
                                 "a residual can only subtract areas")
            if ex.area_id not in out:
                out.append(ex.area_id)
    return out


def residual_minus(ef: EntryFile) -> EntryFile:
    """Every `within` extent of a default under a residual scope (E and its tidal sub-scopes) takes `outside_areas` =
    `sibling_areas`, whatever the file held: the file's copy is rewritten on the next save."""
    residual = [s for s in ef.scopes if s.kind == "residual"]
    if not residual:
        return ef
    by_id = {s.scope_id: s for s in residual}
    minus: dict[str, List[str]] = {}          # computed only when a default under it has an extent (a fresh seed has none)
    for loc in ef.locations:
        if loc.kind not in _RESIDUAL_DEFAULT_KINDS or not any(ex.op == "within" for ex in loc.binding.extents):
            continue
        hit = [r for r in ef.chain_for(loc.section) if r in by_id]
        if not hit:
            continue
        if hit[0] not in minus:
            minus[hit[0]] = sibling_areas(ef, by_id[hit[0]])
        loc.binding.extents = [ex.model_copy(update={"outside_areas": list(minus[hit[0]])}) if ex.op == "within" else ex
                               for ex in loc.binding.extents]
    return ef


def save(ef: EntryFile, entries_dir: Path = ENTRIES_DIR) -> Path:
    """(T435) VALIDATED, THEN ATOMIC: the file must read back as itself (`load`'s parse of `to_dict`) before it is
    written, and it lands by temp + replace — a crash or a bad entry never leaves a half or unreadable region file."""
    from pipeline.regs.parsing.io import atomic_write
    d = residual_minus(ef).to_dict()
    if _from_dict(json.loads(json.dumps(d))).to_dict() != d:
        raise ValueError(f"dfo entries region {ef.region}: does not read back as itself; nothing written")
    p = path_for(ef.region, entries_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(p, json.dumps(d, indent=2, ensure_ascii=False) + "\n")
    return p


# ---------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str, limit: int = 34) -> str:
    s = _SLUG_RE.sub("-", normalize(text)).strip("-")
    return s[:limit].rstrip("-")


def make_water_id(loc: Location) -> Optional[str]:
    """`6:babine-lake`. Stable, and shared by every reach on that water."""
    if loc.kind != "water" or not loc.water:
        return None
    return f"{loc.region}:{_slugify(loc.water, 48)}"


def _ensure_water(ef: "EntryFile", loc: Location) -> Optional[str]:
    """Create the water record on first sight; never overwrite a curated one."""
    wid = make_water_id(loc)
    if wid is None:
        return None
    w = ef.water(wid)
    if w is None:
        w = WaterBinding(water_id=wid, region=loc.region, region_number=loc.region_number,
                         name=loc.water, aliases=list(loc.aliases),
                         tributaries=loc.tributaries)
        ef.waters.append(w)
    for a in loc.aliases:
        if a not in w.aliases:
            w.aliases.append(a)
    if loc.section and loc.section not in w.sections:
        w.sections.append(loc.section)
    return wid


def make_location_id(loc: Location, taken: set) -> str:
    """Readable and stable: `6:babine-lake:upstream-of-bymac-bridge`.

    Readability is the point — a human reviews these. Collisions get a numeric suffix
    rather than a hash, so the id stays diffable.
    """
    if loc.kind != "water":
        base = f"{loc.region}:{(loc.section or 'region').lower()}:{loc.kind.replace('_default','')}"
        if loc.areas:
            base += ":areas-" + "-".join(str(a) for a in loc.areas)
    else:
        scope = _slugify(loc.specific_area) or "whole"
        base = f"{loc.region}:{_slugify(loc.water)}:{scope}"
    candidate, n = base, 2
    while candidate in taken:
        candidate = f"{base}~{n}"
        n += 1
    return candidate


# ---------------------------------------------------------------------------
# Reconcile
# ---------------------------------------------------------------------------


@dataclass
class Outcome:
    status: str                 # ok | dormant | revived | drift | new | section_moved
    fingerprint: str
    location_id: Optional[str]
    water: str
    specific_area: str
    detail: str = ""
    candidates: List[dict] = field(default_factory=list)


@dataclass
class Report:
    region: str
    outcomes: List[Outcome] = field(default_factory=list)
    structural: List[str] = field(default_factory=list)

    @property
    def severity(self) -> int:
        s = max([SEVERITY[o.status] for o in self.outcomes], default=0)
        return max(s, SEVERITY["structural"]) if self.structural else s

    @property
    def needs_human(self) -> List[Outcome]:
        return [o for o in self.outcomes if SEVERITY[o.status] >= SEVERITY["drift"]]

    @property
    def publishable(self) -> bool:
        """A structural change holds the whole region; anything else holds only itself."""
        return not self.structural

    def counts(self) -> Dict[str, int]:
        c: Dict[str, int] = {}
        for o in self.outcomes:
            c[o.status] = c.get(o.status, 0) + 1
        return c


def _candidates(loc: Location, ef: EntryFile, limit: int = 3) -> List[dict]:
    """Rank existing locations that might be `loc` reworded. Proposals only.

    Two signals, because neither is sufficient alone (measured): text similarity misses
    wholesale rewrites, and landmark overlap misses short scopes. **Nothing auto-binds
    on these** — an exact fingerprint is the only thing that binds without a human.
    """
    out = []
    want = set(loc.landmarks)
    for cand in ef.locations:
        if key(cand.water) != key(loc.water):            # the same water's name (names.key)
            continue
        text = difflib.SequenceMatcher(
            None, normalize(cand.source_text.get("specific_area", "")),
            normalize(loc.specific_area)).ratio()
        have = set(cand.source_text.get("landmarks") or [])
        overlap = len(want & have) / len(want | have) if (want | have) else 0.0
        score = max(text, overlap)
        if score >= 0.5:
            out.append({"location_id": cand.location_id, "score": round(score, 3),
                        "text_similarity": round(text, 3), "landmark_overlap": round(overlap, 3),
                        "specific_area": cand.source_text.get("specific_area", "")})
    return sorted(out, key=lambda x: -x["score"])[:limit]


def reconcile(ef: EntryFile, locations: List[Location], structure: dict) -> Report:
    """Diff a fresh scrape against the curated file. Never mutates `ef`."""
    rep = Report(region=ef.region)
    index = ef.by_fingerprint()
    matched_ids: set = set()

    # --- structural checks: is this the same table at all? -----------------
    if ef.structure:
        old_secs, new_secs = ef.structure.get("sections") or [], structure.get("sections") or []
        if old_secs != new_secs:
            rep.structural.append(
                f"section set changed: {old_secs} -> {new_secs}")
        old_n = ef.structure.get("n_locations") or 0
        new_n = structure.get("n_locations") or 0
        if old_n and abs(new_n - old_n) / old_n > _STRUCTURAL_COUNT:
            rep.structural.append(
                f"location count moved {old_n} -> {new_n} "
                f"({(new_n - old_n) / old_n:+.0%}, threshold ±{_STRUCTURAL_COUNT:.0%})")

    for loc in locations:
        hit = index.get(loc.fingerprint)
        if hit is not None:
            matched_ids.add(hit.location_id)
            if (hit.section or "") != (loc.section or ""):
                rep.outcomes.append(Outcome(
                    "section_moved", loc.fingerprint, hit.location_id, loc.water,
                    loc.specific_area,
                    detail=f"section {hit.section!r} -> {loc.section!r}; "
                           "re-scopes everything bound under it"))
            elif hit.status == "dormant":
                rep.outcomes.append(Outcome(
                    "revived", loc.fingerprint, hit.location_id, loc.water,
                    loc.specific_area, detail="dormant location is published again"))
            else:
                rep.outcomes.append(Outcome(
                    "ok", loc.fingerprint, hit.location_id, loc.water, loc.specific_area))
            continue

        cands = _candidates(loc, ef)
        status = "drift" if cands else "new"
        rep.outcomes.append(Outcome(
            status, loc.fingerprint, None, loc.water, loc.specific_area,
            detail=("wording changed; confirm the rebind" if cands
                    else "no known location on this water"),
            candidates=cands))

    for cand in ef.locations:
        # T349: a page default is the page's sentence, not a table row — never "absent from the page"
        if cand.location_id not in matched_ids and cand.status == "active" and cand.kind != PAGE_DEFAULT:
            rep.outcomes.append(Outcome(
                "dormant", cand.fingerprints[-1] if cand.fingerprints else "", cand.location_id,
                cand.water, cand.source_text.get("specific_area", ""),
                detail="absent from the page; binding kept"))

    unbound = sum(1 for o in rep.outcomes if o.status in ("drift", "new"))
    if locations and unbound / len(locations) > _STRUCTURAL_UNBOUND:
        rep.structural.append(
            f"{unbound}/{len(locations)} locations unbound "
            f"({unbound / len(locations):.0%} > {_STRUCTURAL_UNBOUND:.0%}) — "
            "the table was probably rebuilt; hold the region")
    return rep


def apply_seed(ef: EntryFile, locations: List[Location], structure: dict, *,
               scopes: Optional[List[Scope]] = None,
               mark_dormant: bool = True) -> tuple[int, int]:
    """Add unbound records for locations not yet in the file. Never edits an existing one.

    Returns (added, dormant_marked). Safe to re-run: a locked, hand-bound record is
    untouched, so this is how a new season's locations enter the file.

    `mark_dormant=False` is for superset seeding across historical versions, where a
    location missing from *one* old page says nothing about whether it is current.
    """
    index = ef.by_fingerprint()
    taken = {l.location_id for l in ef.locations}
    added = 0
    present: set = set()
    for loc in locations:
        hit = index.get(loc.fingerprint)
        if hit is not None:
            present.add(hit.location_id)
            continue
        lid = make_location_id(loc, taken)
        taken.add(lid)
        present.add(lid)
        wid = _ensure_water(ef, loc)
        ef.locations.append(EntryLocation(
            water_id=wid,
            location_id=lid, region=loc.region, region_number=loc.region_number,
            kind=loc.kind, precedence=loc.precedence, water=loc.water,
            aliases=list(loc.aliases), section=loc.section,
            section_history=[loc.section] if loc.section else [],
            inherits=list(loc.inherits), fingerprints=[loc.fingerprint],
            source_text={"waters": loc.water, "specific_area": loc.specific_area,
                         "landmarks": loc.landmarks, "op": loc.op,
                         "anchor_types": loc.anchor_types, "excludes": loc.excludes,
                         "areas": loc.areas},
            binding=Binding(tributaries=loc.tributaries, notes=list(loc.notes),
                            spatial_caveat=loc.narrows_extent),
        ))
        added += 1

    dormant = 0
    for cand in ef.locations:
        if cand.kind == PAGE_DEFAULT:
            continue                  # T349: kept by the page's sentence (`cascade.statement_locators`), not a row
        if cand.location_id not in present and cand.status == "active":
            if mark_dormant:
                cand.status = "dormant"
                dormant += 1
        elif cand.location_id in present and cand.status == "dormant":
            cand.status = "active"
    if scopes:
        ef.scopes = scopes
    if structure:
        ef.structure = structure
    return added, dormant


# ---------------------------------------------------------------------------
# Hand-off to the reach builder
# ---------------------------------------------------------------------------


def to_reach_input(loc: EntryLocation, rules: List[dict],
                   water: Optional[WaterBinding] = None,
                   siblings: Optional[Dict[str, "EntryLocation"]] = None) -> tuple[dict, List[dict]]:
    """Shape one curated location for `pipeline.atlas.reach.build.build_reach`.

    Returns `(entry, rules)` in the dict form that builder consumes, so the DFO side
    reuses the provincial resolver, tributary walk and carve-out blocking rather than
    reimplementing them. `water` supplies the registry items; without it the entry has
    nothing matched and resolves to nothing, which is the correct behaviour for a water
    a curator has not bound yet.

    Call `build_reach(entry, rule, registry, graph)` per rule. **Not** the layers
    underneath: the review app once resolved and classified without expanding, and
    showed a curator the mainstem of a reach that included its whole tributary system.
    """
    # The registry item lives on the WATER — one answer serves all of a river's
    # reaches — so a location with no water record binds nothing.
    item_ids = list(water.item_ids) if water else []
    # THE CATALOGUE SHAPE, the one the builder reads: the entry's tributary flag flat on the
    # entry, and everything that differs per rule — its extents, `tributaries_only`, its carve-
    # outs — on the rule. `build_reach` consumes plain dicts; the models are what we STORE, so
    # they are dumped at the boundary rather than kept as dicts in the file.
    entry = {
        "entry_id": loc.location_id,
        "matched": item_ids,
        # The location's own flag, else its water's. A rule's `None` inherits this (AGENTS 9).
        "includes_tributaries": bool(loc.binding.tributaries
                                     if loc.binding.tributaries is not None
                                     else (water.tributaries if water else False)),
    }
    extents = [e.model_dump(mode="json") for e in loc.binding.extents]
    excludes = [e.model_dump(mode="json") for e in loc.binding.tributary_excludes]
    # ONLY WHAT RESOLUTION READS. The scraped row's species, dates and gear say what the rule
    # IS; none of them moves where it applies, and passing them would hand the resolver fields
    # it has no business reading.
    out = [{
        "rule_id": f"{loc.location_id}#{i}",
        "extents": list(extents),
        "includes_tributaries": loc.binding.tributaries,
        "tributaries_only": loc.binding.tributaries_only,
        "tributary_excludes": list(excludes),
    } for i, _ in enumerate(rules)]
    # A "rest" (owner 2026-11-02: "Except in those areas and times listed below") is the water minus
    # the places it names — other LOCATIONS on the water. The builder resolves a rest against the
    # entry's rules, so each named place rides along as a rule of this entry (its own extent).
    # owner 2026-11-02: "the rest-of op should be after all extents are placed; it will be the remaining
    # parts without any other extent applying" — so a DFO rest is resolved HERE, at build time: its
    # siblings are EVERY other location on the same water with a placed extent (not a rest itself, not
    # an unplaced one). A place placed later drops out of the rest without re-marking. An explicit list
    # on the extent (older marks) is ignored for DFO: the current placements decide.
    # "ALL THE OTHER PLACES ON THIS WATER" (T607, owner 2026-11-10: DFO's "all open portions of the Little Qualicum
    # River", once saved as the whole river, emptied the rest — "this has to apply to all already matched items
    # instead"). A place of this kind has NO stretch of its own: it binds the UNION of every other placed place on its
    # water (a rest included), is NEVER subtracted from a rest, and is never one of another such place's siblings.
    # Resolved at build time from the placements of the day, like a rest: a place placed later joins without re-marking;
    # an unplaced one is simply not in the union.
    # THE PLACE AS HANDED IN is what it is (a preview hands in an extent the file does not hold yet): among the
    # siblings it stands as itself, so a rest resolved inside its union never subtracts the place's OLD extent
    siblings = {**(siblings or {}), loc.location_id: loc}

    def kind(x, op: str) -> bool:
        return any(e.op == op for e in x.binding.extents)

    def on_water(of) -> list:
        return [x for lid, x in (siblings or {}).items()
                if lid != of.location_id and x.water_id and x.water_id == of.water_id and x.binding.extents
                and not kind(x, "other_places") and not (x.is_variant and x.duplicate_of == of.location_id)]

    def rule_of(sib, rest_ids: Optional[list] = None) -> dict:
        ex = [e.model_dump(mode="json") for e in sib.binding.extents]
        if rest_ids is not None:
            ex = [({**e, "siblings": rest_ids} if e.get("op") == "rest" else e) for e in ex]
        return {"rule_id": sib.location_id, "extents": ex,
                # explicit, never inherited: a sibling rides in ANOTHER place's entry, whose own flag is not its own
                "includes_tributaries": (sib.binding.tributaries if sib.binding.tributaries is not None
                                         else bool(water.tributaries if water else False)),
                "tributaries_only": sib.binding.tributaries_only,
                "tributary_excludes": [e.model_dump(mode="json") for e in sib.binding.tributary_excludes]}

    def rest_siblings(of) -> list:
        return [x for x in on_water(of) if not kind(x, "rest")]

    if kind(loc, "rest"):
        placed = rest_siblings(loc)
        ids = [x.location_id for x in placed]
        for r in out:
            r["extents"] = [({**e, "siblings": ids} if e.get("op") == "rest" else e) for e in r["extents"]]
        entry["rules"] = out + [{**rule_of(sib), "includes_tributaries": sib.binding.tributaries} for sib in placed]
    elif kind(loc, "other_places"):
        placed = on_water(loc)
        ids = [x.location_id for x in placed]
        for r in out:
            r["extents"] = [({**e, "siblings": ids} if e.get("op") == "other_places" else e) for e in r["extents"]]
        # a rest among them is resolved against ITS siblings (every other placed stretch), which ride along already
        entry["rules"] = out + [rule_of(sib, [x.location_id for x in rest_siblings(sib)] if kind(sib, "rest") else None)
                                for sib in placed]
    return entry, out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def seed_from_history(slug: str, ef: EntryFile, history_dir: Path) -> tuple[int, int]:
    """Seed the SUPERSET of every location ever published, oldest version first.

    These pages list *openings*, so a location leaves when its fishery closes and comes
    back later — the Kispiox River Resort reach has cycled out and back four times since
    2024, and Region 2's location set more than doubles once history is included
    (26 live, 66 across the archive). Curating the union once means a revival is an
    exact fingerprint hit against an already-bound record instead of a review item.

    Ordering matters: oldest first, so `location_id`s read in the order DFO introduced
    them. Nothing is marked dormant here — absence from a 2017 page says nothing about
    today. The live pass that follows sets the real active/dormant state.
    """
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.parse import parse_region
    from pipeline.regs.dfo_salmon.untangle import untangle

    files = sorted(Path(history_dir).glob(f"region{slug}_*.html"))
    added = versions = 0
    for f in files:
        try:
            u = untangle(parse_region(f.read_text(encoding="utf-8", errors="replace"), slug))
        except Exception as exc:
            logger.warning("region %s: %s unparseable (%s); skipped", slug, f.name, exc)
            continue
        locs, _rules, _sig = extract(u)
        n, _ = apply_seed(ef, locs, {}, mark_dormant=False)
        added += n
        versions += 1
    return added, versions


def _load_scrape(slug: str, out_dir: Path):
    from pipeline.regs.dfo_salmon.locations import Location as L

    p = out_dir / "locations" / f"region-{slug}.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    return [L(**x) for x in d["locations"]], d["structure"]


def _untangled(slug: str, out_dir: Path):
    """The live untangled region, for deriving the scope tree."""
    from pipeline.regs.dfo_salmon.parse import parse_cached
    from pipeline.regs.dfo_salmon.untangle import untangle

    return untangle(parse_cached(slug))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["seed", "reconcile", "group", "contest", "bind-whole", "bind-scopes",
                                        "page-defaults"])
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS)
    ap.add_argument("--scrape", type=Path, default=GENERATED.regs.dfo_salmon)
    ap.add_argument("--entries-dir", type=Path, default=ENTRIES_DIR)
    ap.add_argument("--history", action="store_true",
                    help="seed: also take every location from cached historical versions, "
                         "so a seasonal revival is an exact hit rather than a review item")
    ap.add_argument("--history-dir", type=Path, default=Path("cache/dfo_salmon/history"))
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="bind-whole: report what would be written, change nothing")
    ap.add_argument("--threshold", type=float, default=DUPLICATE_THRESHOLD,
                    help="group: scope similarity above which two reaches are the same")
    ap.add_argument("--location-id", help="contest: the location to break out of its group")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    todo = args.regions or [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    worst = 0

    if args.command == "contest":
        if not args.location_id:
            print("--location-id is required"); return 2
        for slug in todo:
            ef = load(normalize_slug(slug), args.entries_dir)
            if contest(ef, args.location_id):
                save(ef, args.entries_dir)
                print(f"contested: {args.location_id} is now its own location in region {slug}")
                return 0
        print(f"no such location: {args.location_id}")
        return 1

    if args.command == "bind-whole":
        wrote = held = 0
        for slug in todo:
            slug = normalize_slug(slug)
            ef = load(slug, args.entries_dir)
            w, h = bind_whole(ef)
            if w and not args.dry_run:
                save(ef, args.entries_dir)
            wrote += len(w); held += len(h)
            print(f"region {slug:<3} {ef.region_name:<38} whole={len(w):<4} held={len(h)}")
            if args.verbose:
                for lid, why in h:
                    print(f"      held  {lid:<44} {why}")
        print(f"\n{wrote} locator(s) bound to the whole water; {held} held for a curator"
              + (" (dry run, nothing written)" if args.dry_run else ""))
        return 0

    if args.command == "page-defaults":
        from pipeline.regs.dfo_salmon.cascade import statements
        for slug in todo:
            slug = normalize_slug(slug)
            ef = load(slug, args.entries_dir)
            src = Path(args.scrape) / "rules" / f"region-{slug}.json"
            # (T435) a missing scrape file or preamble is refused, never "+0 page defaults"
            if not src.exists():
                raise SystemExit(f"page-defaults: no scrape file {src} (scrape region {slug} first)")
            pre = json.loads(src.read_text(encoding="utf-8")).get("preamble")
            if pre is None:
                raise SystemExit(f"page-defaults: {src} has no preamble (a scrape that predates it?)")
            added = seed_page_defaults(ef, statements(pre))
            if added and not args.dry_run:
                save(ef, args.entries_dir)
            print(f"region {slug:<3} {ef.region_name:<38} page defaults +{len(added)} {added}")
        return 0

    if args.command == "bind-scopes":
        wrote = held = 0
        for slug in todo:
            slug = normalize_slug(slug)
            ef = load(slug, args.entries_dir)
            w, h = bind_scopes(ef)
            if w and not args.dry_run:
                save(ef, args.entries_dir)
            wrote += len(w); held += len(h)
            if w or h:
                print(f"region {slug:<3} {ef.region_name:<38} bound={len(w):<3} held={len(h)}")
            for lid, why in h:
                print(f"      held  {lid:<34} {why}")
        print(f"\n{wrote} cascade default(s) bound; {held} held"
              + (" (dry run, nothing written)" if args.dry_run else ""))
        return 0

    if args.command == "group":
        total = folded = 0
        for slug in todo:
            slug = normalize_slug(slug)
            ef = load(slug, args.entries_dir)
            changed = propose_groups(ef, args.threshold)
            save(ef, args.entries_dir)
            n = sum(len(g) for _p, g in changed)
            total += len(ef.locations); folded += n
            print(f"region {slug:<3} {ef.region_name:<38} folded {n} variant(s) "
                  f"into {len(changed)} primaries")
            if args.verbose:
                for primary, group in changed:
                    print(f"    {primary}")
                    for vid, score in group:
                        print(f"       + {vid}   ({score})")
        print(f"\n{folded} variants folded. Contest one with:")
        print("    python -m pipeline.regs.dfo_salmon.entries contest --location-id <id>")
        return 0

    for slug in todo:
        slug = normalize_slug(slug)
        try:
            locs, structure = _load_scrape(slug, args.scrape)
        except FileNotFoundError:
            logger.warning("region %s: no scrape; run pipeline.regs.dfo_salmon.locations first", slug)
            continue
        ef = load(slug, args.entries_dir)

        if args.command == "seed":
            hist = hv = 0
            if args.history:
                hist, hv = seed_from_history(slug, ef, args.history_dir)
            scopes = build_scopes(_untangled(slug, args.scrape))
            added, dormant = apply_seed(ef, locs, structure, scopes=scopes)
            save(ef, args.entries_dir)
            bound = sum(1 for l in ef.locations if l.binding.resolved)
            extra = f" +{hist} from {hv} archived" if args.history else ""
            print(f"region {slug:<3} {ef.region_name:<38} +{added:<4} dormant={dormant:<3} "
                  f"total={len(ef.locations):<4} scopes={len(ef.scopes):<3} bound={bound}{extra}")
        else:
            rep = reconcile(ef, locs, structure)
            worst = max(worst, rep.severity)
            c = rep.counts()
            flag = "HOLD REGION" if rep.structural else ("review" if rep.needs_human else "clean")
            print(f"region {slug:<3} {ef.region_name:<32} {flag:<12} "
                  + " ".join(f"{k}={v}" for k, v in sorted(c.items())))
            for s in rep.structural:
                print(f"    !! STRUCTURAL: {s}")
            for o in rep.needs_human if args.verbose else rep.needs_human[:5]:
                print(f"    [{o.status}] {o.water or '(default)'}: {o.specific_area[:70]}")
                if o.detail:
                    print(f"        {o.detail}")
                for cnd in o.candidates:
                    print(f"        candidate {cnd['score']:.2f} {cnd['location_id']}")

    if args.command == "reconcile":
        print(f"\nworst severity: {worst} "
              f"({[k for k, v in SEVERITY.items() if v == worst][0] if worst else 'ok'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
