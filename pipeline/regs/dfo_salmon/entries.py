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
from typing import Dict, List, Optional

from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES, normalize_slug
from pipeline.regs.dfo_salmon.cascade import Scope, build_scopes, resolution_chain
from pipeline.regs.parsing.entry_models import Extent, Tributaries
from pipeline.regs.dfo_salmon.locations import Location, normalize
from pipeline.common.curated import CURATED, GENERATED, SOURCE

logger = logging.getLogger(__name__)

ENTRIES_DIR = CURATED.regulations.entries.dfo_salmon

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
            extents=[Extent(**x) for x in d.get("extents") or []],
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

    @property
    def is_variant(self) -> bool:
        """Folded into another location — unless the curator contested it."""
        return self.duplicate_of is not None and self.duplicate_confirmed is not False

    def to_dict(self) -> dict:
        d = asdict(self, dict_factory=lambda kv: {k: v for k, v in kv if k != "binding"})
        d["binding"] = self.binding.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "EntryLocation":
        d = dict(d)
        d["binding"] = Binding.from_dict(d.get("binding"))
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


#: How each cascade default's scope becomes an extent. Keyed by `(region slug, section)`.
#:
#: TWO THINGS THIS TABLE ENCODES, both measured rather than assumed:
#:
#: 1. **A WATERSHED IS A WALK, NOT A POLYGON.** `{op: whole, item_id: <mainstem>}` with
#:    tributaries on IS the watershed — the walk follows the flow graph, so it is exact where a
#:    drainage polygon is an approximation. Measured: Skeena 84,624 sections, Nass 39,097,
#:    Fraser 325,602, and the three basins share EXACTLY ZERO sections with one another. No
#:    `areas.json` row and no atlas rebuild are needed, and `item_id` supplies the universe a
#:    cascade default lacks (it has no water record of its own).
#:
#: 2. **DFO REGION 6 IS NOT PROVINCIAL REGION 6.** DFO puts Haida Gwaii in Region 6 section D;
#:    the province puts it in Region 1, and `area:region:6` contains none of it (measured: the
#:    islands are 100% inside `area:region:1`). Neither authority is wrong and neither region may
#:    be changed — the provincial corpus subtracts Haida Gwaii from its Region 1 table for the
#:    same reason (`z1:hg_quota`, and the case `Extent.outside_area` was written for). So section
#:    A is the UNION of the two, which `extents` already expresses because a rule's extents union.
#:    Section A = 586,828 + 25,772 = 612,600 sections, exactly additive.
#: Haida Gwaii. DFO puts it in Region 6 section D; the province puts it in Region 1, and
#: `area:region:6` contains none of it — so section A must name both.
HAIDA_GWAII = "area:mu_group:management_units_6_12_and_6_13"

#: The B(i)/B(ii) divide, authored in `splits.json` and verified 14.9 m off the Skeena mainstem at
#: route measure ~131,894 m — between the Zymoetz confluence and the Usk gauge, i.e. at Terrace.
CNR_BRIDGE = "skeena_river__cnr_railway_bridge_terrace"

SCOPE_EXTENTS: Dict[tuple, List[dict]] = {
    ("6", "A"): [{"op": "within", "area_id": "area:region:6"},
                 {"op": "within", "area_id": HAIDA_GWAII}],
    # A WATERSHED IS A PREFIX OF THE FWA CODE, so the registry can hold it as an area rather than
    # every consumer repeating a walk. Cross-checked: of the 84,624 sections `build_reach` reaches
    # walking the Skeena, 84,617 carry prefix `400-` — 99.99%. `area:basin:400-` is minted by
    # `pipeline.atlas.registry.build`.
    # Bound by WALK, not by `area:basin:`, because the walk resolves against the atlas that exists
    # today while a basin area is minted only by the next registry build. The two agree to 99.99%,
    # so switching later is safe — but not at the cost of unbinding something that works now.
    ("6", "B"): [{"op": "whole", "item_id": "gnis:2936"}],            # Skeena watershed
    ("6", "C"): [{"op": "whole", "item_id": "gnis:3206"}],            # Nass watershed
    ("6", "D"): [{"op": "within", "area_id": HAIDA_GWAII}],
    # B(i)/B(ii) — ONE CUT, TWO SCOPES. The bridge divides the Skeena WATERSHED, not just the
    # mainstem, so the directional op is scoped to the Skeena item and the tributary walk does the
    # rest: `build_reach` hands the resolved measure window to the walk, which is what makes this
    # "everything above the bridge" rather than "the mainstem above it plus every tributary".
    ("6", "B(i)"): [{"op": "upstream_of", "splits": [CNR_BRIDGE], "item_id": "gnis:2936"}],
    ("6", "B(ii)"): [{"op": "downstream_of", "splits": [CNR_BRIDGE], "item_id": "gnis:2936"}],
    # F is "the portions of the FRASER watershed IN REGION 6" — a watershed meeting an
    # administrative area, which no op expresses because every op only ever ADDS water.
    # `within_area` is exactly that intersection.
    # F is "the portions of the Fraser watershed IN REGION 6" — a watershed meeting an
    # administrative area, which no op expresses because every op only ever ADDS water.
    # MEASURED: bound as the bare Fraser walk this reached 325,598 sections of which 300,569 were
    # OUTSIDE Region 6 — a Region 6 closure shutting salmon across the Fraser in regions 2, 3, 5,
    # 7 and 8. `within_area` is the intersection, and it works here (and not on a walk) because
    # both sides are area lookups: `within_area` is applied inside `resolve_extent`, BEFORE the
    # tributary walk, so on a walk-based extent it clips the seed and the walk re-escapes.
    ("6", "F"): [{"op": "within", "area_id": "area:basin:100-",
                  "within_area": "area:region:6"}],
    # E IS A RESIDUAL: "Other Mainland Watersheds" — the region minus its siblings. A rule's
    # extents UNION, so this cannot be written as more extents; `outside_areas` is the subtraction.
    # The list mirrors `Scope.minus` (B, C, D, F) and must be regenerated from it, never hand-held,
    # or a newly added section silently leaves E too wide.
    ("6", "E"): [{"op": "within", "area_id": "area:region:6",
                  "outside_areas": ["area:basin:400-", "area:basin:500-",
                                    "area:basin:100-", HAIDA_GWAII]},
                 {"op": "within", "area_id": HAIDA_GWAII,
                  "outside_areas": [HAIDA_GWAII]}],
    ("4", "region"): [{"op": "within", "area_id": "area:region:4"}],
    # 5b is the Cariboo's COASTAL half (5a is the Fraser half). Bound by the MU list the page
    # prints, not by the watershed its title names: "region 5 minus the Fraser basin" tiles region
    # 5 exactly but is only 54% MUs 5-6 to 5-11 — it also pulls in 5-12, 5-2 and 5-13, because MU
    # boundaries do not follow the Fraser divide. Measured before this was written.
    # The area itself is `mu_group_cariboo_coastal` in areas.json; it exists after the next build.
    ("5b", "region"): [{"op": "within",
                        "area_id": "area:mu_group:management_units_5_6_to_5_11"}],
}

#: Scopes deliberately NOT in the table, and why. Held rather than guessed.
SCOPE_HELD = {
    ("6", "E"): "a RESIDUAL: region 6 minus B, C, D and F. A rule's extents UNION, so the "
                "subtraction cannot be written as more extents, and `outside_area` takes an "
                "`area:` id while B/C/F are walks. Needs a general `minus`.",
    ("5b", "region_PLACEHOLDER"): "scoped to Management Units 5-6 to 5-11, which has no area yet. Two "
                      "readings agree on the water and either would do: the MU list the page "
                      "prints (an `areas.json` mu_group row, like Haida Gwaii's), or the "
                      "watershed the page's own title names — 5b is the COASTAL half of region "
                      "5 and 5a the Fraser half, so 5b is region 5 minus the Fraser walk, which "
                      "needs the same `minus` as section E. The MU list is the operative text, "
                      "so prefer it and use the watershed as the cross-check. NOTE its "
                      "'in the aggregate (combined total)' is a RULE-side quota shared across "
                      "waters, not a geography: `CatalogueRule.combined` + `aggregation_domain` "
                      "already carry exactly that (the Kootenay West Arm kokanee quota is the "
                      "worked precedent).",
}

#: A tidal-area scope is keyed by the AREAS it names, not by its section letter — three of them
#: sit under E and would otherwise collect E's own reason.
_TIDAL_HELD = ("'all streams flowing into tidal water Area N' is a drainage relation, not "
               "containment: the streams are freshwater ABOVE the marine area, so no polygon "
               "contains them. Needs the PFMA subarea layer re-fetched — the local copy has been "
               "dissolved to a single row — then a drainage membership pass at atlas build.")


#: The tidal scopes, keyed by the Area numbers the page names. `feature_types` is load-bearing:
#: Area 5 and Area 6 say "streams", Areas 3-4-5-6 says "streams AND LAKES", and that is the only
#: field that carries the difference.
TIDAL_EXTENTS: Dict[tuple, List[dict]] = {
    (5,): [{"op": "within", "area_id": "area:drains_to:pfma_5", "feature_types": ["stream"]}],
    (6,): [{"op": "within", "area_id": "area:drains_to:pfma_6", "feature_types": ["stream"]}],
    (3, 4, 5, 6): [{"op": "within", "area_id": f"area:drains_to:pfma_{n}"} for n in (3, 4, 5, 6)],
}


def bind_scopes(ef: EntryFile) -> tuple[list, list]:
    """Write the cascade defaults' extents from `SCOPE_EXTENTS`. Returns (wrote, held).

    Never overwrites an existing binding, exactly like `bind_whole`. A scope with no table entry
    is HELD with the reason from `SCOPE_HELD`, so an unbound default is always explained rather
    than silently empty.
    """
    wrote, held = [], []
    for loc in ef.locations:
        if loc.water_id or loc.binding.extents:
            continue                      # a named water, or already bound
        areas_named = tuple((loc.source_text or {}).get("areas") or ())
        if areas_named:
            exts = TIDAL_EXTENTS.get(areas_named)
            if not exts:
                held.append((loc.location_id, f"no tidal extent defined for Areas {list(areas_named)}"))
                continue
            loc.binding.extents = [Extent(**e) for e in exts]
            if loc.binding.tributaries is None:
                loc.binding.tributaries = False   # the drainage area already IS the whole basin
            wrote.append(loc.location_id)
            continue
        key = (ef.region, loc.section or "")
        exts = SCOPE_EXTENTS.get(key)
        if not exts:
            held.append((loc.location_id, SCOPE_HELD.get(key, f"no extent defined for {key}")))
            continue
        loc.binding.extents = [Extent(**e) for e in exts]
        if loc.binding.tributaries is None:
            # A watershed scope reaches its tributaries by definition; that IS the watershed.
            loc.binding.tributaries = any(e.get("item_id") for e in exts)
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


def load(slug: str, entries_dir: Path = ENTRIES_DIR) -> EntryFile:
    p = path_for(slug, entries_dir)
    if not p.exists():
        return EntryFile(region=slug, region_number=PAGES[slug].region,
                         region_name=PAGES[slug].name)
    d = json.loads(p.read_text(encoding="utf-8"))
    return EntryFile(
        region=d["region"], region_number=d["region_number"], region_name=d["region_name"],
        structure=d.get("structure") or {},
        scopes=[Scope(**x) for x in d.get("scopes") or []],
        waters=[WaterBinding(**x) for x in d.get("waters") or []],
        locations=[EntryLocation.from_dict(x) for x in d.get("locations") or []],
    )


def save(ef: EntryFile, entries_dir: Path = ENTRIES_DIR) -> Path:
    p = path_for(ef.region, entries_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(ef.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
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
        if normalize(cand.water) != normalize(loc.water):
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
        if cand.location_id not in matched_ids and cand.status == "active":
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
                   water: Optional[WaterBinding] = None) -> tuple[dict, List[dict]]:
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
    # `build_reach` consumes plain dicts; the models are what we STORE, so they are
    # dumped at the boundary rather than kept as dicts in the file.
    excludes = [e.model_dump(mode="json") for e in loc.binding.tributary_excludes]
    entry = {
        "entry_id": loc.location_id,
        "matched": item_ids,
        "tributaries": Tributaries(
            included=bool(loc.binding.tributaries
                          if loc.binding.tributaries is not None
                          else (water.tributaries if water else False)),
            only=bool(loc.binding.tributaries_only),
            excludes=list(loc.binding.tributary_excludes),
        ).model_dump(mode="json"),
        "scope": [],
    }
    out = []
    for i, r in enumerate(rules):
        out.append({
            "rule_id": f"{loc.location_id}#{i}",
            "extents": [e.model_dump(mode="json") for e in loc.binding.extents],
            # Three-valued: None inherits entry.tributaries.included (AGENTS rule 9).
            "includes_tributaries": loc.binding.tributaries,
            "tributaries_only": loc.binding.tributaries_only,
            "tributary_excludes": excludes,
            "species": r.get("species"), "dates": r.get("dates"),
            "limits_gear": r.get("limits_gear"),
        })
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
    ap.add_argument("command", choices=["seed", "reconcile", "group", "contest", "bind-whole", "bind-scopes"])
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
