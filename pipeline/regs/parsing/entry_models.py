"""The op + split binding a rule or entry uses to select sections: `Op` and `Extent`.

Entries are `catalogue.CatalogueEntry`; these two are the validated form of one extent, and the
DFO salmon locations store their bindings with them.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Extent — the op + split binding
# ---------------------------------------------------------------------------


_FEATURE_TYPES = frozenset({"stream", "lake", "wetland"})


class Op(str, Enum):
    """How a rule/scope selects sections from the entry's reach.

    THE REACH IS `entry.matched` — a LIST. One synopsis row can cover several registry items
    ("CHILLIWACK / VEDDER RIVERS" = the Chilliwack + the Vedder + the Vedder Canal), so "the matched
    item" is not a single water and the ops must be defined across all of them:

    - `whole` = every section of every covered item.
    - The directional ops FOLLOW THE WATER, not the item boundary: `downstream_of X` is every covered
      section downstream of X in the flow graph, crossing from one covered item into the next. That is
      what the synopsis means — "from the Brilliant Dam to the confluence with the Columbia River"
      runs to the Kootenay's mouth regardless of which blue line carries it — and it is why a
      name-change junction (Chilliwack -> Vedder) needs no special case.
    - `item_id` NARROWS an extent to one covered item, and is how a reach that stops at a junction is
      expressed: "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge" is
      `downstream_of tamihi` + `item_id=<Chilliwack>`, because the Chilliwack ENDS at Vedder Crossing.
      A split named by an `item_id`-scoped extent must be a cut-point on that item (enforced by
      `validate_entry_splits`).

    NOTE: nothing resolves these ops to sections yet — they are recorded intent. This docstring is the
    contract that resolver must implement.
    """

    WHOLE = "whole"                 # every section of every covered item (no split refs)
    UPSTREAM_OF = "upstream_of"     # sections above split s, following the water  (1 split)
    DOWNSTREAM_OF = "downstream_of" # sections below split s, following the water  (1 split)
    BETWEEN = "between"             # sections between a and b                     (2 splits)
    WITHIN = "within"              # sections inside an area/polygon (area, not splits)
    #: THE REST OF THE WATER — "Other parts: trout/char daily quota = 1" (Bull River), "All other
    #: parts" (Elk River). The rule's water (as `whole` selects it, with the rule's own tributary
    #: walk) MINUS every section the named SIBLING rules of the same entry bind, their walks
    #: included. Resolved by `reach.build.build_reach`, never by `extent.resolve_extent` alone: a
    #: complement is a statement about other rules' reaches. A sibling that does not bind makes the
    #: complement UNKNOWN (`Reason.complement_unknown`) — never guessed as the whole water.
    REST = "rest"                   # the water minus the named siblings' sections   (0 splits)
    #: THE STEELHEAD WATERS — every section the reach builder marks steelhead KNOWN
    #: (`pipeline.atlas.reach.steelhead`): every water any rule of a row naming steelhead binds, the
    #: tributary walk of every row flagged `anadromous_rainbow` (held to no region), and the curated
    #: steelhead list (`data/curated/regulations/steelhead_waters.json`) with its walk — MINUS every
    #: section the named `siblings` bind (the rule or licensing record this one is the twin of). How
    #: the provincial steelhead set (zp:steelhead r1b/r2b/r4b and the stamp twin) reaches a known water
    #: its stream-only base does not: a lake (Tenas, Khartoum, Lois), a stream past the steelhead
    #: regions (the Thompson's Nicola headwaters in Region 8) — user ruling 2026-10-02. A fact of the
    #: WHOLE corpus, so only `reach.build.build_reaches` resolves it; `build_reach` alone (the review
    #: app) leaves it unresolved `needs_corpus`.
    STEELHEAD_WATERS = "steelhead_waters"  # the known steelhead waters minus siblings'  (0 splits)


class Extent(BaseModel):
    """One `op + split ids` binding. A rule's `extents` is a list → UNION (covers "A plus B").

    `item_id` scopes this extent to ONE registry item — either one of the several the entry covers
    (`entry.matched`), or a different item entirely ("…plus Tenas Lake", a named side channel).
    `item_ids` is the same thing over SEVERAL items, for a reach whose two ends sit on different
    waters: "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge" is bounded by a cut on the
    Chilliwack and a cut on the Vedder, so scoping it to either one alone puts the other end out of
    scope and the reach cannot be resolved at all. Set one or the other, never both.
    Without either, a directional op follows the water across every covered item; see `Op`.
    `area_id`/`area_kind` carry a `within(area)`.

    `watershed` turns a directional op on ONE river into that river's WATERSHED on that side of the
    cut, decided by FWA code position rather than by the tributary walk — see the field.
    """

    # FORBID, not ignore: `within_area` was silently dropped for years because an unknown key was
    # ignored, and `watershed` misspelt would bind the river alone while reading like a watershed.
    model_config = ConfigDict(frozen=True, extra="forbid")

    op: Op
    splits: List[str] = Field(default_factory=list, description="curated split ids this extent binds to")
    item_id: Optional[str] = Field(default=None, description="registry id, if this extent scopes a different item")
    item_ids: List[str] = Field(
        default_factory=list,
        description="registry ids, when this extent spans SEVERAL items (a reach whose two cut-points "
        "sit on different waters). Mutually exclusive with item_id.",
    )
    area_id: Optional[str] = Field(default=None, description="area id (op=within), e.g. 'area:watershed:liard_river'")
    area_kind: Optional[str] = Field(
        default=None,
        description="admin area FAMILY (op=within), used INSTEAD of area_id when a regulation "
        "is written against every area of a kind: 'national_parks', 'ecological_reserves', "
        "'restricted_land_access', 'indigenous_land', 'wma', 'park'. The resolver takes the "
        "union of every `area:<kind>:*` item, so 'prohibited in National Parks' is one extent "
        "rather than seven ids that go stale when the province gazettes another one.",
    )
    feature_types: List[str] = Field(
        default_factory=list,
        description="restrict what this extent selects to these feature kinds (subset of "
        "stream/lake/wetland); empty = every kind. On `within` it filters the area's members; on "
        "any op it also filters the rule's reach AFTER the tributary walk (`reach.classify`), so "
        "'lakes of the Fraser watershed' is the Fraser and its tributaries, lakes only. Every "
        "extent of a rule says it or none does (`CatalogueRule` refuses a mixture).",
    )
    within_area: Optional[str] = Field(
        default=None,
        description="INTERSECT whatever this extent selects with an area polygon — 'any stream in "
        "the Fraser River Watershed OF REGION 5' is a watershed meeting an administrative area, "
        "which no op can express because every op only ever ADDS water. Combines with any op. "
        "DECLARED HERE BECAUSE IT WAS NOT: the resolver has always read it and the prompt has "
        "always documented it, but the model did not, so `extra='ignore'` dropped it on every "
        "round-trip — silently widening the three curated regulations that use it to the whole "
        "watershed.",
    )
    outside_area: Optional[str] = Field(
        default=None,
        description="SUBTRACT an area polygon from whatever this extent selects — the mirror of "
        "`within_area`, for a regulation whose own header carves one out. Region 1's quota table "
        "is printed '(excluding Haida Gwaii)' and Haida Gwaii's table is printed beside it; "
        "without this the book's own exclusion is unsayable, both tables bind the Yakoun River, "
        "and the screen states Trout 4 and Trout/char 5 with no way to choose. Applied AFTER any "
        "tributary walk, for the same reason `within_area` is.",
    )

    watershed: bool = Field(
        default=False,
        description="A PART OF A WATERSHED, cut by FWA code. On `upstream_of` / `downstream_of` / "
        "`between` scoped to one river, select the river's WATERSHED on that side of the cut(s) — "
        "every lake and stream whose FWA code (or `basin_wsc`) lies in the river's basin and whose "
        "tributary group joins the river on that side — instead of the river alone. 'CLOSED in the "
        "Fraser River Watershed upstream of Williams Lake River' is `upstream_of` the WLR split on "
        "the Fraser with `watershed: true`. The side is read from the code position (the group "
        "number after the river's code is the confluence's distance along it, in millionths), not "
        "from a walk: the walk follows real bifurcations across a divide (Dewar Lake drains to both "
        "the WLR and Hawks Creek) and stops where a lake is mid-reach. A tributary whose mouth IS "
        "the cut is on NEITHER side — the book names it when it is meant ('downstream of AND "
        "INCLUDING Williams Lake River' adds `within area:basin:100-382626-`). The river's own "
        "pieces are cut by route measure exactly as without the flag. A watershed already holds "
        "its tributaries, so the rule must not walk (`includes_tributaries` is refused); "
        "`tributaries_only` means the part without the river, and `feature_types` still limit it.",
    )

    siblings: List[str] = Field(
        default_factory=list,
        description="op=rest (and op=steelhead_waters) only: the rule ids (same entry) whose "
        "sections this extent is the COMPLEMENT of — on steelhead_waters, the rule (or, on a "
        "licensing record, the record) whose sections are taken out of the known steelhead waters. 'Other parts' is the rest of the water after the rules that name parts; "
        "each is listed explicitly so the complement never depends on rule order, and "
        "`CatalogueEntry` checks each exists and draws its place.",
    )

    @property
    def scope_ids(self) -> List[str]:
        """The registry items this extent is scoped to — [] meaning "every item the entry covers"."""
        if self.item_ids:
            return list(self.item_ids)
        return [self.item_id] if self.item_id else []

    outside_areas: List[str] = Field(
        default_factory=list,
        description="SUBTRACT SEVERAL areas — `outside_area` for more than one carve-out. A rule's "
        "extents UNION, so a subtraction can never be written as more extents; and a RESIDUAL "
        "scope needs several at once. DFO Region 6 section E is the case: 'Other Mainland "
        "Watersheds' is the region minus the Skeena, the Nass, the Fraser and Haida Gwaii, and "
        "with only a single-valued field it cannot be said at all. The two fields are unioned, so "
        "`outside_area` keeps working and needs no migration.",
    )
    outside_area_kind: Optional[str] = Field(
        default=None,
        description="SUBTRACT A WHOLE FAMILY of areas — the mirror of `area_kind` on `within`. "
        "'Basic and supplementary licences and stamps are not valid in National Parks' is about "
        "all seven parks: `outside_area_kind: national_parks` takes every `area:national_parks:*` "
        "item out of what this extent selects, after any walk, like `outside_area`.",
    )
    outside_items: List[str] = Field(
        default_factory=list,
        description="SUBTRACT WATERS — registry item ids (`wbk:…`, `gnis:…`) — from whatever this "
        "extent selects, after any walk, like `outside_area`. A `within(area)` takes every water "
        "the polygon touches (the atlas flags a lake that reaches into an area, because a lake is "
        "never cut), so an area row can land on a lake that is almost wholly outside it: the "
        "Creston Valley WMA's 'bass daily quota = unlimited' bound all of Kootenay Lake (1% inside "
        "the WMA), the Chilkoot Trail's 'No Fishing' all of Bennett Lake (0.5% inside). This is "
        "how such a lake is taken back out; it is also the 'except X Lake' a `within` could not say.",
    )

    walk_past: bool = Field(
        default=False,
        description="ON A `tributary_excludes` CARVE-OUT ONLY: remove the named water's own "
        "sections from the rule but keep walking above them. A carve-out otherwise removes the "
        "water AND everything upstream of it. 'Elk River's tributaries — see separate listings "
        "for … Fording R. downstream of Josephine Falls': the lower Fording has its own row, "
        "which does not include its tributaries, so the creeks feeding it are still the Elk "
        "row's. Use it where the excluded water's own row does not take its tributaries.",
    )

    @model_validator(mode="after")
    def _check_arity(self) -> "Extent":
        n = len(self.splits)
        if self.item_id and self.item_ids:
            raise ValueError("set item_id or item_ids, not both")
        if len(set(self.item_ids)) != len(self.item_ids):
            raise ValueError(f"item_ids has duplicates: {self.item_ids}")
        if self.op in (Op.UPSTREAM_OF, Op.DOWNSTREAM_OF) and n != 1:
            raise ValueError(f"op {self.op.value} needs exactly 1 split id, got {n}")
        if self.op == Op.BETWEEN and n != 2:
            raise ValueError(f"op between needs exactly 2 split ids, got {n}")
        if self.op == Op.WHOLE and n != 0:
            raise ValueError(f"op whole takes no split ids, got {n}")
        if self.outside_area and self.outside_area in self.outside_areas:
            raise ValueError(f"{self.outside_area!r} is in both outside_area and outside_areas")
        if len(set(self.outside_items)) != len(self.outside_items):
            raise ValueError(f"outside_items has duplicates: {self.outside_items}")
        if any(str(x).startswith("area:") for x in self.outside_items):
            raise ValueError("outside_items names waters; an area is subtracted by outside_area(s)")
        if set(self.outside_items) & set(self.scope_ids):
            raise ValueError("an extent cannot subtract the water it is scoped to")
        if self.watershed:
            if self.op not in (Op.UPSTREAM_OF, Op.DOWNSTREAM_OF, Op.BETWEEN):
                raise ValueError(f"watershed cuts a river's basin at a split — op {self.op.value} "
                                 f"has no cut (a whole watershed is `within area:basin:<code>-`)")
            if self.item_ids:
                raise ValueError("watershed is the basin of ONE river — scope it with item_id")
            if self.area_id or self.area_kind:
                raise ValueError("watershed takes its basin from the river's code, not an area_id")
        if self.op == Op.REST:
            # THE COMPLEMENT OF NAMED SIBLINGS, over the rule's water (or the items it scopes).
            # Everything that would add or cut water is refused: a complement of a cut or an area
            # is a different shape, and the one the book prints is "the rest of this water".
            if not self.siblings:
                raise ValueError("op rest needs `siblings` — the rule ids whose sections it is "
                                 "the rest of")
            if len(set(self.siblings)) != len(self.siblings):
                raise ValueError(f"siblings has duplicates: {self.siblings}")
            extra = [k for k in ("splits", "area_id", "area_kind", "within_area", "outside_area",
                                 "outside_areas", "outside_area_kind", "outside_items", "watershed")
                     if getattr(self, k)]
            if extra:
                raise ValueError(f"op rest is the rest of the rule's water after its siblings — "
                                 f"it takes no {extra}")
        elif self.op == Op.STEELHEAD_WATERS:
            # THE KNOWN STEELHEAD WATERS, minus what the named siblings bind. Nothing that adds or
            # cuts water: the set is the reach builder's, never shaped by hand. `outside_area_kind`
            # is the one limit (the stamp stops at national parks, as its base does).
            if len(set(self.siblings)) != len(self.siblings):
                raise ValueError(f"siblings has duplicates: {self.siblings}")
            extra = [k for k in ("splits", "item_id", "item_ids", "area_id", "area_kind",
                                 "within_area", "outside_area", "outside_areas", "outside_items",
                                 "watershed", "feature_types")
                     if getattr(self, k)]
            if extra:
                raise ValueError(f"op steelhead_waters is the reach builder's known steelhead "
                                 f"waters — it takes no {extra}")
        elif self.siblings:
            raise ValueError(f"siblings belongs to op rest, not {self.op.value}")
        if self.op == Op.WITHIN and not (self.area_id or self.area_kind or self.splits):
            raise ValueError("op within needs an area, an area_kind, or bounding split ids")
        if self.feature_types:
            # ANY OP, not only `within`. Seven zone rules — "lakes of the Fraser watershed", "any
            # stream upstream of the Forrest Kerr canyon" — carry it on a `whole` or a reach, and
            # the builder applies it to their reach after the tributary walk. This model refused
            # the shape the resolver was built to read, so the review app flagged all seven.
            bad = [t for t in self.feature_types if t not in _FEATURE_TYPES]
            if bad:
                raise ValueError(f"invalid feature_types {bad}; allowed: {sorted(_FEATURE_TYPES)}")
        return self
