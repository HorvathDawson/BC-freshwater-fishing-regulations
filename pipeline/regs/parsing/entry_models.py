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
    """

    model_config = ConfigDict(frozen=True)

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
