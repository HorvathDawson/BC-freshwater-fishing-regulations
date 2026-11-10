"""A reviewer's MARK, the append-only store that holds them, and the one staleness decision.

    data/curated/review/marks.jsonl      (`CURATED.review.marks`)

One JSON object per line, each a `Mark` (pydantic, `extra=forbid`). APPEND-ONLY: a later mark on
the same target supersedes an earlier one (`latest`), so the file is the history and git holds it.
It is REVIEWED curated data (AGENTS 35): no program regenerates it, a migration only APPENDS
re-stamped copies (`carry.carry_marks`).

A mark is taken against the fingerprints of its target AS IT WAS (`fingerprint`), and
`stale_reasons` compares them with the target NOW. The answer is a list of typed reasons
(`StaleReason`), empty while the mark still holds — never one undifferentiated "changed".

RULE IDENTITY (PLAN §4): a record of the mark follows the current record with the same key AND
the same meaning; failing that, a record of the same kind (rule / licensing) with the same
meaning that nothing else claimed (a re-parse renumbered the rules); failing that it is "rule
changed". A mark never lands silently on another rule.
"""
from __future__ import annotations

import datetime as dt
import fcntl
import json
import os
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Optional, Sequence, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StaleReason(str, Enum):
    """Why a mark no longer holds — which of the reviewer's questions a change reopens."""
    book_text_changed = "book text changed"
    rule_changed = "rule changed"
    binding_moved = "binding moved"
    answer_changed = "answer changed"
    awaiting_build = "awaiting build"


#: the order reasons are reported in (and the order of the enum)
STALE_REASONS: tuple[StaleReason, ...] = tuple(StaleReason)

Scope = Literal["entry", "rule", "split", "dfo", "tidal", "slough", "q154", "name"]
#: T55 (owner 2026-10-28): the owner's call on a slough / canal candidate (`review.slough`)
SloughCall = Literal["stream", "not stream"]
#: Q154 (owner 2026-10-28): the owner's call on a sample small pond a river runs through (`review.q154`)
Q154Call = Literal["pool", "real lake"]
#: T224 (owner 2026-10-28): the owner's call on a name-variant candidate (`review.names`)
NameCall = Literal["accept", "reject"]
#: T359 (owner 2026-11-03): the owner's call on a tidal candidate — "whole": the whole water is tidal (a Fraser delta
#: channel). Place y with no cut; the tidal writer (`tools.tidal_limits`) makes it a `whole` row
TidalCall = Literal["whole"]
#: T449 (owner 2026-11-06): the owner's call on a DFO place PROPOSED as wholly in tidal water (`review.dfo.tidal_proposal`)
#: — "tidal water": accepted (a verified mark; the write-through sets the place's `tidal_water`, it binds nothing and is
#: not missing); "not tidal": rejected (the place keeps needing its freshwater extent; the proposal is no longer shown)
DfoCall = Literal["tidal water", "not tidal"]
#: T185 (owner 2026-10-27): THE MARKS ARE THE DECISIONS. The retired accept step wrote marks of
#: scope `proposal` (P y / P n on a proposal's digest); the store is append-only, so a line of it
#: may still be there — `load` skips it, and nothing makes a new one.
RETIRED_SCOPES = ("proposal",)
Verdict = Literal["verified", "wrong", "unsure", "deferred", "cleared"]
#: owner 2026-10-29: A CLEARED MARK takes back the mark before it — the store is append-only, so an
#: accidental mark (an "accept all visible" that took too much) is undone by a newer line, never by an
#: edit. Its target reads UNMARKED (`latest` drops it): no queue, health count, apply path, proposal or
#: build ever acts on it. A later mark of any other verdict stands as usual.
CLEARED = "cleared"
Answer = Literal["y", "n", "?"]
#: T189: the reviewer's call on tributaries — the water's tributaries should be in, or out
Tributaries = Literal["include", "exclude"]
#: the key a `Mark.tributaries` call on the ENTRY's own scope is made under (any other is a rule id)
ENTRY_KEY = "entry"
#: the checks each scope answers (PLAN §2: T text, R rules, P place, A answer; a split is a place)
CHECKS: dict[str, tuple[str, ...]] = {"entry": ("T", "R", "P", "A"),
                                      "rule": ("T", "R", "P", "A"),
                                      "split": ("P",),
                                      # T164: a DFO salmon location (`review.dfo`) — T the notice's
                                      # words for the place, R its scraped rows, P the extent lands
                                      # right. No A yet: the app serves no DFO rule (T164 step 5)
                                      "dfo": ("T", "R", "P"),
                                      # T212: a proposed tidal boundary (`review.tidal`) — P only: the
                                      # cut is where the tidal boundary is. Accepting it is P y with
                                      # the proposed cut as a new split
                                      "tidal": ("P",),
                                      # T55: a slough / canal candidate (`review.slough`) — P only:
                                      # the call (`Mark.call`) is right. Accepting the pre-mark is
                                      # P y with call "stream"
                                      "slough": ("P",),
                                      # Q154: a sample pond (`review.q154`) — P only: the call
                                      # (pool = river rules / real lake) is right; unsure is P ?
                                      "q154": ("P",),
                                      # T224: a name-variant candidate (`review.names`) — P only: the
                                      # call (accept / reject the name for that water) is right
                                      "name": ("P",)}

#: THE PROBLEM CATEGORIES a "wrong" mark names (PLAN §2), grouped by the check that finds them.
#: The review screen offers exactly these; the store keeps any string (an old mark stays valid).
PROBLEMS: dict[str, tuple[str, ...]] = {
    "T": ("text differs from the book", "wrong page"),
    "R": ("clause missing", "rule invented", "species", "quota", "size", "dates", "gear",
          "tributary scope"),
    "P": ("wrong water", "split misplaced", "ambiguous cut pick", "tributary walk over-reaches",
          "undrawn part / unresolved place", "split missing"),
    "A": ("app answer differs", "open/closed wrong"),
}


def verdict_from_answers(scope: str, answers: Mapping[str, str]) -> Optional[str]:
    """THE VERDICT THE ANSWERS GIVE (PLAN §2): any n -> wrong; else any ? -> unsure; every check of
    the scope y -> verified; otherwise none yet (a check unanswered). `deferred` is never derived:
    it is the reviewer's own call, with a note. `Mark._consistent` refuses any other verdict."""
    got = [answers.get(k) for k in CHECKS[scope]]
    if "n" in got:
        return "wrong"
    if "?" in got:
        return "unsure"
    if got == ["y"] * len(got):
        return "verified"
    return None


#: THE REVIEWER'S WORDS for a refused mark (T142): the model's message (pydantic prefixes it
#: "Value error, ") -> what the person at the review screen is told to do. Matched by the model's
#: own phrase, so a reworded model message falls through to itself, never to a wrong instruction.
REFUSAL_WORDS: tuple[tuple[str, str], ...] = (
    ("wrong needs a problem category and a note",
     "A wrong mark needs a problem (pick one from the list under the checklist) and a note "
     "saying what is wrong."),
    ("wrong needs a check answered n", "A wrong mark needs a check answered n."),
    ("verified needs every check", "Verified needs every check answered y."),
    ("unsure needs a check answered ?", "Unsure needs a check answered ? and none answered n."),
    ("deferred needs a note saying why", "To defer, write a note saying why."),
    ("cleared needs a note saying why", "To clear a mark, write a note saying why."),
    ("nothing to clear", "There is no mark in force on it to clear."),
    ("a mark needs the reviewer's name", "Type your name first: every mark names its reviewer."),
    ("an extent fix is proposed on a wrong or unsure mark",
     "A proposed extent fix says the extent is wrong: answer P n (or ?) to record it."),
    ("a new split is proposed on a wrong or unsure mark",
     "A proposed new split says the extent is missing a cut: answer P n (or ?) to record it."),
)


def unanswered_words(scope: str, *, awaiting: bool = False) -> str:
    """What a reviewer is told when the answers give no verdict yet (a check left unanswered).
    `awaiting`: the target awaits its build (`vintage.entry_state`), so only T and R can be
    answered (`vintage.CHECKS_WHILE_AWAITING`) and T y R y is no verdict (Q96, not built) —
    "answer every check" would ask for what the screen will not let them answer."""
    if awaiting:
        from pipeline.regs.review.vintage import CHECKS_WHILE_AWAITING
        can = [c for c in CHECKS[scope] if c in CHECKS_WHILE_AWAITING]
        wait = [c for c in CHECKS[scope] if c not in CHECKS_WHILE_AWAITING]
        return (f"Until it is built only {' and '.join(can)} can be answered and "
                f"{' and '.join(wait)} wait, so \"all fine\" waits for the build: record it "
                f"wrong (answer n) or unsure (answer ?), or defer it with a note.")
    return (f"Answer every check ({' '.join(CHECKS[scope])}) with y, n or ? before recording "
            f"the mark, or defer it with a note.")


def refusal_words(msg: str, field: str | None = None) -> str:
    """A refused mark's message in the reviewer's words (`REFUSAL_WORDS`); `field` is the payload
    field the refusal names, if any (an empty `reviewer` is refused by its length, not a phrase).
    A message no phrase matches (the gate's, awaiting build's — already sentences) is passed through
    as it is, less pydantic's "Value error, " prefix."""
    m = str(msg).removeprefix("Value error, ")
    if field == "reviewer":
        return dict(REFUSAL_WORDS)["a mark needs the reviewer's name"]
    for phrase, words in REFUSAL_WORDS:
        if phrase in m:
            return words
    return m


#: T191: THE EXTENT OPS A REVIEWER CAN PROPOSE, by the catalogue's own names (`entry_models.Op`),
#: and how many split points each takes. The screen shows the owner's words (`OP_WORDS`).
#: `within` names an area instead of points; `rest` and `steelhead_waters` take neither (their
#: siblings are the entry's other rules — the proposal says which in the note).
OP_POINTS: dict[str, int] = {"whole": 0, "upstream_of": 1, "downstream_of": 1, "between": 2,
                             "within": 0, "rest": 0, "steelhead_waters": 0}
OP_WORDS: dict[str, str] = {"whole": "whole", "upstream_of": "upstream",
                            "downstream_of": "downstream", "between": "between",
                            "within": "within an area", "rest": "the rest of the water",
                            "steelhead_waters": "steelhead waters"}
#: T191: what a proposed split point is anchored to. `split`: a curated split (splits.json id);
#: the others are a NEW point, by the anchor types the atlas resolves (`AnchorType`, T164):
#: `confluence` (a named tributary's mouth), `lake_inlet` / `lake_outlet` (a lake's edge where the
#: water enters / leaves it — a `lake` anchor) and `coord` (a map click, lon/lat — a `point` anchor).
#: T164: `new` is a NEW split proposed in the same mark (`Mark.new_splits`), its `ref` in `split_id`.
PointKind = Literal["split", "confluence", "lake_inlet", "lake_outlet", "coord", "new"]


class FixPoint(BaseModel):
    """One split point of a proposed extent (T191). Never a file edit: T185 turns it into a
    proposal (an existing split's id, or a NEW split of this anchor type)."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: PointKind
    split_id: Optional[str] = Field(default=None, description="kind split: the curated split's id")
    name: Optional[str] = Field(default=None, description="the tributary (confluence) or lake named")
    item_id: Optional[str] = Field(default=None, description="its registry id, when picked")
    coord: Optional[tuple[float, float]] = Field(default=None, description="kind coord: (lon, lat)")

    @model_validator(mode="after")
    def _anchored(self) -> "FixPoint":
        if self.kind in ("split", "new"):
            if not (self.split_id or "").strip():
                raise ValueError("a split point names its split" if self.kind == "split"
                                 else "a new-split point names the proposal (its ref)")
        elif self.kind == "coord":
            if self.coord is None:
                raise ValueError("a map point needs its coordinates")
            lon, lat = self.coord
            if not (-140.0 <= lon <= -113.0 and 48.0 <= lat <= 60.5):
                raise ValueError(f"a map point is in BC (lon, lat), not at {list(self.coord)}")
        elif not ((self.name or "").strip() or (self.item_id or "").strip()):
            raise ValueError(f"a {self.kind} point names its "
                             + ("tributary" if self.kind == "confluence" else "lake"))
        return self


class CheckedRow(BaseModel):
    """T532: one checklist row as the reviewer saw it — its number, kind and state (`review.checklist.rows`), whether
    they ticked it (checked a ? / ✗ row by eye) and whether they picked it as wrong."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    n: int = Field(..., ge=1)
    kind: str
    state: Literal["ok", "unsure", "bad"]
    ticked: bool = False
    wrong: bool = False


class Checklist(BaseModel):
    """T532: THE CHECKLIST A MARK WAS MADE ON — the rows' digest (`checklist.digest`: the words and the extent) and the
    rows. A check whose digest is not the place's now is stale (re-checked, never silently kept)."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    digest: str = Field(..., pattern=r"^[0-9a-f]{16}$")
    rows: list[CheckedRow] = Field(..., min_length=1)


class SeenExtent(BaseModel):
    """T525: THE EXTENT A DFO MARK WAS MADE ON — the place's curated extents and its own tributary flag, as the file had
    them when the owner looked (the review app's `dfo.extent_now`). The save refuses a mark whose place has moved since
    (`dfo.mark_for`), and so does the apply (`proposals.build`): a mark never confirms or replaces an extent nobody saw."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    extents: list[dict[str, Any]]
    tributaries: Optional[bool] = None


class ExtentFix(BaseModel):
    """THE REVIEWER'S PROPOSED EXTENT for the entry's scope or one rule (T191): `op` (the
    catalogue's own name, `OP_POINTS`), its split points (as many as the op takes), the items it
    holds on (registry ids; empty = the entry's own waters) and whether it takes in its
    tributaries (None = no call). Recorded in the mark; T185 turns it into a proposed edit."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    scope: str = Field(..., min_length=1, description='"entry" or a rule id')
    op: str
    points: list[FixPoint] = Field(default_factory=list)
    items: list[str] = Field(default_factory=list)
    tributaries: Optional[bool] = None
    area: Optional[str] = Field(default=None, description="op within: the area (id or name)")

    @model_validator(mode="after")
    def _shape(self) -> "ExtentFix":
        if self.op not in OP_POINTS:
            raise ValueError(f"an extent fix's op is one of {list(OP_POINTS)}, not {self.op!r}")
        want = OP_POINTS[self.op]
        if len(self.points) != want:
            raise ValueError(f"extent op {OP_WORDS[self.op]} takes {want} split point"
                             f"{'' if want == 1 else 's'}, not {len(self.points)}")
        if (self.op == "within") != bool((self.area or "").strip()):
            raise ValueError("within names its area; no other op does")
        if len(set(self.items)) != len(self.items):
            raise ValueError("an extent fix lists each item once")
        return self


def pieces_ok(fixes) -> None:
    """T524: SEVERAL PIECES OF ONE DFO PLACE (`Mark.extent_fix` on a DFO mark) — refused, naming why, unless they make one
    union: every piece on the place's own scope, no piece twice, no `rest` beside another piece (a rest IS the remainder
    of the water, `CatalogueEntry` holds it to be the only extent), and one tributary call for the place (its binding
    has one `tributaries` flag) — pieces that call it differently are refused, never one silently kept."""
    if len(fixes) < 2:
        return
    if any(f.scope != ENTRY_KEY for f in fixes):
        raise ValueError("a DFO place's pieces are all on the place itself (scope entry)")
    if any(f.op == "rest" for f in fixes):
        raise ValueError("a rest is the remainder of the water: it is the place's only piece")
    # a piece is its op, its cuts in order and the SET of waters it holds on (the order they were ticked in is no
    # difference) — as `dfo.fix_vs_current` compares an extent
    seen = [{**f.model_dump(mode="json", exclude={"tributaries"}), "items": sorted(f.items)} for f in fixes]
    if any(a == b for i, a in enumerate(seen) for b in seen[i + 1:]):
        raise ValueError("a DFO place lists each piece once")
    calls = {f.tributaries for f in fixes if f.tributaries is not None}
    if len(calls) > 1:
        raise ValueError("a DFO place's pieces make one tributary call (include or exclude), not both")


#: T164: WHAT A NEW SPLIT IS ANCHORED TO — every anchor a DFO notice names a place by. `confluence`
#: (a named tributary's mouth), `lake_inlet` / `lake_outlet` (where the water enters / leaves a lake),
#: `bridge`, `dam`, `sign` (a crossing, a dam or a boundary sign — placed by a map point), `coord`
#: (a bare map point) and `split` (an existing curated split, reused for this water).
AnchorKind = Literal["confluence", "lake_inlet", "lake_outlet", "lake", "bridge", "dam", "sign", "coord",
                     "split", "area_boundary", "border"]
#: the anchors placed by a map point (the reviewer clicks where the bridge / dam / sign is)
COORD_ANCHORS = ("bridge", "dam", "sign", "coord")
#: the anchors found from a named registry item (the tributary, the lake; `lake` = both its edges)
ITEM_ANCHORS = ("confluence", "lake_inlet", "lake_outlet", "lake")
#: T355: the anchors the build can OFFSET along the water (`SplitAnchor.from_dict`: point / confluence /
#: lake; an existing split is re-anchored on its own). An area boundary or the border cannot.
OFFSET_ANCHORS = COORD_ANCHORS + ITEM_ANCHORS + ("split",)
#: T355: the polygon layers an `area_boundary` split may name, and the field its name is matched on
#: (`SplitAnchor.area_layer` / `area_name_field`; `build.get_area_polys` reads them)
AREA_LAYERS = {"parks_bc": "PROTECTED_LANDS_NAME", "parks_nat": "ENGLISH_NAME",
               "osm_admin_boundaries": "name"}
#: T355: NewSplit fields left out of a mark's line (and the export) while unset, so a line written
#: before they existed is the same line
NEW_SPLIT_OPTIONAL = ("area", "area_end", "area_layer", "area_field", "wsc_descendants", "exclude_gnis",
                      "amends")
#: which side of the cut the extent lies on
Side = Literal["upstream", "downstream"]
#: (T207) how sure the proposer is of where a NEW cut stands — `models.splits.CONFIDENCE`; it
#: rides into splits.json with the split (absent there is high)
Confidence = Literal["high", "medium", "low"]


def default_confidence(anchor: str) -> str:
    """A NEW split's confidence when nobody set one (owner 2026-10-27): a point placed by hand on
    the map (bridge / dam / sign / map point) is MEDIUM; a registry anchor (a tributary's mouth, a
    lake's edge) or an existing split is exact, HIGH."""
    return "medium" if anchor in COORD_ANCHORS else "high"


def _in_bc(coord) -> bool:
    lon, lat = coord
    return -140.0 <= lon <= -113.0 and 48.0 <= lat <= 60.5


class NewSplit(BaseModel):
    """A NEW SPLIT PROPOSED IN A MARK (T164): a cut the extent needs that splits.json does not
    have. No tool writes it to splits.json: T185 turns it into a proposal the owner accepts.
    `ref` names it inside the mark (an extent fix's point `{kind: new, split_id: ref}`); `on_item`
    is the water it cuts; `blk` / `measure` / `coord` are where the server resolved it (stamped at
    the mark's write by the review app's `dfo.check_anchor`); `side` is which side of the cut the
    extent lies on; `offset_m` an offset from the anchor along the water (+ upstream, - down:
    "500 m below the bridge" is -500); `quote` the notice's words it came from."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    #: the cut's id in the mark — its REAL splits.json id (T355, `proposals.place_groups`: up to 40 + 2 + 32
    #: + a clash suffix); older marks hold a local ref ("n1-2"), resolved to the real id when read
    ref: str = Field(..., min_length=1, max_length=96)
    anchor: AnchorKind
    name: Optional[str] = Field(default=None, description="the tributary / lake / bridge named")
    item_id: Optional[str] = Field(default=None, description="the tributary's or lake's registry id")
    split_id: Optional[str] = Field(default=None, description="anchor split: the curated split")
    on_item: Optional[str] = Field(default=None, description="the water it cuts (registry id)")
    coord: Optional[tuple[float, float]] = Field(default=None, description="(lon, lat)")
    blk: Optional[str] = None
    measure: Optional[float] = None
    side: Optional[Side] = None
    offset_m: Optional[float] = None
    #: owner 2026-10-27: a proposed split carries its confidence everywhere; None = the anchor's
    #: default (`default_confidence`: a hand-placed point medium, a registry anchor high)
    confidence: Optional[Confidence] = None
    #: anchor area_boundary (owner 2026-11-02, Little Qualicum Falls park): the protected area's
    #: name as the parks layer spells it, and which end of it the cut is (`SplitAnchor.side`:
    #: upstream = where the water enters the area, downstream = where it leaves; None = both)
    area: Optional[str] = None
    area_end: Optional[Side] = None
    #: T355 area_boundary: the polygon layer (`AREA_LAYERS`; None = parks_bc), its name field (None =
    #: the layer's own), the WSC subtree cut too, and named waters (gnis ids) it must not cut
    area_layer: Optional[str] = None
    area_field: Optional[str] = None
    wsc_descendants: Optional[bool] = None
    exclude_gnis: Optional[tuple[str, ...]] = None
    #: T355 EDIT IN PLACE: this proposal REPLACES the proposed cut it names — named as an override's
    #: `match` is, by its anchor (`same_anchor`), never by an id. The mark is the only write, so the edit
    #: is a proposal in a mark: that cut's group takes this split, and every mark that named it names this.
    amends: Optional["NewSplit"] = None
    quote: str = ""
    note: str = ""

    @property
    def confidence_now(self) -> str:
        """This proposal's confidence: the one set, else its anchor's default."""
        return self.confidence or default_confidence(self.anchor)

    @model_validator(mode="after")
    def _anchored(self) -> "NewSplit":
        if self.coord is not None and not _in_bc(self.coord):
            raise ValueError(f"a new split's point is in BC (lon, lat), not at {list(self.coord)}")
        if self.anchor in COORD_ANCHORS and self.coord is None:
            raise ValueError(f"a new split at a {self.anchor} needs its point: click the map")
        if self.anchor in ITEM_ANCHORS and not ((self.name or "").strip()
                                                or (self.item_id or "").strip()):
            raise ValueError(f"a new split at a {self.anchor.replace('_', ' ')} names its "
                             + ("tributary" if self.anchor == "confluence" else "lake"))
        if self.anchor == "split" and not (self.split_id or "").strip():
            raise ValueError("a new split on an existing split names that split")
        if (self.anchor == "area_boundary") != bool((self.area or "").strip()):
            raise ValueError("a new split at an area's boundary names the area; no other anchor does")
        if self.area_end and self.anchor != "area_boundary":
            raise ValueError("area_end is the end of an area_boundary anchor")
        if self.anchor != "area_boundary" and any(
                getattr(self, k) for k in ("area_layer", "area_field", "wsc_descendants", "exclude_gnis")):
            raise ValueError("area_layer / area_field / wsc_descendants / exclude_gnis belong to an "
                             "area_boundary anchor")
        if self.area_layer and self.area_layer not in AREA_LAYERS:
            raise ValueError(f"area_layer is one of {', '.join(AREA_LAYERS)}, not {self.area_layer!r}")
        # T355: the build offsets a point / confluence / lake cut only (`SplitAnchor.from_dict`); an
        # area's boundary and the border are where they are
        if self.offset_m and self.anchor == "area_boundary":
            raise ValueError("an area boundary takes no offset (the build cannot offset one)")
        if self.offset_m and self.anchor == "border":
            raise ValueError("the border takes no offset (the build cannot offset one)")
        return self

    @property
    def area_name_field(self) -> str:
        """The field an area_boundary's name is matched on: the one set, else its layer's."""
        return self.area_field or AREA_LAYERS.get(self.area_layer or "parks_bc", "PROTECTED_LANDS_NAME")


NewSplit.model_rebuild()


class RecordPrint(BaseModel):
    """One record's fingerprints (`fingerprint.entry_records`)."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    key: str = Field(..., pattern=r"^(entry|rule:.+|lic:.+|split:.+|slough:.+|q154:.+|name:.+)$")
    meaning: str
    binding: str
    reach: Optional[str] = None
    answer: Optional[str] = None


class Prints(BaseModel):
    """What a mark is held against: the book text (None for a split) and every record's prints."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    text: Optional[str] = None
    records: tuple[RecordPrint, ...]


class Mark(BaseModel):
    """One reviewer's call on one target. `reviewer` is required; `pdf_sha256` is the book the
    reviewer read (the repo copy's bytes, which differ from gov.bc.ca's); `pages` are PRINTED page
    numbers. `carried_by` names the migration that re-stamped a copy of an earlier mark."""
    model_config = ConfigDict(extra="forbid", frozen=True)

    scope: Scope
    target: str = Field(..., min_length=1, description="entry_id, or a split id")
    rule_id: Optional[str] = Field(default=None, description="scope rule: the rule marked")
    verdict: Verdict
    answers: dict[Literal["T", "R", "P", "A"], Answer]
    problem: Optional[str] = None
    note: str = ""
    reviewer: str = Field(..., min_length=1)
    date: str = Field(..., description="ISO date the mark was made (carried: the original's)")
    pdf_sha256: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    pages: list[int] = Field(default_factory=list, description="printed pages read")
    prints: Prints
    carried_by: Optional[str] = None
    #: THE REVIEWER'S TIME on the target before this mark, by the review screen's clock (seconds
    #: from the target opening — or the previous mark of a zone entry's rules — to this one): the
    #: T136 pilot's "time per entry". None for a mark made outside the screen (a migration's copy,
    #: a script).
    seconds: Optional[float] = Field(default=None, ge=0)
    #: THE REVIEWER'S CALL ON TRIBUTARIES (T189), structured, never a file edit (T185: a mark is
    #: the only write): `"entry"` (the entry's own scope) or a rule id -> "include" (it should
    #: take in its tributaries) or "exclude" (it should not). Only what the reviewer said is
    #: here; a key left out is no call. A rule mark calls only its own rule.
    tributaries: dict[str, Tributaries] = Field(default_factory=dict)
    #: T189: "in the future this will need a lake split" (owner, on Dragon Lake) — the target is
    #: a lake divided into areas, deferred (RULINGS K5); `lake_splits_later` lists every one.
    lake_split_later: bool = False
    #: T191: THE REVIEWER'S PROPOSED EXTENT FIX(ES), structured — at most one per scope ("entry"
    #: or a rule id), on the same keys as `tributaries` — except a DFO mark, whose fixes are the PIECES of its place
    #: (T524, `pieces_ok`: the place binds their union). Only on a wrong or unsure mark: those are
    #: the marks that become tickets (`tickets.TICKET_VERDICTS`), and a fix says the extent is wrong.
    extent_fix: list[ExtentFix] = Field(default_factory=list)
    #: T164: NEW SPLITS the reviewer proposes (a cut the extent needs that does not exist) — on a
    #: wrong or unsure mark, like an extent fix; never written to splits.json (T185).
    new_splits: list[NewSplit] = Field(default_factory=list)
    #: T55: THE OWNER'S CALL on a slough / canal candidate — "stream" (fold it into its river) or
    #: "not stream" (it stays a lake). Only on a slough mark; a verified slough mark carries one.
    #: Q154: "pool" (takes the river's rules) or "real lake" on a q154 sample mark
    #: T224: "accept" / "reject" on a name-variant candidate (scope name)
    #: T449: "tidal water" / "not tidal" on a DFO place proposed as wholly tidal (scope dfo, `DfoCall`)
    call: Optional[Union[SloughCall, Q154Call, NameCall, TidalCall, DfoCall]] = None
    #: owner 2026-10-28: THE GROUP a DFO mark was recorded with — historic places that reword a
    #: current one (`review.dfo.groups`) take one extent decision, one mark per member, linked here
    group: Optional[str] = None
    #: owner 2026-11-02: THIS WORDING IS A VARIANT of another DFO location (a rewording the grouping
    #: missed): T185 writes the target's `duplicate_of` (confirmed), so the wording rides that place's
    #: extent. A DFO mark only; its one check is T (the words name that place).
    variant_of: Optional[str] = None
    #: T525: the extent this DFO mark was made on (`SeenExtent`); a mark from before T525 has none
    extent_before: Optional[SeenExtent] = None
    #: T532: the checklist this mark answers (a DFO place or a regulation rule): its one check is P, the place
    checklist: Optional[Checklist] = None

    @field_validator("reviewer")
    @classmethod
    def _reviewer_named(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("a mark needs the reviewer's name")
        return v.strip()

    @field_validator("date")
    @classmethod
    def _iso_date(cls, v: str) -> str:
        dt.date.fromisoformat(v)
        return v

    @model_validator(mode="after")
    def _consistent(self) -> "Mark":
        need = CHECKS[self.scope]
        if self.variant_of is not None:
            if self.scope != "dfo":
                raise ValueError("only a DFO wording is attached as a variant")
            if self.variant_of == self.target:
                raise ValueError("a wording is not a variant of itself")
            need = ("T",)
        if self.checklist is not None and self.verdict != CLEARED:
            # T532: a checklist checks the PLACE only — every row ✓ or ticked is P y; a row picked wrong is P n
            if self.scope not in ("dfo", "rule"):
                raise ValueError("a checklist is on a DFO place or a regulation rule")
            need = ("P",)
            open_rows = [r.n for r in self.checklist.rows if r.state != "ok" and not r.ticked and not r.wrong]
            if self.verdict == "verified" and (open_rows or any(r.wrong for r in self.checklist.rows)):
                raise ValueError(f"all correct needs every row ✓ or ticked (open: {open_rows})")
            if self.verdict == "wrong" and not any(r.wrong for r in self.checklist.rows):
                raise ValueError("a wrong checklist names the row(s) that are wrong")
        if self.group is not None and self.scope != "dfo":
            raise ValueError("only a DFO mark is recorded with a group")
        if (self.scope == "rule") != (self.rule_id is not None):
            raise ValueError("a rule mark names its rule_id; an entry or split mark does not")
        if self.verdict == CLEARED:
            # a cleared mark says only who took the mark back and why; it holds the prints of the
            # mark it clears (`cleared_mark`), and carries nothing a reader could act on
            if not self.note.strip():
                raise ValueError("cleared needs a note saying why")
            if (self.answers or self.call is not None or self.problem or self.tributaries
                    or self.extent_fix or self.new_splits or self.lake_split_later
                    or self.group is not None or self.variant_of is not None):
                raise ValueError("a cleared mark carries no answers, call or proposal")
            return self
        extra = sorted(set(self.answers) - set(need))
        if extra:
            raise ValueError(f"a {self.scope} mark answers {list(need)}, not {extra}")
        got = [self.answers.get(k) for k in need]
        if self.verdict == "verified" and got != ["y"] * len(need):
            raise ValueError(f"verified needs every check {list(need)} answered y")
        if self.verdict == "wrong":
            if "n" not in got:
                raise ValueError("wrong needs a check answered n")
            if not (self.problem or "").strip() or not self.note.strip():
                raise ValueError("wrong needs a problem category and a note")
        if self.verdict == "unsure" and ("?" not in got or "n" in got):
            raise ValueError("unsure needs a check answered ? and none answered n")
        if self.verdict == "deferred" and not self.note.strip():
            raise ValueError("deferred needs a note saying why")
        keys = [r.key for r in self.prints.records]
        if self.scope == "slough":
            if keys != [f"slough:{self.target}"] or self.prints.text is not None:
                raise ValueError("a slough mark holds exactly its candidate's prints")
            if self.verdict == "verified" and self.call is None:
                raise ValueError("a verified slough mark says stream or not stream")
            if (self.tributaries or self.extent_fix or self.new_splits or self.lake_split_later
                    or self.rule_id is not None):
                raise ValueError("a slough mark carries only its call")
            if self.call not in (None, "stream", "not stream"):
                raise ValueError("a slough call is stream or not stream")
            return self
        if self.scope == "q154":
            if keys != [f"q154:{self.target}"] or self.prints.text is not None:
                raise ValueError("a q154 mark holds exactly its sample pond's prints")
            if self.verdict == "verified" and self.call not in ("pool", "real lake"):
                raise ValueError("a verified q154 mark says pool or real lake")
            if self.verdict != "verified" and self.call is not None:
                raise ValueError("only a verified q154 mark carries a call")
            if (self.tributaries or self.extent_fix or self.new_splits or self.lake_split_later
                    or self.rule_id is not None):
                raise ValueError("a q154 mark carries only its call")
            return self
        if self.scope == "name":
            if keys != [f"name:{self.target}"] or self.prints.text is not None:
                raise ValueError("a name mark holds exactly its candidate's prints")
            if self.verdict == "verified" and self.call not in ("accept", "reject"):
                raise ValueError("a verified name mark says accept or reject")
            if self.verdict != "verified" and self.call is not None:
                raise ValueError("only a verified name mark carries a call")
            if (self.tributaries or self.extent_fix or self.new_splits or self.lake_split_later
                    or self.rule_id is not None):
                raise ValueError("a name mark carries only its call")
            return self
        if self.scope == "tidal" and self.call is not None:
            # T359: "the whole water is tidal" — Place y, and no cut (the whole item is tidal water)
            if self.call != "whole":
                raise ValueError("a tidal call is whole")
            if self.verdict != "verified" or self.new_splits:
                raise ValueError("the whole water is tidal is a Place y mark with no cut")
        elif self.scope == "dfo" and self.call is not None:
            # T449: a DFO place wholly in tidal water — accepted ("tidal water": every check y, and the place binds
            # nothing, so no extent, cut, variant or lake split rides it) or rejected ("not tidal")
            if self.call not in ("tidal water", "not tidal"):
                raise ValueError("a DFO call is tidal water or not tidal")
            if self.variant_of is not None:
                raise ValueError("a variant mark makes no tidal call")
            if self.call == "tidal water":
                if self.verdict != "verified":
                    raise ValueError("tidal water is a verified mark (every check y)")
                if (self.extent_fix or self.new_splits or self.lake_split_later or self.tributaries):
                    raise ValueError("a place in tidal water binds nothing: no extent, new split, tributary call "
                                     "or lake split rides it")
        elif self.call is not None:
            raise ValueError("only a slough, q154, name, tidal or DFO mark carries a call")
        if self.scope in ("split", "tidal"):
            if keys != [f"split:{self.target}"] or self.prints.text is not None:
                raise ValueError(f"a {self.scope} mark holds exactly its "
                                 + ("split's" if self.scope == "split" else "candidate's") + " prints")
        else:
            if self.prints.text is None or "entry" not in keys:
                raise ValueError("an entry or rule mark holds the text and the entry record")
            if self.scope == "rule" and sorted(keys) != sorted(["entry", f"rule:{self.rule_id}"]):
                raise ValueError("a rule mark holds the entry record and its own rule's")
        if self.tributaries:
            if self.scope in ("split", "tidal"):
                raise ValueError("a split mark makes no tributary call")
            if self.scope == "rule":
                ok = {self.rule_id}
            else:
                ok = {ENTRY_KEY} | {k.split(":", 1)[1] for k in keys if k.startswith("rule:")}
            bad = sorted(set(self.tributaries) - ok)
            if bad:
                raise ValueError(f"a tributary call names {bad}: not "
                                 + ("this mark's rule" if self.scope == "rule"
                                    else "the entry or one of its rules"))
        if self.extent_fix:
            if self.scope in ("split", "tidal"):
                raise ValueError("a split mark proposes no extent fix")
            # T210: a DFO location whose extent is the existing cuts (no NEW point, no new split)
            # may say Place y: the cuts are right and the fix only names the extent to curate
            only_existing = (self.scope == "dfo" and not self.new_splits
                             and all(p.kind == "split" for f in self.extent_fix for p in f.points))
            if self.verdict not in ("wrong", "unsure") and not (
                    only_existing and self.verdict == "verified"):
                raise ValueError("an extent fix is proposed on a wrong or unsure mark")
            scopes = [f.scope for f in self.extent_fix]
            if self.scope == "dfo":
                # T524: A DFO PLACE'S EXTENT IS ITS PIECES — several fixes on the place's one scope, each an op on
                # its own water(s) ("the Chilliwack below the Slesse line, and the Sumas below Barrow Town Pump
                # Station"); the place binds their UNION. One piece is the mark as it always was.
                pieces_ok(self.extent_fix)
            elif len(set(scopes)) != len(scopes):
                raise ValueError("one extent fix per scope")
            ok = ({self.rule_id} if self.scope == "rule" else
                  {ENTRY_KEY} | {k.split(":", 1)[1] for k in keys if k.startswith("rule:")})
            bad = sorted(set(scopes) - ok)
            if bad:
                raise ValueError(f"an extent fix names {bad}: not "
                                 + ("this mark's rule" if self.scope == "rule"
                                    else "the entry or one of its rules"))
        refs = [n.ref for n in self.new_splits]
        if self.new_splits:
            if self.scope == "split":
                raise ValueError("a split mark proposes no new split")
            # T212: ACCEPTING a tidal candidate is Place y carrying its cut as the new split
            if self.verdict not in ("wrong", "unsure") and not (
                    self.scope == "tidal" and self.verdict == "verified"):
                raise ValueError("a new split is proposed on a wrong or unsure mark")
            if len(set(refs)) != len(refs):
                raise ValueError("each new split has its own ref")
        if (self.scope == "tidal" and self.verdict == "verified" and self.call is None
                and len(self.new_splits) != 1):
            raise ValueError("an accepted tidal boundary carries exactly its one cut as a new split")
        dangling = sorted({p.split_id for f in self.extent_fix for p in f.points
                           if p.kind == "new"} - set(refs))
        if dangling:
            raise ValueError(f"an extent fix names new split(s) {dangling} the mark does not propose")
        if self.scope == "dfo" and (self.rule_id is not None or keys != ["entry"]):
            raise ValueError("a DFO mark holds exactly its location's prints")
        if self.extent_before is not None and self.scope != "dfo":
            raise ValueError("only a DFO mark names the extent it was made on")
        if self.lake_split_later and self.scope in ("split", "tidal"):
            raise ValueError("a lake split later is marked on an entry or a rule, not a split")
        return self

    @property
    def key(self) -> tuple:
        """What a later mark supersedes this one on."""
        return (self.scope, self.target, self.rule_id)


# --------------------------------------------------------------------------------------------
# The store
# --------------------------------------------------------------------------------------------

#: fields a line leaves out while they hold their default (T189 added them): a mark without them
#: is the same line it was before they existed
_OMIT_DEFAULT = {"tributaries": {}, "lake_split_later": False, "extent_fix": [],
                 "new_splits": [], "call": None, "group": None, "variant_of": None, "extent_before": None,
                 "checklist": None}


def line(mark: Mark) -> str:
    """THE ONE SERIALISATION of a mark: one compact JSON line, keys in model order."""
    got = json.loads(mark.model_dump_json())
    for k, v in _OMIT_DEFAULT.items():
        if got.get(k) == v:
            got.pop(k)
    for n in got.get("new_splits") or ():       # a new split's unset confidence: the line it was
        if n.get("confidence") is None:
            n.pop("confidence", None)
        for k in NEW_SPLIT_OPTIONAL:            # owner 2026-11-02: only a park-boundary cut has them
            if n.get(k) is None:
                n.pop(k, None)
        if n.get("amends"):                     # T355: the cut it edits, by its anchor — only what is set
            n["amends"] = {k: v for k, v in n["amends"].items()
                           if v not in (None, "") and k not in ("amends", "quote", "note")}
    return json.dumps(got, ensure_ascii=False, separators=(",", ":")) + "\n"


def _retired(text: str) -> bool:
    try:
        return json.loads(text).get("scope") in RETIRED_SCOPES
    except (ValueError, AttributeError):
        return False                          # not JSON: the model says why, below


def load(path: Path) -> list[Mark]:
    """Every mark in the file, in order. A MISSING FILE RAISES (AGENTS 37: absent curated data is a
    bug, never an empty set) — the store is checked in empty. A bad line raises naming its number."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"no marks store at {p} — it is checked in (empty) under "
                                f"data/curated/review/")
    out: list[Mark] = []
    for n, text in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
        if not text.strip():
            continue
        if '"proposal"' in text and _retired(text):
            continue                          # T185: a decision of the retired accept step
        try:
            out.append(Mark.model_validate_json(text))
        except ValueError as ex:
            raise ValueError(f"{p}:{n}: not a mark: {ex}") from ex
    return out


def appended(path: Path, marks: Sequence[Mark]) -> str:
    """The store's text with `marks` appended — what a writer commits (the review app through its
    backed-up `writes.commit`, `append` through an atomic write)."""
    p = Path(path)
    before = p.read_text(encoding="utf-8") if p.exists() else ""
    if before and not before.endswith("\n"):
        before += "\n"
    return before + "".join(line(m) for m in marks)


@contextmanager
def store_lock(path: Path):
    """THE STORE'S LOCK ACROSS PROCESSES: an exclusive `flock` on the store's DIRECTORY, held for a
    whole read-append-write. A writer reads the file, appends and replaces it (`atomic_write`), so
    two processes doing that at once (the review app and a migration's `append`) dropped one's
    lines; a lock on the file itself would not do, since the replace gives the next writer a new
    inode. The review app's thread lock (`writes.LOCK`) serialises its own threads only."""
    d = Path(path).parent
    fd = os.open(d, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def append(path: Path, marks: Sequence[Mark]) -> None:
    """Append `marks` to the store (atomic, under `store_lock`). A reviewer's NEW mark asks the
    gate first (`refuse_on_gate`; the review app's `post_mark`)."""
    from pipeline.regs.parsing.io import atomic_write
    if not Path(path).exists():
        load(path)                                             # raises: the store is checked in
    with store_lock(path):
        atomic_write(Path(path), appended(path, marks))


def refuse_on_gate(target: str, gate_errors: Sequence) -> None:
    """MARKS ARE REFUSED ON AN ENTRY THAT FAILS THE GATE (PLAN §4 phase 1): what the reviewer would
    sign off is not what ingest accepts. `gate_errors` is the gate's answer for the entry as
    it is on disk (the review app's `check_entry`, `validate_catalogue` beneath it)."""
    if gate_errors:
        msgs = [e.get("msg", str(e)) if isinstance(e, Mapping) else str(e) for e in gate_errors]
        raise ValueError(f"{target} fails the gate, so it cannot be marked: " + "; ".join(msgs[:5]))


def latest(marks: Iterable[Mark]) -> dict[tuple, Mark]:
    """The mark in force per target: the LAST one appended (the store is the history). A target
    whose last mark is CLEARED has none in force: it is left out, so every reader sees it unmarked."""
    out: dict[tuple, Mark] = {}
    for m in marks:
        out[m.key] = m
    return {k: m for k, m in out.items() if m.verdict != CLEARED}


def uncleared(marks: Iterable[Mark]) -> list[Mark]:
    """THE HISTORY WITHOUT WHAT WAS TAKEN BACK, in store order: a cleared mark and every earlier mark
    on its key are dropped (a later mark stays). For readers of the whole history (the shared
    proposed splits, the pilot's measure) — `latest` is this for the marks in force."""
    out: list[Mark] = []
    for m in marks:
        if m.verdict == CLEARED:
            out = [x for x in out if x.key != m.key]
        else:
            out.append(m)
    return out


def cleared_mark(inforce: Mark, *, reviewer: str, note: str, date: str) -> Mark:
    """THE MARK THAT CLEARS `inforce` (owner 2026-10-29): same key, verdict cleared, the reviewer's
    note why; it holds the cleared mark's prints and book (it answers nothing). Writes nothing."""
    if inforce.verdict == CLEARED:
        raise ValueError("nothing to clear")
    return Mark.model_validate({"scope": inforce.scope, "target": inforce.target,
                                "rule_id": inforce.rule_id, "verdict": CLEARED, "answers": {},
                                "note": note, "reviewer": reviewer, "date": date,
                                "pdf_sha256": inforce.pdf_sha256, "pages": [],
                                "prints": inforce.prints.model_dump()})


def lake_splits_later(marks: Iterable[Mark]) -> list[dict]:
    """THE DEFERRED LAKE-SPLIT LIST, AS REVIEWERS FEED IT (T189; RULINGS K5, T100): every target
    whose mark IN FORCE says "needs a lake split later", in the store's order — {target, rule_id,
    reviewer, date, note}. A later mark without the flag takes the target off the list."""
    return [{"target": m.target, "rule_id": m.rule_id, "reviewer": m.reviewer, "date": m.date,
             "note": m.note}
            for m in latest(marks).values() if m.lake_split_later]


# --------------------------------------------------------------------------------------------
# The one staleness decision
# --------------------------------------------------------------------------------------------

def _kind(key: str) -> str:
    return key.split(":", 1)[0]


def match_records(marked: Sequence[RecordPrint], now: Sequence[RecordPrint]
                  ) -> tuple[list[tuple[RecordPrint, RecordPrint]], list[RecordPrint], list[RecordPrint]]:
    """RULE IDENTITY: pair each marked record with the current record it IS, in passes, each
    taking only records no earlier pass claimed:

      1. same key, meaning and binding        (nothing moved)
      2. same kind, meaning and binding       (a re-parse renumbered it)
      3. same key and meaning                 (its binding moved)
      4. same kind and meaning — ONLY when the marked key is gone from `now` (renumbered AND moved)

    Binding before key: 38 rules in 16 entries share their meaning with a sibling ("No fishing" on
    two reaches), and pairing on meaning alone sent a mark to the sibling — a renumber read as
    "binding moved", a changed rule as "binding moved" instead of "rule changed". Pass 4 never
    runs while the key still names a record: that record IS the rule the mark was on, changed.
    Never by id alone: a record whose meaning changed has no pair — "rule changed".
    Returns (pairs, marked records with no current one, current records no mark holds)."""
    free = list(now)
    keys_now = {c.key for c in now}
    pairs: list[tuple[RecordPrint, RecordPrint]] = []
    left: list[RecordPrint] = list(marked)
    for same in (lambda m, c: c.key == m.key and c.meaning == m.meaning and c.binding == m.binding,
                 lambda m, c: (_kind(c.key) == _kind(m.key) and c.meaning == m.meaning
                               and c.binding == m.binding),
                 lambda m, c: c.key == m.key and c.meaning == m.meaning,
                 lambda m, c: (m.key not in keys_now and _kind(c.key) == _kind(m.key)
                               and c.meaning == m.meaning)):
        still: list[RecordPrint] = []
        for m in left:
            hit = next((c for c in free if same(m, c)), None)
            if hit is None:
                still.append(m)
            else:
                pairs.append((m, hit))
                free.remove(hit)
        left = still
    return pairs, left, free


def held_rule(mark: Mark, now: Prints | Mapping | None) -> str | None:
    """The rule_id a RULE mark is on NOW (`match_records`: renumbered rules are followed), or None
    when its rule has no pair (changed or gone) or `mark` is not a rule mark."""
    if mark.scope != "rule" or now is None:
        return None
    if not isinstance(now, Prints):
        now = Prints.model_validate(now)
    pairs, _, _ = match_records(mark.prints.records, _held_against(mark, now))
    return next((cur.key.split(":", 1)[1] for was, cur in pairs
                 if was.key == f"rule:{mark.rule_id}"), None)


def _held_against(mark: Mark, now: Prints) -> list[RecordPrint]:
    """The current records a mark is compared with: a rule mark, its entry's record and the
    entry's rules (its own may have been renumbered); any other mark, every record."""
    if mark.scope == "rule":
        return [r for r in now.records if r.key == "entry" or _kind(r.key) == "rule"]
    return list(now.records)


#: WHICH PART OF A MARK'S PRINTS MOVED (`moved`) -> the reason it gives. `records`: a record the
#: mark held is gone, or (not a rule mark) a new one appeared; `unbuilt`: a bundle half the mark
#: had is not built now.
MOVED_REASONS: dict[str, StaleReason] = {
    "text": StaleReason.book_text_changed,
    "records": StaleReason.rule_changed,
    "meaning": StaleReason.rule_changed,
    "binding": StaleReason.binding_moved,
    "reach": StaleReason.binding_moved,
    "answer": StaleReason.answer_changed,
    "unbuilt": StaleReason.awaiting_build,
}
#: the parts a BUILD writes (the bundle's half) — every other part is read from the curated files.
#: A mark gone stale with only these moved was moved by a rebuild alone, not by an edit: across a
#: rebuild of unchanged inputs that is a FALSE STALE (the T136 gate: 0).
BUILT_PARTS = frozenset({"reach", "answer", "unbuilt"})


def _place_only(records: Sequence[RecordPrint]) -> list[RecordPrint]:
    """(T366, owner 2026-11-03: "only location ones we need to really really review") A DFO mark is
    held against its PLACE — the notice's words (`text`) and the curated geography (`binding`) —
    never the scraped rows riding on it (`meaning`): DFO turns its rows over every season (T361's
    six went "rule changed" on in-season notices), and every row change goes to the DFO digest
    instead (`dfo_salmon.refresh`). The stamped meaning stays in the mark; it is just not compared."""
    return [r.model_copy(update={"meaning": ""}) for r in records]


def moved(mark: Mark, now: Prints | Mapping | None) -> frozenset[str]:
    """THE ONE COMPARISON of a mark's prints with its target's prints NOW (`now`: from
    `fingerprint`; None when the target is gone): which parts moved (`MOVED_REASONS`' keys).
    Empty = the mark holds. `stale_reasons` is this, named; the T136 gate asks it which moves a
    rebuild alone made (`BUILT_PARTS`)."""
    if now is None:
        return frozenset({"records"})
    if not isinstance(now, Prints):
        now = Prints.model_validate(now)
    got: set[str] = set()
    if mark.prints.text != now.text:
        got.add("text")
    # a rule mark is held against its entry's record and ITS rule — the entry's other rules are
    # not what it answered for; its rule may have been renumbered (`match_records`)
    marked, held = mark.prints.records, _held_against(mark, now)
    if mark.scope == "dfo":                       # T366: a DFO mark answers for the PLACE only
        marked, held = _place_only(marked), _place_only(held)
    pairs, gone, new = match_records(marked, held)
    if gone or (new and mark.scope != "rule"):
        got.add("records")
    for was, cur in pairs:
        if was.meaning != cur.meaning:
            got.add("meaning")
        if was.binding != cur.binding:
            got.add("binding")
        for part in ("reach", "answer"):
            a, b = getattr(was, part), getattr(cur, part)
            if a is not None and b is None:
                got.add("unbuilt")
            elif a is not None and a != b:
                got.add(part)
    return frozenset(got)


def stale_reasons(mark: Mark, now: Prints | Mapping | None) -> list[StaleReason]:
    """Why `mark` no longer holds on its target as it is NOW (`now`: the target's prints from
    `fingerprint`; None when the target is gone). Empty = the mark holds. Reasons in
    `STALE_REASONS` order, each once — `moved`, named by `MOVED_REASONS`."""
    got = {MOVED_REASONS[p] for p in moved(mark, now)}
    return [r for r in STALE_REASONS if r in got]


def target_prints(mark: Mark, entries: Mapping[str, Mapping], ctx) -> Optional["Prints"]:
    """THE PRINTS OF A MARK'S TARGET NOW, as staleness reads them (`moved` / `stale_reasons`): a
    split mark, its split's (None when splits.json no longer has it); any other, its WHOLE entry's
    (a rule mark is matched against every rule of its entry, so a renumbered rule is followed —
    `_held_against`); None when the entry is gone. `ctx` is a `fingerprint.Context` of the vintage
    on disk. Ticket export (`tickets.statuses_now`) and the false-stale gate (`gate.stale_report`)
    both ask here (T136 adversarial review: the gate had grown its own copy)."""
    from pipeline.regs.review import fingerprint as F
    if mark.scope == "dfo":                       # T164: a DFO location (`review.dfo`)
        from pipeline.regs.review import dfo as D
        got = D.prints_of(mark.target)
        return Prints.model_validate(got) if got is not None else None
    if mark.scope == "tidal":                     # T212: a proposed tidal boundary (`review.tidal`)
        from pipeline.regs.review import tidal as T
        got = T.prints_of(mark.target)
        return Prints.model_validate(got) if got is not None else None
    if mark.scope == "q154":                      # Q154: a sample small pond (`review.q154`)
        from pipeline.regs.review import q154 as Q
        got = Q.prints_of(mark.target)
        return Prints.model_validate(got) if got is not None else None
    if mark.scope == "name":                      # T224: a name-variant candidate (`review.names`)
        from pipeline.regs.review import names as N
        got = N.prints_of(mark.target)
        return Prints.model_validate(got) if got is not None else None
    if mark.scope == "slough":                    # T55: a slough / canal candidate (`review.slough`)
        from pipeline.regs.review import slough as S
        got = S.prints_of(mark.target)
        return Prints.model_validate(got) if got is not None else None
    if mark.scope == "split":
        try:
            return Prints.model_validate(F.split_fingerprints(mark.target, ctx))
        except KeyError:                          # the split is gone from splits.json
            return None
    e = entries.get(mark.target)
    return Prints.model_validate(F.entry_fingerprints(e, ctx)) if e is not None else None


def split_states(marks: Iterable[Mark], ctx) -> dict[str, dict]:
    """Every split in splits.json (`ctx`, a `fingerprint.Context`) -> its `status` now. A split
    nobody has confirmed is `unmarked`, so a split ADDED mid-review queues itself for its own
    `split` mark (owner 2026-10-23); a moved one reads stale, "binding moved"."""
    from pipeline.regs.review import fingerprint as F
    held = {m.target: m for m in latest(marks).values() if m.scope == "split"}
    return {sid: status(held.get(sid), Prints.model_validate(F.split_fingerprints(sid, ctx)))
            for sid in sorted(ctx.split_index)}


#: what LANDING A MARK'S OWN FIX moves on its target (owner 2026-10-28): the curated binding and
#: what the build makes of it. A mark whose fix the files now say (`proposals.applied_keys`) still
#: holds when only these moved — the owner's work is never undone by applying it.
APPLIED_PARTS = frozenset({"binding", "reach", "answer", "unbuilt"})


def held_by_landing(mark: Mark, now: Prints | Mapping | None, applied: Iterable = ()) -> bool:
    """Does `mark` hold only because its own fix landed? Its key is in `applied` (its extent fix
    is what the files say now), its target still exists, and only `APPLIED_PARTS` moved."""
    if now is None or mark.key not in set(applied):
        return False
    parts = moved(mark, now)
    return bool(parts) and parts <= APPLIED_PARTS


def status(mark: Mark | None, now: Prints | Mapping | None, *, applied: Iterable = ()) -> dict:
    """What the queue shows for one target: {status, verdict, reasons}. `status` is `unmarked`, the
    verdict while the mark holds, or `stale`. `applied`: the keys of marks whose own fix has landed
    (`proposals.applied_keys`) — such a mark holds over the move its landing made (`landed` true)."""
    if mark is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    if held_by_landing(mark, now, applied):
        return {"status": mark.verdict, "verdict": mark.verdict, "reasons": [], "landed": True}
    reasons = stale_reasons(mark, now)
    return {"status": "stale" if reasons else mark.verdict, "verdict": mark.verdict,
            "reasons": [r.value for r in reasons]}


def health(marks: Iterable[Mark], *, entries: Mapping[str, Mapping], ctx,
           applied: Iterable = ()) -> dict:
    """EVERY MARK IN FORCE, accounted for (owner 2026-10-28: a mark must never silently fall back
    to "not done"). Per scope: `held` (the mark holds), `landed` (holds over its own fix landing),
    `stale` (its target moved: a re-check, with reasons) and `orphan` (its target is GONE — a
    renamed / regenerated id: the mark counts nowhere). `orphans` and `stale_rows` list them with
    what the mark said, so the review app can put them on a banner instead of hiding them."""
    applied = set(applied)
    by: dict[str, dict] = {}
    orphans, stale_rows = [], []
    for m in latest(marks).values():
        if m.scope in RETIRED_SCOPES:
            continue
        c = by.setdefault(m.scope, {"held": 0, "landed": 0, "stale": 0, "orphan": 0})
        try:
            now = target_prints(m, entries, ctx)
        except Exception as ex:                  # a reader that cannot answer is an orphan, said
            now, err = None, f"{type(ex).__name__}: {ex}"
        else:
            err = None
        said = {"scope": m.scope, "target": m.target, "rule_id": m.rule_id, "verdict": m.verdict,
                "date": m.date, "reviewer": m.reviewer, "note": m.note,
                "new_splits": [n.name or n.ref for n in m.new_splits],
                "text": (m.prints.text or "")[:160]}
        if now is None:
            c["orphan"] += 1
            orphans.append({**said, **({"error": err} if err else {})})
            continue
        st = status(m, now, applied=applied)
        if st.get("landed"):
            c["landed"] += 1
        elif st["status"] == "stale":
            c["stale"] += 1
            stale_rows.append({**said, "reasons": st["reasons"]})
        else:
            c["held"] += 1
    return {"scopes": by, "orphans": orphans, "stale_rows": stale_rows,
            "n_orphans": len(orphans), "n_stale": len(stale_rows)}


# --------------------------------------------------------------------------------------------
# Proposed splits, shared (T210)
# --------------------------------------------------------------------------------------------

#: two proposals this close (metres along one blk, or on the ground) are one cut
SAME_CUT_M = 10.0


def _norm(s: Optional[str]) -> str:
    return " ".join((s or "").casefold().split())


def _ground_m(a, b) -> float:
    import math
    (x1, y1), (x2, y2) = a, b
    k = 111_320.0
    return math.hypot((x1 - x2) * k * math.cos(math.radians((y1 + y2) / 2)), (y1 - y2) * k)


def same_anchor(a: NewSplit, b: NewSplit) -> bool:
    """Do two proposed new splits name the SAME cut? The same confluence / lake anchor (the same
    tributary or lake, the same offset), or the same blk with the measure within `SAME_CUT_M`, or
    coordinates within `SAME_CUT_M`. The name, side and quote are not part of it. Two cuts off ONE
    reference with DIFFERENT offsets (100 m up and 100 m down of a weir) are never the same cut
    (owner 2026-11-02: an offset is the split's own attribute, never baked into its point)."""
    if (a.offset_m or 0) != (b.offset_m or 0):
        return False
    if a.anchor == b.anchor == "area_boundary":
        return _norm(a.area) == _norm(b.area) and a.area_end == b.area_end and (
            (a.area_layer or "parks_bc") == (b.area_layer or "parks_bc")) and (
            not a.on_item or not b.on_item or a.on_item == b.on_item)
    if a.anchor == b.anchor == "border":                 # one water crosses the border at one place
        return bool(a.on_item) and a.on_item == b.on_item and (
            not a.coord or not b.coord or _ground_m(a.coord, b.coord) <= 1000.0)
    if a.anchor == b.anchor and a.anchor in ITEM_ANCHORS:
        if a.item_id and b.item_id:
            return a.item_id == b.item_id and (not a.on_item or not b.on_item
                                               or a.on_item == b.on_item)
        if not a.item_id and not b.item_id and _norm(a.name) and _norm(a.name) == _norm(b.name):
            return True
    if a.anchor == b.anchor == "split" and a.split_id == b.split_id:
        return True
    if (a.blk and a.blk == b.blk and a.measure is not None and b.measure is not None
            and abs(a.measure - b.measure) <= SAME_CUT_M):
        return True
    if a.coord and b.coord and _ground_m(a.coord, b.coord) <= SAME_CUT_M:
        return True
    return False


class ConfidenceOverride(BaseModel):
    """ONE OWNER CALL ON A PROPOSED SPLIT, kept beside the marks (`proposal_overrides.json`) because
    the marks store is append-only and never edited: `match` names the cut as a `NewSplit` would
    (`same_anchor`, against any proposal of it). The call is any of:

      confidence  the cut's confidence
      set         `NewSplit` fields to change (a moved point: coord / blk / measure; another anchor:
                  anchor / area / area_end …) — what a mark's note asked of the PROPOSED split
      offsets     the cut is a REFERENCE, not a cut: one cut per offset off it, along the water
                  (`NewSplit.offset_m`, + upstream, - downstream) — "100 m up and down of the weir"

    The result is still a PROPOSAL (T185 lands it after its side rebuild); nothing is accepted here."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    match: NewSplit
    confidence: Optional[Confidence] = None
    set: dict = Field(default_factory=dict)
    offsets: list[float] = Field(default_factory=list)
    by: str = Field(..., min_length=1)
    date: str
    note: str = ""

    @model_validator(mode="after")
    def _some_call(self) -> "ConfidenceOverride":
        if not (self.confidence or self.set or self.offsets):
            raise ValueError("an override makes a call: confidence, set or offsets")
        if any(not o for o in self.offsets) or len(set(self.offsets)) != len(self.offsets):
            raise ValueError("offsets are distinct and non-zero")
        bad = set(self.set) - set(NewSplit.model_fields) | ({"ref"} & set(self.set))
        if bad:
            raise ValueError(f"set changes NewSplit fields other than ref, not {sorted(bad)}")
        self.amended(self.match)                 # the change makes a valid NewSplit
        return self

    def amended(self, n: NewSplit) -> NewSplit:
        """`n` with `set` applied (validated)."""
        if not self.set:
            return n
        return NewSplit.model_validate({**n.model_dump(), **self.set})

    def matches(self, members: Sequence[NewSplit]) -> bool:
        return any(same_anchor(self.match, n) for n in members)


def load_overrides(path: Optional[Path]) -> list[ConfidenceOverride]:
    """The confidence overrides (`CURATED.review.proposal_overrides`); [] for None."""
    if path is None:
        return []
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    return [ConfidenceOverride.model_validate(o) for o in doc.get("overrides") or ()]


def confidence_of(group: Sequence[NewSplit], overrides: Sequence[ConfidenceOverride] = ()) -> str:
    """A PROPOSED SPLIT'S CONFIDENCE (owner 2026-10-27): an owner override naming the cut, else the
    LAST proposal of it that set one, else the first proposal's anchor default."""
    for o in overrides:
        if o.confidence and o.matches(group):
            return o.confidence
    for n in reversed(list(group)):
        if n.confidence:
            return n.confidence
    return default_confidence(group[0].anchor)


def proposal_groups(marks: Iterable[Mark]) -> list[dict]:
    """THE PROPOSED SPLITS, SHARED AND DEDUPED, before any confidence: every `new_splits` entry of
    EVERY mark (superseded ones too — the store is the history), one group per distinct cut
    (`same_anchor`), in the order first proposed: {id, split (the FIRST proposal's `NewSplit`),
    members (every `NewSplit` that named it), marks: [{target, ref, reviewer, date}]}. `id` is the
    first proposal's ref, made unique with a `-2` suffix when another cut already holds it. What
    `review.export.decide` holds (T266); `proposals` adds the confidence."""
    out: list[dict] = []
    for m in uncleared(marks):              # a cleared mark's proposals are taken back with it
        for n in m.new_splits:
            # T355: an EDIT of a proposed cut takes that cut's place — the group keeps its id, every
            # member that named it, and this split is what it now is
            i = next((k for k, p in enumerate(out) if n.amends is not None
                      and any(same_anchor(n.amends, q) for q in p["members"])), None)
            if i is not None:
                out[i]["split"] = n
            else:
                i = next((k for k, p in enumerate(out) if any(same_anchor(n, q) for q in p["members"])), None)
            if i is None:
                pid, k = n.ref, 1
                while any(p["id"] == pid for p in out):
                    k += 1
                    pid = f"{n.ref}-{k}"
                out.append({"id": pid, "split": n, "members": [], "marks": []})
                i = len(out) - 1
            out[i]["members"].append(n)
            who = {"target": m.target, "ref": n.ref, "reviewer": m.reviewer, "date": m.date}
            if who not in out[i]["marks"]:
                out[i]["marks"].append(who)
    return out


def _offset_words(off: float) -> str:
    return f"{abs(off):g} m {'upstream' if off > 0 else 'downstream'}"


def amend(groups: Iterable[dict], overrides: Sequence[ConfidenceOverride] = ()) -> list[dict]:
    """`proposal_groups` rows with the owner's overrides' `set` / `offsets` applied: a moved or
    re-anchored cut keeps its id; a REFERENCE with offsets becomes one row per offset (id
    `<id>-u100` / `<id>-d100`, the split the reference + `offset_m`). `members` stay the proposals
    as the marks made them, so a mark's own proposal still finds its row(s)."""
    out: list[dict] = []
    for g in groups:
        members = [m if isinstance(m, NewSplit) else NewSplit.model_validate(m) for m in g["members"]]
        split = g["split"] if isinstance(g["split"], NewSplit) else NewSplit.model_validate(g["split"])
        # T355: a cut the owner EDITED in a mark (`NewSplit.amends`) is as the edit says: the edit is
        # the later call, so an earlier override's set / offsets no longer apply to it
        o = None if split.amends else next(
            (o for o in overrides if (o.set or o.offsets) and o.matches(members)), None)
        if o is not None:
            split = o.amended(split)
        base = {**g, "split": split, "members": members}
        if o is None or not o.offsets:
            out.append({**base, **({"amended": o.note or "owner override"} if o is not None else {})})
            continue
        for off in o.offsets:
            nm = f"{split.name or split.anchor} ({_offset_words(off)})"
            out.append({**base, "id": f"{g['id']}-{'u' if off > 0 else 'd'}{abs(off):g}",
                        "split": split.model_copy(update={"offset_m": float(off), "name": nm}),
                        "amended": o.note or "owner override", "reference": split})
    return out


def with_confidence(groups: Iterable[dict], overrides: Sequence[ConfidenceOverride] = ()) -> list[dict]:
    """`proposal_groups` rows as proposals (`amend`ed by the overrides): {id, split, marks, members,
    confidence (`confidence_of`, `overrides` first), and `amended` / `reference` when an override
    changed it}."""
    return [{"id": g["id"], "split": g["split"], "marks": [dict(w) for w in g["marks"]],
             "members": g["members"], "confidence": confidence_of(g["members"], overrides),
             **({"amended": g["amended"]} if g.get("amended") else {}),
             **({"reference": g["reference"]} if g.get("reference") is not None else {})}
            for g in amend(groups, overrides)]


def proposals(marks: Iterable[Mark], overrides: Sequence[ConfidenceOverride] = ()) -> list[dict]:
    """THE PROPOSED SPLITS, SHARED AND DEDUPED (`proposal_groups`), each with its confidence
    (`confidence_of`, `overrides` first). Read only; the store is never edited."""
    return with_confidence(proposal_groups(marks), overrides)
