"""THE ANSWERS FILE'S TYPES (answers/2, DATAFLOW P7) — every frame and table a section ships, as a
strict pydantic model: `strict=True` (no coercion: a "2" is not a 2), `extra="forbid"` (a stray
field is refused), `frozen=True`, and every `X | None` paired, in a model validator, with the
condition that makes it null. Vocabularies are the closed enums of `pipeline.deliver.types`.

The build validates every DISTINCT value of every section through its model BEFORE encoding
(`validate_section`), so a wrong shape stops the build naming the section and the field; the
JSON Schema of each model ships in the file's `spec` (`json_schemas`) and is emitted for the app
(`python -m pipeline.deliver.answers.model --check`, AGENTS 40).

Field sets are the measured ones of the shipped file (every key, every optional key), not a guess.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Mapping, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat, StrictInt, StrictStr, \
    TypeAdapter, model_validator

from pipeline.deliver import types as T
from pipeline.deliver.calendar import WEEKDAYS


class Model(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


RuleIx = Annotated[StrictInt, Field(ge=0)]
LicIx = Annotated[StrictInt, Field(ge=0)]
Count = Annotated[StrictInt, Field(ge=0)]
Positive = Annotated[StrictInt, Field(gt=0)]
Cm = Union[StrictInt, StrictFloat]                   # band edges: whole or half cm
Fish = StrictStr                                      # a FishCode value (checked below)
ClauseRef = Tuple[RuleIx, Count]                      # (rule, clause index into its `gear`)


def _enum(enum, name: str):
    """A str field restricted to an enum's values (the JSON Schema says `enum`)."""
    return Literal[tuple(m.value for m in enum)]  # type: ignore[valid-type]


FishLit = _enum(T.FishCode, "FishCode")
Decided_ = _enum(T.DecidedStatus, "DecidedStatus")
RoleLit = _enum(T.Role, "Role")
LineLit = _enum(T.LineType, "LineType")
CondLit = _enum(T.CondType, "CondType")
BadgeLit = _enum(T.BadgeOf, "BadgeOf")
SteelLineLit = _enum(T.SteelheadLine, "SteelheadLine")
OriginLit = Literal["hatchery", "wild"]


# --------------------------------------------------------------------------------------------
# rows: lines, facts, conditions, the decided answer, the card's rows
# --------------------------------------------------------------------------------------------

class _LineBase(Model):
    r: RuleIx


class LRel(_LineBase):
    t: Literal["rel"]
    a: Cm
    b: Optional[Cm]                                   # None = no top
    carve: Optional[StrictBool] = None


class LCap(_LineBase):
    t: Literal["cap"]
    a: Cm
    b: Optional[Cm]
    take: Count
    carve: Optional[StrictBool] = None


class LOuterSize(_LineBase):
    t: Literal["outersize"]
    a: Cm
    b: Optional[Cm]
    take: Count
    outer: RuleIx


class LOuterCap(_LineBase):
    t: Literal["outercap"]
    outer: RuleIx


class LAlso(_LineBase):
    t: Literal["also"]
    capped: StrictBool


class LCaution(_LineBase):
    t: Literal["caution"]
    says: StrictStr


class LTnote(_LineBase):
    t: Literal["tnote"]
    only: StrictBool


class LBare(_LineBase):
    t: Literal["subcap", "steel", "outer", "orphan", "partly", "annual", "possession_cap", "duty",
               "record"]


Line = Annotated[Union[LRel, LCap, LOuterSize, LOuterCap, LAlso, LCaution, LTnote, LBare],
                 Field(discriminator="t")]


class _FactBase(Model):
    r: RuleIx
    members: Tuple[Fish, ...]
    rules: Tuple[RuleIx, ...]


class FRel(_FactBase):
    t: Literal["rel"]
    a: Cm
    b: Optional[Cm]
    carve: Optional[StrictBool] = None


class FCap(_FactBase):
    t: Literal["cap"]
    a: Cm
    b: Optional[Cm]
    take: Count
    carve: Optional[StrictBool] = None
    general: Optional[Literal[True]] = None
    carve_of: Optional[Tuple[Cm, Optional[Cm], Count]] = None


class FOuterSize(_FactBase):
    t: Literal["outersize"]
    a: Cm
    b: Optional[Cm]
    take: Count
    outer: RuleIx


class FOuterCap(_FactBase):
    t: Literal["outercap"]
    outer: RuleIx


class FAlso(_FactBase):
    t: Literal["also"]
    capped: StrictBool


class FCaution(_FactBase):
    t: Literal["caution"]
    says: StrictStr


class FTnote(_FactBase):
    t: Literal["tnote"]
    only: StrictBool


class FOrigin(_FactBase):
    t: Literal["origin"]
    o: OriginLit
    keepO: OriginLit
    status: Decided_


class FOrigin2(_FactBase):
    t: Literal["origin2"]
    o: OriginLit
    daily: Optional[Count]                            # None = no limit
    min: Optional[Cm]
    max: Optional[Cm]


class FExc(_FactBase):
    t: Literal["exc"]
    status: Decided_


class FXref(_FactBase):
    t: Literal["xref"]
    daily: Optional[Count]                            # None = no limit


class FBare(_FactBase):
    t: Literal["subcap", "steel", "outer", "orphan", "partly", "annual", "possession_cap", "duty",
               "record"]


Fact = Annotated[Union[FRel, FCap, FOuterSize, FOuterCap, FAlso, FCaution, FTnote, FOrigin, FOrigin2,
                       FExc, FXref, FBare], Field(discriminator="t")]


class KeepBand(Model):
    a: Optional[Cm] = None
    b: Optional[Cm] = None
    origin: Optional[OriginLit] = None


class _CondBase(Model):
    r: Optional[RuleIx]


class COrigin(_CondBase):
    c: Literal["origin"]
    o: OriginLit


class CSize(_CondBase):
    c: Literal["size"]
    a: Cm
    b: Optional[Cm]


class CBack(_CondBase):
    c: Literal["back"]
    status: Decided_
    who: Tuple[Fish, ...]


class CGroup(_CondBase):
    c: Literal["group"]
    keep: Tuple[KeepBand, ...]
    sub: Optional[Count]
    who: Tuple[Fish, ...]


class CCap(_CondBase):
    c: Literal["cap"]
    a: Cm
    b: Optional[Cm]
    take: Count
    sub: Optional[Count]
    general: Optional[StrictBool] = None
    who: Optional[Tuple[Fish, ...]] = None
    of: Optional[Tuple[Fish, ...]] = None
    except_: Optional[Tuple[Fish, ...]] = Field(default=None, alias="except")

    @model_validator(mode="after")
    def _general_or_who(self):
        if self.general and self.who:
            raise ValueError("a cap is general or for some fish, not both")
        return self


class CSubcap(_CondBase):
    c: Literal["subcap"]
    members: Tuple[Fish, ...]
    sub: Optional[Count]


class COuterCap(_CondBase):
    c: Literal["outercap"]
    sub: Optional[Count]


class COuterSize(_CondBase):
    c: Literal["outersize"]
    a: Cm
    b: Optional[Cm]
    take: Count
    sub: Optional[Count]


class COrigin2(_CondBase):
    c: Literal["origin2"]
    o: OriginLit
    daily: Optional[Count]
    min: Optional[Cm]
    max: Optional[Cm]
    sub: Optional[Count]
    who: Optional[Tuple[Fish, ...]] = None


class CStreamcap(_CondBase):
    c: Literal["streamcap"]
    sub: Optional[Count]


Cond = Annotated[Union[COrigin, CSize, CBack, CGroup, CCap, CSubcap, COuterCap, COuterSize, COrigin2,
                       CStreamcap], Field(discriminator="c")]

LiftNote = Tuple[RuleIx, Dict[Literal["when_targeting", "while", "lengths"], Any]]
RoleOf = Tuple[RuleIx, RoleLit, Optional[RuleIx]]


class Decided(Model):
    """One fish's decided answer for one origin (`rows.eval_sp`)."""
    status: Decided_
    win: RuleIx
    daily: Optional[Positive]                         # set <=> status == keep
    narrow: Optional[RuleIx]                          # the clause that lowered `daily`
    lines: Tuple[Line, ...]
    roles: Tuple[RoleOf, ...]
    lift_notes: Tuple[LiftNote, ...]

    @model_validator(mode="after")
    def _daily_iff_keep(self):
        if (self.daily is not None) != (self.status == "keep"):
            raise ValueError(f"daily {self.daily} with status {self.status}: set <=> keep")
        if self.narrow is not None and self.status != "keep":
            raise ValueError("only a keep answer is narrowed")
        return self


class OriginLine(Model):
    o: OriginLit
    n: Optional[Count] = None
    lo: Optional[Cm] = None
    hi: Optional[Cm] = None
    rel: Optional[StrictBool] = None


class Item(Model):
    """5.8: one per kind of fish; each fish in exactly one item."""
    members: Tuple[Fish, ...]
    bands: Optional[Tuple[Tuple[Cm, Optional[Cm], Optional[Count]], ...]]
    back: StrictBool
    xref: StrictBool
    sub: Optional[Count]
    conds: Tuple[Count, ...]
    against: Optional[Union[Positive, Literal["unlimited"]]] = None
    origins: Optional[Tuple[OriginLine, ...]] = None
    #: gap G4 (answers 2.3): a cross-reference of SEVERAL fish, each fish's keep range
    #: [fish, from_cm, to_cm | None (no top)] (its own decided answer); a fish keeping none is absent
    ranges: Optional[Tuple[Tuple[FishLit, Cm, Optional[Cm]], ...]] = None

    @model_validator(mode="after")
    def _xref_fields(self):
        if (self.against is not None) != self.xref:
            raise ValueError("`against` is set <=> the item is a cross-reference")
        if self.origins is not None and not (self.xref and len(self.members) == 1):
            raise ValueError("`origins` only on a one-fish cross-reference")
        if self.ranges is not None:
            if not (self.xref and len(self.members) > 1):
                raise ValueError("`ranges` only on a cross-reference of several fish")
            if not {f for f, _, _ in self.ranges} <= set(self.members):
                raise ValueError("`ranges` names a fish that is not the item's")
        return self


class SharedCap(Model):
    take: Count
    over_cm: Cm


class SharedCount(Model):
    """rows decision F11: a count limit several kinds share ("1 trout from streams"): together
    they give at most `take`."""
    take: Positive
    members: Tuple[Fish, ...] = Field(min_length=2)


class RealDaily(Model):
    """5.7, "Really {sum} a day here": every kind of the row capped below the row's number."""
    n: Count
    all: StrictBool                                   # every kind is capped
    sum: Optional[Count]                              # set <=> all
    capped_sum: Count
    rb: StrictBool
    shared_cap: Optional[Tuple[SharedCap, ...]]       # None: no shared cap binds
    shared_count: Optional[Tuple[SharedCount, ...]] = None   # None: no shared count limit binds
    capped: Tuple[Fish, ...]                          # the fish of the capped kinds
    open: Tuple[Fish, ...]                            # the fish of the kinds the row's number holds

    @model_validator(mode="after")
    def _sum_iff_all(self):
        if (self.sum is not None) != self.all or self.all == bool(self.open):
            raise ValueError("`sum` is set <=> every kind is capped (no open kind)")
        return self


class Badge(Model):
    of: BadgeLit
    entry: Optional[StrictStr]                         # the area's or region's entry; None: water
    share: StrictBool
    apart: StrictBool

    @model_validator(mode="after")
    def _entry_iff_not_water(self):
        if (self.entry is None) != (self.of == "water"):
            raise ValueError("`entry` names the area or region, never a water")
        return self


class Group(Model):
    members: Tuple[Fish, ...]
    facts: Tuple[Fact, ...]


class Row(Model):
    kind: Decided_
    pool: Optional[RuleIx]
    win: Optional[RuleIx]
    members: Tuple[Fish, ...]
    all_members: Tuple[Fish, ...]
    daily: Optional[Count]
    narrow: Optional[RuleIx]
    everyone: Tuple[Fact, ...]
    groups: Tuple[Group, ...]
    prot: Optional[Tuple[Fish, ...]]
    wins: Optional[Tuple[RuleIx, ...]]
    lift_notes: Tuple[LiftNote, ...]
    scope: Optional[Badge]                            # set <=> a keep row
    conds: Optional[Tuple[Cond, ...]] = None          # set <=> the row has a pool
    items: Optional[Tuple[Item, ...]] = None          # set <=> the row has a pool
    real_daily: Optional[RealDaily] = None            # only on a pool row

    @model_validator(mode="after")
    def _pool_rows(self):
        has_pool = self.pool is not None
        if has_pool != (self.kind in ("keep", "no_limit")):
            raise ValueError(f"a {self.kind} row with pool {self.pool}: pool <=> keep or no_limit")
        if has_pool == (self.win is not None):
            raise ValueError("a row names its pool or its winner, exactly one")
        if (self.conds is None) == has_pool or (self.items is None) == has_pool:
            raise ValueError("conds and items are set <=> the row has a pool")
        if (self.scope is not None) != (self.kind == "keep"):
            raise ValueError("the badge is set <=> a keep row")
        if self.real_daily is not None and not has_pool:
            raise ValueError("a real daily limit only on a pool row")
        if (self.daily is not None) != (self.kind == "keep"):
            raise ValueError(f"daily {self.daily} on a {self.kind} row: set <=> keep")
        return self


class FishAnswer(Model):
    hatchery: Optional[Decided]                       # None = no rule in scope speaks
    wild: Optional[Decided]


class RowsFrame(Model):
    spp: Tuple[Fish, ...]
    fish: Dict[Fish, FishAnswer]
    rows: Tuple[Row, ...]
    steelhead_line: Optional[SteelLineLit]


# --------------------------------------------------------------------------------------------
# gear
# --------------------------------------------------------------------------------------------

class Circumstance(Model):
    clause: ClauseRef
    targeting: Optional[Tuple[StrictStr, ...]] = None
    note: Optional[StrictStr] = None
    while_: Optional[Tuple[StrictStr, ...]] = Field(default=None, alias="while")


class CountPick(Model):
    by: ClauseRef
    over: Tuple[ClauseRef, ...]
    also: Optional[Tuple[Circumstance, ...]] = None


class ElementPick(Model):
    verdict: Literal["ban", "allow"]
    by: ClauseRef
    over: Tuple[Tuple[RuleIx, Count, Literal["ban", "allow"]], ...]


class BaitPick(Model):
    element: Literal["worms", "roe", "invertebrate", "fin_fish"]
    ok: StrictBool
    by: Optional[ClauseRef]
    why: Optional[StrictStr] = None
    carry_kg: Optional[Union[StrictInt, StrictFloat]] = None
    also_allowed: Optional[Tuple[Circumstance, ...]] = None


class Device(Model):
    must_be: Tuple[StrictStr, ...]
    by: Tuple[ClauseRef, ...]
    within_mm: Optional[Union[StrictInt, StrictFloat]] = None


class Way(Model):
    method: StrictStr
    allowed: StrictBool
    by: Optional[ClauseRef]
    why: Optional[StrictStr] = None
    not_for: Optional[Tuple[StrictStr, ...]] = None
    for_: Optional[Tuple[StrictStr, ...]] = Field(default=None, alias="for")
    while_: Optional[Tuple[Circumstance, ...]] = Field(default=None, alias="while")
    conduct: Optional[Tuple[StrictStr, ...]] = None
    while_rules: Optional[Tuple[RuleIx, ...]] = None
    devices: Optional[Dict[Literal["downrigger", "light"], Device]] = None


class Vessel(Model):
    active: Tuple[RuleIx, ...]
    timed: Tuple[RuleIx, ...]


class Overruled(Model):
    rule: RuleIx
    state: _enum(T.RuleState, "RuleState")
    reason: _enum(T.LossReason, "LossReason")
    by: RuleIx


class GearAnswer(Model):
    counts: Dict[StrictStr, CountPick]
    specs: Dict[StrictStr, Tuple[ClauseRef, ...]]
    elements: Dict[StrictStr, ElementPick]
    main: Tuple[ClauseRef, ...]
    circumstantial: Tuple[Circumstance, ...]
    hook: Literal["single", "any", "single_barbless", "any_barbless", "trebles_and_barbs"]
    fly: Optional[Literal["artificial_fly_only", "fly_fishing_only"]]
    bait: Tuple[BaitPick, ...]
    bait_ban: StrictBool
    ways: Tuple[Way, ...]
    conduct: Dict[Literal["release", "keep", "carry", "never", "also"],
                  Tuple[Tuple[StrictStr, Tuple[RuleIx, ...]], ...]]
    vessel: Vessel
    timed: Tuple[RuleIx, ...]
    in_part: Tuple[RuleIx, ...]
    side: Tuple[RuleIx, ...]
    while_rules: Tuple[RuleIx, ...]
    #: a duty for a fish CAUGHT some way in force here (`caught`, user ruling Q38, answers 2.3):
    #: "Any fish snagged — even by accident — must be released" (`display.rules[].plain`)
    caught: Tuple[RuleIx, ...]
    overruled: Tuple[Overruled, ...]
    decides: Tuple[RuleIx, ...]
    repeats: Tuple[RuleIx, ...]


class TidalState(Model):
    """FIX D12: tidal water's documented state — one value, never a computed one."""
    tidal: Literal[True]
    note: StrictStr
    see: Tuple[StrictStr, ...]
    licence: StrictStr


GearFrame = Union[GearAnswer, TidalState]


# --------------------------------------------------------------------------------------------
# licence
# --------------------------------------------------------------------------------------------

class Length(Model):
    min_cm: Cm


class When(Model):
    act: Literal["fishing", "targeting", "retaining", "retaining_recorded", "guiding"]
    species: Optional[Tuple[StrictStr, ...]] = None
    lengths: Optional[Tuple[Length, ...]] = None
    on: Optional[Literal["classified_period", "steelhead_period"]] = None


class Prices(Model):
    year: Optional[Union[StrictInt, StrictFloat]] = None
    day: Optional[Tuple[Union[StrictInt, StrictFloat], ...]] = None
    eight_days: Optional[Tuple[Union[StrictInt, StrictFloat], ...]] = None


class DocumentOr(Model):
    """licence decision L9: another way to satisfy what the document is bought for."""
    need: Tuple[StrictStr, ...] = Field(min_length=1)
    alt: Optional[LicIx] = None                      # the alternative record that offers it
    prices: Dict[StrictStr, Prices]


class DocumentNeed(Model):
    doc: StrictStr
    when: When                                       # the broadest requirement's (decision L10)
    base: StrictBool
    prices: Prices
    also_when: Optional[Tuple[When, ...]] = None     # narrower requirements needing it too
    or_: Optional[Tuple[DocumentOr, ...]] = Field(default=None, alias="or")


class Accompanied(Model):
    holding: StrictStr
    who: Dict[StrictStr, Any]


class Path_(Model):
    need: Optional[Tuple[StrictStr, ...]] = None
    freed: Optional[Tuple[StrictStr, ...]] = None
    accompanied_by: Optional[Accompanied] = None
    as_: Optional[Dict[StrictStr, Any]] = Field(default=None, alias="as")
    quota: Optional[Literal["own", "counts_to_companion"]] = None
    alt: Optional[LicIx] = None


class RequirementPath(Model):
    req: LicIx
    when: When
    paths: Tuple[Path_, ...]
    displaced_by: Optional[Tuple[LicIx, ...]] = None
    presumes_freed: Optional[Literal[True]] = None
    presumes_by: Optional[LicIx] = None
    terms: Optional[Tuple[LicIx, ...]] = None


class Exempt(Model):
    by: Tuple[LicIx, ...]
    from_: Tuple[StrictStr, ...] = Field(alias="from")


class PrintedRequirement(Model):
    """gap G2 (answers 2.3): another angler's or a guide's requirement, with how it is met as the
    record prints it (`satisfied_by`; nothing freed, nothing to buy: it is not this angler's)."""
    req: LicIx
    paths: Tuple[Path_, ...]


class ProfileAnswer(Model):
    documents: Tuple[DocumentNeed, ...]
    none_needed: StrictBool
    requirements: Tuple[RequirementPath, ...]
    exempt: Optional[Exempt] = None
    others: Optional[Tuple[PrintedRequirement, ...]] = None
    guiding: Optional[Tuple[PrintedRequirement, ...]] = None


class TidalProfile(Model):
    tidal: Literal[True]


class Holds(Model):
    holds: Tuple[LicIx, ...]
    designations: Tuple[LicIx, ...]
    stamp_period: StrictBool
    contested: StrictBool
    considered: Tuple[LicIx, ...]
    wrong_water: Tuple[LicIx, ...]
    waived: Tuple[LicIx, ...]
    not_yet_mapped: Tuple[LicIx, ...]
    displaced: Dict[StrictStr, Tuple[LicIx, ...]]
    also_printed: Dict[StrictStr, Tuple[LicIx, ...]]


class TidalHolds(Model):
    tidal: TidalState


class LicenceFrame(Model):
    holds: Union[Holds, TidalHolds]
    profiles: Tuple[Union[ProfileAnswer, TidalProfile], ...]

    @model_validator(mode="after")
    def _sixty(self):
        if len(self.profiles) != 60:
            raise ValueError(f"{len(self.profiles)} profiles, not the 60 angler profiles")
        tidal = isinstance(self.holds, TidalHolds)
        if any(isinstance(p, TidalProfile) != tidal for p in self.profiles):
            raise ValueError("a tidal frame is the documented state on every profile, only there")
        return self


# --------------------------------------------------------------------------------------------
# display
# --------------------------------------------------------------------------------------------

class StatusFrame(Model):
    status: Literal["base", "own", "closed"]
    #: gap G1 (answers 2.1): every full closure that speaks, not partly lifted, for some game
    #: fish, with the game fish it closes (`verdicts.project.closing`); status `closed` <=> every
    #: game fish is under one of them
    closing: Tuple[Tuple[RuleIx, Tuple[FishLit, ...]], ...]
    #: user ruling Z12/Q41 (answers 2.3): the closing rules printed "unless opened" whose proviso is
    #: the answer here (`display.rules[].unless_opened`: "Closed unless opened by Parks Canada — a
    #: national park fishing permit is required."); absent where none is (a national park
    #: RESERVE's own closure says plainly closed)
    unless_opened: Optional[Tuple[RuleIx, ...]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _closing_closes(self):
        if any(not fs for _, fs in self.closing):
            raise ValueError("a closing rule closes at least one fish")
        if self.unless_opened is not None and \
                not set(self.unless_opened) <= {r for r, _ in self.closing}:
            raise ValueError("an `unless_opened` rule is one of the closing rules")
        if self.status == "closed":
            shut = {f for _, fs in self.closing for f in fs}
            if not set(T.GAME_FISH) <= shut:
                raise ValueError("status closed, but the closing rules leave a game fish open")
        return self


class TidalStatus(Model):
    status: Literal["tidal"]
    tidal: Literal[True]
    note: StrictStr
    see: Tuple[StrictStr, ...]
    licence: StrictStr


DisplayFrame = Union[StatusFrame, TidalStatus]


class SubsetSay(Model):
    """gap G3 (answers 2.3): the rule said for SOME of its fish, as a row's or an item's ladder lists
    it (`display.subset_asks`): `plain` the sentence for those fish (None: the rule has none), `for`
    the qualifier the page writes beside a rule with no sentence ("for rainbow trout")."""
    fish: Tuple[FishLit, ...] = Field(min_length=1)
    for_: StrictStr = Field(alias="for")
    plain: Optional[StrictStr] = None


class RuleFacts(Model):
    kind: _enum(T.DisplayKind, "DisplayKind")
    closure: Optional[Literal[True]] = None          # set <=> a gate that closes
    bands: Optional[Tuple[Tuple[Cm, Optional[Cm], Optional[Count]], ...]] = None
    plain: Optional[StrictStr] = None                # None: the page shows the label
    #: Z12/Q41: a closure printed "unless opened", in the user's accepted words
    unless_opened: Optional[StrictStr] = None
    #: G3: the rule said for the fish subsets the ladders list it for
    subsets: Optional[Tuple[SubsetSay, ...]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _closure_is_a_gate(self):
        if self.closure and self.kind != "gate":
            raise ValueError("`closure` marks a gate")
        if self.unless_opened is not None and not self.closure:
            raise ValueError("`unless_opened` marks a closure")
        return self


class PartFacts(Model):
    order: Count
    label: StrictStr
    runs: StrictStr
    place: Optional[StrictStr]                        # None: no entry heading
    hint: StrictStr
    km: Optional[Union[StrictInt, StrictFloat]]       # None: no run carries a measure
    closed_all_year: StrictBool
    paper_licence: Tuple[RuleIx, ...]


class Choice(Model):
    parts: Tuple[Count, ...]
    closed: StrictBool
    sections: Positive
    heading: StrictStr
    text: StrictStr


class Picker(Model):
    choices: Tuple[Choice, ...]
    headed: StrictBool


class WaterFacts(Model):
    parts: Tuple[Optional[PartFacts], ...]
    picker: Picker
    unresolved_licensing: Tuple[LicIx, ...]


# --------------------------------------------------------------------------------------------
# The top level: a segment's MOMENT (answers 2.1)
# --------------------------------------------------------------------------------------------

WeekdayLit = Literal[WEEKDAYS]                     # THE CALENDAR SPEC's names, Monday first


class ClockTime(Model):
    at: Optional[StrictStr] = None                   # "HH:MM" — or a solar event:
    solar: Optional[Literal["sunrise", "sunset"]] = None
    offset_min: StrictInt

    @model_validator(mode="after")
    def _one(self):
        if (self.at is None) == (self.solar is None):
            raise ValueError("a time is a clock time (`at`) or a solar event (`solar`), not both")
        return self


class MomentHours(Model):
    start: ClockTime
    end: ClockTime
    in_: StrictBool = Field(alias="in")              # inside the window (else every other hour)
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, populate_by_name=True)


class Moment(Model):
    """When in the week and the day a segment holds (`calendar.Moment.as_json`)."""
    weekdays: Tuple[WeekdayLit, ...] = Field(min_length=1, max_length=7)
    hours: Optional[MomentHours]                     # None: every hour of those days


# --------------------------------------------------------------------------------------------
# The sections' frames, and validation
# --------------------------------------------------------------------------------------------

#: One adapter per section's frame (per (key, segment) value), and per static table.
FRAMES: Dict[str, TypeAdapter] = {
    "rows": TypeAdapter(RowsFrame),
    "gear": TypeAdapter(GearFrame),
    "licence": TypeAdapter(LicenceFrame),
    "display": TypeAdapter(DisplayFrame),
}
# --------------------------------------------------------------------------------------------
# The top level: the GLOSSARY (answers 2.2, user decision J)
# --------------------------------------------------------------------------------------------

class GlossaryTerm(Model):
    """One piece of jargon the page shows, in plain words (`glossary.py`: generated from the data
    and the book's text, never written per water)."""
    id: Annotated[StrictStr, Field(pattern=r"^[a-z0-9_]+$")]
    term: StrictStr
    says: StrictStr                                   # the plain-language explanation
    example: Optional[StrictStr] = None
    pages: Tuple[Positive, ...] = Field(min_length=1)  # PRINTED book pages
    quote: StrictStr                                  # the book's own words, verbatim
    source: StrictStr                                 # the data it was generated from


class Glossary(Model):
    version: Positive
    terms: Tuple[GlossaryTerm, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self):
        ids = [t.id for t in self.terms]
        if len(set(ids)) != len(ids):
            raise ValueError("two glossary terms share an id")
        return self


#: The top-level tables' models (not sections): `moments`, `glossary`.
TOP: Dict[str, TypeAdapter] = {"moments": TypeAdapter(Tuple[Moment, ...]),
                               "glossary": TypeAdapter(Glossary)}

STATICS: Dict[Tuple[str, str], TypeAdapter] = {
    ("display", "rules"): TypeAdapter(Tuple[RuleFacts, ...]),
    ("display", "waters"): TypeAdapter(Dict[StrictStr, WaterFacts]),
}


class ShapeError(ValueError):
    """A section value the answers/2 model refuses: the build stops, naming it."""


def _wire(x) -> str:
    """The value as the wire holds it (JSON): what the models validate, in strict JSON mode."""
    return json.dumps(x)


def validate_section(name: str, values, statics: Optional[dict] = None) -> int:
    """Validate every DISTINCT value of a section (and its static tables) against its model.
    Returns how many distinct values were validated; raises `ShapeError` on the first refusal."""
    ad = FRAMES.get(name)
    seen = set()
    n = 0
    if ad is not None:
        for v in values:
            s = json.dumps(v, sort_keys=True)
            if s in seen:
                continue
            seen.add(s)
            try:
                ad.validate_json(_wire(v))
            except Exception as e:                      # pydantic's ValidationError, named
                raise ShapeError(f"answers/2: a `{name}` frame is not its model: {e}") from None
            n += 1
    for t, v in (statics or {}).items():
        sa = STATICS.get((name, t))
        if sa is None:
            continue
        try:
            sa.validate_json(_wire(v))
        except Exception as e:
            raise ShapeError(f"answers/2: `{name}.{t}` is not its model: {e}") from None
    return n


def json_schemas() -> dict:
    """The JSON Schema of every frame and static model (shipped in the file's `spec`)."""
    out = {name: ad.json_schema(by_alias=True) for name, ad in FRAMES.items()}
    out.update({f"{n}.{t}": ad.json_schema(by_alias=True) for (n, t), ad in STATICS.items()})
    out.update({f"top.{t}": ad.json_schema(by_alias=True) for t, ad in TOP.items()})
    return out


def validate_top(name: str, value) -> None:
    """Validate a top-level table (`TOP`) against its model."""
    try:
        TOP[name].validate_json(_wire(value))
    except Exception as e:
        raise ShapeError(f"answers/2: top-level `{name}` is not its model: {e}") from None


# --------------------------------------------------------------------------------------------
# Across the language boundary (AGENTS 40): the schema, emitted, never typed twice
# --------------------------------------------------------------------------------------------

_CORE = Path(__file__).resolve().parents[3] / "app" / "packages" / "core" / "src"
SCHEMA_OUT = _CORE / "answers.generated.json"
TS_OUT = _CORE / "answers.generated.ts"


def emitted_schema() -> str:
    return json.dumps({"$comment": "GENERATED by python -m pipeline.deliver.answers.model — do not "
                                   "edit. The answers/2 frame models (pipeline/deliver/answers/"
                                   "model.py) as JSON Schema; enums are the pipeline's closed "
                                   "vocabularies.",
                       "schemas": json_schemas()}, indent=1, sort_keys=True) + "\n"


def _ts(node: dict, defs: dict) -> str:
    """One JSON Schema node as a TypeScript type (the subset pydantic emits for these models)."""
    if "$ref" in node:
        return node["$ref"].rsplit("/", 1)[1]
    if "const" in node:
        return json.dumps(node["const"])
    if "enum" in node:
        return " | ".join(json.dumps(v) for v in node["enum"])
    for k in ("anyOf", "oneOf"):
        if k in node:
            return " | ".join(sorted({_ts(x, defs) for x in node[k]}))
    t = node.get("type")
    if t == "array":
        if "prefixItems" in node:
            items = [_ts(x, defs) for x in node["prefixItems"]]
            rest = node.get("items")
            return "[" + ", ".join(items) + (f", ...({_ts(rest, defs)})[]" if rest else "") + "]"
        return f"readonly ({_ts(node.get('items') or {}, defs)})[]"
    if t == "object":
        if "properties" in node:
            req = set(node.get("required") or [])
            body = "; ".join(f"{json.dumps(k)}{'' if k in req else '?'}: {_ts(v, defs)}"
                             for k, v in node["properties"].items())
            return "{ " + body + " }"
        extra = node.get("additionalProperties")
        if isinstance(extra, dict):
            key = "string"
            if "propertyNames" in node and "enum" in node["propertyNames"]:
                key = " | ".join(json.dumps(v) for v in node["propertyNames"]["enum"])
                return f"{{ [K in {key}]?: {_ts(extra, defs)} }}"
            return f"Record<{key}, {_ts(extra, defs)}>"
        return "Record<string, unknown>"
    return {"string": "string", "integer": "number", "number": "number", "boolean": "boolean",
            "null": "null"}.get(t, "unknown")


def emitted_ts() -> str:
    """Every frame's and table's model as TypeScript (from the same JSON Schema): the page never
    hand-types an enum or a frame (AGENTS 40)."""
    lines = ["// GENERATED by python -m pipeline.deliver.answers.model — do not edit.",
             "// The answers/2 frames (pipeline/deliver/answers/model.py), from their JSON Schema.",
             ""]
    seen: Dict[str, str] = {}
    tops = []
    for name, schema in sorted(json_schemas().items()):
        defs = schema.get("$defs", {})
        for d, node in sorted(defs.items()):
            text = _ts(node, defs)
            if seen.setdefault(d, text) != text:
                raise ValueError(f"model: two shapes named {d}")
        alias = "".join(w[:1].upper() + w[1:] for w in name.replace(".", "_").split("_")) + "Value"
        tops.append(f"export type {alias} = {_ts({k: v for k, v in schema.items() if k != '$defs'}, defs)};")
    for d, text in sorted(seen.items()):
        lines.append(f"export type {d} = {text};")
    lines.append("")
    lines += tops
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    outs = ((SCHEMA_OUT, emitted_schema()), (TS_OUT, emitted_ts()))
    if "--check" in argv:
        stale = [str(p) for p, text in outs if not p.exists() or p.read_text() != text]
        if stale:
            print(f"stale: {stale} — run `python -m pipeline.deliver.answers.model`", file=sys.stderr)
            return 1
        return 0
    for p, text in outs:
        p.write_text(text)
        print(f"wrote {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
