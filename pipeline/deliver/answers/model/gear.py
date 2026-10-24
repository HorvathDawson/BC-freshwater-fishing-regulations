"""The `gear` section's models: the gear answer of a part on a segment (`GearAnswer`), or its tidal state."""
from __future__ import annotations

from typing import Dict, Literal, Optional, Tuple, Union
from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr
from pipeline.deliver import types as T
from pipeline.deliver.answers.model.base import ClauseRef, Count, Model, RuleIx, _enum


class Circumstance(Model):
    clause: ClauseRef
    targeting: Optional[Tuple[StrictStr, ...]] = None
    note: Optional[StrictStr] = None
    while_: Optional[Tuple[StrictStr, ...]] = Field(default=None, alias="while")


class CountPick(Model):
    by: ClauseRef
    over: Tuple[ClauseRef, ...]
    also: Optional[Tuple[Circumstance, ...]] = None
    #: ROUND10 W4: every rule deciding the row (`by`'s, then each `also`'s), each cited for its Book page
    cites: Tuple[RuleIx, ...] = Field(min_length=1)
    #: ROUND10 W4: the Lines row in plain words ("1 line (rod)" — ROUND11 A2; "1 line (rod); 2 while
    #: alone in a boat"); `lines_per_angler` only
    says: Optional[StrictStr] = None


class ElementPick(Model):
    verdict: Literal["ban", "allow"]
    by: ClauseRef
    over: Tuple[Tuple[RuleIx, Count, Literal["ban", "allow"]], ...]


class ExceptedFrom(Model):
    """ROUND20h 1: the ban an allowed bait element is an exception to — the banned `element` and its clause (`by`): the
    roe the province's fin-fish ban excepts ("fin fish … other than roe", p.8), on the days that ban is in force."""
    element: Literal["worms", "roe", "invertebrate", "fin_fish"]
    by: ClauseRef


class BaitPick(Model):
    element: Literal["worms", "roe", "invertebrate", "fin_fish"]
    ok: StrictBool
    by: Optional[ClauseRef]
    why: Optional[StrictStr] = None
    carry_kg: Optional[Union[StrictInt, StrictFloat]] = None
    also_allowed: Optional[Tuple[Circumstance, ...]] = None
    #: ROUND20h 1: the ban this allowed element is an exception to (only on an allowed element)
    excepted_from: Optional[ExceptedFrom] = None


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


class SummaryPart(Model):
    """One part of the gear sentence: its words, the rules deciding it (Book pages), its glossary
    terms."""
    part: Literal["mode", "lines", "hook", "attach", "weight"]
    says: StrictStr = Field(min_length=1)
    cites: Tuple[RuleIx, ...]
    terms: Tuple[StrictStr, ...]


class SummaryChange(Model):
    """Days of the year whose sentence differs: {when: "May 1–31", says: "fly fishing only"}."""
    when: StrictStr = Field(min_length=1)
    says: StrictStr = Field(min_length=1)
    cites: Tuple[RuleIx, ...]


class GearSummary(Model):
    """ROUND11 A2: the day's terminal gear in ONE sentence (ROUND12 5: "1 line with a single barbless
    hook (one hook, lure or fly). Weight up to 1 kg."), made from the count / hook / barb / lure / weight
    answers; `parts` its pieces, `terms` the glossary keys for the ⓘ (`glossary` ids), `cites` every
    rule deciding it, `changes` the other days of the year that read differently (at this moment)."""
    says: StrictStr = Field(min_length=1)
    parts: Tuple[SummaryPart, ...] = Field(min_length=1)
    terms: Tuple[StrictStr, ...]
    cites: Tuple[RuleIx, ...]
    changes: Tuple[SummaryChange, ...]


class GearAnswer(Model):
    #: ROUND11 A2 (gear 7): the day's terminal gear in one sentence, its terms and dated changes
    summary: GearSummary
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
