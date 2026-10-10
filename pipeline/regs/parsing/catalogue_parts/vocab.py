"""The closed vocabularies: rule types, methods, periods, water and closure kinds, and the
`_Terse` base every record serialises through.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from enum import Enum
from pydantic import BaseModel, model_serializer


class RuleType(str, Enum):
    retention_limit = "retention_limit"
    stop_fishing_after_quota = "stop_fishing_after_quota"
    bait_restriction = "bait_restriction"
    tackle_restriction = "tackle_restriction"
    method_rule = "method_rule"
    vessel_rule = "vessel_rule"
    #: `angling_from_vessel_prohibited` WAS HERE: "No angling from boats" is HOW you may fish, not
    #: what your boat may do — a `method_rule` banning `angling` `when: {angler: in_boat}` (or
    #: `in_powered_boat`). As a vessel type it could not meet the province's angling allow on the
    #: ladder at all. Refused on load, like `document_required`.
    navigation_duty = "navigation_duty"
    #: A CLOSURE FOR ONE KIND OF ANGLER — "Angling prohibited for non-guided non-resident aliens
    #: on Saturdays and Sundays". It was `access_permission` + `permitted: false`, a polarity bit
    #: on a type that also held permits and reciprocity. It is its own type, not `retention_limit`
    #: + a who, because the override ladder keys on (type, dimension) and ignores who: filed as a
    #: retention limit, a section-scoped alien-only closure would share a key with — and could
    #: displace — a zone quota that binds everyone. `document_required` and `access_permission`
    #: are GONE (refused on load); licensing lives on `CatalogueEntry.licensing`.
    angler_closure = "angler_closure"
    handling_rule = "handling_rule"
    hazard = "hazard"
    advisory = "advisory"
    program_membership = "program_membership"
    facility = "facility"


class Method(str, Enum):
    """`sport fishing` is DEFINED as angling, spear fishing, set lining and crayfish trapping."""
    angling = "angling"
    set_lining = "set_lining"
    spear_fishing = "spear_fishing"
    crayfish_trapping = "crayfish_trapping"
    ice_fishing = "ice_fishing"
    netting = "netting"
    snagging = "snagging"
    #: `chumming` had no value here, so `method: "other"` plus `reason: "chumming"` carried it —
    #: the prohibited act's own NAME in a free-text field.
    chumming = "chumming"
    #: `downrigger` AND `light` WERE MEMBERS HERE AND ARE NOT ANY MORE.
    #:
    #: The goal was one vocabulary for `while`, and that is met a better way (below). What putting
    #: a device in `method` cost was a FABRICATED PERMISSION. A "provided that" sentence becomes
    #: two rules — one allowing the means, one holding the condition — and run through that
    #: template both of these produce `method: {allow: [...]}`:
    #:
    #:   zp:allowable_methods   "angle with a downrigger, PROVIDED the line is attached by a
    #:                           quick-release"        — an ALLOWABLE list. The grant is real.
    #:   zp:terminal_tackle     "Use a light … UNLESS submerged and within 1 m of the hook"
    #:                          — from a list headed "It is UNLAWFUL to". It grants nothing.
    #:
    #: "Provided" and "unless" are opposite polarity through one template, so a Region 6 lake
    #: answered "what may I fish with here" with `light` — the one piece of tackle the province
    #: forbids outright. That is `{method: "ice_fishing", permitted: true}` on a hut-removal
    #: warning, rebuilt on a new field; `Conduct` below names that failure.
    #:
    #: `while` DRAWS FROM METHOD MEMBERS (means) AND TWO DEVICES (`downrigger`, `light`) — see
    #: `WHILE_TOKENS`. So `while: ["downrigger"]` has a referent without `downrigger` being a way
    #: of fishing, and the only thing lost is the ability to write the permission the book never
    #: printed.
    other = "other"


class Period(str, Enum):
    daily = "daily"
    possession = "possession"
    annual = "annual"          # the LICENCE year, Apr 1 - Mar 31
    monthly = "monthly"        # defined provincially; unused in this corpus


class WaterKind(str, Enum):
    stream = "stream"
    lake = "lake"


class ClosureKind(str, Enum):
    """The season a region's blanket closure is NAMED for — "Spring closure", "Summer closure",
    "the existing winter/spring closure regulation". A name, not a date range: see
    `CatalogueRule.closure_kind`."""
    spring = "spring"
    summer = "summer"
    winter = "winter"


#: "spring closure", "Summer closure:", "(spring closure)", "spring stream closure",
#: "winter/spring closure" (both).
_PRINTED_CLOSURE_KIND = re.compile(
    r"\b(spring|summer|winter)(?:\s*/\s*(spring|summer|winter))?\s+(?:stream\s+)?closure\b", re.I)


def printed_closure_kinds(text: str) -> frozenset:
    """The kinds of closure a sentence NAMES by their season word ("Exempt from spring closure",
    "Summer closure: No Fishing …"). Empty when it names none — "EXEMPT from the Apr 1-June 14
    closure" names dates, not a kind, and "Mainstem open all year" names nothing."""
    out = set()
    for m in _PRINTED_CLOSURE_KIND.finditer(text or ""):
        out.update(g.lower() for g in m.groups() if g)
    return frozenset(out)


class Origin(str, Enum):
    hatchery = "hatchery"
    wild = "wild"


class ChannelSide(str, Enum):
    """One half of a river's channel, lengthwise, by compass side (`CatalogueRule.side`)."""
    north = "north"
    south = "south"
    east = "east"
    west = "west"

    @property
    def opposite(self) -> "ChannelSide":
        return {ChannelSide.north: ChannelSide.south, ChannelSide.south: ChannelSide.north,
                ChannelSide.east: ChannelSide.west, ChannelSide.west: ChannelSide.east}[self]


class LifeStage(str, Enum):
    """A LIFE STAGE THE BOOK DEFINES (`CatalogueRule.life_stage`). One: an ADULT chinook (p.77,
    "Definition of Adult Chinook in Non-Tidal Waters": over 50 cm nose to fork in most non-tidal
    waters, over 62 cm in some rivers). The length is the definition's, and it differs by water,
    so the stage is held as the book's word — never as `lengths`."""
    adult = "adult"


#: The fish a life stage is defined for (p.77 defines "adult" for chinook only).
LIFE_STAGE_FISH = {LifeStage.adult: "CH"}
#: "adult chinook" as a sentence prints it.
_ADULT_CHINOOK = re.compile(r"\badult\s+chinook\b", re.I)


#: "on the west half of river" — a rule printed for one half of a channel (`CatalogueRule.side`).
#: A lake's half ("south half only", Premier Lake) is a part of the lake, `undrawn_part`.
HALF_OF_CHANNEL = re.compile(r"\b(north|south|east|west)\s+half\s+of\s+(?:the\s+)?"
                             r"(?:river|stream|creek|channel)\b", re.I)


class VesselAspect(str, Enum):
    propulsion = "propulsion"
    speed = "speed"
    towing = "towing"


class PropulsionLevel(str, Enum):
    """ONE ORDERED SCALE, strictest first. `r2:sasamat_lake@2-8` holds two of these seasonally,
    which is the proof they are one field and not four types."""
    none = "none"                    # no vessels at all
    unpowered = "unpowered"          # no powered boats
    #: "Electric motor only: you may use only battery-powered electric motors - MAX 7.5 kW."
    #: The 7.5 is part of the definition, so the 161 rules repeating it are restating the page,
    #: not setting a per-water cap. Store max_power_kw only when a water differs.
    electric_only = "electric_only"
    power_capped = "power_capped"


class Document(str, Enum):
    basic_licence = "basic_licence"
    steelhead_stamp = "steelhead_stamp"
    salmon_stamp = "salmon_stamp"
    kootenay_rainbow_stamp = "kootenay_rainbow_stamp"
    shuswap_char_stamp = "shuswap_char_stamp"
    shuswap_rainbow_stamp = "shuswap_rainbow_stamp"
    white_sturgeon_licence = "white_sturgeon_licence"
    classified_waters_licence = "classified_waters_licence"
    national_park_permit = "national_park_permit"
    angling_guide_licence = "angling_guide_licence"
    #: "A person commits an offence if they do not hold a valid angling guide OR ASSISTANT angling
    #: guide licence" — two documents, either of which satisfies; it was folded into one.
    assistant_angling_guide_licence = "assistant_angling_guide_licence"
    #: A third-party permit: "A permit is required for fishing on all waters within the Creston
    #: Valley Wildlife Management Area". It was an `access_permission` with a free-text grantor.
    creston_valley_wma_permit = "creston_valley_wma_permit"
    #: RECIPROCITY: "B.C. and Yukon angling licences are valid on all parts of Morley Lake".
    yukon_angling_licence = "yukon_angling_licence"
    #: A landowner's permission, named: "angling access requires permission of the Creston Valley
    #: Rod & Gun Club". Each grantor is its own member, added by reviewed change.
    creston_valley_rod_and_gun_club_permission = "creston_valley_rod_and_gun_club_permission"


class Obligation(str, Enum):
    """Law or advice. `z7b:ice_fishing_huts_notice` carries BOTH in one sentence — huts *should*
    show contact details, and failing to remove one *is an offence*. Rendering advice as law is the
    mirror of rendering law as advice, and both are in this corpus."""
    must = "must"
    should = "should"


class _Terse(BaseModel):
    """DUMPS ONLY WHAT WAS SAID. Every list on a gear clause defaults empty and an empty `allow`,
    `only` or `ban` is refused, so an empty list here never carries meaning — yet each one shipped
    as `except: [], members: [], must_be: [], of: []` on every clause in the bundle, four keys of
    noise a reader has to prove are noise."""

    @model_serializer(mode="wrap")
    def _terse(self, handler):
        return {k: v for k, v in handler(self).items()
                if v != [] and v != {} and v != "" and v is not None and v is not False}
