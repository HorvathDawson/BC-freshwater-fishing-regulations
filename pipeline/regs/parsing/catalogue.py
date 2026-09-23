"""The rule catalogue — 15 types, their conditions, and what makes two rules comparable.

Replaces hand-written `Rule.details` with a TYPE plus named CONDITIONS. The label is generated
from those (see `label()`), so it cannot drift from the numbers the way the prose did:
`r6:bennett_lake` said "tiered size limit" while its own verbatim said "only 1 over 90 cm, none
between 60 cm and 90 cm".

Spec: pipeline/docs/17-rule-catalogue.md. Source text: data/curated/regulations/reference/.

THE ONE INVARIANT: a type boundary is a wall the override cannot cross. Two rules that could ever
displace one another must share a type and differ only in `dimension`; two rules that never compete
must not. `z4:bass_closed` (closure) and `r4:wasa_lake` (harvest) are one subject at two values, and
filing them apart is why the override never fired.
"""

from __future__ import annotations

import re

from enum import Enum
from typing import List, Optional


from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator


class RuleType(str, Enum):
    retention_limit = "retention_limit"
    stop_fishing_after_quota = "stop_fishing_after_quota"
    bait_restriction = "bait_restriction"
    tackle_restriction = "tackle_restriction"
    method_rule = "method_rule"
    vessel_rule = "vessel_rule"
    angling_from_vessel_prohibited = "angling_from_vessel_prohibited"
    navigation_duty = "navigation_duty"
    document_required = "document_required"
    access_permission = "access_permission"
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
    #: `while` DRAWS FROM METHOD MEMBERS AND SPEC-SLOT NAMES, and a spec slot's name already IS a
    #: means token — `set_lining`, `crayfish_trapping`. So `while: ["downrigger"]` has a referent
    #: without `downrigger` being a way of fishing, and the only thing lost is the ability to
    #: write the permission the book never printed.
    other = "other"


class Period(str, Enum):
    daily = "daily"
    possession = "possession"
    annual = "annual"          # the LICENCE year, Apr 1 - Mar 31
    monthly = "monthly"        # defined provincially; unused in this corpus


class WaterKind(str, Enum):
    stream = "stream"
    lake = "lake"


class Origin(str, Enum):
    hatchery = "hatchery"
    wild = "wild"


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


class Residency(str, Enum):
    resident = "resident"
    non_resident = "non_resident"
    non_resident_alien = "non_resident_alien"


class Obligation(str, Enum):
    """Law or advice. `z7b:ice_fishing_huts_notice` carries BOTH in one sentence — huts *should*
    show contact details, and failing to remove one *is an offence*. Rendering advice as law is the
    mirror of rendering law as advice, and both are in this corpus."""
    must = "must"
    should = "should"


#: Dash variants the extraction emits interchangeably; all mean "-" for comparison.
_DASH = dict.fromkeys(map(ord, "‐‑‒–—―−"), "-")


def squash(text: str) -> str:
    """Normalise away what carries no meaning when comparing a quote to its source: EMPHASIS,
    bullets, blockquote markers, dash variants, whitespace, case.

    Markdown is OUR annotation, added by extraction — 629 of 1393 batch rows carry `**`. The
    printed regulation has no asterisks in it, so a model quoting the sentence it can see writes
    it without them, and a raw substring check then calls a perfect quote a fabrication. It called
    700 of them that. What is STORED is still the batch's own text, byte for byte; only the
    comparison is normalised."""
    t = (text or "").translate(_DASH).replace("*", "")
    t = re.sub(r"(?m)^\s*[>|]\s?", " ", t)
    t = re.sub(r"(?m)^\s*[-•]\s+", " ", t)
    return re.sub(r"\s+", " ", t).strip().lower()


class AnglerClass(BaseModel):
    """WHO may fish — four orthogonal axes, every one of them used for real. A guided non-resident
    alien and a non-guided one buy different licences; a 15-year-old B.C. resident needs none."""
    model_config = ConfigDict(frozen=True)
    residency: Optional[Residency] = None
    guided: Optional[bool] = None
    age: Optional[str] = Field(default=None, pattern="^(under_16|16_plus)$")
    status: Optional[str] = Field(default=None, pattern="^(indian_bc_resident|metis|disabled)$")
    #: Youth/Disabled Accompanied Waters: "An authorized angler can be accompanied by up to two
    #: companion anglers." A companion may not fish there alone.
    companions: Optional[int] = None

    def is_empty(self) -> bool:
        return not any((self.residency, self.guided is not None, self.age, self.status,
                        self.companions is not None))


#: THE GROUPS THE SYNOPSIS ACTUALLY PRINTS, and what each one covers.
#:
#: The source of these memberships is the closed game-fish list in `reference/definitions.md`
#: ("Freshwater game fish — the closed list"), not a genus walk. That matters: *Oncorhynchus*
#: covers salmon AND rainbow/cutthroat, so taxonomy cannot draw the line the regulations draw.
#:
#: WHY A GROUP AND NOT ITS MEMBERS. "Trout/char: 5" is ONE claim about trout and char. Stored as
#: nine codes it becomes nine claims that merely coincide: a later correction has to find all
#: nine, the reader cannot see which word the synopsis used, and the sentence is no longer
#: recoverable from the rule. `expand_species` turns a group back into members where a caller
#: needs the set — nothing is lost by storing the word the page printed.
SPECIES_GROUPS: dict[str, tuple[str, ...]] = {
    "TROUT":      ("RB", "ST", "CT", "WCT", "CCT", "GB", "GT"),
    # NO ANADROMOUS FORMS. "ADV" (Dolly Varden, anadromous) and "AEB" (brook trout,
    # anadromous) were members here and NAMED BY NO RULE IN THE CORPUS — 0 of 3,422. They
    # existed only to be filtered back out again, and they leaked: 74 size statements told a
    # reader their limit was shared with "brook trout (anadromous)", a name that appears in no
    # heading on any of the 22 tables. The one anadromous form the book actually regulates is
    # the steelhead, which has its own code and its own rules.
    "CHAR":       ("DV", "BT", "LT", "EB", "AC", "SPK"),
    "WHITEFISH":  ("LW", "MW"),
    "BASS":       ("LMB", "SMB"),
}
#: "Trout rules apply to char unless char are excluded" (definitions.md). The synopsis prints one
#: quota line for both, and it is the single commonest species value in the corpus.
SPECIES_GROUPS["TROUT_CHAR"] = SPECIES_GROUPS["TROUT"] + SPECIES_GROUPS["CHAR"]
#: The closed list. A rule that applies to "everything" applies to THIS set, never the empty set.
SPECIES_GROUPS["ALL_GAME_FISH"] = SPECIES_GROUPS["TROUT_CHAR"] + SPECIES_GROUPS["WHITEFISH"] + \
    SPECIES_GROUPS["BASS"] + ("KO", "GR", "BB", "WSG", "BCB", "NP", "YP", "WP", "GE", "IN", "CRA")

#: NON-GAME FISH — a real rule subject, not a leftover. "Only non-game fish (such as carp) may be
#: speared, except burbot" (provincial-regulations.md) is a rule ABOUT this set, and without a name
#: for it the only way to write it is species_except with all 30 game codes, which states the rule
#: as a coincidence of thirty exclusions rather than as the one thing it says.
#:
#: It is deliberately EMPTY rather than enumerated. It is the complement of the game-fish list —
#: every carp, sucker, chub, sculpin and lamprey in the table and anything the province adds — so
#: listing members would be a guess that goes stale. `expand_species` leaves it alone for exactly
#: that reason: a caller that needs the set computes the complement, and one that does not is not
#: silently handed an empty list. `CP` (Carp) is nameable on its own because the sentence names it.
SPECIES_GROUPS["NON_GAME_FISH"] = ()

#: ALL FIN FISH — everything with fins, which is wider than the game-fish list.
#:
#: Three rules in the corpus say a set the vocabulary could not name, and all three were written
#: down as `ALL_GAME_FISH` because the menu said to use it for "everything":
#:
#:   "any fish willfully or accidentally snagged must be released immediately"  (snagging)
#:   "You must release all fin fish caught in your trap."                       (crayfish traps)
#:   "Catch and release all fish upstream of the Hasler Road Bridge"            (Pine River)
#:
#: `ALL_GAME_FISH` is the provincial closed list. It excludes salmon, which are federal, and every
#: non-game fish. So each of those sentences came out of the pipeline narrower than it was
#: written — the page told a reader that a snagged carp or a snagged coho need not be released.
#: That is the one direction a regulation must never be wrong in.
#:
#: It is EMPTY for the same reason `NON_GAME_FISH` is: it is not a list the province publishes,
#: it is "everything that is a fish", and half of it (the non-game half) is an open complement
#: that would go stale the moment it was written down. `expand_species` passes an empty group
#: through, so a caller sees the claim the rule actually made rather than a silently short list.
#:
#: It does NOT include crayfish, which is exactly why the book's two sentences are worded the way
#: they are: "All other methods of taking fin fish AND CRAYFISH are illegal" names both, and
#: "release all fin fish caught in your trap" names only one — the crayfish in the trap are the
#: point of the trap.
SPECIES_GROUPS["ALL_FIN_FISH"] = ()

#: PROTECTED SPECIES — the twelve taxa it is illegal to fish for at all, listed by name in the
#: provincial regulations. Unlike NON_GAME_FISH this one IS enumerated, because the book
#: enumerates it: a closed list of named fish is exactly what can be written down.
#:
#: It exists because the alternative was catastrophic. The parser could not name these taxa —
#: eight of them had no code — so it fell back to ALL_GAME_FISH, and "it is illegal to fish for
#: the fish listed below" became a retention limit of zero on every game fish in British
#: Columbia, with `may_target: false` and no method to narrow it. That is the exact shape of a
#: total closure, written on a PROVINCIAL entry that binds every water in the province.
#:
#: Region 2 adds green sturgeon, so this is a floor and not the complete set — which is why
#: a water may still carry its own protected-species rule on top.
#:
#: WHITE STURGEON IS NOT IN IT, because the book does not put the FISH in it. The list reads
#: "White Sturgeon (Nechako, Upper Fraser, Kootenay and Columbia populations)" — a qualifier
#: no other entry carries, and the whole difference between "closed everywhere" and "closed
#: where those populations are". A bare WSG here made the provincial rule a superior closure
#: on every water in the province, over the regional tables that state the fishery — "White
#: Sturgeon: CATCH AND RELEASE ONLY" in Regions 1, 2 and 3, and Region 5 downstream of
#: Williams Lake River — and every Fraser stretch read "you may not fish for it" on the one
#: water in the province with a legal sturgeon fishery. The four populations ARE stated, as
#: the regional closures that name them: Region 4 (Kootenay, Columbia), Regions 6 and 7A
#: (Nechako, Upper Fraser), Region 5 upstream of Williams Lake River, and `z7a:sara_sturgeon`
#: for the SARA listing itself. The regional tables carry the qualifier the group cannot.
SPECIES_GROUPS["PROTECTED_SPECIES"] = (
    "CCL", "ELS", "MLS", "NDC", "PLS", "RMS", "SHS", "SSU", "VCS", "VLA", "WBL",
    "GSG",   # green sturgeon — Region 2 p.21 names it; a protection on no water without this
)

#: Salmon are federal, not on the provincial game-fish list, and so are NOT in ALL_GAME_FISH.
#: They are here because the synopsis names them anyway (bait bans "when fishing for salmon",
#: Region 1/3 notices) and because the DFO corpus moves to this format next.
SPECIES_GROUPS["SALMON"] = ("CH", "CO", "SK", "PK", "CM")


#: A SIZE THE BOOK PUTS IN THE DEFINITION, NOT IN A QUOTA. Page 86: "steelhead: a rainbow
#: trout longer than 50 cm in waters where anadromous rainbow trout are found." So a steelhead
#: under 50 cm does not exist, and a table that offers a number for one is describing a fish
#: nobody can catch — Region 2 printed "up to 50 cm: 4 / over 50 cm: 2" where only the 2 is
#: real. Held here rather than as a curated rule because no regional table states it: it is
#: what the word MEANS, everywhere in the book.
DEFINITIONAL_SIZE = {
    "ST": {"min_cm": 50,
           #: WHAT IT IS INSTEAD — recorded, but NOT applied when settling. The definition
           #: holds only "in waters where anadromous rainbow trout are found": steelhead are
           #: sea-going, so a table naming trout and not steelhead is describing a landlocked
           #: rainbow the 50 cm boundary says nothing about. Substituting globally also broke
           #: the invariant the table rests on — a verdict about steelhead came to be decided
           #: by a trout/char counter that is not on the steelhead row. Applying this needs a
           #: per-water "are there steelhead here" fact the corpus does not carry.
           "below": "RB",
           "applies_where": "anadromous rainbow trout are found",
           "says": "a steelhead is a rainbow trout longer than 50 cm, so there is no "
                   "such thing as a smaller one",
           "source": "fishing_synopsis.pdf \u00b7 2025-2027 \u00b7 page 86, Definitions"},
}

#: "SA" IS THE CSV'S NAME FOR THE SAME GROUP, NOT A FISH. Held as a leaf it named nobody, so
#: "No spear fishing of Pacific salmon" (zp:spear_fishing.r4) reached neither coho nor chinook
#: — a closure that protected none of the fish it is written about.
SPECIES_GROUPS["SA"] = SPECIES_GROUPS["SALMON"]

#: Every code a rule may name: the groups above, their members, and the individuals that appear
#: alone. A rule naming anything else is refused at validation rather than printing a raw code.
KNOWN_SPECIES = frozenset(
    set(SPECIES_GROUPS) | {c for members in SPECIES_GROUPS.values() for c in members} | {
        "SA", "SLV", "WF", "BS", "SG", "P",          # the CSV's own "General" rows
        "CP",                                        # named by the spear rule: "such as carp"
        "PW", "RW", "BG", "PMB", "GSG",              # named alone in the tables
        "NDC", "SSU", "CCL",                         # protected, never retainable
    }
)


def expand_species(codes: List[str]) -> List[str]:
    """A species list with every group replaced by its members, de-duplicated, order preserved.
    Anything that is not a group passes through untouched.

    TRANSITIVELY, AND DOWN TO LEAVES. A group may hold a group. Expanding one level left `TROUT`
    unexpanded inside `TROUT_CHAR`, so a rule about trout did not register under a rule about
    trout and char; and `ALL_GAME_FISH` holds the individual fish but not the code `TROUT_CHAR`,
    so comparing sets that still held group codes said "all game fish does not cover trout and
    char" — and a river closed to every game fish reported a keep limit of 5 for trout. The
    group tables happen to be flat today, so one level would give the same answer; this does not
    depend on their staying that way.

    (There were two implementations of this. `table/subject.py` had the transitive one and this
    had the one-level one, and they disagreed on exactly one thing — see below — which is the
    kind of difference that is invisible until it is a wrong number on a page. This is the only
    one now.)

    AN EMPTY GROUP PASSES THROUGH. `NON_GAME_FISH` and `ALL_FIN_FISH` are complements — "every
    fish that is not on the provincial list", "everything with fins" — not memberships, and
    expanding them to `[]` erases the rule. Three rules say it, including "any fish willfully or
    accidentally snagged must be released immediately". The caller sees the claim that was
    actually made and decides what to do with it.
    """
    out: List[str] = []
    stack = list(reversed(codes))
    while stack:
        c = stack.pop()
        kids = SPECIES_GROUPS.get(c)
        if kids:
            stack.extend(reversed([k for k in kids]))
        elif c in SPECIES_GROUPS:            # a group that expands to nothing: the claim itself
            if c not in out:
                out.append(c)
        elif c not in out:
            out.append(c)
    return out


#: Types whose label is otherwise the bare quote, and which a `required: false` turns into a
#: prohibition. The other types build their own words and say it themselves ("Netting is
#: prohibited", "A basic angling licence is not required").
_PROHIBITABLE = frozenset({RuleType.handling_rule, RuleType.navigation_duty,
                           RuleType.method_rule, RuleType.tackle_restriction})


#: TIER ONE. Every type belongs to exactly one family; the reader sees these as sections.
_FAMILY = {
    RuleType.retention_limit: "retention",
    RuleType.stop_fishing_after_quota: "retention",
    RuleType.bait_restriction: "gear_and_method",
    RuleType.tackle_restriction: "gear_and_method",
    RuleType.method_rule: "gear_and_method",
    RuleType.vessel_rule: "vessel",
    RuleType.angling_from_vessel_prohibited: "vessel",
    RuleType.navigation_duty: "vessel",
    RuleType.document_required: "licensing",
    RuleType.access_permission: "licensing",
    RuleType.handling_rule: "conduct",
    RuleType.hazard: "information",
    RuleType.advisory: "information",
    RuleType.program_membership: "information",
    RuleType.facility: "information",
}


class Exempts(BaseModel):
    """What this rule LIFTS. A field, not a type — an exemption takes the type of whatever it
    lifts, which is why `bait_restriction` carries `allowed: true` for the Fraser sturgeon rules.

    `default_id` names a rule from the closed vocabulary and resolves PER SECTION against whichever
    zone rule of that id is in force there — 959,116 of 959,143 sections carrying a spring closure
    carry exactly one, so the resolution is effectively unique. `target` is filled only when the
    exemption names another WATER's rule ("EXEMPT from Slocan River's closure")."""
    model_config = ConfigDict(frozen=True)
    default_id: Optional[str] = None
    target: Optional[str] = None
    note: str = ""

    @model_validator(mode="after")
    def _one_of(self) -> "Exempts":
        if not self.default_id and not self.target:
            raise ValueError("exempts needs a default_id or a target")
        return self


#: Month name -> number, and the last day of each. February is 29 ON PURPOSE: the book says
#: "February", which includes the 29th in the years it exists, and 28 would quietly shorten it.
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_LAST_DAY = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
             7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def parse_clock(text: str) -> Optional["Clock"]:
    """A printed time -> a `Clock`. "21:00" and "21:00 hours" are the same instant spelled two
    ways and both sat in the corpus; "one hour after sunset" is not a clock time at all."""
    t = " ".join((text or "").split()).lower().replace(" hours", "")
    if not t:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", t)
    if m:
        return Clock(at=f"{int(m.group(1)):02d}:{m.group(2)}")
    m = re.fullmatch(r"(?:(one|an|\d+)\s*(hour|hours|minute|minutes|min)\s*)?"
                     r"(before|after)?\s*(sunrise|sunset)", t)
    if not m:
        return None
    n, unit, side, ev = m.groups()
    mins = 0
    if n:
        v = 1 if n in ("one", "an") else int(n)
        mins = v * (60 if unit.startswith("hour") else 1)
        if side == "before":
            mins = -mins
    return Clock(solar=Solar(ev), offset_min=mins)


def parse_date_range(text: str) -> Optional["DateRange"]:
    """One printed window -> a `DateRange`. Returns None where the text does not parse, so a
    caller fails loudly rather than inventing a season.

    Handles the four spellings the corpus uses: "Nov 1-Apr 30", "Nov 1 - Apr 30" (the same range,
    and 74 rules split between them), "May 1-31" (same month, end unqualified) and a bare month.
    """
    t = " ".join((text or "").split())
    if not t:
        return None
    parts = re.split(r"\s*(?:-|\u2013|\u2014|\bto\b)\s*", t, maxsplit=1)
    lo = _point(parts[0])
    if lo is None:
        return None
    if len(parts) == 1:
        if lo[1] is not None:
            return None                                  # a lone date, not a range
        return DateRange(from_month=lo[0], from_day=1,
                         to_month=lo[0], to_day=_LAST_DAY[lo[0]])
    hi = _point(parts[1])
    if hi is None:
        m = re.fullmatch(r"(\d{1,2})", parts[1].strip())  # "May 1-31": the month carries over
        if not m or lo[1] is None:
            return None
        hi = (lo[0], int(m.group(1)))
    if lo[1] is None or hi[1] is None:
        return None
    return DateRange(from_month=lo[0], from_day=lo[1], to_month=hi[0], to_day=hi[1])


def _point(text: str):
    """"Nov 1" -> (11, 1); "February" -> (2, None); anything else -> None."""
    t = text.strip().rstrip(",")
    m = re.fullmatch(r"([A-Za-z]+)\.?\s*(\d{1,2})", t)
    if m:
        mo = _month(m.group(1))
        return (mo, int(m.group(2))) if mo else None
    m = re.fullmatch(r"([A-Za-z]+)\.?", t)
    if m:
        mo = _month(m.group(1))
        return (mo, None) if mo else None
    return None


def _month(name: str) -> Optional[int]:
    n = name.strip().lower()
    return _MONTHS.get("sep" if n.startswith("sept") else n[:3])


def _day_index(month: int, day: int) -> int:
    return sum(_LAST_DAY[m] for m in range(1, month)) + day


def complement(ranges: List["DateRange"]) -> List["DateRange"]:
    """THE DAYS THESE RANGES DO NOT COVER, on a circular year.

    This is what retires `windows_are: "excepts"`. "Open June 16-Apr 30 each year" is a CLOSURE
    whose printed dates are the days it does not apply; its complement, May 1 - June 15, is the
    closure's own season and needs no flag to read correctly.
    """
    covered = set()
    for r in ranges:
        a, b = _day_index(r.from_month, r.from_day), _day_index(r.to_month, r.to_day)
        days = range(a, b + 1) if a <= b else list(range(a, 367)) + list(range(1, b + 1))
        covered.update(days)
    total = sum(_LAST_DAY.values())
    free = [d for d in range(1, total + 1) if d not in covered]
    if not free:
        return []
    runs, start = [], free[0]
    for prev, cur in zip(free, free[1:]):
        if cur != prev + 1:
            runs.append((start, prev)); start = cur
    runs.append((start, free[-1]))
    # A run ending on Dec 31 and one starting Jan 1 are ONE run on a circle.
    if len(runs) > 1 and runs[0][0] == 1 and runs[-1][1] == total:
        runs = [(runs[-1][0], runs[0][1])] + runs[1:-1]
    return [DateRange(from_month=_md(a)[0], from_day=_md(a)[1],
                      to_month=_md(b)[0], to_day=_md(b)[1]) for a, b in runs]


def _md(idx: int):
    for m in range(1, 13):
        if idx <= _LAST_DAY[m]:
            return m, idx
        idx -= _LAST_DAY[m]
    raise ValueError(idx)


class Solar(str, Enum):
    sunrise = "sunrise"
    sunset = "sunset"


class Clock(BaseModel):
    """A TIME OF DAY, either off the clock or off the sun.

    The book writes both and the corpus stored both as prose — "21:00", "21:00 hours" and "one
    hour after sunset" all sat in the same string field, so the first two were the same instant
    spelled two ways and the third was not a time at all. A solar time cannot be resolved to a
    clock without a date and a latitude, which is the client's to do and not the parser's, so it
    is carried as what it is.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    at: Optional[str] = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    solar: Optional[Solar] = None
    #: Minutes from the solar event. NEGATIVE IS BEFORE — "30 minutes before sunrise" is
    #: `{solar: sunrise, offset_min: -30}`, and the sign is the whole difference between
    #: fishing legally and not.
    offset_min: int = 0

    @model_validator(mode="after")
    def _one_kind(self) -> "Clock":
        if bool(self.at) == bool(self.solar):
            raise ValueError("a time is a clock time OR a solar time, not both and not neither")
        if self.at and self.offset_min:
            raise ValueError("an offset belongs to a solar time; put it in the clock time")
        return self

    def words(self) -> str:
        if self.at:
            return self.at
        n = abs(self.offset_min)
        if not n:
            return self.solar.value
        unit = f"{n} minutes" if n % 60 else ("one hour" if n == 60 else f"{n // 60} hours")
        return f"{unit} {'before' if self.offset_min < 0 else 'after'} {self.solar.value}"


class DateRange(BaseModel):
    """A RANGE OF CALENDAR DAYS, no year. Both ends INCLUSIVE, per the synopsis: "When no date
    is listed, the regulations apply ALL YEAR. Start and end dates are INCLUSIVE."

    A range may WRAP the year end — "Nov 1-Apr 30" is one winter, not an error — so `to` before
    `from` is meaningful and is not rejected.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    from_month: int = Field(ge=1, le=12)
    from_day: int = Field(ge=1, le=31)
    to_month: int = Field(ge=1, le=12)
    to_day: int = Field(ge=1, le=31)

    @model_validator(mode="after")
    def _real_days(self) -> "DateRange":
        for m, d, side in ((self.from_month, self.from_day, "from"),
                           (self.to_month, self.to_day, "to")):
            if d > _LAST_DAY[m]:
                raise ValueError(f"{side}: day {d} does not exist in month {m}")
        return self

    def words(self) -> str:
        nm = {v: k.capitalize() for k, v in _MONTHS.items()}
        return f"{nm[self.from_month]} {self.from_day}-{nm[self.to_month]} {self.to_day}"


class Hours(BaseModel):
    """A RANGE WITHIN THE DAY. Either end may be a clock time or a solar one, so "from one hour
    after sunset to one hour before sunrise" is sayable. It WRAPS midnight the same way a
    `DateRange` wraps the year end, and needs no flag for that either."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    start: Clock
    end: Clock

    def words(self) -> str:
        return f"{self.start.words()} to {self.end.words()}"


class When(BaseModel):
    """WHEN A RULE BINDS — the days, the hours and the weekdays, said once.

    This replaces `windows` (210 distinct FREE-TEXT strings, where "Nov 1-Apr 30" and
    "Nov 1 - Apr 30" were the same range spelled two ways), `windows_are`, `from_time`, `to_time`
    and `weekdays`.

    THERE IS NO `excepts` FLAG. `windows_are: "excepts"` meant "these are the days the rule does
    NOT hold" — a flag that inverted the field beside it, which is exactly what `band` did to the
    size fields. Three rules carried it. The complement of a circular range is another circular
    range, so the days a rule DOES hold are always writable: Fulton River's "Open June 16-Apr 30
    each year" is a closure, and it is stored as the closure's own days, May 1 - June 15.

    `dates` EMPTY MEANS ALL YEAR, per the synopsis: "When no date is listed, the regulations apply
    ALL YEAR. Start and end dates are INCLUSIVE."
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    dates: List[DateRange] = Field(default_factory=list)
    hours: Optional[Hours] = None
    weekdays: List[str] = Field(default_factory=list)
    #: SEASONS THE PARSER COULD NOT READ, kept verbatim. This is the time analogue of
    #: `unresolved_locators`, and it exists for the same reason: an unparsed season and an ABSENT
    #: one are opposite facts, and a rule published as though it had no season when its source
    #: says "To be determined" is open all year to a reader. The DFO feed scrapes rows whose date
    #: cell is prose, and refusing them would have meant dropping the rule or inventing a window.
    unparsed: List[str] = Field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.dates or self.hours or self.weekdays or self.unparsed)

    def words(self) -> str:
        bits = [" and ".join(d.words() for d in self.dates)] if self.dates else []
        if self.hours:
            bits.append(self.hours.words())
        if self.weekdays:
            bits.append(" and ".join(f"{d}s" for d in self.weekdays))
        if self.unparsed:
            bits.append(" and ".join(self.unparsed))
        return ", ".join(b for b in bits if b)


class Slot(str, Enum):
    """WHAT A GEAR CLAUSE CONSTRAINS. One name per MEASURAND, which is the whole point.

    `hook_count` used to be three different quantities under one name — hooks on the line, POINTS
    on one hook, and attachments of any kind on the line — and a treble is one hook with three
    points, so the same record permitted it and banned it. The curator now decides once, in the
    name, instead of leaving it to whichever neighbouring field happens to be present.
    """
    # CHOSEN FROM A SET — `allow` lists what is permitted, and `allow: []` IS the ban.
    bait = "bait"                       # roe, invertebrate, dead_fin_fish, any
    lure = "lure"                       # the terminal object: artificial_fly, artificial_lure
    method = "method"                   # how you fish: fly_fishing, set_lining, ice_fishing
    barb = "barb"                       # barbed | barbless
    # SPEC SLOTS — `must_be`, how the thing must be built or carried.
    #
    # THE SLOT AND THE MEANS SHARE ONE TOKEN. A set line IS how you set-line and a crayfish trap
    # IS how you crayfish-trap, so the slot is named for the means and `while: ["set_lining"]`
    # needs no lookup to reach `set_lining: {must_be: [...]}`. They were `set_line` and
    # `crayfish_trap`, which forced a correspondence map in the register for no gain.
    set_lining = "set_lining"
    crayfish_trapping = "crayfish_trapping"
    downrigger = "downrigger"
    light = "light"
    # …EXCEPT WHERE THE OBJECT IS NOT THE MEANS. An ice hut is not how you ice fish; it is a thing
    # you leave on a lake. So this slot keeps its own name and carries `while: ["ice_fishing"]`.
    ice_hut = "ice_hut"
    # COUNTED — `max`/`min` are whole numbers of the thing the name says.
    hooks_per_line = "hooks_per_line"
    points_per_hook = "points_per_hook"
    lines_per_angler = "lines_per_angler"
    flies_per_line = "flies_per_line"
    terminal_attachments_per_line = "terminal_attachments_per_line"
    # MEASURED — `max`/`min` in the unit the name carries. One unit per quantity: `min_gap_cm: 3`
    # and `max_gap_mm: 15` were the same measurement of the same object in two units.
    hook_gap_mm = "hook_gap_mm"
    weight_per_line_kg = "weight_per_line_kg"
    bait_possession_kg = "bait_possession_kg"
    #: "unless the light is submerged and attached to the fishing line WITHIN 1 M of the hook" —
    #: a distance, so it is its own slot with the unit in the name. Written as a bound on `light`
    #: it read as "at most one light", which is the founding complaint of this enum.
    light_to_hook_mm = "light_to_hook_mm"


#: Which slots take a SET and which take a NUMBER. A slot cannot take both, and `_check` refuses
#: the mixture — that is what stops a count being written where a whitelist belongs.
_SET_SLOTS = frozenset({Slot.bait, Slot.lure, Slot.method, Slot.barb})

#: The slots whose bound is `must_be` — how the thing must be built or carried, never a count and
#: never a whitelist. Presence asserts; absence is silence; there is no negation to write.
_SPEC_SLOTS = frozenset({Slot.set_lining, Slot.crayfish_trapping, Slot.downrigger, Slot.light,
                         Slot.ice_hut})

#: The slots whose bound is a MEASUREMENT rather than a count, and so may be fractional.
_MEASURED = frozenset({Slot.hook_gap_mm, Slot.weight_per_line_kg, Slot.bait_possession_kg,
                       Slot.light_to_hook_mm})


class AnglerState(str, Enum):
    alone_in_boat = "alone_in_boat"
    in_boat = "in_boat"
    from_shore = "from_shore"


class _Terse(BaseModel):
    """DUMPS ONLY WHAT WAS SAID. Every list on a gear clause defaults empty and an empty `allow`,
    `only` or `ban` is refused, so an empty list here never carries meaning — yet each one shipped
    as `except: [], members: [], must_be: [], of: []` on every clause in the bundle, four keys of
    noise a reader has to prove are noise."""

    @model_serializer(mode="wrap")
    def _terse(self, handler):
        return {k: v for k, v in handler(self).items() if v != [] and v != "" and v is not None}


class GearWhen(_Terse):
    """WHEN A GEAR CLAUSE APPLIES — and a CLOSED vocabulary on purpose.

    `reason` was a free-text field that ended up carrying six different jobs: a carve-out, a
    gating condition, an obligation, the prohibited act's own name, and once an actual reason.
    An open `when` would be that field again under a better name. So every condition here is a
    named term, and anything the book says that does not fit goes in `note` — which REQUIRES a
    `review_reason` on the rule, so the gap is visible rather than absorbed.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    water: Optional[WaterKind] = None
    method: Optional[Method] = None          # "…when set lining"
    targeting: List[str] = Field(default_factory=list)   # species codes
    angler: Optional[AnglerState] = None
    gear_in_use: Optional[str] = None        # "…does not apply to downrigger weights"
    note: str = ""                           # the escape, and it costs a review_reason

    def is_empty(self) -> bool:
        return not (self.water or self.method or self.targeting or self.angler
                    or self.gear_in_use or self.note)


class GearSpec(_Terse):
    """HOW THE THING MUST BE BUILT OR CARRIED, for the clause to hold.

    Three sentences needed this and only this, and without it all three read as unrepresentable:

      "angle with a downrigger, PROVIDED the fishing line is attached to the downrigger by a
       quick-release mechanism"     -> the proviso constrains the downrigger in the same clause,
                                       not some second thing
      "unless the light is SUBMERGED and ATTACHED to the fishing line WITHIN 1 M of the hook"
                                    -> three properties of the one light
      "traps with minimally-sized CIRCULAR openings"
                                    -> the shape is a property; only the size has no number

    A CLOSED vocabulary on purpose. `reason` was open and ended up carrying six different jobs;
    an open spec would be that field again. What does not fit goes in `note`, which costs a
    `review_reason`, so the gap stays visible instead of being absorbed.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    #: `submerged: Optional[bool]` WAS HERE and it was the last polarity flag in the model.
    #: `submerged: false` reads as "a light is lawful only if it is NOT submerged", the inverse of
    #: the printed clause — `{barbless: true, required: false}` in a new house, evicted from
    #: `GearClause` and re-admitted one object down. A state the thing must be IN is a `must_be`
    #: token on the spec slot, where negation is unwriteable.
    attached_to: Optional[str] = None        # fishing_line
    attachment: Optional[str] = None         # quick_release
    within_m_of_hook: Optional[float] = None
    opening_shape: Optional[str] = None      # circular
    note: str = ""

    def is_empty(self) -> bool:
        return not (self.attached_to or self.attachment
                    or self.within_m_of_hook or self.opening_shape or self.note)


class GearClause(_Terse):
    """ONE THING CONSTRAINED, AND HOW FAR. Ordered within `gear`; FIRST MATCH WINS.

    THERE IS NO POLARITY FLAG. `allowed`, `permitted` and `required` were three booleans on one
    axis, `required` was `false` on all 25 of its uses, and on the province's most-cited stream
    rule `{barbless: true, required: false}` read as "barbless is not required" — barbed hooks
    legal in every stream in B.C. A clause carries its verdict in the same object as its numbers,
    so there is no neighbour left to invert.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    slot: Slot
    #: WHICH members this clause speaks about. Absent = ALL of them, so "bait ban" is a bare
    #: `{slot: bait, allow: []}` while "fin fish is prohibited" is `of: ["fin_fish"]` with the
    #: same empty allow — a TOTAL ban and a PARTIAL one, which without `of` are one record.
    of: List[str] = Field(default_factory=list)
    #: THREE BOUNDS, ONE MEANING EACH. `allow` permits and says nothing about the rest; `only` is
    #: a whitelist that closes the slot ("fly fishing only"); `ban` prohibits what it names. With
    #: `allow` alone, a permission and a whitelist are the same record and a reader must guess.
    allow: Optional[List[str]] = None
    only: Optional[List[str]] = None
    ban: Optional[List[str]] = None
    #: MEMBERS THIS CLAUSE'S BOUND DOES NOT REACH. "The use of fin fish (dead or alive) or parts
    #: of fin fish OTHER THAN ROE is prohibited throughout the province" is one sentence with one
    #: bound and one carve-out, and `gear` being a map means a ban and an allow cannot both sit on
    #: `bait` in one rule. Without this the exception produced NO FIELD, and its whole force fell
    #: on one register line parenting `roe` to `any_bait` rather than to `fin_fish` — a line that
    #: reads like under-specification, whose correction would ban roe province-wide, and which no
    #: verbatim-seeded check could catch because there was no field to compare against.
    except_: List[str] = Field(default_factory=list, alias="except")
    members: List[str] = Field(default_factory=list)  # a choice of ONE from several kinds
    max: Optional[float] = None
    min: Optional[float] = None
    when: Optional[GearWhen] = None
    #: HOW THE THING ITSELF MUST BE — see `GearSpec`. A property of the subject this clause
    #: names, not a separate condition on the angler or the water.
    requires: Optional[GearSpec] = None
    must_be: List[str] = Field(default_factory=list)
    #: WHAT LIFTS THIS CLAUSE. "more than 1 kg of weight … (this does not apply to downrigger
    #: weights)" is one clause with one escape; the carve-out used to sit in `reason`, the same
    #: free-text field that elsewhere held an obligation and elsewhere the prohibited act's name.
    unless: List[GearWhen] = Field(default_factory=list)

    @model_validator(mode="after")
    def _shape(self) -> "GearClause":
        counted = self.max is not None or self.min is not None
        bounds = [b for b in (self.allow, self.only, self.ban) if b is not None]
        if self.slot in _SPEC_SLOTS:
            # NO BOUND AT ALL ON A SPEC SLOT. `light: {max: 1}` read as "at most one light" when
            # the printed 1 is METRES — "within 1 m of the hook" — and `light` was the one slot
            # admitted to a number without the unit in its name, which is the founding complaint
            # of this enum.
            if bounds or counted:
                raise ValueError(f"{self.slot.value} says HOW the thing must be: use `must_be` "
                                 f"(a distance is its own slot, with the unit in the name)")
            if not self.must_be and not self.requires:
                raise ValueError(f"{self.slot.value} needs a `must_be`")
        elif self.slot in _SET_SLOTS:
            if counted:
                raise ValueError(f"{self.slot.value} is chosen from a set: "
                                 f"use allow/only/ban, not max/min")
            if len(bounds) != 1:
                raise ValueError(f"{self.slot.value} takes exactly one of allow / only / ban")
            # A BAN NAMES ITS MEMBERS. An empty list is also what a dropped key, a failed parse and
            # a half-filled field produce, so `ban: []` would make the most dangerous statement in
            # the schema the easiest one to write by accident.
            # NO EMPTY LIST, ON ANY OF THE THREE. An empty list is what a dropped key, a failed
            # parse and a half-filled field all produce. `allow: []` was the sanctioned spelling
            # of "everything in this slot is banned" — the widest statement in the schema, written
            # in the one shape three accidents also write. "Bait ban" is `ban: ["any_bait"]`, and
            # the whole-slot member has to be typed.
            if self.except_ and self.ban is None:
                raise ValueError(f"{self.slot.value}: `except` carves members out of a ban")
            for key, val in (("allow", self.allow), ("only", self.only), ("ban", self.ban)):
                if val is not None and not val:
                    raise ValueError(
                        f"{self.slot.value}: `{key}: []` says nothing or says everything, and is "
                        f"what a dropped key also writes — name the members "
                        f"(a total ban is the whole-slot member, e.g. any_bait)")
        else:
            if self.except_:
                raise ValueError(f"{self.slot.value} is counted: `except` carves out set members")
            # ALL THREE SET BOUNDS, not just `allow`. `{hooks_per_line, only: ["single"], max: 1}`
            # rebuilds the exact collapse this enum exists to end — a statement about hook TYPE
            # riding on the slot that COUNTS hooks.
            if bounds:
                raise ValueError(f"{self.slot.value} is counted or measured: "
                                 f"use max/min, not allow/only/ban")
            # `members` QUALIFIES a bound and never substitutes for one. Without this,
            # "only one hook, one lure OR one fly is attached" — the basic licence entitlement,
            # on every angler on every water — inverts into unlimited terminal tackle.
            if not counted:
                raise ValueError(f"{self.slot.value} needs a max or a min")
        # AN ESCAPE THAT MATCHES EVERYTHING LIFTS EVERYTHING. `GearWhen()` with every field at its
        # default is a valid object, so "(this does not apply to downrigger weights)" with the one
        # key dropped becomes "there is no weight limit in B.C." — and a dropped key is exactly
        # what a parse that scanned the parenthetical and matched nothing produces.
        for u in self.unless:
            if u.is_empty():
                raise ValueError(f"{self.slot.value}: an `unless` with no condition lifts the "
                                 f"clause everywhere — say what lifts it")
        if self.requires is not None and self.requires.is_empty():
            raise ValueError(f"{self.slot.value}: `requires` states nothing")
        if self.max is not None and self.min is not None and self.min > self.max:
            raise ValueError(f"min {self.min} > max {self.max} permits nothing")
        # A COUNT IS A WHOLE NUMBER. Only the measured slots carry a fraction, and letting a
        # count be 1.5 would make "one and a half hooks" a storable rule.
        # THE BOOK PRINTS BOTH UNITS — "gap not less than 3 cm" on a set line, "no hooks greater
        # than 15 mm from point to shank" on a hook. One unit in the slot name is what stops them
        # diverging, and the cost is that an unconverted 3 is a LEGAL value meaning 3 mm, which is
        # no constraint at all. Nothing in the corpus goes below 15.
        if self.slot is Slot.hook_gap_mm:
            for v in (self.max, self.min):
                if v is not None and v < 10:
                    raise ValueError(f"hook_gap_mm {v} is smaller than any gap the book prints — "
                                     f"the synopsis states this in cm as well as mm, so confirm "
                                     f"the unit ('3 cm' is 30)")
        if self.slot not in _MEASURED:
            for v in (self.max, self.min):
                if v is not None and float(v) != int(v):
                    raise ValueError(f"{self.slot.value} counts things; {v} is not a whole number")
        return self


#: WHAT YOU MUST AND MUST NOT DO, as act tokens NAMED IN THEIR LAWFUL DIRECTION.
#:
#: `do_not_waste_catch`, never `waste_catch` plus a flag. A `must`/`must_not` key beside a token
#: that already carries its direction is a second place to state polarity — and `must:
#: "do_not_waste_catch"` is a double negative that reads as law. `required: false` was that flag:
#: it sat on "Waste the fish you catch" AND on "Fish with nets", both off a printed "You must
#: not" list, and read literally it made them optional; it was `false` on all 25 of its uses and
#: ABSENT on the requirements beside them, so its presence meant nothing and its absence meant
#: nothing.
#:
#: The vocabulary is open by necessity — each synopsis edition can print a new duty — so it is
#: held by a registry rather than a type: a token must appear here, and adding one is a reviewed
#: change carrying the verbatim that motivated it.
CONDUCT_ACTS = {
    "do_not_waste_catch": "Do not waste the fish you catch",
    "do_not_release_harmfully": "Do not release fish in a harmful manner",
    "do_not_buy_sell_or_barter_catch": "Do not buy, sell or barter your catch",
    "do_not_possess_or_move_live_fish": "Do not possess or move live fish or invertebrates",
    "do_not_can_bottle_or_fillet_away_from_residence":
        "Do not can, bottle or fillet your catch away from your residence",
    "do_not_freeze_in_unrecognizable_block": "Do not freeze fish in an unrecognizable block",
    "do_not_interfere_with_furbearer_trap": "Do not damage or interfere with a furbearer trap",
    "no_gear_in_water_during_closure": "Do not place gear in the water during a No Fishing period",
    "leave_head_tail_and_fins_until_residence":
        "Leave the head, tail and fins on your catch until your residence",
    "release_immediately": "Release it immediately, where you caught it",
    "warn_others_of_ice_hole": "Warn others of your ice hole",
    "remove_ice_hut_before_breakup": "Remove your ice hut before breakup",
    "mark_set_line_with_contact_details":
        "Mark your set line with your name, address and telephone number",
    "return_unfit_fish_gently": "Return a fish you cannot keep gently to the water",
}


class LengthBand(BaseModel):
    """ONE RANGE OF FISH LENGTHS, AND HOW MANY OF THEM YOU MAY KEEP.

    `min_cm` and `max_cm` are INCLUSIVE, and null is open at that end. `take` is how many of
    THESE you may keep; omitted, the rule's own `take` applies to them.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    min_cm: Optional[int] = None
    max_cm: Optional[int] = None
    take: Optional[int] = None

    @model_validator(mode="after")
    def _real(self) -> "LengthBand":
        if self.min_cm is None and self.max_cm is None:
            raise ValueError("a length band open at both ends is every fish; say nothing instead")
        if self.min_cm is not None and self.max_cm is not None and self.min_cm >= self.max_cm:
            raise ValueError(f"min_cm {self.min_cm} >= max_cm {self.max_cm} is an empty range")
        if self.take is not None and self.take < 0:
            raise ValueError("take cannot be negative")
        return self

    def holds(self, cm: int) -> bool:
        return ((self.min_cm is None or cm >= self.min_cm) and
                (self.max_cm is None or cm <= self.max_cm))


class CatalogueRule(BaseModel):
    """One regulation, typed. `verbatim` is the synopsis sentence and is REQUIRED — the generated
    label is a summary and never a replacement, so the words the law used must always be reachable.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    rule_id: str
    type: RuleType
    verbatim: str = Field(..., min_length=1, description="the synopsis sentence, exactly")

    @model_validator(mode="after")
    def _acts_are_registered(self) -> "CatalogueRule":
        for act in self.conduct:
            if act not in CONDUCT_ACTS:
                raise ValueError(f"conduct: {act!r} is not a registered act — adding one is a "
                                 f"reviewed change that carries the verbatim motivating it")
        return self

    @model_validator(mode="after")
    def _ordered_within_a_slot(self) -> "CatalogueRule":
        """A clause with no condition matches everything, so nothing after it on that slot is ever
        reached. Writing the general case first silently deletes its own exception."""
        seen_open: set = set()
        for c in self.gear:
            if c.slot in seen_open:
                raise ValueError(
                    f"{c.slot.value}: a clause follows one that has no `when`, so it can never be "
                    f"reached — put the narrow case first and the general case last")
            if c.when is None or c.when.is_empty():
                seen_open.add(c.slot)
        return self

    @model_validator(mode="after")
    def _circumstances_are_real(self) -> "CatalogueRule":
        known = {m.value for m in Method} | {s.value for s in _SPEC_SLOTS}
        for w in self.while_:
            if w not in known and w != "alone_in_a_boat":
                raise ValueError(f"while: {w!r} is not a means of fishing or a spec slot")
        # AN EXEMPTION WITH NO CIRCUMSTANCE LIFTS EVERYWHERE. The same argument that refuses
        # `ban: []` — an empty list is what a dropped key also writes — applies here, and the
        # stakes are higher: a lift that should have been narrow and is not is the Babine failure.
        # A LIFT MUST BE SCOPED BY SOMETHING. A PLACE counts: `zp:set_lining.r1` permits set
        # lining in the lakes of Region 6 and 7A and lifts the province-wide ban, and its extents
        # are what narrow it — it needs no circumstance because it names where instead. What is
        # refused is a lift scoped by NOTHING, which applies everywhere and deletes the rule it
        # was meant to narrow. That is the Babine failure, and it cost that river a season.
        if self.exempts and self.gear and not self.while_ and not self.extents:
            if not any(c.when is not None and not c.when.is_empty() for c in self.gear):
                raise ValueError("a gear rule that exempts another must say WHERE or WHEN it "
                                 "lifts it — `extents`, `while`, or a `when` on the clause; a "
                                 "lift scoped by nothing applies everywhere and deletes the rule "
                                 "it was meant to narrow")
        return self

    obligation: Obligation = Obligation.must

    # --- who / what / when -------------------------------------------------
    species: List[str] = Field(default_factory=list)
    species_except: List[str] = Field(default_factory=list)
    angler_class: Optional[AnglerClass] = None
    #: "When no date is listed, the regulations apply ALL YEAR. Start and end dates are
    #: INCLUSIVE." So an empty list is a fact, never "unknown".
    #: HOW YOU MAY FISH. Clauses on DIFFERENT slots are unordered and all apply — they constrain
    #: different things and never compete. Clauses on the SAME slot are ORDERED and FIRST MATCH
    #: WINS, which is the one place an exception can live inside the rule it modifies.
    #:
    #: "It is unlawful to angle with more than one line, EXCEPT a person who is alone in a boat on
    #: a lake may angle with two lines" is ONE printed sentence. Stored as two rules it produced
    #: two records with BYTE-IDENTICAL extents, so the scope ladder — which orders by place — could
    #: not rank them, and something unwritten decided which won. Stored as two bounds on
    #: `lines_per_angler`, the narrow one first, the general one last, there is nothing to rank
    #: and the general case cannot be lost.
    #:
    #: A conditionless clause on a slot is therefore the LAST word on it: anything after it is
    #: unreachable. See `GearClause`. This replaced `allowed`, `barbless`, `hook_count`,
    #: `lure`, `bait`, `max_lines`, `max_flies`, `max_weight_kg`, `min_gap_cm` and `max_gap_mm`,
    #: which are gone from the model and refused on load.
    #: See pipeline/docs/07-gear-representation.md.
    gear: List[GearClause] = Field(default_factory=list)
    #: A RULE THE BOOK ASSERTS BUT DOES NOT PRINT AS ITS OWN CLAUSE, naming the rule it was read
    #: out of. "You may ONLY fish with a set line in lakes of Region 6 and Region 7A" makes two
    #: claims: a permission in those lakes, which is printed, and a ban everywhere else, which is
    #: the word "only" and is printed nowhere. The corpus stored just the permission, so set
    #: lining in a Region 5 lake was unconstrained in the data and unlawful in the book.
    #:
    #: The second half has to be authored, and a reader auditing against the synopsis would
    #: otherwise find a rule with no source text. This says which sentence licensed it. Such a
    #: rule carries its source's `verbatim`, as every other rule split from one sentence does.
    derived_from: Optional[str] = None

    #: THE OTHER HALF OF ONE SENTENCE, BY ID. "You may use a downrigger, PROVIDED the line has a
    #: quick-release" is two rules — one allows the means, one holds the condition — and the only
    #: thing linking them used to be that they shared a `verbatim`. Two unrelated rules share a
    #: verbatim by accident: `z7b:single_barbless_hook.r1` and `z7b:bait.r1` both read "all
    #: streams of Zone B, all year." and are a hook rule and a bait ban. Matching on the string
    #: glues them together.
    #:
    #: A rule id cannot collide by accident, and the corpus already names a parent this way —
    #: a sub-quota carries `within: "trout_quota.r1"`.
    condition_of: Optional[str] = None

    #: WHAT YOU ARE DOING, for a clause that binds only then. Drawn from `Method` members AND
    #: spec-slot names, because a spec slot's name IS a means token — see the note on `Method`.
    #: A LIFT WHOSE CIRCUMSTANCE IS NOT MET IS NOT APPLIED, which is the Babine rule on a new
    #: axis: "dead fin fish when set lining" applied everywhere deleted the province-wide fin
    #: fish ban and a water's bait tile went from "banned" to "no rule at all".
    while_: List[str] = Field(default_factory=list, alias="while")

    #: ACTS — what you must and must not DO, kept apart from gear so a duty is never stored as a
    #: permission. "Set lines must be marked with angler's name, address, and telephone number"
    #: was `{permitted: true, reason: "marked with…"}`: a duty demoted to free text with a grant
    #: invented to house it.
    conduct: List[str] = Field(default_factory=list)

    #: WHEN THIS RULE BINDS, said once — see `When`. This replaces `windows` (210 distinct free
    #: text strings), `windows_are` (a flag that INVERTED the field beside it, the `band`
    #: failure), `from_time`, `to_time` and `weekdays`. All five are now REFUSED (`extra=forbid`):
    #: a rule still spelling its season the old way is sent back, not converted.
    when: Optional[When] = None
    when_open: bool = False

    # --- retention ---------------------------------------------------------
    take: Optional[int] = None
    unlimited: bool = False
    may_target: Optional[bool] = None
    period: Period = Period.daily
    per_daily: Optional[int] = None
    within: Optional[str] = None
    #: WHICH FISH, BY LENGTH, AND HOW MANY — an ORDERED list, FIRST MATCH WINS.
    #:
    #: This replaces reading `over_cm`/`under_cm`/`band` and guessing. Those three said what the
    #: numbers WERE and left what they MEANT to be reconstructed from the fields around them,
    #: and `over_cm` alone meant three different things: the ceiling on a granted fish ("quota 2,
    #: none over 50 cm"), the class a number COUNTS ("only 1 over 40 cm", inside a clause), and
    #: the fish denied outright ("no trout over 50 cm"). Six branches told them apart. Every
    #: consumer that re-derived those branches got them wrong differently — and the one flag that
    #: did carry meaning, `band`, was set backwards on four rules, permitting exactly the fish
    #: they protect.
    #:
    #: A band writes the range and its number, so there is nothing left to infer and the
    #: inversion is not expressible:
    #:
    #:   "Trout daily quota = 2 (none over 50 cm)"   [{max_cm: 50}, {min_cm: 50, take: 0}]
    #:   "1 bull trout over 60 cm"                   [{min_cm: 60}, {max_cm: 60, take: 0}]
    #:   "only 1 over 40 cm" (a clause)              [{min_cm: 40}]
    #:   "20-30 cm only", quota 2                    [{min_cm: 20, max_cm: 30},
    #:                                                {max_cm: 20, take: 0},
    #:                                                {min_cm: 30, take: 0}]
    #:   "none between 70 cm and 100 cm"             [{min_cm: 70, max_cm: 100, take: 0}]
    #:
    #: ORDER SETTLES THE SHARED ENDPOINT. A grant is written before the denial beneath it, so a
    #: fish of exactly 60 cm is granted rather than denied. (The book's "over 60" and "60 cm or
    #: more" differ by one fish and `over_cm`/`under_cm` never stored which was meant; that is a
    #: pre-existing loss and is not invented here.)
    #:
    #: A LENGTH NO BAND COVERS IS NOT SPOKEN ABOUT by this rule — at the top level nothing else
    #: grants it, and inside a `within` clause the parent quota governs it. That is what makes
    #: "northern pike = 4" + "only 1 over 100 cm, none between 70 and 100" come out right: the
    #: old reading capped a 50 cm pike at 1, when the book allows the parent's 4.
    lengths: Optional[List[LengthBand]] = None

    #: `combined` WAS HERE, and it said nothing. It marked 31 rules as "the count is shared
    #: across the species" — and every one of those names more than one fish, as do 1,157 rules
    #: that were never flagged. The flag was a subset of a fact already on the rule, and its
    #: absence was stored as an explicit `false` on 3,379 rules that ARE shared, which reads as a
    #: denial. A consumer that believed it turned Region 2's "Bass: 20" into twenty of each.
    #:
    #: NAMING MORE THAN ONE FISH IS WHAT MAKES A NUMBER SHARED. Nothing in the corpus means "one
    #: each": the four group quotas whose sentence says "each" all say "each day" or "each year".
    #: `aggregation_domain` WAS HERE and said "across what is this number pooled" in prose. Four
    #: of its five uses restated the entry: two were `"province"` on `zp:` rules, which IS
    #: province-wide, and two were the same kokanee rule on the Upper and Lower West Arm entries,
    #: where sitting on both is the sharing. The fifth said "both lakes" and its own verbatim
    #: already says "in the aggregate from both lakes". Nothing could act on any of them.
    #:
    #: The real idea underneath — a pool SHARED across entries, not a number repeated at each —
    #: wants a structural link like `within`, not a sentence. It is not in the corpus today.
    record_retention: bool = False

    # --- shared scoping ----------------------------------------------------
    water: Optional[WaterKind] = None
    origin: Optional[Origin] = None
    method: Optional[Method] = None

    # --- gear / tackle / bait: see `gear` -----------------------------------
    #: The species a bait or tackle rule is ABOUT — "no natural bait when fishing for salmon".
    #: Distinct from `species`, which on those types would mean the ban is scoped to what you may
    #: CATCH, and the tables say it is not: "banned for all angling and for all species". A stream
    #: can carry a salmon bait ban and no other, so the scoping is by TARGET, not by catch.
    when_targeting: List[str] = Field(default_factory=list)
    #: access_permission only: whether the access is granted.
    permitted: Optional[bool] = None

    # --- vessel ------------------------------------------------------------
    aspect: Optional[VesselAspect] = None
    level: Optional[PropulsionLevel] = None
    max_power_kw: Optional[float] = None
    max_kmh: Optional[float] = None

    # --- licensing ---------------------------------------------------------
    document: Optional[Document] = None
    required: bool = True
    water_class: Optional[str] = Field(default=None, pattern="^(I|II)$")
    licence_name: Optional[str] = None
    includes_tributaries: Optional[bool] = None
    allocation: Optional[str] = None
    issuing_jurisdiction: Optional[str] = None
    on_retention: bool = False
    grantor: Optional[str] = None

    # --- relations / provenance -------------------------------------------
    #: Walk the tributaries WITHOUT the mainstem. `z5:spring_stream_closure` is the case:
    #: "No fishing in any stream in the Fraser River Watershed ... EXCEPT the mainstem of the
    #: Fraser River." Binding it with includes_tributaries CLOSES the mainstem the rule exempts.
    tributaries_only: bool = False
    #: Where THIS rule applies, when it differs from the entry's. `None` inherits the entry.
    #: A row routinely binds its rules to different reaches — "no fishing above the falls, bait ban
    #: throughout" — and without this the narrower rule silently widens to the whole water.
    extents: Optional[List[dict]] = None
    #: WATER THIS RULE REACHES BY THE TRIBUTARY WALK AND MUST NOT. Subtracted from this rule's
    #: tributary set only — the entry-wide `Tributaries.excludes` carves every rule in the row,
    #: which is wrong where one rule is ABOUT the water another must not touch.
    #:
    #: The Atnarko is the case. "No Fishing from Tenas Lake to the Atnarko Park campsite" runs up
    #: the SOUTH Atnarko, and the row says "includes tributaries" — so the walk reaches the fork
    #: and climbs the MAIN Atnarko above it, which the sentence never mentions. Its sibling rule
    #: "No Fishing upstream of Tweedsmuir Park, Apr 1-June 30" is precisely about that upper
    #: water, so an entry-wide exclude would delete the rule that belongs there.
    #:
    #: Each entry is an extent, resolved by the same machinery as any other and passed to the walk
    #: as BLOCKED, so it removes the named water *and everything above it* and the walk cannot
    #: descend through it either. `pipeline/atlas/reach/build.py::resolve_carve_outs` has read this
    #: field since the catalogue landed; it was only ever declared on the retired prose model, so
    #: nothing could set it. Curator-filled — the parser never writes one.
    tributary_excludes: List[dict] = Field(default_factory=list)
    exempts: List[Exempts] = Field(default_factory=list)
    standing: bool = False
    authority: Optional[str] = Field(default=None, pattern="^superior$")
    reason: str = ""
    extent_text: str = ""
    #: Locator phrases that could not be bound to a cut-point — "the outlet", "signs 500 m below
    #: the falls". Non-empty forces `needs_review`; curation maps each to a split id.
    #:
    #: The prompt, the batch envelope and the no-registry instructions all told the model to use
    #: this, and it existed only on the RETIRED prose Rule — so a model that followed the
    #: instruction exactly was refused with "extra inputs are not permitted". The reach it could
    #: not express is precisely what must not be dropped silently, which is the whole point of the
    #: field.
    unresolved_locators: List[str] = Field(default_factory=list)
    #: `needs_review` WAS HERE and was exactly `bool(review_reason)`: not one rule in the corpus
    #: carried the flag without a reason, and every producer that set it passed one in the same
    #: call. A flag whose only job is to say that the field beside it is filled in.
    #:
    #: It is still ACCEPTED and dropped, like the old size fields, because the parser writes it.
    #: A reason with no flag is now simply a rule needing review, which is what it always meant —
    #: and that resolves the 7 rules that had one and were not flagged.
    review_reason: str = ""

    # ------------------------------------------------------------------ #
    @property
    def family(self) -> str:
        """The first tier. Types group into families, and the grouping is not decoration — it is
        how the reader's screen is sectioned, and it is the level at which "does this rule compete"
        is *usually* obvious before you look at the dimension."""
        return _FAMILY[self.type]

    @property
    def dimension(self) -> str:
        """WHAT THIS RULE CONTROLS — the second half of the comparison key.

        Without one, every rule of a type on a water shares a single key and they collide. Tackle
        is the sharpest case: a fly-only rule and a barbless rule are ADDITIVE (a fly must be
        barbless), so their dimension is the FACET each constrains, not the type."""
        t = self.type
        if t is RuleType.retention_limit:
            return f"{self.period.value}{'/size' if self.lengths and self.take is None else ''}"
        if t is RuleType.vessel_rule:
            return self.aspect.value if self.aspect else "unspecified"
        if t is RuleType.document_required:
            return self.document.value if self.document else "unspecified"
        if t is RuleType.method_rule:
            if self.method:
                return self.method.value
            return ",".join(sorted({c.slot.value for c in self.gear})) or "unspecified"
        if t is RuleType.tackle_restriction:
            # THE SET OF SLOTS CONSTRAINED. A water's "single barbless hook" displaces the zone's
            # "single barbless hook"; its bare "barbless hook" does not, because displacing would
            # drop the zone's one-point cap, which the water never lifted.
            return ",".join(sorted({c.slot.value for c in self.gear})) or "unspecified"
        if t is RuleType.bait_restriction:
            # A salmon bait ban and a general bait ban are different subjects, not two values of
            # one — a stream can carry both. The TARGET is part of what the rule controls, and so
            # is WHICH bait: a water's "dead fin fish may be used" displaces nothing about roe.
            tgt = ("/" + ",".join(sorted(self.when_targeting))) if self.when_targeting else ""
            named = sorted({m for c in self.gear if c.slot is Slot.bait
                            for m in (c.of or (c.allow or []) + (c.only or []) + (c.ban or []))})
            return f"bait:{','.join(named) or 'any_bait'}{tgt}"
        return t.value

    @model_validator(mode="after")
    def _check(self) -> "CatalogueRule":
        e: List[str] = []
        t = self.type

        # species=[] is an ERROR, not "all" — 115 rules store it while naming a species in
        # their own text, and the blanket default states a trout limit on bass and burbot.
        if t is RuleType.retention_limit and not self.species:
            e.append("retention_limit needs species (use ALL_GAME_FISH for everything)")
        if self.species_except and not self.species:
            e.append("species_except needs a species set to subtract from")
        unknown = (set(self.species) | set(self.species_except)) - KNOWN_SPECIES
        if unknown:
            e.append(f"unknown species code(s): {sorted(unknown)}")

        if t is RuleType.retention_limit:
            # NO DEFAULT. `may_target=True` as a default meant that forgetting the field turned a
            # closure into a catch-and-release permission — the exact 605-rule defect this module
            # exists to prevent, reintroduced as a default value.
            if self.take == 0 and self.may_target is None:
                e.append("take=0 needs an explicit may_target: false = may not fish for it, "
                         "true = fish for it and release it")
            if self.take is not None and self.take < 0:
                e.append("take cannot be negative")
            if self.unlimited and self.take is not None:
                e.append("unlimited and take are mutually exclusive")
            # take=0 does NOT mean closed. may_target is the bit that separates "do not fish for
            # this" from "fish for it, release it", and 15 kokanee rules depend on it.
            if self.take == 0 and self.may_target and self.period is not Period.daily:
                e.append("a release rule is a daily-period rule")
            if self.per_daily is not None and self.period is not Period.possession:
                e.append("per_daily is a possession multiplier")
            # `lengths` IS THE ANSWER, NOT A COPY OF THE OTHER THREE. It used to be checked
            # for equality against over_cm/under_cm/band, which quietly made it subordinate to
            # the fields it exists to replace: it could only ever say what THEY could say.
            # "Wild cutthroat trout daily quota = 2 (none 40 cm or more)" is the case that
            # proves it — 40 cm is on the forbidden side, `over_cm: 40` has no way to record
            # that, and under the equality check the corrected value was rejected as a mismatch.
            #
            # So where `lengths` is written it WINS, and where it is absent it is derived
            # (`_fill_lengths`). The other three are legacy the next parse run removes; until
            # then a rule whose hand-written `lengths` differs from them is a CORRECTION, and
            # the difference is the point.
        else:
            # `lengths` IS NOT ON THIS LIST. `band` was, because a band was only ever a
            # retention thing — but a size on a document rule names WHICH FISH need the stamp
            # ("required to catch and keep rainbow trout over 50 cm"), so `lengths` belongs to
            # both and refusing it here rejected the two rules that prove the distinction.
            for f in ("take", "unlimited", "per_daily", "within"):
                if getattr(self, f) not in (None, False):
                    e.append(f"{f} belongs to retention_limit, not {t.value}")

        # A bait or tackle rule is not scoped to what you may CATCH — "banned for all angling and
        # for all species". 18 corpus rules carry codes leaked from a co-located catch-and-release
        # clause ("Trout/char catch and release, bait ban"), which reads narrower than the law.
        # But a rule may be scoped to what you are FISHING FOR — a stream can carry a salmon bait
        # ban and no other — and that is `when_targeting`.
        if t in (RuleType.bait_restriction, RuleType.tackle_restriction):
            if self.species:
                e.append(f"{t.value} must not carry `species` — a bait or hook rule binds all "
                         f"species you may catch. If the rule applies only when fishing FOR "
                         f"something, use `when_targeting`.")
            unknown = set(self.when_targeting) - KNOWN_SPECIES
            if unknown:
                e.append(f"unknown when_targeting code(s): {sorted(unknown)}")
        elif self.when_targeting:
            e.append("when_targeting belongs to bait_restriction and tackle_restriction")

        if t is RuleType.vessel_rule:
            if self.aspect is None:
                e.append("vessel_rule needs an aspect")
            elif self.aspect is VesselAspect.propulsion and self.level is None:
                e.append("propulsion needs a level")
            elif self.aspect is VesselAspect.speed and self.max_kmh is None \
                    and not self.review_reason:
                e.append("speed needs max_kmh, or a review_reason if the synopsis states none")
            if self.level is PropulsionLevel.power_capped and self.max_power_kw is None:
                e.append("power_capped needs max_power_kw")
        if t is RuleType.document_required and self.document is None:
            e.append("document_required needs a document")
        # A GEAR RULE SAYS WHAT IT CONSTRAINS IN `gear` OR `conduct`. The direction lives inside
        # the clause that carries the subject; with neither, the rule states nothing a reader
        # can act on. An exemption is the one other thing such a rule may be — it lifts a clause
        # stated elsewhere ("EXEMPT from single barbless hooks").
        said = bool(self.gear) or bool(self.conduct) or bool(self.exempts)
        if t in (RuleType.method_rule, RuleType.tackle_restriction,
                 RuleType.bait_restriction, RuleType.handling_rule) and not said:
            e.append(f"{t.value} needs `gear`, `conduct` or `exempts` — it states nothing else")
        if (t is RuleType.access_permission and self.permitted is None and not self.grantor
                and not self.gear and not self.conduct):
            e.append("access_permission needs permitted or a grantor")

        if self.take is not None and self.take < 0:
            e.append("take cannot be negative")
        for f in ("max_kmh", "max_power_kw"):
            v = getattr(self, f)
            if v is not None and v <= 0:
                e.append(f"{f} must be positive")
        if set(self.species) & set(self.species_except):
            e.append("a species cannot be both included and excepted")
        if self.unresolved_locators and not self.review_reason:
            e.append("unresolved_locators is set with no review_reason — a locator nobody "
                     "could bind is exactly what a human has to look at")
        if self.standing and not self.review_reason:
            e.append("a standing rule must be flagged: its extent is unknowable, not merely absent")

        if e:
            raise ValueError(f"{self.rule_id}: " + "; ".join(e))
        return self


# --------------------------------------------------------------------------------------- #
# Label generation — ONE place structure becomes English.
#
# `details` used to be typed beside the number and drifted from it. Everything below is derived,
# so it cannot. The verbatim stays on the rule and is always shown underneath.
# --------------------------------------------------------------------------------------- #

_DOC_WORDS = {
    "basic_licence": "basic angling licence",
    "steelhead_stamp": "Steelhead Conservation Surcharge Stamp",
    "salmon_stamp": "Conservation Surcharge Stamp for salmon",
    "kootenay_rainbow_stamp": "Conservation Surcharge Stamp for Kootenay Lake rainbow trout",
    "shuswap_char_stamp": "Conservation Surcharge Stamp for Shuswap Lake char",
    "shuswap_rainbow_stamp": "Conservation Surcharge Stamp for Shuswap rainbow trout",
    "white_sturgeon_licence": "White Sturgeon Conservation Licence",
    "classified_waters_licence": "Classified Waters Licence",
    "national_park_permit": "National Park Fishing Permit",
    "angling_guide_licence": "angling guide licence",
}

_HP = {7.5: 10, 15.0: 20}          # the synopsis PRINTS these. 7.5 kW computes to 10.06 hp.

_SPECIES_WORDS = {
    # groups — the words the synopsis itself prints
    "ALL_GAME_FISH": "All game fish", "TROUT_CHAR": "Trout and char", "TROUT": "Trout",
    "CHAR": "Char", "WHITEFISH": "Whitefish", "BASS": "Bass", "SALMON": "Salmon",
    "NON_GAME_FISH": "Non-game fish", "ALL_FIN_FISH": "All fish",
    # the CSV's own "General" rows, kept distinct from our groups above
    "SLV": "Char", "WF": "Whitefish", "BS": "Bass", "SA": "Salmon", "SG": "Sturgeon",
    "P": "Perch",
    # trout. GB is Brown Trout (Salmo trutta) in the official table — it was labelled
    # "Gerrard rainbow trout" here, a strain name that appears nowhere in the synopsis.
    "RB": "Rainbow trout", "ST": "Steelhead", "CT": "Cutthroat trout",
    "WCT": "Westslope cutthroat trout", "CCT": "Coastal cutthroat trout",
    "GB": "Brown trout", "GT": "Golden trout",
    # char
    "DV": "Dolly Varden", "BT": "Bull trout", "LT": "Lake trout", "EB": "Brook trout",
    "AC": "Arctic char",
    "SPK": "Splake",
    # whitefish
    "LW": "Lake whitefish", "MW": "Mountain whitefish", "PW": "Pygmy whitefish",
    "RW": "Round whitefish",
    # bass and sunfish
    "LMB": "Largemouth bass", "SMB": "Smallmouth bass", "BCB": "Black crappie",
    "BG": "Bluegill", "PMB": "Pumpkinseed",
    # salmon — federal, not game fish, but named in the synopsis and used by the DFO corpus
    "CH": "Chinook salmon", "CO": "Coho salmon", "SK": "Sockeye salmon",
    "PK": "Pink salmon", "CM": "Chum salmon",
    # everything else on the closed list
    "KO": "Kokanee", "GR": "Arctic grayling", "BB": "Burbot", "WSG": "White sturgeon",
    "GSG": "Green sturgeon", "NP": "Northern pike", "YP": "Yellow perch", "WP": "Walleye",
    "GE": "Goldeye", "IN": "Inconnu", "CRA": "Crayfish", "CP": "Carp",
    # The group word, so a rule naming the whole protected list can render one.
    "PROTECTED_SPECIES": "Protected species",
    # protected — never retainable, but nameable
    "NDC": "Nooksack dace", "SSU": "Salish sucker", "CCL": "Cultus Lake sculpin",
    "ELS": "Enos Lake stickleback", "MLS": "Misty Lake stickleback",
    "PLS": "Paxton Lake stickleback", "VCS": "Vananda Creek stickleback",
    "RMS": "Rocky Mountain sculpin", "SHS": "Shorthead sculpin",
    "VLA": "Vancouver lamprey", "WBL": "Western brook lamprey",
}


#: Individuals worth listing on their own line, by family, in the order a reader expects.
_MENU_FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Trout",            SPECIES_GROUPS["TROUT"]),
    ("Char",             SPECIES_GROUPS["CHAR"]),
    ("Whitefish",        SPECIES_GROUPS["WHITEFISH"] + ("PW", "RW")),
    ("Bass and sunfish", SPECIES_GROUPS["BASS"] + ("BCB", "BG", "PMB")),
    ("Salmon",           SPECIES_GROUPS["SALMON"]),
    ("Other game fish",  ("KO", "GR", "BB", "WSG", "NP", "YP", "WP", "GE", "IN", "CRA")),
    ("Non-game",         ("CP",)),
    ("Protected — never retainable", SPECIES_GROUPS["PROTECTED_SPECIES"]),
)


def species_menu() -> str:
    """The species vocabulary as the parser sees it: the synopsis's own group words first, then
    the individuals, and the rule for choosing between them.

    This REPLACES `species.prompt_menu()`, which listed the official CSV codes. That menu and this
    catalogue had drifted into two different languages: it offered seven codes validation refuses
    and omitted every group the corpus actually uses, TROUT_CHAR among them — the single commonest
    species value in 350 curated rules. A menu is a promise that what it lists will be accepted, so
    it is generated from `KNOWN_SPECIES` and can no longer disagree with it."""
    out = ["**Use the word the regulation itself uses.** If the line says \"Trout/char: 5\", the",
           "species is `TROUT_CHAR` — one claim, not fifteen. Name an individual fish only when the",
           "sentence names that fish (\"Bull trout: release\" -> `BT`). Never expand a group yourself.",
           "",
           "GROUPS — prefer these:"]
    for code in ("ALL_GAME_FISH", "ALL_FIN_FISH", "TROUT_CHAR", "TROUT", "CHAR", "WHITEFISH",
                 "BASS", "SALMON", "NON_GAME_FISH"):
        members = SPECIES_GROUPS[code]
        gloss = {"ALL_GAME_FISH": "everything on the provincial closed list; NOT salmon",
                 "TROUT_CHAR": "the usual quota line — trout rules cover char unless char are excluded",
                 "SALMON": "federal; not part of ALL_GAME_FISH",
                 "NON_GAME_FISH": "carp, suckers, chub and the rest — the spear rule's subject",
                 "ALL_FIN_FISH": ("anything with fins — game fish, salmon AND non-game. Use it when "
                                  "the sentence says \"any fish\" or \"fin fish\", NOT ALL_GAME_FISH, "
                                  "which excludes salmon and every non-game fish")}.get(code, "")
        if not members:
            out.append(f"  `{code}` — {_SPECIES_WORDS[code]}" + (f"  · {gloss}" if gloss else ""))
            continue
        names = ", ".join(_SPECIES_WORDS[m] for m in members[:4])
        more = f", +{len(members) - 4} more" if len(members) > 4 else ""
        out.append(f"  `{code}` — {_SPECIES_WORDS[code]} ({len(members)} spp: {names}{more})"
                   + (f"  · {gloss}" if gloss else ""))
    out.append("")
    out.append("INDIVIDUALS — only when the sentence names one:")
    for family, codes in _MENU_FAMILIES:
        out.append(f"  {family}: " + " · ".join(f"`{c}` {_SPECIES_WORDS[c]}" for c in codes))
    out += ["",
            "Leaving `species` empty is NOT 'all species' — it is refused on a retention rule.",
            "Use `ALL_GAME_FISH` for the game-fish list, or `ALL_FIN_FISH` when the sentence",
            "says \"any fish\" / \"all fin fish\" and so covers salmon and non-game fish too.",
            "Bait and tackle rules take no `species` at all (use",
            "`when_targeting` if the rule only applies when fishing FOR something)."]
    return "\n".join(out)


def species_words(codes: List[str], excepts: List[str] | None = None) -> str:
    if not codes:
        return ""
    names = [_SPECIES_WORDS.get(c, c) for c in codes]
    out = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    if excepts:
        ex = [_SPECIES_WORDS.get(c, c).lower() for c in excepts]
        out += " other than " + (ex[0] if len(ex) == 1 else ", ".join(ex[:-1]) + " and " + ex[-1])
    return out


def _gear_words(r: CatalogueRule) -> str:
    """`gear` and `conduct`, in words. ONE place, so a reader and a renderer cannot disagree.

    The direction is never inferred: it is the key the clause used (`allow` / `only` / `ban` /
    `must_be`) or the act token's own name. That is what makes the label impossible to invert —
    `{barbless: true, required: false}` printed "barbless" and meant the opposite, because the
    words came from one field and the polarity from another.
    """
    UNIT = {"hook_gap_mm": "mm", "light_to_hook_mm": "mm",
            "weight_per_line_kg": "kg", "bait_possession_kg": "kg"}
    bits = []
    gear = list(r.gear)

    def plain(c: GearClause) -> bool:
        return c.when is None and not c.unless and not c.of and not c.except_

    # THE BOOK'S OWN WORDS FOR ITS COMMONEST SHAPES. 800 waters print "Single barbless hook" and
    # "Bait ban"; spelled clause by clause those came out "barbless only; at most 1 point per
    # hook" and "no any bait", which are right and which nobody says.
    barb = next((c for c in gear if c.slot is Slot.barb and c.only == ["barbless"] and plain(c)), None)
    one = next((c for c in gear if c.slot is Slot.points_per_hook and c.max == 1
                and c.min is None and plain(c)), None)
    if barb or one:
        bits.append(f"{'single ' if one else ''}{'barbless ' if barb else ''}hook")
        gear = [c for c in gear if c is not barb and c is not one]
    for c in gear:
        name = c.slot.value.replace("_", " ")
        if c.slot is Slot.bait and c.ban == ["any_bait"] and plain(c):
            bits.append("bait ban")
        elif c.slot is Slot.hook_gap_mm and c.max is not None and c.min is None and plain(c):
            bits.append(f"no hook more than {c.max:g} mm from point to shank")
        elif c.allow is not None:
            bits.append(f"{', '.join(c.allow).replace('_', ' ')} may be used")
        elif c.only is not None:
            bits.append(f"{', '.join(c.only).replace('_', ' ')} only")
        elif c.ban is not None:
            got = f"no {', '.join(c.ban).replace('_', ' ')}"
            if c.except_:
                got += f" other than {', '.join(c.except_).replace('_', ' ')}"
            bits.append(got)
        elif c.must_be:
            bits.append(f"{name} must be {', '.join(c.must_be).replace('_', ' ')}")
        else:
            u = UNIT.get(c.slot.value, "")
            for kind, v in (("at most", c.max), ("at least", c.min)):
                if v is None:
                    continue
                if u:
                    bits.append(f"{kind} {v:g}{u} {name.removesuffix(' ' + u)}")
                else:
                    # "at most 1 points per hook" — the slot name is plural because it names a
                    # measurand, and a bound of one reads as a count.
                    head, _, tail = name.partition(" per ")
                    one = ({"flies": "fly"}.get(head, head.removesuffix("s"))
                           if v == 1 else head)
                    bits.append(f"{kind} {v:g} {one}" + (f" per {tail}" if tail else ""))
        if c.when is not None and not c.when.is_empty():
            w = [x for x in (getattr(c.when.water, "value", None),
                             getattr(c.when.angler, "value", None)) if x]
            if w:
                bits[-1] += " (" + ", ".join(x.replace("_", " ") for x in w) + ")"
    for act in r.conduct:
        bits.append(CONDUCT_ACTS.get(act, act.replace("_", " ")))
    out = "; ".join(bits)
    out = out[:1].upper() + out[1:]
    if r.when_targeting and out:
        out += f" when fishing for {species_words(r.when_targeting).lower()}"
    if r.while_ and out:
        out += " — while " + " or ".join(w.replace("_", " ") for w in r.while_)
    return out


def _dates(r: CatalogueRule) -> str:
    """The season, in words. There is no "except …" branch any more: `windows_are: excepts` stored
    the days a rule did NOT hold and `When` stores the days it does, so the phrase is always the
    same shape and the reader is never asked to invert it."""
    if not r.when or not r.when.dates:
        return ""
    return ", " + " and ".join(d.words() for d in r.when.dates)


def _who(r: CatalogueRule) -> str:
    """WHO the rule applies to. Rendered on every type, not just access_permission.

    Left out, `zp:basic_licence` prints "A basic angling licence is required" and "A basic angling
    licence is not required" side by side with nothing to tell them apart — and three of the five
    rules would tell an unqualified reader they need no licence."""
    ac = r.angler_class
    if not ac or ac.is_empty():
        return ""
    bits = []
    if ac.age:
        bits.append("under 16" if ac.age == "under_16" else "16 and over")
    if ac.guided is not None:
        bits.append("guided" if ac.guided else "non-guided")
    if ac.residency:
        bits.append({"resident": "B.C. residents",
                     "non_resident": "non-residents",
                     "non_resident_alien": "non-resident aliens"}[ac.residency.value])
    if ac.status:
        bits.append({"indian_bc_resident": "Indians resident in B.C.",
                     "metis": "Metis anglers",
                     "disabled": "disabled anglers"}[ac.status])
    return " — for " + ", ".join(bits)


def _scope(r: CatalogueRule, taking: bool = True) -> str:
    """`taking` distinguishes "2 from streams" (a retention limit) from "no fishing in streams"
    (a prohibition). Same field, opposite preposition, and the wrong one reads as nonsense."""
    bits = []
    if r.water:
        bits.append(f"{'from' if taking else 'in'} {r.water.value}s")
    if r.origin:
        bits.append(f"{r.origin.value} only")
    if r.method:
        bits.append("taken on a set line" if r.method is Method.set_lining
                    else f"taken by {r.method.value.replace('_', ' ')}")
    if r.when and r.when.weekdays:
        bits.append("on " + " and ".join(f"{d}s" for d in r.when.weekdays))
    if r.when and r.when.hours:
        bits.append(r.when.hours.words())
    if r.when_open:
        bits.append("where open")
    return (", " + ", ".join(bits)) if bits else ""


def _size(r: CatalogueRule) -> str:
    """The size limit, in words, READ OFF `lengths`.

    POLARITY USED TO BE THE WHOLE JOB HERE. "not more than 1 over 50 cm" ALLOWS one big fish;
    "none over 50 cm" FORBIDS them — and `over_cm` carried both, so which was meant had to be
    worked out from `take`, `within` and `period`. This function held one copy of that reasoning
    and a migration shim held the other; two copies of a six-way branch is how "1 bull
    trout over 60 cm" got rendered "none over 60 cm", inverting the rule on the fish it exists
    to protect.

    `lengths` has already decided. What is left is reading a shape and naming it: a range with a
    zero is fish going back, a range without one is fish you may keep.
    """
    bands = list(r.lengths or [])
    if not bands:
        return ""
    denied = [b for b in bands if b.take == 0]
    granted = [b for b in bands if b.take != 0]

    # A HOLE protects the middle. `r6:bennett_lake` is "only 1 over 90 cm, NONE between 60 and
    # 90" — two facts, and naming only the hole swallows the number.
    hole = next((b for b in denied if b.min_cm is not None and b.max_cm is not None), None)
    if hole:
        between = f"none between {hole.min_cm} cm and {hole.max_cm} cm"
        return f" ({between})" if r.take is None else f" (no more than {r.take}, {between})"

    # A WINDOW is the opposite: the middle is the only part you may keep. A window inside a
    # parent still carries its own COUNT — `z7a` is "not more than 1 bull trout, 30-50 cm", and
    # dropping the 1 turns a one-fish allowance into an unlimited one.
    win = next((b for b in granted if b.min_cm is not None and b.max_cm is not None), None)
    if win:
        slot = f"{win.min_cm}\u2013{win.max_cm} cm only"
        return f" (no more than {r.take}, {slot})" if (r.within and r.take) else f" ({slot})"

    if not granted:
        # NOTHING IS GRANTED, so the range names the fish that go back and nothing else is
        # claimed. "no trout over 50 cm" says nothing about a 40 cm trout.
        #
        # THE PARENTHESES ARE ABOUT THE SENTENCE, NOT THE SIZE. An explicit `take: 0` has
        # already put "release all" in front of this, so the bound appends bare — "release all
        # over 50 cm". With no take there is no quota phrase to append to, and the bare form
        # reads as a description of a fish rather than a prohibition: "Trout under 25 cm"
        # instead of "Trout (none under 25 cm)". That is the one thing `lengths` cannot say,
        # because both spellings of the prohibition make the same band.
        b = denied[0]
        end, cm = ("over", b.min_cm) if b.min_cm is not None else ("under", b.max_cm)
        return f" {end} {cm} cm" if r.take == 0 else f" (none {end} {cm} cm)"

    g = granted[0]
    if not denied:
        # A GRANT WITH NO DENIAL BENEATH IT COUNTS A SIZE CLASS rather than bounding one.
        # "Rainbow trout: 5 over 50 cm" (annual) counts the big ones and does not forbid keeping
        # smaller ones, which the daily quota governs; printing "(none under 50 cm)" put a
        # minimum size on the page that neither the Shuswap nor the Kootenay chapter states.
        if g.min_cm is not None:
            n = g.take if g.take is not None else r.take
            return (f" (no more than {n} over {g.min_cm} cm)" if r.within and n
                    else f" over {g.min_cm} cm")
        return f" under {g.max_cm} cm"

    # A GRANT WITH A DENIAL BENEATH IT is bounded: the fish you keep must lie on this side.
    # ASYMMETRIC ON PURPOSE — a maximum caps how big a kept fish may be, a minimum is a floor on
    # every fish kept, and "no more than 1 under 60 cm" would say the opposite of
    # `r2:cultus_lake`'s "1 bull trout over 60 cm", where the fish you keep must BE over 60.
    if g.max_cm is not None:
        return f" (none over {g.max_cm} cm)"
    n = g.take if g.take is not None else r.take
    return (f" (no more than {n}, none under {g.min_cm} cm)" if r.within and n
            else f" (none under {g.min_cm} cm)")


def _where(r: CatalogueRule) -> str:
    """The extent, appended. §5: generation is lossless ONLY where the extent survives alongside.
    533 of 644 closures have a label of exactly "No fishing" — everything distinguishing one from
    another is in the reach."""
    return f" — {r.extent_text}" if r.extent_text else ""


def _because(r: CatalogueRule) -> str:
    """The condition that has no field of its own, appended.

    `reason` is the catalogue's slot for a qualifier the schema cannot hold structurally — "not
    required until reopened to steelhead fishing", "alone in a boat". Five rules carried one and
    NOTHING rendered it: it reached `conditions` in the bundle and stopped there, so a reader was
    shown a licence requirement without the condition that lifts it.

    It is appended, never substituted. A reason narrows the sentence in front of it; printed on
    its own it would read as the whole rule.
    """
    return f" — {r.reason}" if r.reason else ""


def label(r: CatalogueRule) -> str:
    """The line a reader sees. Verbatim is always available underneath."""
    # GEAR AND CONDUCT ARE READ FIRST, FOR EVERY TYPE. A converted rule has no `method`,
    # `permitted`, `allowed`, `required`, `barbless` or `max_lines` — the direction now lives in
    # the clause that carries the subject, which is the whole point of the field. Wired into one
    # type's branch instead, every OTHER type fell through to printing its bare verbatim: a
    # "You must not:" fragment then reads as a permission, which is the inversion this replaced.
    if r.gear or r.conduct:
        said = _gear_words(r)
        if said:
            # `_scope` must not re-state a method a clause already named — "no netting, taken by
            # netting". Where gear says it, gear is the one that says it.
            named = {m for c in r.gear for m in ((c.allow or []) + (c.only or []) + (c.ban or []))}
            bare = r.model_copy(update={"method": None}) if (
                r.method and r.method.value in named) else r
            return said + _scope(bare) + _dates(bare) + _where(bare) + _because(bare)
    t = r.type
    sp = species_words(r.species, r.species_except)

    if t is RuleType.retention_limit:
        # BRANCH ON may_target FIRST. take=0 alone is ambiguous, and reading it as "release all"
        # turns all 605 "No fishing" rules into a catch-and-release PERMISSION.
        if r.take == 0 and r.may_target is False:
            if sp == "All game fish" and r.species_except:
                head = f"No fishing except for {species_words(r.species_except).lower()}"
            elif sp == "All game fish":
                head = "No fishing"
            else:
                head = f"No fishing for {sp.lower()}"
            if r.water:                       # "in streams" reads as part of the phrase, not an aside
                head += f" in {r.water.value}s"
            if r.method:                      # "No fishing by spear fishing", not ", by spear fishing"
                head += f" by {r.method.value.replace('_', ' ')}"
            rest = _scope(r.model_copy(update={"water": None, "method": None}), taking=False)
            return head + rest + _dates(r) + _where(r) + _because(r)
        if r.take == 0:
            # Both arms of a conditional here produced the SAME string — it read the size fields
            # and did nothing with them. `_size` appends the bound afterwards either way.
            head = f"{sp} — release all"
        elif r.unlimited:
            head = f"{sp} — no limit"
        elif r.take is not None:
            noun = {Period.daily: "per day", Period.possession: "in possession",
                    Period.annual: "per licence year", Period.monthly: "per month"}[r.period]
            if r.within and r.lengths:
                head = sp                                # the size phrase carries the count
            else:
                head = f"{sp} — {r.take} {noun}"
                if len(expand_species(list(r.species or []))) > 1:
                    head += ", all species combined"
        elif r.per_daily is not None:
            head = f"{sp or 'All game fish'} — possession quota is {r.per_daily} daily quota" \
                   + ("s" if r.per_daily != 1 else "")
        elif r.lengths:
            head = sp                      # a size gate with no count: the region supplies it
        else:
            return r.verbatim              # nothing numeric to generate from
        out = head + _size(r) + _who(r) + _scope(r) + _dates(r) + _where(r) + _because(r)
        if r.record_retention:
            out += " — record your retention on your licence immediately"
        return out

    if t in (RuleType.bait_restriction, RuleType.tackle_restriction, RuleType.method_rule):
        # Everything these types say is in `gear`/`conduct`, rendered above. What reaches here
        # is an exemption with no clause of its own, and the sentence IS the rule.
        return r.verbatim

    if t is RuleType.vessel_rule:
        if r.aspect is VesselAspect.speed:
            head = f"Speed restriction ({r.max_kmh:g} km/h)" if r.max_kmh else "Speed restriction"
        elif r.aspect is VesselAspect.towing:
            head = "No towing"
        else:
            kw = r.max_power_kw
            head = {PropulsionLevel.none: "No vessels",
                    PropulsionLevel.unpowered: "No powered boats",
                    PropulsionLevel.electric_only:
                        f"Electric motor only (max {kw:g} kW)" if kw
                        else "Electric motor only (max 7.5 kW)",
                    PropulsionLevel.power_capped:
                        f"Engine power restriction {kw:g} kW ({_HP.get(kw, '')} hp)" if kw
                        else "Engine power restriction"}[r.level]
        return head + _dates(r) + _where(r) + _because(r)

    if t is RuleType.angling_from_vessel_prohibited:
        return "No angling from boats" + _scope(r) + _dates(r)

    if t is RuleType.document_required:
        doc = (r.licence_name + " classified licence") if r.licence_name else \
            _DOC_WORDS.get(r.document.value, r.document.value.replace("_", " "))
        # "A angling guide licence". A fixed article is wrong for the one document in
        # `_DOC_WORDS` that starts with a vowel, and for any classified water whose name does.
        art = "An" if doc[:1].lower() in "aeiou" else "A"
        head = (f"{art} {doc} is required" if r.required else f"{art} {doc} is not required")
        if r.water_class:
            head = f"Class {r.water_class} water — " + head[0].lower() + head[1:]
        if r.on_retention:
            head += ", only if you keep the fish"
        # A STAMP MAY EXCLUDE A FISH. "Required to keep a salmon of any legal size or species
        # (OTHER THAN KOKANEE) from non-tidal waters" — this branch never read `species_except`,
        # so the one fish the stamp does not cover was dropped from the only sentence about it.
        if r.species_except:
            head += " (not " + species_words(r.species_except).lower() + ")"
        if r.issuing_jurisdiction:
            head += f"; a {r.issuing_jurisdiction} licence is also valid"
        if r.allocation:
            head += " (" + r.allocation.replace("_", " ") + ")"
        # `taking=False`: you do not need a licence FROM a stream, you need one IN one. Its own
        # docstring says the wrong preposition reads as nonsense, and it did — "A Classified
        # Waters Licence is required, from streams".
        return head + _who(r) + _scope(r, taking=False) + _dates(r) + _where(r) + _because(r)

    if t is RuleType.access_permission:
        # ONE WHO-BUILDER. This branch had its own, and the two disagreed: it spelled
        # `non_resident_alien` as "non-resident-aliens" where `_who` spells it "non-resident
        # aliens", and seven live rules printed the hyphenated form. Worse, it tested
        # `guided is False` only, so a rule scoped to GUIDED anglers rendered no marker at all
        # and read as applying to everyone — and a guided non-resident alien and a non-guided
        # one buy different licences, which is the whole reason the axis exists.
        subject = (_who(r)[len(" — for "):] if _who(r) else "anglers")
        if r.grantor:
            return f"Permission of the {r.grantor} is required" + _dates(r)
        head = f"Angling prohibited for {subject}" if r.permitted is False \
            else f"{subject[:1].upper() + subject[1:]} may fish here"
        return head + _scope(r, taking=False) + _dates(r) + _where(r) + _because(r)

    if r.required is False and t in _PROHIBITABLE:
        return _quoted_prohibition(r)

    # navigation_duty, handling_rule (required), hazard, advisory, program_membership, facility
    return r.verbatim


def _quoted_prohibition(r: "CatalogueRule", quote_is_whole: bool = False) -> str:
    """"Do not …" around a quote whose own sentence carried the prohibition.

        THE PROHIBITION IS IN THE HEADING, NOT THE BULLET. The synopsis prints these under
    The synopsis prints these under "It Is Unlawful To…" and each bullet is a fragment —
    "Waste the fish you catch." Quoting the bullet is FAITHFUL, and every gate passes it: the
    words are in the passage, the passage is the batch's own text. But these types have
    nothing to generate a label from, so the quote BECAME the label, and the app told anglers
    to waste their catch. Eleven rules read that way.

    So the prohibition lives in the DATA — `required: false` — and is rendered here.
    """
    body = r.verbatim.strip().rstrip(".")
    # `quote_is_whole`: the caller is the duty branch, where the quoted sentence already says
    # everything — its `reason` is there to mark it as a duty, not to add to it. Appending it
    # printed "…during a No Fishing period — gear in the water during a No Fishing period".
    tail = _dates(r) + _where(r) + ("" if quote_is_whole else _because(r))
    return "Do not " + body[0].lower() + body[1:] + tail


# --------------------------------------------------------------------------------------- #
# Entries
# --------------------------------------------------------------------------------------- #

class CatalogueEntry(BaseModel):
    """One row of the synopsis — a water, a zone, or the province — and its typed rules.

    `regs_verbatim` is the whole printed passage; every rule's `verbatim` must be a contiguous
    substring of it. That chain of custody is what caught an invented 50 cm sub-limit on the first
    sample, and what refused a Classified Waters entry whose text mentioned Kootenay Class II
    waters that no rule covered.
    """
    model_config = ConfigDict(frozen=True)

    entry_id: str
    name: str
    display_name: str = ""
    region: str = ""
    scope_note: str = ""
    regs_verbatim: str = Field(..., min_length=1)
    source_pages: List[int] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    matched: List[str] = Field(default_factory=list)
    extents: List[dict] = Field(default_factory=list)
    includes_tributaries: Optional[bool] = None
    rules: List[CatalogueRule]

    @model_validator(mode="after")
    def _chain_of_custody(self) -> "CatalogueEntry":
        e: List[str] = []
        seen: set[str] = set()
        haystack = squash(self.regs_verbatim)
        for r in self.rules:
            if r.rule_id in seen:
                e.append(f"duplicate rule_id {r.rule_id!r}")
            seen.add(r.rule_id)
            needle = squash(r.verbatim)
            if needle not in haystack:
                e.append(f"{r.rule_id}: verbatim is not a contiguous substring of regs_verbatim")
        if not self.rules:
            e.append("an entry with no rules says nothing")
        if e:
            raise ValueError(f"{self.entry_id}: " + "; ".join(e))
        return self


class CatalogueFile(BaseModel):
    model_config = ConfigDict(frozen=True)
    region: str
    entries: List[CatalogueEntry]

    @model_validator(mode="after")
    def _unique_ids(self) -> "CatalogueFile":
        ids = [x.entry_id for x in self.entries]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate entry_id(s): {sorted(dupes)}")
        return self
