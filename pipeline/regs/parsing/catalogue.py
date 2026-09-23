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

from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict, Field, model_validator


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


class Bait(str, Enum):
    any = "any"
    fin_fish = "fin_fish"
    dead_fin_fish = "dead_fin_fish"
    invertebrate = "invertebrate"
    roe = "roe"


class Lure(str, Enum):
    """NEVER merge these two. 'Artificial fly' constrains the lure; 'fly fishing' additionally
    forbids floats and sinkers on the line. `r1:campbell_river@1-10` uses both on adjacent reaches
    with different windows."""
    artificial_fly = "artificial_fly"
    fly_fishing = "fly_fishing"


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


class WindowsAre(str, Enum):
    """`applies` is the default. `excepts` marks a window that says when the rule does NOT apply —
    `r4:kootenay_lake_upper_west_arm` stores [Apr 1-3, Jul 1-2] on a catch-and-release rule, and
    read as `applies` it says kokanee may be kept the other 360 days."""
    applies = "applies"
    excepts = "excepts"


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


def lengths_from_bounds(r) -> Optional[List["LengthBand"]]:
    """`over_cm`/`under_cm`/`band` -> `lengths`. The ONLY place those three are interpreted.

    This is the migration, and it is also the proof: run against the corpus it reproduces the
    old reading for 264 of the 270 rules carrying a size, length by length from 1 to 400 cm. The
    six it changes are Bennett Lake's "only 1 over 100 cm, none between 70 cm and 100 cm" under
    a parent quota of 4, where the old reading capped a 50 cm pike at 1 and the book allows 4.
    """
    o, u, t = r.over_cm, r.under_cm, r.take
    denies = t == 0 or t is None
    if not o and not u:
        return None
    # A SIZE ON A RULE THAT IS NOT ABOUT KEEPING SAYS WHICH FISH THE RULE IS ABOUT. "Conservation
    # Surcharge Stamp required to catch and keep rainbow trout over 50 cm" limits nobody's fish —
    # it says the stamp is needed for the big ones. Read with the retention branches below it came
    # out as "keep zero rainbow trout over 50 cm", which is the sentence inverted: a paperwork rule
    # turned into a ban. A range with no `take`, on a rule with no `take`, says exactly what is
    # meant — these are the fish this rule speaks about, and it sets no number.
    if getattr(r, "type", None) not in (RuleType.retention_limit, None):
        return [LengthBand(min_cm=o) if o else LengthBand(max_cm=u)]
    if o and u:
        if r.band:
            # A HOLE. The band names the fish you may NOT keep, and that is the claim. Where the
            # rule also carries a number the number belongs to the piece ABOVE the hole; the
            # piece below is left unspoken, because the parent quota governs it.
            out = [LengthBand(min_cm=u, max_cm=o, take=0)]
            return out + ([LengthBand(min_cm=o)] if t is not None else [])
        # A WINDOW: keep only inside it, and none outside it either way.
        return [LengthBand(min_cm=u, max_cm=o), LengthBand(max_cm=u, take=0),
                LengthBand(min_cm=o, take=0)]
    if o:
        # `over_cm` IS THE OVERLOADED ONE — it bounds the fish granted, unless the number COUNTS
        # the big ones instead, which is what a clause and an annual ceiling both do.
        if denies:
            return [LengthBand(min_cm=o, take=0)]
        if r.within or r.period is Period.annual:
            return [LengthBand(min_cm=o)]
        return [LengthBand(max_cm=o), LengthBand(min_cm=o, take=0)]
    # `under_cm` IS ALWAYS A FLOOR. "1 bull trout over 60 cm" and "none under 60 cm" are one
    # sentence said two ways and are stored identically. THE FLOOR IS ABSOLUTE even inside a
    # clause: "no more than 1 char (none under 60 cm)" forbids a 50 cm char outright rather than
    # handing it back to the parent quota.
    if denies:
        return [LengthBand(max_cm=u, take=0)]
    # AN ANNUAL QUOTA COUNTS A SIZE CLASS AND FORBIDS NOTHING. "Rainbow trout: 5 over 50 cm" is
    # five big ones per licence year and says nothing whatever about a 30 cm rainbow, which the
    # DAILY quota governs. Adding the floor beneath it fabricated an annual ban on every small
    # rainbow in Shuswap and Kootenay Lake.
    if r.period is Period.annual:
        return [LengthBand(min_cm=u)]
    return [LengthBand(min_cm=u), LengthBand(max_cm=u, take=0)]


#: Month name -> number, and the last day of each. February is 29 ON PURPOSE: the book says
#: "February", which includes the 29th in the years it exists, and 28 would quietly shorten it.
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_LAST_DAY = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
             7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


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
    model_config = ConfigDict(frozen=True, extra="forbid")

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
    model_config = ConfigDict(frozen=True, extra="forbid")

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


class LengthBand(BaseModel):
    """ONE RANGE OF FISH LENGTHS, AND HOW MANY OF THEM YOU MAY KEEP.

    `min_cm` and `max_cm` are INCLUSIVE, and null is open at that end. `take` is how many of
    THESE you may keep; omitted, the rule's own `take` applies to them.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

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
    model_config = ConfigDict(frozen=True, extra="forbid")

    rule_id: str
    type: RuleType
    verbatim: str = Field(..., min_length=1, description="the synopsis sentence, exactly")

    @model_validator(mode="before")
    @classmethod
    def _adopt_lengths(cls, v):
        """ACCEPT THE OLD SIZE FIELDS, KEEP THE NEW ONE. `over_cm`, `under_cm` and `band` are no
        longer fields on this model: `lengths` says everything they said and says it once. They
        are still ACCEPTED here, converted, and dropped — because the parser still writes them
        until a run with the new prompt lands, and the DFO salmon feed builds its rules in code
        from scraped rows. Refusing them would make `lengths` something only re-parsed data
        could have, which is the opposite of the point.

        This shim is the whole migration. When the parser emits `lengths` directly it goes, and
        `lengths_from_bounds` with it."""
        if not isinstance(v, dict):
            return v
        # `needs_review` is accepted and dropped: it only ever meant "review_reason is filled".
        if "needs_review" in v:
            flag, why = v.get("needs_review"), (v.get("review_reason") or "").strip()
            if flag and not why:
                raise ValueError("needs_review is set with no review_reason to say why")
            v = {k: x for k, x in v.items() if k != "needs_review"}
        old = {k: v.get(k) for k in ("over_cm", "under_cm", "band") if k in v}
        if not old:
            return v
        v = {k: x for k, x in v.items() if k not in ("over_cm", "under_cm", "band")}
        if v.get("lengths") is not None:
            return v
        # A BAND NEEDS BOTH ENDS. Dropping the field took this check with it, and a malformed
        # band was then silently REINTERPRETED as a ceiling rather than refused — the parser
        # would have been told its mistake was fine. `lengths` cannot express the error at all,
        # which is the point, but the old spelling can still arrive and must still be caught.
        if old.get("band") and not (old.get("over_cm") and old.get("under_cm")):
            raise ValueError("band needs both over_cm and under_cm")
        v = dict(v, over_cm=old.get("over_cm"), under_cm=old.get("under_cm"),
                 band=old.get("band") or False)
        if not (v.get("over_cm") or v.get("under_cm")):
            return {k: x for k, x in v.items() if k not in ("over_cm", "under_cm", "band")}
        # AN IMPOSSIBLE SLOT IS REPORTED IN THE WORDS THE CURATOR WROTE. Deriving first would
        # refuse it as "min_cm 90 >= max_cm 60", naming two fields that are not in the file;
        # `_check` below says "under_cm 90 >= over_cm 60", which is the line to go and fix.
        if v.get("over_cm") and v.get("under_cm") and v["under_cm"] >= v["over_cm"]:
            raise ValueError(f"under_cm {v['under_cm']} >= over_cm {v['over_cm']} is an "
                             f"impossible slot")
        got = lengths_from_bounds(SimpleNamespace(
            over_cm=v.get("over_cm"), under_cm=v.get("under_cm"), band=v.get("band") or False,
            take=v.get("take"), within=v.get("within"), type=v.get("type"),
            period=Period(v.get("period") or "daily")))
        v = {k: x for k, x in v.items() if k not in ("over_cm", "under_cm", "band")}
        return dict(v, lengths=[b.model_dump(exclude_none=True) for b in got]) if got else v
    obligation: Obligation = Obligation.must

    # --- who / what / when -------------------------------------------------
    species: List[str] = Field(default_factory=list)
    species_except: List[str] = Field(default_factory=list)
    angler_class: Optional[AnglerClass] = None
    #: "When no date is listed, the regulations apply ALL YEAR. Start and end dates are
    #: INCLUSIVE." So an empty list is a fact, never "unknown".
    windows: List[str] = Field(default_factory=list)
    windows_are: WindowsAre = WindowsAre.applies
    weekdays: List[str] = Field(default_factory=list)
    from_time: Optional[str] = None
    to_time: Optional[str] = None
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

    # --- gear / tackle / bait ---------------------------------------------
    bait: Optional[Bait] = None
    allowed: Optional[bool] = None
    #: The species a bait or tackle rule is ABOUT — "no natural bait when fishing for salmon".
    #: Distinct from `species`, which on those types would mean the ban is scoped to what you may
    #: CATCH, and the tables say it is not: "banned for all angling and for all species". A stream
    #: can carry a salmon bait ban and no other, so the scoping is by TARGET, not by catch.
    when_targeting: List[str] = Field(default_factory=list)
    barbless: Optional[bool] = None
    hook_count: Optional[int] = None
    max_gap_mm: Optional[int] = None
    min_gap_cm: Optional[int] = None
    lure: Optional[Lure] = None
    max_flies: Optional[int] = None
    max_weight_kg: Optional[float] = None
    max_lines: Optional[int] = None
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
            return self.method.value if self.method else "unspecified"
        if t is RuleType.tackle_restriction:
            for facet in ("lure", "barbless", "hook_count", "max_gap_mm",
                          "max_flies", "max_weight_kg", "max_lines"):
                if getattr(self, facet) is not None:
                    return facet
            return "unspecified"
        if t is RuleType.bait_restriction:
            # A salmon bait ban and a general bait ban are different subjects, not two values of
            # one — a stream can carry both. The TARGET is part of what the rule controls.
            tgt = ("/" + ",".join(sorted(self.when_targeting))) if self.when_targeting else ""
            return f"bait:{self.bait.value if self.bait else 'any'}{tgt}"
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
        if t is RuleType.method_rule:
            if self.method is None:
                e.append("method_rule needs a method")
            if self.permitted is None:
                e.append("method_rule needs permitted — None read as 'prohibited' here and as "
                         "'permitted' in access_permission, from the same absent value")
        if t is RuleType.access_permission and self.permitted is None and not self.grantor:
            e.append("access_permission needs permitted or a grantor")
        if t is RuleType.bait_restriction and self.allowed is None:
            e.append("bait_restriction needs allowed (a permission is a rule too)")

        for f in ("take", "hook_count", "max_lines", "max_gap_mm"):
            v = getattr(self, f)
            if v is not None and v < 0:
                e.append(f"{f} cannot be negative")
        for f in ("max_kmh", "max_power_kw", "max_weight_kg"):
            v = getattr(self, f)
            if v is not None and v <= 0:
                e.append(f"{f} must be positive")
        if set(self.species) & set(self.species_except):
            e.append("a species cannot be both included and excepted")
        if self.from_time and not self.to_time or self.to_time and not self.from_time:
            e.append("a time-of-day window needs both ends, or it renders as no window at all")
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


def _dates(r: CatalogueRule) -> str:
    if not r.windows:
        return ""
    joined = " and ".join(r.windows)
    return f", except {joined}" if r.windows_are is WindowsAre.excepts else f", {joined}"


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
    if r.weekdays:
        bits.append("on " + " and ".join(f"{d}s" for d in r.weekdays))
    if r.from_time and r.to_time:
        bits.append(f"{r.from_time} to {r.to_time}")
    if r.when_open:
        bits.append("where open")
    return (", " + ", ".join(bits)) if bits else ""


def _size(r: CatalogueRule) -> str:
    """The size limit, in words, READ OFF `lengths`.

    POLARITY USED TO BE THE WHOLE JOB HERE. "not more than 1 over 50 cm" ALLOWS one big fish;
    "none over 50 cm" FORBIDS them — and `over_cm` carried both, so which was meant had to be
    worked out from `take`, `within` and `period`. This function held one copy of that reasoning
    and `lengths_from_bounds` holds the other; two copies of a six-way branch is how "1 bull
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

    if t is RuleType.bait_restriction:
        what = {Bait.any: "Bait", Bait.fin_fish: "Fin fish", Bait.dead_fin_fish: "Dead fin fish",
                Bait.invertebrate: "Freshwater invertebrates", Bait.roe: "Roe"}[r.bait or Bait.any]
        head = f"{what} may be used" if r.allowed else (
            "Bait ban" if (r.bait or Bait.any) is Bait.any else f"{what} may not be used as bait")
        if r.when_targeting:
            head += f" when fishing for {species_words(r.when_targeting).lower()}"
        return head + _scope(r) + _dates(r) + _where(r) + _because(r)

    if t is RuleType.tackle_restriction:
        if r.lure:
            head = ("Artificial fly only" if r.lure is Lure.artificial_fly else "Fly fishing only")
        elif r.max_lines is not None:
            head = ("Unlimited rods" if r.max_lines == 0
                    else f"{r.max_lines} line{'s' if r.max_lines != 1 else ''} per angler")
        elif r.max_weight_kg is not None:
            head = f"No more than {r.max_weight_kg:g} kg of weight on the line"
        elif r.max_flies is not None:
            head = f"No more than {r.max_flies} artificial fly on the line"
        elif r.hook_count is None and r.barbless is None and r.max_gap_mm is None:
            return r.verbatim              # no facet to generate from — the sentence IS the rule
        else:
            head = (f"{'Single ' if r.hook_count == 1 else ''}"
                    f"{'barbless ' if r.barbless else ''}hook").strip().capitalize()
            if r.max_gap_mm:
                head += f" (no more than {r.max_gap_mm} mm from point to shank)"
        if r.when_targeting:
            head += f" when fishing for {species_words(r.when_targeting).lower()}"
        return head + _scope(r) + _dates(r) + _where(r) + _because(r)

    if t is RuleType.method_rule:
        if (r.extent_text or r.reason) and not any((r.max_lines, r.hook_count, r.min_gap_cm)):
            # a procedural duty; no template renders a duty — but a FORBIDDEN one still has
            # to read as forbidden, or the quote is the label and the label is inverted.
            #
            # `reason` counts as well as `extent_text`. This test used `extent_text` alone as
            # the signal, which meant a duty had to claim a PLACE to be printed at all — and a
            # rule that claims a place the atlas cannot draw binds to no section and is lost on
            # every water. Four provincial duties sat in exactly that trap ("set lines must be
            # marked with the angler's name", "chumming is prohibited", the ice-hut duty, "do
            # not place gear in the water during a No Fishing period"); moving their words to
            # `reason` lets the atlas place them, and this keeps them readable.
            return (_quoted_prohibition(r, quote_is_whole=True) if r.required is False
                    else r.verbatim)
        m = r.method.value.replace("_", " ")
        head = f"{m.capitalize()} is permitted" if r.permitted else f"{m.capitalize()} is prohibited"
        rig = []
        if r.max_lines: rig.append(f"{r.max_lines} line")
        if r.hook_count: rig.append(f"{r.hook_count} hook")
        if r.min_gap_cm: rig.append(f"gap {r.min_gap_cm} cm or more from point to shank")
        if rig: head += " — " + ", ".join(rig)
        rest = _scope(r.model_copy(update={"method": None}))
        return head + rest + _dates(r) + _where(r) + _because(r)

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
