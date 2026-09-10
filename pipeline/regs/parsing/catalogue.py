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

from enum import Enum
from typing import List, Optional

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
    "CHAR":       ("DV", "BT", "LT", "EB", "AC", "ADV", "AEB", "SPK"),
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

#: Salmon are federal, not on the provincial game-fish list, and so are NOT in ALL_GAME_FISH.
#: They are here because the synopsis names them anyway (bait bans "when fishing for salmon",
#: Region 1/3 notices) and because the DFO corpus moves to this format next.
SPECIES_GROUPS["SALMON"] = ("CH", "CO", "SK", "PK", "CM")

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
    Anything that is not a group passes through untouched."""
    out: List[str] = []
    for c in codes:
        # An empty group (NON_GAME_FISH) is a complement, not a membership: expanding it to []
        # would erase the rule. Pass it through so a caller sees the claim it actually made.
        for m in (SPECIES_GROUPS.get(c) or (c,)):
            if m not in out:
                out.append(m)
    return out


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


class CatalogueRule(BaseModel):
    """One regulation, typed. `verbatim` is the synopsis sentence and is REQUIRED — the generated
    label is a summary and never a replacement, so the words the law used must always be reachable.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    rule_id: str
    type: RuleType
    verbatim: str = Field(..., min_length=1, description="the synopsis sentence, exactly")
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
    over_cm: Optional[int] = None
    under_cm: Optional[int] = None
    band: bool = False
    combined: bool = False
    aggregation_domain: Optional[str] = None
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
    exempts: List[Exempts] = Field(default_factory=list)
    standing: bool = False
    authority: Optional[str] = Field(default=None, pattern="^superior$")
    reason: str = ""
    extent_text: str = ""
    needs_review: bool = False
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
            return f"{self.period.value}{'/size' if (self.over_cm or self.under_cm) and self.take is None else ''}"
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
            if self.band and not (self.over_cm and self.under_cm):
                e.append("band needs both over_cm and under_cm")
            if self.over_cm and self.under_cm and self.under_cm >= self.over_cm:
                e.append(f"under_cm {self.under_cm} >= over_cm {self.over_cm} is an impossible slot")
        else:
            for f in ("take", "unlimited", "per_daily", "within", "band", "combined"):
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
            elif self.aspect is VesselAspect.speed and self.max_kmh is None and not self.needs_review:
                e.append("speed needs max_kmh, or needs_review if the synopsis states none")
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

        for f in ("over_cm", "under_cm", "take", "hook_count", "max_lines", "max_gap_mm"):
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
        if self.needs_review and not self.review_reason:
            e.append("needs_review requires a review_reason")
        if self.standing and not self.needs_review:
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
    "NON_GAME_FISH": "Non-game fish",
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
    "AC": "Arctic char", "ADV": "Dolly Varden (anadromous)", "AEB": "Brook trout (anadromous)",
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
    # protected — never retainable, but nameable
    "NDC": "Nooksack dace", "SSU": "Salish sucker", "CCL": "Cultus Lake sculpin",
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
    ("Protected — never retainable", ("NDC", "SSU", "CCL", "GSG")),
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
    for code in ("ALL_GAME_FISH", "TROUT_CHAR", "TROUT", "CHAR", "WHITEFISH", "BASS", "SALMON",
                 "NON_GAME_FISH"):
        members = SPECIES_GROUPS[code]
        gloss = {"ALL_GAME_FISH": "everything on the provincial closed list; NOT salmon",
                 "TROUT_CHAR": "the usual quota line — trout rules cover char unless char are excluded",
                 "SALMON": "federal; not part of ALL_GAME_FISH",
                 "NON_GAME_FISH": "carp, suckers, chub and the rest — the spear rule's subject"}.get(code, "")
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
            "Use `ALL_GAME_FISH`. Bait and tackle rules take no `species` at all (use",
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
    """POLARITY IS THE WHOLE JOB HERE.

    "not more than 1 over 50 cm" ALLOWS one big fish; "none over 50 cm" FORBIDS them. The same two
    fields carry both, and which one is meant depends on `take`:

        take = 0            the size says WHICH fish must go back  -> "none over 50 cm"
        take = n, within    the size says which fish the CAP counts -> "no more than n over 50 cm"

    `r2:cultus_lake` is the corpus proving it matters: "1 bull trout over 60 cm" means the one you
    keep must BE over 60, and rendering it "none over 60 cm" inverts the rule on the fish it exists
    to protect."""
    if r.over_cm and r.under_cm:
        if r.band:
            # A BAND protects the middle. `r6:bennett_lake` is "only 1 over 90 cm, NONE between
            # 60 and 90" — two facts, and returning only the band swallowed the number the module
            # docstring cites as its motivating case. A band with a take needs a second rule.
            return (f" (none between {r.under_cm} cm and {r.over_cm} cm)" if r.take is None
                    else f" (no more than {r.take}, none between {r.under_cm} cm and "
                         f"{r.over_cm} cm)")
        slot = f"{r.under_cm}–{r.over_cm} cm only"
        # A slot inside a parent still carries its own COUNT. `z7a` is "not more than 1 bull trout,
        # 30-50 cm" — dropping the 1 turns a one-fish allowance into an unlimited one.
        return f" (no more than {r.take}, {slot})" if (r.within and r.take) else f" ({slot})"
    bound = "over" if r.over_cm else ("under" if r.under_cm else None)
    if bound is None:
        return ""
    cm = r.over_cm or r.under_cm
    if r.take == 0:
        return f" {bound} {cm} cm"                       # which fish go back
    if r.period is not Period.daily and r.take:
        # "Rainbow trout: 5 over 50 cm" (annual) COUNTS fish over 50 cm; it does not forbid
        # keeping smaller ones, which the daily quota governs. Printing "(none under 50 cm)" put a
        # minimum size on the page that neither the Shuswap nor the Kootenay chapter states.
        return f" over {cm} cm" if bound == "under" else f" {bound} {cm} cm"
    if r.within and r.take:
        # ASYMMETRIC ON PURPOSE. over_cm caps how many BIG fish the allowance includes; under_cm is
        # a FLOOR on every fish kept. "no more than 1 under 60 cm" would say the opposite of
        # `r2:cultus_lake`'s "1 bull trout over 60 cm", where the fish you keep must BE over 60.
        return (f" (no more than {r.take} over {cm} cm)" if bound == "over"
                else f" (no more than {r.take}, none under {cm} cm)")
    return f" (none {bound} {cm} cm)"                    # a flat size prohibition


def _where(r: CatalogueRule) -> str:
    """The extent, appended. §5: generation is lossless ONLY where the extent survives alongside.
    533 of 644 closures have a label of exactly "No fishing" — everything distinguishing one from
    another is in the reach."""
    return f" — {r.extent_text}" if r.extent_text else ""


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
            return head + rest + _dates(r) + _where(r)
        if r.take == 0:
            head = f"{sp} — release all" if not (r.over_cm or r.under_cm) else f"{sp} — release all"
        elif r.unlimited:
            head = f"{sp} — no limit"
        elif r.take is not None:
            noun = {Period.daily: "per day", Period.possession: "in possession",
                    Period.annual: "per licence year", Period.monthly: "per month"}[r.period]
            if r.within and (r.over_cm or r.under_cm):
                head = sp                                # the size phrase carries the count
            else:
                head = f"{sp} — {r.take} {noun}"
                if r.combined:
                    head += ", all species combined"
        elif r.per_daily is not None:
            head = f"{sp or 'All game fish'} — possession quota is {r.per_daily} daily quota" \
                   + ("s" if r.per_daily != 1 else "")
        elif r.over_cm or r.under_cm:
            head = sp                      # a size gate with no count: the region supplies it
        else:
            return r.verbatim              # nothing numeric to generate from
        out = head + _size(r) + _who(r) + _scope(r) + _dates(r) + _where(r)
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
        return head + _scope(r) + _dates(r) + _where(r)

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
        return head + _scope(r) + _dates(r) + _where(r)

    if t is RuleType.method_rule:
        if r.extent_text and not any((r.max_lines, r.hook_count, r.min_gap_cm)):
            return r.verbatim          # a procedural duty; no template renders a duty
        m = r.method.value.replace("_", " ")
        head = f"{m.capitalize()} is permitted" if r.permitted else f"{m.capitalize()} is prohibited"
        rig = []
        if r.max_lines: rig.append(f"{r.max_lines} line")
        if r.hook_count: rig.append(f"{r.hook_count} hook")
        if r.min_gap_cm: rig.append(f"gap {r.min_gap_cm} cm or more from point to shank")
        if rig: head += " — " + ", ".join(rig)
        rest = _scope(r.model_copy(update={"method": None}))
        return head + rest + _dates(r) + _where(r)

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
        return head + _dates(r) + _where(r)

    if t is RuleType.angling_from_vessel_prohibited:
        return "No angling from boats" + _scope(r) + _dates(r)

    if t is RuleType.document_required:
        doc = (r.licence_name + " classified licence") if r.licence_name else \
            _DOC_WORDS.get(r.document.value, r.document.value.replace("_", " "))
        head = (f"A {doc} is required" if r.required else f"A {doc} is not required")
        if r.water_class:
            head = f"Class {r.water_class} water — " + head[0].lower() + head[1:]
        if r.on_retention:
            head += ", only if you keep the fish"
        if r.issuing_jurisdiction:
            head += f"; a {r.issuing_jurisdiction} licence is also valid"
        if r.allocation:
            head += " (" + r.allocation.replace("_", " ") + ")"
        return head + _who(r) + _scope(r) + _dates(r) + _where(r)

    if t is RuleType.access_permission:
        who = []
        ac = r.angler_class
        if ac and not ac.is_empty():
            if ac.guided is False:
                who.append("non-guided")
            if ac.residency:
                who.append(ac.residency.value.replace("_", "-") + "s")
            if ac.age:
                who.append("anglers " + ac.age.replace("_", " "))
            if ac.status:
                who.append(ac.status.replace("_", " "))
        subject = " ".join(who) if who else "anglers"
        if r.grantor:
            return f"Permission of the {r.grantor} is required" + _dates(r)
        head = f"Angling prohibited for {subject}" if r.permitted is False \
            else f"{subject.capitalize()} may fish here"
        return head + _scope(r) + _dates(r) + _where(r)

    # navigation_duty, handling_rule, hazard, advisory, program_membership, facility
    return r.verbatim


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
        haystack = " ".join(self.regs_verbatim.split()).lower()
        for r in self.rules:
            if r.rule_id in seen:
                e.append(f"duplicate rule_id {r.rule_id!r}")
            seen.add(r.rule_id)
            needle = " ".join(r.verbatim.split()).lower()
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
