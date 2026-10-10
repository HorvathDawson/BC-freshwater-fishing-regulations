"""`CatalogueRule` and `Exempts` — one rule: a type plus named conditions, and its validators.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .vocab import (
    ChannelSide, ClosureKind, HALF_OF_CHANNEL, LIFE_STAGE_FISH, LifeStage, Obligation, Origin,
    Period, PropulsionLevel, RuleType, VesselAspect, WaterKind, _ADULT_CHINOOK,
    printed_closure_kinds
)
from .dates import When
from .species import FEDERAL_SALMON, SALMON_FISH, _COUNTED_TYPES, _FAMILY, species_problems
from .gear import (
    AnglerState, CAUGHT_HOW, CONDUCT_ACTS, GearClause, Slot, WHILE_DEVICES, WHILE_MEANS,
    WHILE_TOKENS, _SNAGGED
)
from .lengths import LengthBand
from .checks import (
    LIST_MARKER, PLACE_VALUE, _all_species_is_game_fish, _extents_the_resolver_reads,
    _own_dates_carried, bare_whole
)
from .licensing import Who, _check_residency


#: THE ZONE DEFAULTS A WATER MAY LIFT BY NAME (`Exempts.default_id`), each the slug of a zone
#: entry (`z<region>:<slug>`). A closed list, like `CONDUCT_ACTS`: a default_id outside it named
#: nothing a reader could find — `set_lining.r1b` (a rule id) sat here — and adding one is a
#: reviewed change. The bundle resolves each to the zone entry of the lifting rule's own region.
EXEMPTABLE_DEFAULTS = frozenset({
    "spring_stream_closure", "summer_stream_closure", "steelhead_stream_closure",
    "trout_char_winter_release", "bait_ban_streams", "single_barbless_hook",
})


class Exempts(BaseModel):
    """What this rule LIFTS. A field, not a type — an exemption takes the type of whatever it
    lifts, which is why `bait_restriction` carries `allowed: true` for the Fraser sturgeon rules.

    ONE OF TWO, never both:

      `default_id`  a ZONE DEFAULT by its slug — `spring_stream_closure` is `z3:spring_stream_closure`
                    on a Region 3 water. It lifts that zone entry's rules in the rule's own region,
                    never the rule's own entry (a rule that lifts itself deletes itself).
      `target`      ONE RULE by `rule_id`: in this entry, or in `entry_id` when the lifted rule is
                    another entry's ("EXEMPT from Columbia Lake's tributaries closure"). A rule id
                    is unique only within its entry (AGENTS 8), so the entry is named, not guessed.

    The bundle resolves each to exact ids and refuses one that names nothing
    (`pipeline.deliver.bundle.rules._exempts`)."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    default_id: Optional[str] = Field(default=None, pattern=r"^[a-z0-9_]+$")
    target: Optional[str] = Field(default=None, pattern=r"^[a-z0-9_]+\.r\d+[a-z]?$")
    entry_id: Optional[str] = None
    note: str = ""

    @model_validator(mode="after")
    def _one_of(self) -> "Exempts":
        if bool(self.default_id) == bool(self.target):
            raise ValueError("exempts needs exactly one of `default_id` (a zone default) or "
                             "`target` (a rule id)")
        if self.entry_id and not self.target:
            raise ValueError("exempts: `entry_id` names the entry a `target` is in — it means "
                             "nothing beside a `default_id`")
        if self.default_id and self.default_id not in EXEMPTABLE_DEFAULTS:
            raise ValueError(f"exempts: default_id {self.default_id!r} is not a registered zone "
                             f"default ({sorted(EXEMPTABLE_DEFAULTS)}) — a rule of this or "
                             f"another entry is a `target`")
        return self


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
    def _caught_is_a_fish_condition(self) -> "CatalogueRule":
        for c in self.caught:
            if c not in CAUGHT_HOW:
                raise ValueError(f"caught: {c!r} is not a way a fish is caught ({sorted(CAUGHT_HOW)})")
        # Only a retention statement is about a CAUGHT fish; every reader of `caught` (release,
        # closure, statement, key) is a retention reader.
        if self.caught and self.type is not RuleType.retention_limit:
            raise ValueError(f"caught belongs to retention_limit, not {self.type.value}")
        # THE SNAG DUTY IS NOT A MEANS (user ruling Q38): a take-0 retention rule printing a snag
        # names how the fish was caught, `caught`, never `while: snagging` alone (which an
        # accidental snag while angling never meets).
        if self.type is RuleType.retention_limit and self.take == 0 and not self.caught \
                and _SNAGGED.search(self.verbatim or ""):
            raise ValueError("a release of a snagged (foul-hooked) fish binds the FISH, however it "
                             "was hooked: `caught: [foul_hooked]`, not `while: [snagging]` "
                             "(user ruling Q38)")
        return self

    @model_validator(mode="after")
    def _circumstances_are_real(self) -> "CatalogueRule":
        for w in self.while_:
            if w not in WHILE_TOKENS:
                raise ValueError(f"while: {w!r} is neither a means of fishing "
                                 f"({sorted(WHILE_MEANS)}) nor a device ({sorted(WHILE_DEVICES)})")
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
    #: angler_closure only: WHO the water is closed to. The key carries the direction, as `ban`
    #: does in gear — there is no `permitted` bit. `angler_class` (one residency, a `guided`
    #: polarity bit) is gone and refused; see `Who`.
    closed_to: Optional[Who] = None
    #: angler_closure only: the anglers INSIDE `closed_to` the water stays open to. A Youth/Disabled
    #: Accompanied Water is closed to anglers 16 and over EXCEPT disabled B.C. residents and the
    #: companions of an authorized angler — two exceptions no single `Who` can subtract, because a
    #: `Who` is a conjunction of axes. The direction is in the key, as `closed_to`'s is; each
    #: exception must meet `closed_to`, or it subtracts nothing.
    closed_to_except: List[Who] = Field(default_factory=list)
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

    #: WHAT YOU ARE DOING, for a clause that binds only then. Drawn from `WHILE_TOKENS`: a MEANS
    #: (a `Method` member) or a DEVICE in use (`downrigger`, `light`) — see the note there.
    #: A LIFT WHOSE CIRCUMSTANCE IS NOT MET IS NOT APPLIED, which is the Babine rule on a new
    #: axis: "dead fin fish when set lining" applied everywhere deleted the province-wide fin
    #: fish ban and a water's bait tile went from "banned" to "no rule at all".
    while_: List[str] = Field(default_factory=list, alias="while")

    #: HOW THE FISH WAS CAUGHT, for a rule that binds only a fish caught that way — drawn from
    #: `CAUGHT_HOW` (user ruling Q38/G6, 2026-10-07: "Any fish willfully or accidentally snagged must
    #: be released immediately"). A condition on the FISH, like `origin`, never the angler's means
    #: (`while`): the duty binds a fish snagged by accident while angling too. Like every condition it
    #: narrows: a release with `caught` is NEVER an outright release (`rules.release_origins`), never
    #: a closure (`rules.CLOSURE_CONDITIONS`), its own statement (`rules.statement`) and its own key
    #: (`dimension` `…@caught=foul_hooked`) — so it never decides a quota for a fish hooked in the
    #: mouth; it stands beside them, a duty for the snagged fish only.
    caught: List[str] = Field(default_factory=list)

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
    #: `when_open` WAS HERE: "Artificial fly only, where open". It said nothing — a restriction or a
    #: licence only matters while the water is open for fishing at all, so every rule holds "while
    #: open" by construction. It is REFUSED on load (`extra=forbid`); the words stay in `verbatim`.

    # --- retention ---------------------------------------------------------
    take: Optional[int] = None
    unlimited: bool = False
    may_target: Optional[bool] = None
    #: THE CLOCK A NUMBER RUNS ON — only on the two types that count fish (`_COUNTED_TYPES`).
    #: It defaulted to `daily` on EVERY rule, so 1,549 bait bans, boat rules and advisories
    #: shipped `period: "daily"`: a field that does not apply, stated as if it did. Absent on a
    #: counting type is the book's default, daily — read it through `clock`, never `period`. On
    #: any other type it is refused.
    period: Optional[Period] = None
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
    #: A SHARED ENDPOINT IS LEGAL (`EXACT_BOUND_IS_LEGAL`, user ruling Q9 2026-10-07): a band that
    #: keeps none does not hold its own bounds, so a fish of exactly 60 cm is granted whatever the
    #: order, and "none under 30 cm" written alone keeps a 30.0 cm fish too. The book's "60 cm or
    #: more" (the bound IS denied) is the band's `closed: true`.
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
    #: WHICH SEASONAL CLOSURE THIS IS, BY NAME — a region's blanket stream closure only
    #: ("Spring closure: No Fishing in any stream in Region 3", "Summer closure: …"). A water row
    #: printed "Exempt from spring closure" lifts its own region's spring closure, and — where the
    #: water runs into another region — THAT region's spring closure, never its winter or summer
    #: one (user ruling 2026-09-26; the bundle's `_equivalent_closures`). Dates cannot say which:
    #: Region 6's Skeena/Nass winter closure (Jan 1-June 15) overlaps every spring closure in
    #: the province. Authored where the rule's own sentence does not print it (Region 4's and
    #: Region 6's print only dates); where it does, the two must agree (`_check`).
    closure_kind: Optional[ClosureKind] = None

    # --- gear / tackle / bait: see `gear` -----------------------------------
    #: The species a bait or tackle rule is ABOUT — "no natural bait when fishing for salmon".
    #: Distinct from `species`, which on those types would mean the ban is scoped to what you may
    #: CATCH, and the tables say it is not: "banned for all angling and for all species". A stream
    #: can carry a salmon bait ban and no other, so the scoping is by TARGET, not by catch.
    when_targeting: List[str] = Field(default_factory=list)

    # --- vessel ------------------------------------------------------------
    aspect: Optional[VesselAspect] = None
    #: On `vessel_rule(aspect=propulsion)`: the boats allowed ON the water. Nowhere else: "No
    #: angling from powered boats" is a method rule on `angler: in_powered_boat`.
    level: Optional[PropulsionLevel] = None
    max_power_kw: Optional[float] = None
    max_kmh: Optional[float] = None

    # --- licensing: NOT HERE ------------------------------------------------
    #: `document`, `required`, `water_class`, `licence_name`, `allocation`,
    #: `issuing_jurisdiction`, `on_retention`, `grantor` and `permitted` WERE HERE, for the two
    #: licensing rule types. They are gone and REFUSED on load (`extra=forbid`), like the old gear
    #: fields: licensing is `CatalogueEntry.licensing`. `required: false` was the defect that
    #: mattered — nine steelhead-stamp waivers shared a dimension with the provincial "stamp if
    #: you fish for steelhead" and, being narrower, would strike it.
    includes_tributaries: Optional[bool] = None

    # --- relations / provenance -------------------------------------------
    #: Walk the tributaries WITHOUT the mainstem. `z5:spring_stream_closure` is the case:
    #: "No fishing in any stream in the Fraser River Watershed ... EXCEPT the mainstem of the
    #: Fraser River." Binding it with includes_tributaries CLOSES the mainstem the rule exempts.
    tributaries_only: bool = False
    #: WHERE THIS RULE APPLIES, stated on the rule itself. NOTHING IS INHERITED: a rule with no
    #: extents is not given its entry's by any reader — not the reach builder, not the bundle, not
    #: provenance (`deliver.bundle.read.source_of`). A row routinely binds its rules to different
    #: reaches — "no fishing above the falls, bait ban throughout" — and an implied default is how
    #: the narrower rule silently widens to the whole water.
    #:
    #: So every rule in an entry says where it is (`CatalogueEntry` refuses one that does not):
    #: `extents`, or — for a place nothing can draw — `extent_text` / `unresolved_locators`, which
    #: leave it UNBOUND (AGENTS 13). A rule that says nothing about location is given the entry's
    #: reach explicitly, once, at ingest (`validate_catalogue.default_extents`), so the file states
    #: it. On an AREA rule (every extent a `within`), `unresolved_locators` beside the extents is a
    #: carve-out no cut-point expresses — "Bass: 20, excluding Mill Lake" is Region 2 minus one lake
    #: — and it stays unbound too: the extents say whose rule it is, the locator why it cannot be
    #: drawn (`reach.classify.AREA_CARVE_OUTS_UNBIND`).
    extents: Optional[List[dict]] = None
    #: WATER THIS RULE REACHES BY THE TRIBUTARY WALK AND MUST NOT. Subtracted from this rule's
    #: tributary set only. There is no entry-wide carve-out: one would cut every rule in the row,
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
    #: descend through it either (`pipeline/atlas/reach/build.py::resolve_carve_outs`).
    #: Curator-filled — the parser never writes one.
    tributary_excludes: List[dict] = Field(default_factory=list)
    exempts: List[Exempts] = Field(default_factory=list)
    standing: bool = False
    authority: Optional[str] = Field(default=None, pattern="^superior$")
    #: WHERE THE RULE WAS PUBLISHED, when that is a notice and not the synopsis: a DFO fishery
    #: notice number ("FN0679"). This was `reason` — a free-text field that held a citation here,
    #: an explanation ("located in an Ecological Reserve") on three book rules, and a hidden
    #: condition on three more. A citation says where a rule came from, never why or when.
    notice: Optional[str] = Field(default=None, pattern=r"^FN\d{4}$")
    #: THIS RULE IS DORMANT WHILE THAT ONE BINDS — a rule id in the same entry. "Classified Waters
    #: Licence or Steelhead Stamp not required until reopened to steelhead fishing": the licence
    #: requirement sleeps while the steelhead closure stands. It sat in `reason` as prose, so every
    #: reader was shown the requirement and none could tell it was lifted.
    #:
    #: NOT `condition_of`, which points the other way — at the rule this one is the proviso of.
    #: One field for both would be a pointer whose meaning depends on the rule holding it.
    suspended_while: Optional[str] = None
    #: THE PAGE'S WORDS FOR WHERE THE RULE HOLDS. With no `extents` it is a place nothing could
    #: draw, and the rule stays unbound. Beside drawn extents it names the drawn place in the
    #: book's words for the label ("between fishing boundary signs …"). Beside `rest` it is the
    #: book's word for the remainder ("other parts") — the label prints it; the op alone does not.
    #: Refused beside a bare `whole` (the whole water would be closed for a rule about one bay).
    extent_text: str = ""
    #: Locator phrases that could not be bound to a cut-point — "the outlet", "signs 500 m below
    #: the falls". Non-empty means the rule needs review; curation maps each to a split id.
    #:
    #: The prompt, the batch envelope and the no-registry instructions all told the model to use
    #: this, and it existed only on the RETIRED prose Rule — so a model that followed the
    #: instruction exactly was refused with "extra inputs are not permitted". The reach it could
    #: not express is precisely what must not be dropped silently, which is the whole point of the
    #: field.
    unresolved_locators: List[str] = Field(default_factory=list)
    #: `needs_review` WAS HERE and was exactly `bool(review_reason)`: not one rule in the corpus
    #: carried the flag without a reason, and every producer that set it passed one in the same
    #: call. A flag whose only job is to say that the field beside it is filled in. It is REFUSED
    #: on load (`extra=forbid`): a non-empty `review_reason` is what "needs review" means.
    review_reason: str = ""

    #: THIS RULE HOLDS ONLY IN A PART OF WHAT ITS EXTENTS DRAW, AND THE PART IS NOT DRAWN — the
    #: book's words for the part. "No fishing in Salmon Arm Bay, west of the line between
    #: Engineer's Point and Sunnybrae Point": the atlas has no polygon for the bay, so the rule
    #: binds Shuswap Lake (its `extents`) and says here that it holds only in the bay.
    #:
    #: WHY A FIELD AND NOT `extent_text`. `extent_text` beside the bare whole water is REFUSED
    #: (see `_check`): that was the parser's shape for every part-lake rule and the reach builder,
    #: reading only the extent, closed all of Shuswap Lake. Unbinding them instead dropped the
    #: closures from every screen, and on a closure silence reads as permission. This field is
    #: the sanctioned third way: the rule is SHOWN on the water it is in, and a reader is told in
    #: the key itself that it must NOT colour that water by it — render it as a note, "in <part>".
    #: It is the place phrase for such a rule, so it replaces `extent_text` rather than sitting
    #: beside it, and it needs extents to be a part OF (a part of nothing is a place nothing can
    #: draw — that rule keeps `extent_text` and stays unbound, AGENTS 13).
    #:
    #: When the part is drawn (a cut-point, a sub-lake polygon), the extents become the part and
    #: this field goes: then the rule colours what it binds.
    undrawn_part: str = ""

    #: THE HALF OF THE CHANNEL THIS RULE HOLDS ON, lengthwise — the book's "on the west half of
    #: river" (Kitimat River, p.51: "No Fishing on the west half of river between fishing boundary
    #: signs near Kitimat Hatchery outfall"). The atlas draws a river as ONE line, so the rule is
    #: placed on the reach its extents name (the stretch between the signs is drawn) — but it
    #: holds on that half only, and an angler on the other half follows the water's OTHER rules
    #: for the stretch (user ruling 2026-09-28). So the rule is shown BESIDE them on the reach
    #: and displaces none (`read.effective_rules`), and its line says which half in its own part
    #: (`label_parts` "side"). Not `undrawn_part`: the stretch is drawn and the rule is placed on
    #: it; only the width is not. A sentence printing "<side> half of the river" must set it
    #: (`_check`).
    side: Optional[ChannelSide] = None

    #: A LIFE STAGE THE BOOK DEFINES, as the sentence prints it: "record your retention of ADULT
    #: chinook salmon" (p.7; "adult" defined on p.77 by a length that differs by water). The rule
    #: holds for fish of that stage only, and its line says so ("Adult chinook"). Only for the fish
    #: the stage is defined for (`LIFE_STAGE_FISH`), and a sentence printing "adult chinook" must
    #: set it (`_check`).
    life_stage: Optional[LifeStage] = None

    # ------------------------------------------------------------------ #
    @property
    def clock(self) -> Period:
        """The clock a counting rule's number runs on: its `period`, or the book's default,
        daily. Only meaningful on `_COUNTED_TYPES`; every other type refuses a `period`."""
        return self.period or Period.daily

    @property
    def family(self) -> str:
        """The first tier. Types group into families, and the grouping is not decoration — it is
        how the reader's screen is sectioned, and it is the level at which "does this rule compete"
        is *usually* obvious before you look at the dimension."""
        return _FAMILY[self.type]

    @property
    def lift_only(self) -> bool:
        """A RULE WHOSE ONLY CONTENT IS LIFTING ANOTHER — `exempts`, and no number, bound, gear,
        duty or angler of its own. "Exempt from spring closure"; "Exemptions include mainstem
        portions of the Skeena, Nass, Iskut, Stikine and Taku" (z6 steelhead r2).

        Such a rule says nothing a reader could weigh against another rule, so it never COMPETES
        (`dimension` is `lift`, which the ladder never ranks): it only removes what it lifts.
        Keyed by its type, it was a daily steelhead `retention_limit` bound at the water's rank,
        and on the five mainstems it displaced the province's and the region's "release all wild
        steelhead" — a lift with no take silenced a release."""
        return bool(self.exempts) and not (
            self.gear or self.conduct or self.take is not None or self.unlimited
            or self.may_target is not None or self.per_daily is not None or self.lengths
            or self.record_retention or self.closed_to is not None or self.aspect is not None
            or self.level is not None or self.max_kmh is not None
            or self.max_power_kw is not None)

    def _condition_key(self) -> str:
        """The CONDITIONS a retention rule holds under, as a key — the way a method rule's clause
        carries its `when`. A rule that holds only for wild fish, only in streams, only while
        spear fishing, or that is a record-keeping duty is a different subject from the plain
        quota beside it: Region 6's "Trout/char: 5" must not share a key with the province's
        "release all wild steelhead", or the region's number silences the release."""
        bits = []
        if self.origin is not None:
            bits.append(f"origin={self.origin.value}")
        if self.water is not None:
            bits.append(f"water={self.water.value}")
        if self.while_:
            bits.append("while=" + "+".join(sorted(self.while_)))
        if self.caught:
            bits.append("caught=" + "+".join(sorted(self.caught)))
        if self.record_retention:
            bits.append("record")
        return ("@" + "&".join(bits)) if bits else ""

    @property
    def dimension(self) -> str:
        """WHAT THIS RULE CONTROLS — the second half of the comparison key.

        Without one, every rule of a type on a water shares a single key and they collide. Tackle
        is the sharpest case: a fly-only rule and a barbless rule are ADDITIVE (a fly must be
        barbless), so their dimension is the FACET each constrains, not the type.

        A LIFT-ONLY rule (`lift_only`) is `lift` whatever its type, and never competes."""
        t = self.type
        if self.lift_only:
            return "lift"
        if t is RuleType.retention_limit:
            # THE CLOCK, '/size' WHEN THE RULE IS SIZES WITH NO COUNT OF ITS OWN ("none under 30
            # cm"), AND THE CONDITIONS IT HOLDS UNDER (`_condition_key`).
            return (f"{self.clock.value}{'/size' if self.lengths and self.take is None else ''}"
                    + self._condition_key())
        if t is RuleType.vessel_rule:
            return self.aspect.value if self.aspect else "unspecified"
        if t is RuleType.angler_closure:
            # WHO it closes the water to. Two closures for the same anglers compete (a water's
            # displaces a zone's); a closure for aliens never competes with one for everyone,
            # and — being its own type — never with a quota.
            return ("closed_to:" + (self.closed_to.key() if self.closed_to else "unspecified")
                    + "".join(f"-except:{w.key()}" for w in self.closed_to_except))
        # A GEAR OR CONDUCT RULE CARRIES ITS CONDITION AND ITS ACTS IN ITS KEY (answers v1,
        # 2026-10-06: the answers layer's decision G2, fixed at the source). The ladder sets two
        # rules of one (type, dimension) against each other WHOLE, so a key coarser than what the
        # rules say drops clauses the winner never spoke about: Kootenay Lake's "unlimited rods
        # FROM A BOAT" (`lines_per_angler`, its clause `when: in_boat`) displaced the province's
        # line rule and its "1 line" from shore; a region's ice-hut or set-line duty (`conduct`)
        # displaced the province's "no gear in the water during a closure" and "warn others of an
        # ice hole". So a clause's condition is part of its slot's key for tackle as for methods
        # (`slot@when`), a duty's ACTS are its key (`conduct:act+act`), and the MEANS the whole rule
        # holds under (`while`: set lining, ice fishing) is a condition of every clause
        # (`…@while=set_lining`, as on a bait or a retention rule). The water kind is NOT: where
        # two gear rules both bind they bind the same kind of water, and a water row's "single
        # barbless hook" replaces the zone's "single barbless hook in streams".
        def at(c):
            return "@" + c.when.key() if c.when is not None and not c.when.is_empty() else ""
        means = ("@while=" + "+".join(sorted(self.while_))) if self.while_ else ""
        acts = ("conduct:" + "+".join(sorted(self.conduct))) if self.conduct else ""
        if t is RuleType.method_rule:
            # THE METHODS NAMED, not just the slot: every method rule constrains `method`, so the
            # slot alone would let a water's "no ice fishing" displace the zone's "no set lining".
            # AND THE CLAUSE'S CONDITION: a water's "no angling from boats" is `ban: [angling]`
            # when in a boat, and keyed "method:angling" it would DISPLACE the province's
            # unconditional angling allow — shore angling would read as not allowed there. A
            # conditional clause is its own key ("method:angling@angler=in_boat").
            said = sorted({(f"{c.slot.value}:{m}" if c.slot is Slot.method else c.slot.value)
                           + at(c)
                           for c in self.gear
                           for m in ((c.allow or []) + (c.only or []) + (c.ban or []) or [""])})
            return (",".join(said + ([acts] if acts else [])) or "unspecified") + means
        if t is RuleType.tackle_restriction:
            # THE SET OF SLOTS CONSTRAINED. A water's "single barbless hook" displaces the zone's
            # "single barbless hook"; its bare "barbless hook" does not, because displacing would
            # drop the zone's one-point cap, which the water never lifted.
            return (",".join(sorted({c.slot.value + at(c) for c in self.gear}))
                    or "unspecified") + means
        if t is RuleType.handling_rule:
            # A DUTY IS KEYED BY WHAT IT MAKES YOU DO: "keep the head on until home" and "don't
            # sell your catch" never replace each other.
            return (acts or "unspecified") + means
        if t is RuleType.bait_restriction:
            # BAIT IS ONE DOMAIN, ranked by where a rule applies (province < region < water).
            # Invertebrates are a kind of bait, and the province's "you may use freshwater
            # invertebrates in streams as bait UNLESS A BAIT BAN APPLIES" is a PERMISSION: keyed by
            # the bait it names (`bait:invertebrate`) it never met the 412 bait bans
            # (`bait:any_bait`), so every bait-ban stream showed both "Bait ban" and
            # "Invertebrates may be used". So a permission (`allow` / `only`) and a TOTAL ban (one
            # that bans `any_bait`) share ONE key, `bait`: a region's or a water's bait ban speaks
            # over the province's permission, and a water's "EXEMPT from bait ban" meets the
            # region's ban.
            #
            # A PARTIAL BAN KEEPS THE BAIT IT NAMES (`bait:fin_fish`, `bait:live_fin_fish`,
            # `bait:invertebrate`): Zone B's "fin fish may not be used as bait" must not displace the
            # province's ban on invertebrates at a lake, and a water's "EXEMPT from bait ban"
            # (an allow) must not lift the province's live-fish ban. The one printed exemption
            # from a partial ban (dead fin fish for sturgeon) is a lift.
            #
            # Two things still make a different subject. The TARGET: a salmon bait ban and a
            # general bait ban coexist on one stream. And the MEANS (`while`): bait when set
            # lining is not bait when angling ("bait ban … for all angling"). The ROE POSSESSION
            # CAP is not a rule about using bait at all — it is how much you may hold — and has
            # its own key, so no water's bait rule displaces it.
            tgt = ("/" + ",".join(sorted(self.when_targeting))) if self.when_targeting else ""
            means = ("@while=" + "+".join(sorted(self.while_))) if self.while_ else ""
            bait = [c for c in self.gear if c.slot is Slot.bait]
            if self.gear and not bait and all(c.slot is Slot.bait_possession_kg for c in self.gear):
                held = sorted({m for c in self.gear for m in c.of}) or ["any_bait"]
                return f"bait_possession:{','.join(held)}{tgt}{means}"
            whole = any("any_bait" in (c.ban or []) or c.allow is not None or c.only is not None
                        for c in bait)
            if whole or not bait:
                return f"bait{tgt}{means}"
            named = sorted({m for c in bait for m in (c.of or c.ban or [])})
            return f"bait:{','.join(named)}{tgt}{means}"
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
        # THE BOOK'S LIST (p.80), plus the federal salmon the DFO feed types into this model —
        # which a synopsis ENTRY refuses (`CatalogueEntry._book_species_only`).
        e += species_problems((set(self.species) | set(self.species_except)) - FEDERAL_SALMON)

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
            if self.take == 0 and self.may_target and self.clock is not Period.daily:
                e.append("a release rule is a daily-period rule")
            if self.per_daily is not None and self.clock is not Period.possession:
                e.append("per_daily is a possession multiplier")
            # A `take` NO BAND USES SAYS NOTHING, AND IT MADE A SIZE RULE A COUNT (N-5,
            # 2026-09-29). A band without its own `take` uses the rule's; when every band carries
            # one, the rule's fills none. "Rainbow trout over 50 cm catch and release" written
            # `take: 0` + [{min_cm: 50, take: 0}] was keyed `daily` — a count — and silenced
            # Region 6's "Trout/char: 5" for every rainbow at Lakelse Lake, where the 5 still
            # holds every fish under 50 cm. Written [{min_cm: 50, take: 0}] alone it is a size
            # rule (`daily/size`, like "none under 30 cm") and speaks only about the big ones.
            if self.take is not None and self.lengths \
                    and all(b.take is not None for b in self.lengths):
                e.append(f"take={self.take} fills no band — every range in `lengths` carries its "
                         f"own take. Leave `take` out: 'no trout over 50 cm' is "
                         f"[{{min_cm: 50, take: 0}}], a size rule that says nothing about "
                         f"smaller fish")
            # `lengths` IS THE ONLY SIZE FIELD. over_cm/under_cm/band WERE HERE and are refused
            # on load (`extra=forbid`): checked for equality against them, `lengths` could only
            # ever say what they could, and "Wild cutthroat trout daily quota = 2 (none 40 cm or
            # more)" — 40 cm on the forbidden side — was rejected as a mismatch.
        else:
            # `lengths` IS ON THIS LIST. It was kept off it for the document rule type, where a
            # size named WHICH FISH need a stamp — that type is gone, and a licensing record says
            # it in `Doing.lengths`. On a bait or hook rule a size was accepted and dropped from
            # the label: "Bait ban" for a rule the file said held only over 50 cm.
            for f in ("take", "unlimited", "per_daily", "within"):
                if getattr(self, f) not in (None, False):
                    e.append(f"{f} belongs to retention_limit, not {t.value}")
            if self.lengths:
                e.append(f"lengths belongs to retention_limit, not {t.value} — a size a "
                         f"licence depends on is the licensing record's `doing.lengths`")

        # A bait or tackle rule is not scoped to what you may CATCH — "banned for all angling and
        # for all species". 18 corpus rules carry codes leaked from a co-located catch-and-release
        # clause ("Trout/char catch and release, bait ban"), which reads narrower than the law.
        # But a rule may be scoped to what you are FISHING FOR — a stream can carry a salmon bait
        # ban and no other — and that is `when_targeting`.
        # A `note` IS THE ESCAPE FROM A CLOSED VOCABULARY (`GearWhen`, `GearSpec`), and it costs a
        # `review_reason`: without one the gap is absorbed into a clause that reads complete.
        noted = [c.slot.value for c in self.gear
                 if (c.when is not None and c.when.note)
                 or any(u.note for u in c.unless)
                 or (c.requires is not None and c.requires.note)]
        if noted and not self.review_reason:
            e.append(f"gear {sorted(set(noted))}: a `note` says what the vocabulary cannot — give "
                     f"the rule a review_reason naming what is missing")
        if t in (RuleType.bait_restriction, RuleType.tackle_restriction):
            if self.species:
                e.append(f"{t.value} must not carry `species` — a bait or hook rule binds all "
                         f"species you may catch. If the rule applies only when fishing FOR "
                         f"something, use `when_targeting`.")
            e += species_problems(set(self.when_targeting) - FEDERAL_SALMON, "when_targeting")
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
        if t is not RuleType.vessel_rule and self.level is not None:
            e.append("level belongs to vessel_rule — a boat you may not ANGLE from is a method "
                     "rule's `when: {angler: in_boat | in_powered_boat}`")
        # "No angling from POWERED boats" banned as `in_boat` forbids a canoe the book allows.
        if any(c.when is not None and c.when.angler is AnglerState.in_boat for c in self.gear) \
                and re.search(r"\bpowered\s+boats?\b", self.verbatim, re.I):
            e.append("the sentence says POWERED boats — the clause's angler is in_powered_boat, "
                     "or the ban reaches a canoe the book allows")
        if t is not RuleType.vessel_rule and (self.aspect is not None or self.max_power_kw
                                              is not None or self.max_kmh is not None):
            e.append("aspect, max_power_kw and max_kmh belong to vessel_rule")
        if t is RuleType.angler_closure:
            if self.closed_to is None:
                e.append("angler_closure needs closed_to — a closure to everyone is a "
                         "retention_limit with take 0, may_target false")
            if self.species or self.species_except:
                e.append("angler_closure closes the water to an angler, not to a species")
            err = _check_residency(self.closed_to, self.verbatim, "closed_to")
            if err:
                e.append(err)
        elif self.closed_to is not None:
            e.append("closed_to belongs to angler_closure")
        if self.closed_to_except:
            if t is not RuleType.angler_closure or self.closed_to is None:
                e.append("closed_to_except carves anglers out of an angler_closure's closed_to")
            else:
                idle = [w.key() for w in self.closed_to_except if not w.overlaps(self.closed_to)]
                if idle:
                    e.append(f"closed_to_except {idle} does not meet closed_to — it subtracts "
                             f"nothing")
        # A GEAR RULE SAYS WHAT IT CONSTRAINS IN `gear` OR `conduct`. The direction lives inside
        # the clause that carries the subject; with neither, the rule states nothing a reader
        # can act on. An exemption is the one other thing such a rule may be — it lifts a clause
        # stated elsewhere ("EXEMPT from single barbless hooks").
        said = bool(self.gear) or bool(self.conduct) or bool(self.exempts)
        if t in (RuleType.method_rule, RuleType.tackle_restriction,
                 RuleType.bait_restriction, RuleType.handling_rule) and not said:
            e.append(f"{t.value} needs `gear`, `conduct` or `exempts` — it states nothing else")

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
        # THE PROMPT'S "never beside `includes_tributaries: false`": the row's tributaries
        # WITHOUT its own water, and then not its tributaries either, binds nothing at all. (The
        # designation refuses the same pair — `Designation._check`.)
        if self.tributaries_only and self.includes_tributaries is False:
            e.append("tributaries_only with includes_tributaries: false binds nothing — the "
                     "rule walks the row's tributaries without the row's own water")

        # `whole` SAYS THE WHOLE WATER; `extent_text` SAYS A PART NOTHING COULD DRAW. Both at once
        # is the shape the parser wrote on every part-lake rule — "No Fishing in Salmon Arm Bay"
        # stored as `whole` with the bay in `extent_text` — and the reach builder reads only the
        # first, so the closure bound all of Shuswap Lake. A place that cannot be drawn keeps its
        # words and NO extents, and stays unbound (AGENTS 13). A `whole` qualified by an item, an
        # area or a kind is a place of its own; its text only describes it.
        if bare_whole(self.extents) and self.extent_text.strip():
            e.append(f"extents are the whole water but extent_text names a place "
                     f"({self.extent_text[:60]!r}) — a part nothing can draw is written as "
                     f"`undrawn_part` beside the whole water (shown as a note, never coloured), "
                     f"or keeps its words with no extents (unbound), or is bound to the part")
        # `undrawn_part` IS A PART OF WHAT THE EXTENTS DRAW, and it is the rule's place phrase.
        if self.undrawn_part.strip():
            if not self.extents:
                e.append("undrawn_part needs extents — it is a part OF what they draw; a place "
                         "with nothing to be a part of keeps `extent_text` and stays unbound")
            if self.extent_text.strip():
                e.append("undrawn_part and extent_text are both the rule's place phrase — keep "
                         "one: undrawn_part when the rule binds more than it holds in")
            if self.standing:
                e.append("a standing rule holds at places no dataset draws, everywhere — it is "
                         "never also limited to one undrawn part")
        elif self.undrawn_part:
            e.append("undrawn_part is blank")
        # ONE HALF OF THE CHANNEL (`side`): what the sentence prints, both ways — a rule printed
        # "on the west half of river" that lost it would close the whole width on every screen.
        half = HALF_OF_CHANNEL.search(self.verbatim or "")
        printed = ChannelSide(half.group(1).lower()) if half else None
        if printed is not None and self.side is not printed:
            e.append(f"the sentence says the {printed.value} half of the channel — set side: "
                     f"{printed.value} (the rule holds on that half only)")
        if self.side is not None:
            if printed is None:
                e.append(f"side: {self.side.value} — the sentence prints no '{self.side.value} "
                         f"half of the river'")
            if not self.extents:
                e.append("side needs extents — it is a half of the stretch they draw")
            if self.undrawn_part.strip():
                e.append("side and undrawn_part: a half of a drawn stretch is placed on it "
                         "(side); an undrawn part is not — keep one")
            if HALF_OF_CHANNEL.search(self.extent_text or ""):
                e.append("extent_text repeats the half of the channel `side` says — keep only the "
                         "stretch in extent_text")
        # A LIFE STAGE (`life_stage`): what the sentence prints, both ways, and only for the fish
        # the book defines it for — "adult chinook" read as every chinook is a different rule.
        adult = bool(_ADULT_CHINOOK.search(self.verbatim or ""))
        if adult and self.life_stage is not LifeStage.adult:
            e.append("the sentence says 'adult chinook' — set life_stage: adult (the rule holds "
                     "for adult chinook only, p.77)")
        if self.life_stage is not None:
            fish = LIFE_STAGE_FISH[self.life_stage]
            if list(self.species) != [fish]:
                e.append(f"life_stage {self.life_stage.value} is defined for {fish} only (p.77) — "
                         f"species must be [{fish}], not {list(self.species)}")
            if self.life_stage is LifeStage.adult and not adult:
                e.append("life_stage adult — the sentence prints no 'adult chinook'")
        # CHINOOK IS NOT A GAME FISH (p.80; user ruling 2026-09-28): it is a salmon
        # (`SALMON_FISH`). Excepted from a set that holds no salmon, it subtracts nothing — "all
        # game fish other than chinook" reads as if chinook were one.
        salmon_sets = {"SALMON", "ALL_FIN_FISH"} | set(SALMON_FISH)
        idle = [c for c in self.species_except if c in SALMON_FISH]
        if idle and not (set(self.species) & salmon_sets):
            e.append(f"species_except {idle}: chinook is a salmon, not a game fish (p.80) — "
                     f"{list(self.species)} never held it, so the exception subtracts nothing")
        # A PLACE IS NOT A LIST ITEM. The book numbers its lists; a place phrase that starts with
        # a marker was cut out of one, and every label built from it would print the marker.
        for f in ("extent_text", "undrawn_part"):
            if LIST_MARKER.match(getattr(self, f) or ""):
                e.append(f"{f} starts with a list marker ({getattr(self, f)[:20]!r}) — it "
                         f"names a place, not a list item")
            # A PLACE CARRIES NO RULE VALUE — see `PLACE_VALUE`.
            leak = PLACE_VALUE.search(getattr(self, f) or "")
            if leak:
                e.append(f"{f} {getattr(self, f)[:60]!r} carries a rule value "
                         f"({leak.group(0)!r}) — a place says where; the value is the rule's own "
                         f"field")
        # THE CLOCK BELONGS TO A NUMBER OF FISH. On a bait ban or an advisory it says nothing.
        if self.period is not None and t not in _COUNTED_TYPES:
            e.append(f"period belongs to {' and '.join(x.value for x in _COUNTED_TYPES)}, "
                     f"not {t.value} — a clock with no number on it says nothing")
        e += _extents_the_resolver_reads(self.extents)
        # THE DATES ITS OWN SENTENCE PRINTS — see `_own_dates_carried`.
        err = _own_dates_carried(self)
        if err:
            e.append(err)
        # "ALL OTHER SPECIES" IS GAME FISH — see `_all_species_is_game_fish`.
        err = _all_species_is_game_fish(self)
        if err:
            e.append(err)
        # FEATURE TYPES ARE APPLIED TO THE RULE'S WHOLE REACH, after any tributary walk
        # (`reach.classify`), so they must be said on every extent or on none — a union of
        # "lakes of this watershed" with "that river, every kind" has no one filter.
        kinds = [bool(x.get("feature_types")) for x in (self.extents or []) if isinstance(x, dict)]
        if any(kinds) and not all(kinds):
            e.append("feature_types is set on some extents and not others — the builder applies "
                     "one filter to the rule's reach; split the rule")
        # A CLOSURE'S NAME IS A CLOSURE'S. On anything but "no fishing" it names nothing, and
        # where the sentence prints the name itself the field may only repeat it.
        if self.closure_kind is not None:
            if not (t is RuleType.retention_limit and self.take == 0
                    and self.may_target is False):
                e.append("closure_kind names a closure — a retention_limit with take 0 and "
                         "may_target false")
            printed = printed_closure_kinds(self.verbatim)
            if printed and self.closure_kind.value not in printed:
                e.append(f"closure_kind {self.closure_kind.value!r}, but the sentence names "
                         f"the {'/'.join(sorted(printed))} closure")

        if e:
            raise ValueError(f"{self.rule_id}: " + "; ".join(e))
        return self
