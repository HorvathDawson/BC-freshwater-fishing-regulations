"""The gear model: `Slot`, `GearWhen`, `GearSpec`, `GearClause`, and the conduct acts.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from enum import Enum
from typing import Dict, List, Optional
from pydantic import ConfigDict, Field, model_validator

from .vocab import Method, WaterKind, _Terse
from .species import FEDERAL_SALMON, species_problems


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


#: WHAT `while` MAY NAME — TWO KINDS OF TOKEN, and a reader must not treat them alike.
#:
#: MEANS are ways of fishing: every `Method` member. "Sport fishing" is DEFINED as angling, spear
#: fishing, set lining and crayfish trapping, and the book grants each of them (with ice fishing)
#: in its "Allowable Fishing Methods" list; a means the book grants nowhere is not a lawful way to
#: sport fish at all ("All other methods of taking fin fish and crayfish are illegal").
#:
#: DEVICES are things you use WHILE angling — a downrigger, a light. A rule may bind while one is
#: in use (its spec slot says how it must be rigged), but a device is not a way of fishing and
#: nothing grants or bans it as one; that is why they left `Method` (see the note there).
#:
#: TWO TOKENS WERE HERE AND ARE NOT. `alone_in_a_boat` was accepted by the validator alone — no rule
#: used it, and it duplicated `GearWhen.angler`, the one place a boat condition is written.
#: `ice_hut` was accepted because it is a spec slot, but an ice hut is not something you DO: its
#: slot binds `while: ["ice_fishing"]`. Both are refused.
WHILE_MEANS = frozenset(m.value for m in Method)
WHILE_DEVICES = frozenset({Slot.downrigger.value, Slot.light.value})
WHILE_TOKENS = WHILE_MEANS | WHILE_DEVICES

#: HOW A FISH WAS CAUGHT — what `caught` may name, with the words a reader says it in (user ruling
#: Q38/G6, 2026-10-07). A condition on the FISH, never on the angler's means: "Any fish willfully or
#: accidentally snagged must be released immediately" (p.8, p.80) binds a fish hooked anywhere but
#: the mouth however it happened — an accidental snag happens WHILE ANGLING, so `while: snagging`
#: (the angler's means) could not reach it. Each token's words: `as` the fish ("snagged"), `even`
#: what the book adds ("even by accident").
#: The words that print a snag (foul hook) in a rule's sentence (`_caught_is_a_fish_condition`).
_SNAGGED = re.compile(r"\bsnag|\bfoul[- ]?hook", re.I)
CAUGHT_HOW: Dict[str, Dict[str, str]] = {
    "foul_hooked": {"as": "snagged", "even": "even by accident",
                    "term": "snagged (foul-hooked: hooked anywhere but the mouth)"},
}


class AnglerState(str, Enum):
    alone_in_boat = "alone_in_boat"
    in_boat = "in_boat"
    #: "No angling from POWERED boats": a canoe is still allowed, so this is not `in_boat`.
    in_powered_boat = "in_powered_boat"
    from_shore = "from_shore"


#: An angler's state in words, after a gear clause: "no angling (from a boat)".
_ANGLER_WORDS = {"alone_in_boat": "alone in a boat", "in_boat": "from a boat",
                 "in_powered_boat": "from a powered boat", "from_shore": "from shore"}


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

    @model_validator(mode="after")
    def _book_species(self) -> "GearWhen":
        # A target is a species code like any other: the book's list (p.80), never a raw code.
        bad = species_problems(set(self.targeting) - FEDERAL_SALMON, "when.targeting")
        if bad:
            raise ValueError("; ".join(bad))
        return self

    def is_empty(self) -> bool:
        return not (self.water or self.method or self.targeting or self.angler
                    or self.gear_in_use or self.note)

    def key(self) -> str:
        """The condition as a stable key, for a rule's `dimension`: "angler=in_boat"."""
        bits = [f"{k}={getattr(v, 'value', v)}" for k, v in
                (("water", self.water), ("method", self.method), ("angler", self.angler),
                 ("gear_in_use", self.gear_in_use)) if v]
        if self.targeting:
            bits.append("targeting=" + "+".join(sorted(self.targeting)))
        if self.note:
            bits.append("note")
        return "&".join(bits)


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
    #: NO CEILING, SAID OUTRIGHT. "A person in a boat may angle with an unlimited number of rods"
    #: is a bound, not an absence of one: without it the clause has nothing to state and the
    #: sentence could only be an exemption, which hides what it grants. It was once written
    #: `max_lines: 0` — a zero standing for infinity — and converted to "no lines at all". JSON has
    #: no infinity, so the word is the value, as `unlimited` is on a retention rule.
    unlimited: bool = False
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
        counted = self.max is not None or self.min is not None or self.unlimited
        if self.unlimited and (self.max is not None or self.slot in _MEASURED
                               or self.slot in _SET_SLOTS or self.slot in _SPEC_SLOTS):
            raise ValueError(f"{self.slot.value}: `unlimited` lifts the ceiling on a COUNT, and "
                             f"never alongside a `max`")
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
                raise ValueError(f"{self.slot.value} needs a max, a min or `unlimited`")
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
    #: `be_accompanied_by_licensed_adult` WAS HERE, holding up the under-16 non-resident rule: a
    #: `required: false` basic licence with the accompaniment parked on it as a duty. Accompaniment
    #: is a way of SATISFYING a requirement, so it is a `Path` (`accompanied_by`, with the quota
    #: note) on `zp:basic_licence#under_16_non_resident`, and the token is gone.
    "do_not_waste_catch": "Do not waste the fish you catch",
    "do_not_release_harmfully": "Do not release fish in a harmful manner",
    "do_not_buy_sell_or_barter_catch": "Do not buy, sell or barter your catch",
    "do_not_possess_or_move_live_fish": "Do not possess or move live fish or invertebrates",
    # Printed p. 8, "It Is Unlawful To…", the continuation of the live-fish bullet.
    "do_not_keep_catch_alive": "Do not hold your catch alive (livewell, other device or stringer)",
    "do_not_release_aquarium_fish": "Do not release your aquarium fish to the wild",
    "do_not_high_grade": "Do not high-grade",
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
    # Licensing duties — what you must DO with a licence, never whether you need one.
    "produce_licence_on_request":
        "Produce your angling licence and photo ID when an officer asks",
    "carry_paper_licence": "Carry your paper licence",
    # Printed p. 10, "Transporting and Exporting Fish": what you must do when you move fish.
    "keep_licence_handy_while_travelling": "Have your angling licence at hand when you travel with fish",
    "transport_no_more_than_legal_limit": "Do not transport or possess more than your legal limit",
    "keep_catch_identifiable": "Make sure your fish can be identified, counted and measured",
    "carry_signed_letter_when_transporting_for_another":
        "Carry a signed letter from the angler when you transport fish for someone else",
    "show_letter_when_exporting":
        "Carry the letter and show it on request when you export the fish from B.C.",
    "keep_signed_letter_for_gifted_fish":
        "Keep a signed letter from the angler until you have eaten fish given to you",
    "do_not_enter_land_without_permission":
        "Do not enter or cross cultivated, posted or private land, or Indian Reserve land, "
        "without permission",
}

#: THE ACTS THAT ARE ABOUT A DOCUMENT. Producing or carrying a licence means nothing to an angler
#: who need not hold one, so a requirement carrying one of these must say which documents it
#: presumes (`Requirement.presumes`) — that is what lets an exemption from them release it too.
DOCUMENT_ACTS = frozenset({"produce_licence_on_request", "carry_paper_licence"})

#: HOW A SENTENCE NAMES A DOCUMENT A DUTY PRESUMES, on the squashed (lower-cased) text. A document
#: missing here cannot be presumed at all — adding one is a reviewed change, like an act.
_PRESUMED_SAID = {
    #: plural too: "paper licences are required when retaining hatchery steelhead, chinook, …"
    "basic_licence": r"\b(?:angling|paper|basic(?: \w+)?|fishing) licen[cs]es?\b",
    "classified_waters_licence": r"\bclassified waters? licen[cs]e\b",
    "steelhead_stamp": r"\bsteelhead (?:conservation surcharge )?stamp\b",
    "salmon_stamp": r"\bsalmon (?:conservation surcharge )?stamp\b",
}
