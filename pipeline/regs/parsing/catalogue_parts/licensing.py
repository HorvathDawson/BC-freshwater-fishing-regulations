"""Licensing — the records on `CatalogueEntry.licensing` (`Who`, designations, requirements,
licence terms, exemptions, alternatives), not a rule type.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import Annotated, List, Literal, Optional, Union
from pydantic import ConfigDict, Field, model_validator

from .vocab import Document, Origin, WaterKind, _Terse
from .text import squash
from .dates import When, _days
from .species import expand_species, species_problems
from .gear import CONDUCT_ACTS, DOCUMENT_ACTS, _PRESUMED_SAID
from .lengths import LengthBand
from .checks import _dates_are_printed, _extents_the_resolver_reads


# --------------------------------------------------------------------------------------- #
# Licensing — a separate list on the entry, not a rule type.
#
# `document_required` and `access_permission` were rule types, and a type is a wall the
# override cannot cross (the invariant at the top of this module). Licensing never competes:
# the only "overriding" in 169 licensing rules was `required: false`, and that was the defect —
# nine steelhead-stamp waivers shared a dimension with the provincial "stamp if you fish for
# steelhead", so a narrower scope struck it and a steelhead angler on the Chilko needed no stamp.
# Licensing also depends on two inputs no rule takes — WHO the angler is and what they are DOING.
#
# So it is six small records on `CatalogueEntry.licensing`, discriminated by `kind`:
#
#   designation     a FACT about a water: this reach is Classified, class I/II, in licence unit U,
#                   during these days; the classified-water steelhead stamp runs / is waived here;
#                   it sleeps while a named closure binds
#   not_classified  an asserted ABSENCE ("Part described is NOT a Classified Water")
#   requirement     an OBLIGATION, stated once: this WHO, DOING this, WHERE/WHEN, must satisfy
#                   one of these paths (hold documents / be accompanied)
#   licence_terms   how a document is SOLD — never bound to a section, never an obligation
#   exemption       a named WHO released from named documents
#   alternative     a place where another document ALSO satisfies a requirement
#
# LICENSING NEVER VOTES ON OPEN/CLOSED. It is consulted only where a water is open for the
# activity, which is why "Class II water WHEN OPEN" needs no field: a closed water needs no licence
# by construction. The one licensing-adjacent closure — "angling prohibited for non-guided
# non-resident aliens on Saturdays" — is a closure, and stays a RULE (`angler_closure`).
#
# THE ANGLER IS ALWAYS UNKNOWN. Every axis of `Who` is a set with the complement written out, so a
# reader can answer "if you are a non-resident …" without assuming a default profile.
# --------------------------------------------------------------------------------------- #

#: Every axis a `Who` may constrain, and every member of it. An axis left out means ANY member.
WHO_AXES: dict[str, tuple[str, ...]] = {
    "residency": ("resident", "non_resident", "non_resident_alien"),
    "age": ("under_16", "16_plus"),
    "guidance": ("guided", "non_guided"),
    #: `aged_65_plus` (2026-10-03): "Annual Licence for Age 65 Plus" (p.5) is sold to B.C.
    #: residents 65 and over. Not an `age` member — `age` is a partition (under 16 / 16 and over)
    #: and 65-plus lies inside 16-plus — so it is a status, like `disabled`.
    "status": ("indian_bc_resident", "metis", "disabled", "aged_65_plus"),
    #: WHAT THE ANGLER IS DOING FOR SOMEONE ELSE. A Youth/Disabled Accompanied Water (printed
    #: p.4) is closed to every angler who is not an "authorized angler" (under 16, or a disabled
    #: resident) or "a companion to an authorized angler" — and a companion is not a residency,
    #: an age or a status: it is the role of the adult standing beside the child. Not a
    #: partition (most anglers are no one's companion), like `status`.
    "role": ("companion",),
}
#: The axes every angler sits on exactly one member of. Naming all of them is "everyone", which
#: has one spelling: say nothing. `status` is not a partition — most anglers hold none of the
#: three — so naming all three is a real (if odd) set and is not refused.
_PARTITION_AXES = ("residency", "age", "guidance")

Residency = Literal["resident", "non_resident", "non_resident_alien"]
Age = Literal["under_16", "16_plus"]
Guidance = Literal["guided", "non_guided"]
Status = Literal["indian_bc_resident", "metis", "disabled", "aged_65_plus"]
Role = Literal["companion"]


class Who(_Terse):
    """WHICH ANGLERS — a set on every axis, the included members listed.

    This replaces `AnglerClass`, whose `residency` held ONE value. "Non-resident anglers" in the
    synopsis covers non-residents AND non-resident aliens, so `basic_licence.r4` ("not a resident
    of B.C.") left the under-16 alien out; "Canadian resident" became `resident` (= B.C.) and
    dropped every other Canadian. `guided: false` was a polarity bit. Here a list says who is IN,
    and there is nothing to invert.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    residency: List[Residency] = Field(default_factory=list)
    age: List[Age] = Field(default_factory=list)
    guidance: List[Guidance] = Field(default_factory=list)
    status: List[Status] = Field(default_factory=list)
    role: List[Role] = Field(default_factory=list)

    @model_validator(mode="after")
    def _one_spelling(self) -> "Who":
        said = False
        for axis in WHO_AXES:
            vals = getattr(self, axis)
            if len(set(vals)) != len(vals):
                raise ValueError(f"who.{axis}: {vals} names a member twice")
            if axis in _PARTITION_AXES and set(vals) == set(WHO_AXES[axis]):
                raise ValueError(f"who.{axis}: naming every member is everyone — leave the axis "
                                 f"out instead (one spelling for 'any')")
            said = said or bool(vals)
        if not said:
            raise ValueError("an empty `who` is everyone — leave it out instead")
        return self

    def members(self, axis: str) -> frozenset:
        """The members this names on `axis`, or every member when the axis is left out."""
        return frozenset(getattr(self, axis) or WHO_AXES[axis])

    def overlaps(self, other: "Who") -> bool:
        """True when at least one angler is in both sets."""
        return all(self.members(a) & other.members(a) for a in WHO_AXES)

    def key(self) -> str:
        """A canonical spelling, for a dimension or a comparison."""
        return ";".join(f"{a}={','.join(sorted(getattr(self, a)))}"
                        for a in WHO_AXES if getattr(self, a))

    def words(self) -> str:
        """"non-guided non-resident aliens", "non-residents or non-resident aliens under 16"."""
        res = {"resident": "B.C. residents", "non_resident": "non-residents",
               "non_resident_alien": "non-resident aliens"}
        st_noun = {"indian_bc_resident": "Indians resident in B.C.", "metis": "Métis anglers",
                   "disabled": "disabled anglers", "aged_65_plus": "anglers aged 65 and over"}
        st_adj = {"indian_bc_resident": "Indian", "metis": "Métis", "disabled": "disabled"}
        # "aged 65 and over" FOLLOWS the noun: "B.C. residents aged 65 and over"
        aged = "aged 65 and over" if "aged_65_plus" in self.status else ""
        pre = [x for x in self.status if x != "aged_65_plus"]
        adj = " or ".join({"guided": "guided", "non_guided": "non-guided"}[g]
                          for g in self.guidance)
        if self.residency:
            noun = " or ".join(res[r] for r in self.residency)
            if pre:
                noun = " or ".join(st_adj[s] for s in pre) + " " + noun
            if aged:
                noun = f"{noun} {aged}"
        elif self.status:
            noun = " or ".join(st_noun[s] for s in self.status)
        elif self.role:
            noun = " or ".join({"companion": "companions of an authorized angler"}[r]
                               for r in self.role)
        else:
            noun = "anglers"
        age = " or ".join({"under_16": "under 16", "16_plus": "16 and over"}[a]
                          for a in self.age)
        return " ".join(x for x in (adj, noun, age) if x)


def residency_said(text: str) -> Optional[frozenset]:
    """THE RESIDENCY A SENTENCE NAMES, read off its own words — the check the model cannot talk
    its way past, because the verbatim is copied, not authored.

    The synopsis's idioms, and what each one MEANS:
      "non-resident alien(s)"          -> non_resident_alien
      "non-resident(s)" (not "…alien") -> non_resident + non_resident_alien — "non-resident
                                          anglers" covers both, which is the Kootenay defect
      "not a resident of B.C."         -> non_resident + non_resident_alien
      "Canadian resident"              -> resident + non_resident
      "B.C. resident" / "resident of B.C." -> resident
    None when the sentence names no residency at all."""
    t = squash(text)
    out: set = set()
    for pat, got in ((r"canadian residents?", {"resident", "non_resident"}),
                     (r"not a resident of b\.?\s?c", {"non_resident", "non_resident_alien"}),
                     (r"non-resident aliens?", {"non_resident_alien"}),
                     (r"non-residents?", {"non_resident", "non_resident_alien"}),
                     (r"b\.?\s?c\.? residents?|resident of b\.?\s?c", {"resident"})):
        if re.search(pat, t):
            out |= got
            t = re.sub(pat, " ", t)
    return frozenset(out) or None


def guidance_said(text: str) -> Optional[frozenset]:
    """THE GUIDANCE A SENTENCE NAMES: "non-guided"/"unguided" -> non_guided, "guided" -> guided,
    both ("whether GUIDED or NON-GUIDED") -> both, which is everyone. None when it names neither."""
    t = squash(text)
    out: set = set()
    if re.search(r"non-?\s?guided|unguided", t):
        out.add("non_guided")
        t = re.sub(r"non-?\s?guided|unguided", " ", t)
    if re.search(r"\bguided\b", t):
        out.add("guided")
    return frozenset(out) or None


def _check_residency(who: Optional["Who"], verbatim: str, where: str) -> Optional[str]:
    """The sentence's own residency and guidance words bind the `who`, in BOTH directions: a
    `who` that names a different set is wrong, and so is one that names NONE when the sentence
    names one — "angling prohibited for non-guided non-resident aliens" with `closed_to:
    {guidance: [non_guided]}` validated and closed the water to every non-guided resident."""
    have = frozenset(who.residency) if who is not None else frozenset()
    said = residency_said(verbatim)
    if said is not None and said != have:
        return (f"{where}: residency {sorted(have) or 'any'} is not what the sentence says "
                f"({sorted(said)}) — 'non-resident' covers aliens too; 'Canadian resident' covers "
                f"non-residents")
    have_g = frozenset(who.guidance) if who is not None else frozenset()
    said_g = guidance_said(verbatim)
    if said_g is not None:
        want = frozenset() if len(said_g) == 2 else said_g      # both named = everyone
        if have_g != want:
            return (f"{where}: guidance {sorted(have_g) or 'any'} is not what the sentence says "
                    f"({sorted(want) or 'guided or not'})")
    return None


#: A licence unit id — what a non-resident's per-day Classified Waters Licence names.
_SLUG = r"^[a-z0-9]+(_[a-z0-9]+)*$"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")


#: THE PROVINCIAL ANGLER DOCUMENTS — every licence or stamp the Wildlife Act sells an angler.
#: "you are not required to obtain ANY TYPE of fishing licence or stamp" means exactly these, and
#: a test pins the Indian-resident exemption to this set so a stamp added later is not silently
#: left off it.
PROVINCIAL_ANGLER_DOCUMENTS = (
    "basic_licence", "steelhead_stamp", "salmon_stamp", "kootenay_rainbow_stamp",
    "shuswap_char_stamp", "shuswap_rainbow_stamp", "white_sturgeon_licence",
    "classified_waters_licence",
)


class Ref(_Terse):
    """A licensing record in another entry, by `(entry_id, id)` — ids are unique only within an
    entry, as rule ids are (AGENTS 8)."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    entry_id: str = Field(..., min_length=1)
    id: str = Field(..., min_length=1)


class Quote(_Terse):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    verbatim: str = Field(..., min_length=1)


class StampPeriod(_Terse):
    """WHEN the classified-water steelhead stamp runs on this designation, and the words that
    said so."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    when: When
    verbatim: str = Field(..., min_length=1)


class Suspension(_Terse):
    """"… not required until reopened to steelhead fishing": a closure rule in THIS entry, by id,
    while which the designation is dormant. Validated at the entry: the rule must exist and must
    be a closure — a designation can be suspended by a closure, never by a quota."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    rule_id: str = Field(..., min_length=1)
    verbatim: str = Field(..., min_length=1)


_CLASS_SAID = re.compile(r"\bclass (ii|i|1|2) waters?\b")
_WAIVED_SAID = re.compile(r"steelhead stamp not (?:required|mandatory)")
_DURING_SAID = re.compile(r"steelhead stamp (?:is )?mandatory|until\s+reopened")
#: "Steelhead Stamp not required UNLESS fishing for steelhead" — a waiver of the classified-water
#: stamp only (`Designation.waives_every_stamp`).
_CONDITIONAL_WAIVER = re.compile(r"\bunless\b")
_UNIT_SAID = re.compile(r"([a-z][a-z .']*?) classified licence required for non-resident")


class Designation(_Terse):
    """A FACT ABOUT A WATER: while the date is in `when` and a section is bound, that section is a
    Classified Water of class `classified`, in licence unit `unit`.

    It obliges nothing by itself. The provincial requirement ("hold a Classified Waters Licence
    when fishing on a stream during the period when it is classified") fires on it, and so does the
    classified-water steelhead stamp during `steelhead_stamp_during`.

    `steelhead_stamp_waived` comes in two kinds, read off its own quote (`waives_every_stamp`):
      * OUTRIGHT — "(Steelhead Stamp not required)": Chilko upstream of Brittany Creek June 11-Oct
        31, Horsefly, West Road mainstem, both Stellako rows, the Dean from Anahim Lake to the
        Iltasyuko. NO steelhead stamp at all on this designation's sections while it is in force
        (user ruling 2026-10-02): the classified-water stamp, AND the provincial "stamp if you
        fish for steelhead" — which says so itself (`Requirement.waived_where`), so a waiver can
        lift only a requirement that consents to it. The steelhead RULES still hold there (release
        wild, the annual 10, the record duty), and so does the Classified Waters Licence: it is
        still Class II water.
      * CONDITIONAL — "not required unless fishing for steelhead" (Seymour, Ecstall, Skeena River
        2): the classified-water stamp only; whoever fishes for steelhead still needs the
        provincial stamp.

    `unit` is what a non-resident's per-day licence NAMES. "Class II water when open, including
    tributaries - Michel Creek classified licence required for non-resident anglers" made two
    claims — everyone needs a CWL here (the water is classified), and the non-resident's licence
    says "Michel Creek" — and was stored as one rule scoped to non-residents, which told B.C.
    residents they needed nothing on nine Kootenay waters.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["designation"] = "designation"
    id: str = Field(..., min_length=1)
    classified: Literal["I", "II"]
    unit: str = Field(..., pattern=_SLUG)
    unit_name: str = Field(..., min_length=1)
    #: The classified period. Absent or empty = all year — which includes "when open".
    when: Optional[When] = None
    extents: Optional[List[dict]] = None
    includes_tributaries: Optional[bool] = None
    tributaries_only: bool = False
    tributary_excludes: List[dict] = Field(default_factory=list)
    #: AT MOST ONE OF THESE TWO. Direction lives in the key; there is no bool.
    steelhead_stamp_during: Optional[StampPeriod] = None
    steelhead_stamp_waived: Optional[Quote] = None
    suspended_while: List[Suspension] = Field(default_factory=list)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Designation":
        e: List[str] = []
        during, waived = self.steelhead_stamp_during, self.steelhead_stamp_waived
        if during and waived:
            e.append("steelhead_stamp_during and steelhead_stamp_waived are opposite facts — "
                     "one of them, or neither")
        # THE STAMP PERIOD SITS INSIDE THE CLASSIFIED PERIOD, on the circular year. The stamp
        # is a consequence of the water being classified; outside that period there is nothing
        # for it to be a consequence of.
        if during and during.when.dates and self.when and self.when.dates:
            if not _days(during.when.dates) <= _days(self.when.dates):
                e.append(f"the stamp period ({during.when.words()}) runs outside the classified "
                         f"period ({self.when.words()})")
        # IDIOMS SEEDED FROM THE VERBATIM. The quote is copied, not authored, so a field that
        # disagrees with it is the field that is wrong.
        m = _CLASS_SAID.search(squash(self.verbatim))
        if m:
            said = {"i": "I", "1": "I", "ii": "II", "2": "II"}[m.group(1)]
            if said != self.classified:
                e.append(f"verbatim says Class {said}, classified is {self.classified}")
        if during and during.when.is_empty():
            # AN EMPTY PERIOD IS "ALL YEAR" under `When`'s own rule, so an empty stamp period
            # would put the stamp on the water every day of the year — while the page printed a
            # window, or printed that it could not say. A period the page does not give is
            # written in `unparsed` (the Atnarko's "from reopening"), never left empty.
            e.append("steelhead_stamp_during.when is empty, which reads as all year — give the "
                     "printed dates, or say in `unparsed` why there are none")
        if during and not _DURING_SAID.search(squash(during.verbatim)):
            e.append("steelhead_stamp_during quotes no 'Steelhead Stamp mandatory'")
        if during and _WAIVED_SAID.search(squash(during.verbatim)) \
                and "until reopened" not in squash(during.verbatim):
            e.append("steelhead_stamp_during quotes a waiver — that is steelhead_stamp_waived")
        if waived and (not _WAIVED_SAID.search(squash(waived.verbatim))
                       or "until reopened" in squash(waived.verbatim)):
            e.append("steelhead_stamp_waived must quote 'Steelhead Stamp not required/mandatory' "
                     "(and a waiver 'until reopened' is a suspension, not a waiver)")
        quoted = " ".join([self.verbatim] + [x.verbatim for x in (during, waived) if x]
                          + [s.verbatim for s in self.suspended_while])
        if "until reopened" in squash(quoted) and not self.suspended_while:
            e.append("'until reopened' is a suspension — name the closure in suspended_while")
        for err in (_dates_are_printed(self.when, self.verbatim, "when"),
                    _dates_are_printed(during.when if during else None,
                                       during.verbatim if during else "",
                                       "steelhead_stamp_during.when")):
            if err:
                e.append(err)
        m = _UNIT_SAID.search(squash(self.verbatim))
        if m and slug(m.group(1).split(" - ")[-1]) != self.unit:
            e.append(f"verbatim names the {m.group(1).strip()!r} licence; unit is {self.unit!r}")
        if self.tributaries_only and self.includes_tributaries is False:
            e.append("tributaries_only with includes_tributaries: false binds nothing")
        e += _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"designation {self.id}: " + "; ".join(e))
        return self

    @property
    def waives_every_stamp(self) -> bool:
        """An OUTRIGHT waiver — "(Steelhead Stamp not required)", with no "unless": no steelhead
        stamp of any kind on these sections while the designation is in force (see the class).
        "Not required unless fishing for steelhead" waives only the classified-water stamp."""
        return (self.steelhead_stamp_waived is not None
                and not _CONDITIONAL_WAIVER.search(squash(self.steelhead_stamp_waived.verbatim)))


class NotClassified(_Terse):
    """"Part described is NOT a Classified Water" — an asserted ABSENCE. It obliges nothing; the
    build (a later stage) refuses any designation that binds a section this binds, which is what
    forces the carve-out the parent's "including tributaries" implies to be written down."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["not_classified"] = "not_classified"
    id: str = Field(..., min_length=1)
    extents: Optional[List[dict]] = None
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _said(self) -> "NotClassified":
        if "not a classified water" not in squash(self.verbatim):
            raise ValueError(f"not_classified {self.id}: the verbatim does not say "
                             f"'not a Classified Water'")
        e = _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"not_classified {self.id}: " + "; ".join(e))
        return self


class Doing(_Terse):
    """WHAT THE ANGLER IS DOING — the trigger. A closed vocabulary:

      fishing             any sport fishing at all
      targeting           fishing FOR `species` ("if you fish for steelhead … keep or release")
      retaining           KEEPING `species`, of `lengths` when given ("to keep rainbow trout over
                          50 cm") — a stamp you need only if you keep the fish
      retaining_recorded  keeping a fish whose retention must be recorded on the licence; which
                          fish those are is said by the `record_retention` rules, not here
      guiding             acting as a guide for fish
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    act: Literal["fishing", "targeting", "retaining", "retaining_recorded", "guiding"]
    species: List[str] = Field(default_factory=list)
    species_except: List[str] = Field(default_factory=list)
    origin: Optional[Origin] = None
    lengths: Optional[List[LengthBand]] = None

    @model_validator(mode="after")
    def _check(self) -> "Doing":
        e: List[str] = []
        if self.act in ("targeting", "retaining") and not self.species:
            e.append(f"{self.act} needs species — fishing FOR what, keeping what")
        if self.species and self.act not in ("targeting", "retaining"):
            e.append(f"species belongs to targeting or retaining, not {self.act}")
        e += species_problems(set(self.species) | set(self.species_except), "doing.species")
        # AN EXCEPTION THAT SUBTRACTS NOTHING is a claim the data cannot make good on: "a salmon
        # … (other than kokanee)" was `species: [SALMON], species_except: [KO]`, and kokanee
        # is not in SALMON. The sentence is right (it is not a salmon for this purpose); the
        # except was noise that read as a carve-out.
        if self.species_except:
            inside = set(expand_species(list(self.species)))
            idle = [s for s in self.species_except if s not in inside]
            if idle:
                e.append(f"species_except {idle} is not in {self.species} — it subtracts nothing")
        if self.lengths is not None:
            if self.act != "retaining":
                e.append("lengths names WHICH FISH you keep — only with act: retaining")
            if not self.lengths:
                e.append("lengths: [] says nothing — leave it out")
            if any(b.take is not None for b in self.lengths):
                e.append("a band on a requirement names which fish, never how many — no `take`")
        if self.origin is not None and not self.species:
            e.append("origin qualifies a species")
        if e:
            raise ValueError("doing: " + "; ".join(e))
        return self


class Accompaniment(_Terse):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    who: Who
    #: The companion holds whatever THIS fishing requires of them — resolved by the reader, so
    #: the record never has to list documents it cannot know (class, stamp period, species).
    holding: Literal["what_this_fishing_requires"] = "what_this_fishing_requires"


class Path(_Terse):
    """ONE WAY TO SATISFY A REQUIREMENT. Exactly one of `hold` (ALL of these documents),
    `accompanied_by`, or `as` (satisfy the requirements AS this who instead).

    `quota` is a NOTE, not arithmetic: "any fish you keep must be counted as part of the catch and
    possession of your accompanying licence holder" is `quota: counts_to_companion`, which a reader
    renders (an asterisk, a line). The retention model does not aggregate across anglers."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    hold: List[Document] = Field(default_factory=list)
    accompanied_by: Optional[Accompaniment] = None
    as_: Optional[Who] = Field(default=None, alias="as")
    quota: Optional[Literal["own", "counts_to_companion"]] = None

    @model_validator(mode="after")
    def _one(self) -> "Path":
        ways = [bool(self.hold), self.accompanied_by is not None, self.as_ is not None]
        if sum(ways) != 1:
            raise ValueError("a path is exactly one of hold / accompanied_by / as")
        if len(set(self.hold)) != len(self.hold):
            raise ValueError(f"hold names a document twice: {self.hold}")
        # A PATH THAT CHANGES WHO CARRIES THE FISH MUST SAY WHOSE QUOTA IT IS.
        if self.hold and self.quota is not None:
            raise ValueError("quota belongs to a path that changes who carries the fish, "
                             "not to `hold`")
        if not self.hold and self.quota is None:
            raise ValueError("an accompanied_by or as path must say whose quota the catch is "
                             "(own | counts_to_companion)")
        return self


class Requirement(_Terse):
    """AN OBLIGATION, stated once: this `who`, `doing` this, `when` and where, must satisfy ANY ONE
    of `satisfied_by` — or, for a duty, do the `conduct`.

    WHERE is `extents` (None takes the entry's, at placement — `reach.licensing.place_record`;
    never `whole` by default. A RULE, unlike a record, always states its own) and/or
    `on`: `classified_period` is met wherever a designation is in force on a stream,
    `steelhead_period` wherever its `steelhead_stamp_during` also holds. Both together means both.

    `waived_where: steelhead_stamp_waived` is the one place a designation can LIFT a requirement,
    and the requirement consents to it by saying so: it does not hold on a section, on a day, where
    a designation bound to that section is in force (its `when`, and not asleep under
    `suspended_while`) and waives the Steelhead Stamp OUTRIGHT (`Designation.waives_every_stamp`:
    "(Steelhead Stamp not required)" — Chilko upstream of Brittany Creek June 11-Oct 31). User
    ruling 2026-10-02: such a waiver means no steelhead stamp at all there, the provincial
    "Conservation Surcharge Stamp to fish for steelhead" included (`zp:steelhead`
    `steelhead_targeting` and its twin). Only a requirement whose every path holds the Steelhead
    Stamp may carry it. Outside the designation's dates, or off its sections, the requirement holds
    as written. The reader is `read.requirements_in_force`.

    There is no `required` and no `permitted`. A requirement that does not apply to someone is
    `who` / `who_except`, an `Exemption`, or a designation fact — never "not required" as a value.

    `restates` marks the table's own words for an obligation stated elsewhere (Shuswap Lake's row
    repeating the provincial stamp) — kept so the water screen shows what the page printed, and
    pinned by a corpus test to add nothing to what it restates.

    `presumes` is for a DUTY ABOUT A DOCUMENT — "produce your angling licence", "carry your paper
    licence". The duty binds only an angler who must hold the documents it names, so an `Exemption`
    that releases all of them releases the duty too. Without it the duty named no document and an
    Indian resident of B.C., released from every licence, was still told to produce one.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["requirement"] = "requirement"
    id: str = Field(..., min_length=1)
    satisfied_by: List[Path] = Field(default_factory=list)
    conduct: List[str] = Field(default_factory=list)
    #: The documents a `conduct` duty is ABOUT. Named in the sentence, checked against it
    #: (`_PRESUMED_SAID`); required for an act registered in `DOCUMENT_ACTS`.
    presumes: List[Document] = Field(default_factory=list)
    who: Optional[Who] = None
    who_except: Optional[Who] = None
    doing: Doing
    extents: Optional[List[dict]] = None
    includes_tributaries: Optional[bool] = None
    water: Optional[WaterKind] = None
    on: Optional[Literal["classified_period", "steelhead_period"]] = None
    #: Lifted where an OUTRIGHT stamp waiver is in force (see the class docstring).
    waived_where: Optional[Literal["steelhead_stamp_waived"]] = None
    authority: Optional[Literal["superior"]] = None
    when: Optional[When] = None
    restates: Optional[Ref] = None
    #: THIS REQUIREMENT HOLDS ONLY IN A PART OF WHAT ITS EXTENTS DRAW, AND THE PART IS NOT DRAWN —
    #: the rule's `undrawn_part`, for a licensing record (UI consumer's report, 2026-10-03). The
    #: Creston Valley WMA permit holds on "all waters within the Creston Valley Wildlife Management
    #: Area"; only the south end of Kootenay Lake's Main Body lies in it, and a lake is never cut
    #: (AGENTS 13, memory straddling-sections-mark-not-cut). Bound by the area it bound the whole
    #: 389 km² lake. So the area record takes the lake out (`outside_items`) and a second record
    #: binds the lake with this part: it is SHOWN on the lake as a place not yet mapped and never
    #: required of the whole lake (`read.requirements_in_force` lists it under `not_yet_mapped`).
    undrawn_part: str = ""
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Requirement":
        e: List[str] = []
        if self.undrawn_part.strip() and not self.extents:
            e.append("undrawn_part names a part OF the water its extents draw — it needs extents")
        if bool(self.satisfied_by) == bool(self.conduct):
            e.append("exactly one of satisfied_by (what to hold) or conduct (what to do)")
        for act in self.conduct:
            if act not in CONDUCT_ACTS:
                e.append(f"conduct: {act!r} is not a registered act")
        # A DUTY ABOUT A DOCUMENT NAMES IT, and only a duty does: a requirement to HOLD documents
        # already names them in `satisfied_by`.
        if self.presumes and not self.conduct:
            e.append("presumes belongs to a conduct duty — a requirement to hold documents names "
                     "them in satisfied_by")
        if len(set(self.presumes)) != len(self.presumes):
            e.append("presumes names a document twice")
        # A WAIVER OF THE STEELHEAD STAMP LIFTS ONLY A STEELHEAD-STAMP REQUIREMENT: one whose every
        # path holds the stamp. On the Classified Waters Licence it would tell a Chilko angler
        # in June that Class II water needs no licence.
        if self.waived_where is not None and (
                not self.satisfied_by
                or any(Document.steelhead_stamp not in q.hold for q in self.satisfied_by)):
            e.append("waived_where: steelhead_stamp_waived lifts only a requirement to hold the "
                     "Steelhead Stamp (every path holds it)")
        if self.waived_where is not None and self.on == "steelhead_period":
            e.append("waived_where on a steelhead_period requirement: a waiver and a stamp period "
                     "are never on one designation, so it lifts nothing")
        bound = sorted(a for a in self.conduct if a in DOCUMENT_ACTS)
        if bound and not self.presumes:
            e.append(f"conduct {bound} is a duty about a document — name it in `presumes`, or an "
                     f"exemption from that document cannot release the duty")
        said = squash(self.verbatim)
        for d in self.presumes:
            pat = _PRESUMED_SAID.get(d.value)
            if pat is None or not re.search(pat, said):
                e.append(f"presumes {d.value!r}, which the sentence does not name "
                         f"({self.verbatim[:60]!r})")
        if self.who_except is not None and self.who is not None \
                and not self.who.overlaps(self.who_except):
            e.append("who_except does not overlap who — it subtracts nothing")
        if self.authority == "superior":
            prov = [d.value for p in self.satisfied_by for d in p.hold
                    if d.value in PROVINCIAL_ANGLER_DOCUMENTS]
            if prov:
                e.append(f"a superior authority's requirement cannot be met by a provincial "
                         f"document ({prov})")
        err = _check_residency(self.who, self.verbatim, "who")
        if err:
            e.append(err)
        e += _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"requirement {self.id}: " + "; ".join(e))
        return self


class LicenceTerms(_Terse):
    """HOW A DOCUMENT IS SOLD — rendered under the obligation it goes with, NEVER bound to a
    section as one. The Dean's non-guided-alien draw was a `document_required` bound to all 76 Dean
    sections, the Class II upper river included; as terms it attaches only where its unit is."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["licence_terms"] = "licence_terms"
    id: str = Field(..., min_length=1)
    document: Document
    who: Optional[Who] = None
    classified: Optional[Literal["I", "II"]] = None
    #: Licence units these terms are about; empty = every unit.
    units: List[str] = Field(default_factory=list)
    sold: Optional[Literal["per_licence_year", "per_day"]] = None
    covers: Optional[Literal["every_unit", "one_unit"]] = None
    max_consecutive_days: Optional[int] = Field(default=None, gt=0)
    max_days_per_licence_year: Optional[int] = Field(default=None, gt=0)
    max_per_licence_year: Optional[int] = Field(default=None, gt=0)
    max_units_per_licence_year: Optional[int] = Field(default=None, gt=0)
    #: NO DAY LIMIT, SAID OUTRIGHT — "There are no limits on the number of days which a Canadian
    #: resident may fish". JSON has no infinity, so the word is the value, as `unlimited` is on a
    #: retention rule; never alongside a day limit.
    unlimited_days: bool = False
    allocation: Optional[Literal["open", "booking", "draw"]] = None
    needs: List[Literal["angling_guide_number"]] = Field(default_factory=list)
    fee_cad: Optional[float] = Field(default=None, gt=0)
    #: THE LICENCE CLASSES THE BOOK SELLS (p.5, "Licence Fees"; UI consumer's report 2026-10-03).
    #: `name` is the class as the table prints it ("One Day Angling Licence", "Annual Licence for
    #: Age 65 Plus"); `valid_days` the fixed run of consecutive days a short-term licence covers
    #: (One Day = 1, Eight Day = 8 — "covering 8 consecutive days"), never beside `sold:
    #: per_licence_year`; `fees_cad` the printed fee FOR EACH RESIDENCY that may buy it (a ★
    #: "Not available" cell is a residency left out of `who`). Fees are the synopsis's, taxes not
    #: included, and change from year to year — the export says so (`guide.licensing.classes`).
    name: str = ""
    valid_days: Optional[int] = Field(default=None, gt=0)
    fees_cad: dict[Residency, float] = Field(default_factory=dict)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "LicenceTerms":
        e: List[str] = []
        if self.valid_days is not None and self.sold == "per_licence_year":
            e.append("valid_days is a short-term licence's run of days — not on one sold per "
                     "licence year")
        if self.fees_cad:
            if self.fee_cad is not None:
                e.append("fee_cad and fees_cad both — one fee, or one per residency")
            if any(v <= 0 for v in self.fees_cad.values()):
                e.append("fees_cad: a fee is a positive amount")
            sold_to = set(self.who.residency) if self.who is not None and self.who.residency \
                else set(WHO_AXES["residency"])
            extra = sorted(set(self.fees_cad) - sold_to)
            if extra:
                e.append(f"fees_cad prices {extra}, to whom `who` does not sell it")
            missing = sorted(sold_to - set(self.fees_cad))
            if missing:
                e.append(f"fees_cad has no fee for {missing}, to whom `who` sells it — leave a "
                         f"residency the table marks ★ out of `who`")
        terms = (self.sold, self.covers, self.max_consecutive_days,
                 self.max_days_per_licence_year, self.max_per_licence_year,
                 self.max_units_per_licence_year, self.allocation, self.fee_cad,
                 self.valid_days, self.fees_cad or None)
        if all(t is None for t in terms) and not self.needs and not self.unlimited_days:
            e.append("terms that set nothing say nothing")
        if self.unlimited_days and (self.max_consecutive_days or self.max_days_per_licence_year):
            e.append("unlimited_days and a day limit contradict")
        if len(set(self.units)) != len(self.units):
            e.append(f"units named twice: {self.units}")
        for u in self.units:
            if not re.match(_SLUG, u):
                e.append(f"unit {u!r} is not a unit id")
        # "COVERS EVERY UNIT" ABOUT SOME UNITS contradicts itself: the resident's annual licence
        # covers every classified water, and naming units under it would attach it only where
        # those units are — the per-day licence's shape under the annual licence's words.
        if self.covers == "every_unit" and self.units:
            e.append(f"covers: every_unit names units {self.units} — a licence that covers every "
                     f"unit is not about some of them")
        # "ONE ANNUAL LICENCE PER LICENCE YEAR" is about the ANNUAL licence. Without `sold` the
        # count reads as every basic licence, and a visitor may buy as many one-day licences as
        # they like.
        if _ANNUAL_SAID.search(squash(self.verbatim)) and self.sold != "per_licence_year":
            e.append("the sentence is about an annual licence — sold: per_licence_year")
        err = _check_residency(self.who, self.verbatim, "who")
        if err:
            e.append(err)
        if e:
            raise ValueError(f"licence_terms {self.id}: " + "; ".join(e))
        return self


class Exemption(_Terse):
    """A NAMED WHO RELEASED FROM NAMED DOCUMENTS. "you are not required to obtain any type of
    fishing licence or stamp" released only `basic_licence` when it was a `required: false` rule,
    and the CWL and stamps went on applying.

    It releases the `documents` AND every duty whose `presumes` are all among them ("produce your
    angling licence" means nothing to an angler who need not hold one). The angler is unknown, so
    a reader renders the released duty conditionally — "unless you are …" — never drops it."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["exemption"] = "exemption"
    id: str = Field(..., min_length=1)
    who: Who
    documents: List[Document] = Field(..., min_length=1)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Exemption":
        e: List[str] = []
        if len(set(self.documents)) != len(self.documents):
            e.append("a document is named twice")
        # CHECKED AGAINST ITS OWN SENTENCE, like every other `who`. An exemption is the one record
        # that REMOVES an obligation, so a `who` wider than the sentence releases anglers the book
        # never released. A status that implies a residency ("an Indian AND a resident of B.C." is
        # `indian_bc_resident`) satisfies the sentence's residency without restating it.
        t = squash(self.verbatim)
        said_status = frozenset(s for s, pat in _STATUS_SAID if re.search(pat, t))
        if said_status != frozenset(self.who.status):
            e.append(f"who.status {sorted(self.who.status) or 'none'} is not what the sentence "
                     f"names ({sorted(said_status) or 'none'})")
        implied = frozenset().union(*(_STATUS_RESIDENCY.get(s, frozenset())
                                      for s in self.who.status))
        said = residency_said(self.verbatim)
        have = frozenset(self.who.residency) or implied
        if said is not None and said != have:
            e.append(f"who.residency {sorted(have) or 'any'} is not what the sentence says "
                     f"({sorted(said)})")
        if said is None and self.who.residency:
            e.append(f"who.residency {sorted(self.who.residency)} is named nowhere in the sentence")
        g = guidance_said(self.verbatim)
        if (frozenset(self.who.guidance) or None) != (None if g is None or len(g) == 2 else g):
            e.append(f"who.guidance {sorted(self.who.guidance) or 'any'} is not what the "
                     f"sentence says")
        # "ANY TYPE of fishing licence or stamp" is every provincial angler document — a list
        # that leaves one out keeps charging the angler the book released.
        if _ANY_DOCUMENT_SAID.search(t) and \
                {d.value for d in self.documents} != set(PROVINCIAL_ANGLER_DOCUMENTS):
            e.append("the sentence releases ANY licence or stamp — documents must be every "
                     "provincial angler document")
        if e:
            raise ValueError(f"exemption {self.id}: " + "; ".join(e))
        return self


class Alternative(_Terse):
    """A PLACE WHERE ANOTHER DOCUMENT ALSO SATISFIES A REQUIREMENT — "B.C. and Yukon angling
    licences are valid on all parts of Morley Lake". It can only ADD a path, never remove one, and
    it must be place-scoped: a scopeless alternative applies everywhere, which is the Babine
    failure on a new axis."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["alternative"] = "alternative"
    id: str = Field(..., min_length=1)
    alternative_to: Ref
    satisfied_by: List[Path] = Field(..., min_length=1)
    extents: List[dict] = Field(..., min_length=1)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _a_place(self) -> "Alternative":
        # `within` EVERY REGION is a scope in form and everywhere in fact — the scopeless
        # alternative the non-empty `extents` exists to refuse, spelled so that it passes.
        e = [f"extent {i} is the whole province — an alternative is accepted somewhere, not "
             f"everywhere" for i, x in enumerate(self.extents)
             if x.get("op") == "within" and x.get("area_kind") == "region"
             and not x.get("area_id")]
        e += _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"alternative {self.id}: " + "; ".join(e))
        return self


#: Who a sentence names by STATUS, and the residency that status already carries.
_STATUS_SAID = (("indian_bc_resident", r"\bindians?\b"), ("metis", r"\bm[ée]tis\b"),
                ("disabled", r"\bdisab"))
_STATUS_RESIDENCY = {"indian_bc_resident": frozenset({"resident"})}
_ANY_DOCUMENT_SAID = re.compile(r"any (?:type of )?(?:fishing )?licen[cs]e or stamp")
_ANNUAL_SAID = re.compile(r"\bannual (?:\w+ )?licen[cs]e")


LicensingRecord = Annotated[
    Union[Designation, NotClassified, Requirement, LicenceTerms, Exemption, Alternative],
    Field(discriminator="kind")]


def licensing_verbatims(rec) -> List[str]:
    """Every quote a record carries — each must be a contiguous run of the entry's passage."""
    out = [rec.verbatim]
    if isinstance(rec, Designation):
        out += [x.verbatim for x in (rec.steelhead_stamp_during, rec.steelhead_stamp_waived) if x]
        out += [s.verbatim for s in rec.suspended_while]
    return out
