"""THE DELIVERY'S TYPES — every value that crosses a stage, closed and checked (DATAFLOW §1).

The user's condition for the data-flow refactor: "nail down data types". Every stage of the
delivery (bundle -> verdicts -> status index -> export -> answers) hands the next a typed, closed,
checked value, and nothing guesses.

ENUMS ARE BUILT FROM THEIR REGISTRY, NEVER RETYPED. Each `StrEnum` below is made from the tuple or
mapping that already decides its members (the catalogue's species lists, the reader's states and
loss reasons, the closure grades, the registry's water kinds, ...); `test_delivery_types.py` pins
each against its registry. Where no registry existed (the answers' vocabularies: `DisplayKind`,
`Role`, `LineType`, ...), THIS module is the registry, and the producers import it.

An enum's WIRE / SQL CODE is its index in the enum's order (`code`, `by_code`): the order is the
registry's, stated here once. An unknown value raises (`Enum(value)`); nothing defaults.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from itertools import combinations
from typing import Iterable, NamedTuple, NewType, Optional, Tuple, Type, TypeVar

from pipeline.deliver.bundle import read
from pipeline.deliver.bundle import rules as bundle_rules
from pipeline.regs.parsing import catalogue as C

E = TypeVar("E", bound=StrEnum)


def closed_enum(name: str, values: Iterable[str]) -> Type[StrEnum]:
    """A StrEnum whose members are exactly `values`, in order (member names made identifiers)."""
    vals = list(values)
    if len(set(vals)) != len(vals):
        raise ValueError(f"types: {name} lists a value twice: {vals}")
    return StrEnum(name, [(v if v.isidentifier() else f"v_{v}", v) for v in vals],
                   module=__name__)


def code(member: StrEnum) -> int:
    """The member's wire / SQL code: its index in its enum's order."""
    return list(type(member)).index(member)


def by_code(enum: Type[E], i: int) -> E:
    """The member with code `i`; an unknown code raises."""
    members = list(enum)
    if not isinstance(i, int) or isinstance(i, bool) or not 0 <= i < len(members):
        raise ValueError(f"types: {i!r} is not a {enum.__name__} code (0..{len(members) - 1})")
    return members[i]


# --------------------------------------------------------------------------------------------
# Fish
# --------------------------------------------------------------------------------------------

#: Every leaf fish code the reader may be asked about: the book's 22 (p.80), the one salmon it
#: names (chinook), and the protected species.
FishCode = closed_enum("FishCode", tuple(C.BOOK_SPECIES) + tuple(C.SALMON_FISH)
                       + tuple(C.PROTECTED_FISH))

#: The book's closed list minus crayfish (`catalogue.GAME_FISH`): every fish a "no fishing" must
#: hold for before a section is called closed, and every fish the verdicts ask on every key.
GAME_FISH: Tuple[str, ...] = C.GAME_FISH

SpeciesGroup = closed_enum("SpeciesGroup", sorted(C.SPECIES_GROUPS))


# --------------------------------------------------------------------------------------------
# The reader's vocabulary
# --------------------------------------------------------------------------------------------

#: The origin a question is asked for: `none` = the angler does not know it (the reader's
#: `origin=None`), then `read.ASKABLE_ORIGINS`.
AskOrigin = closed_enum("AskOrigin", ("none",) + tuple(read.ASKABLE_ORIGINS))

#: A `when` on one day (`read.in_force`).
InForce = closed_enum("InForce", read.IN_FORCE)

#: A rule's state in a traced answer: the speakers (in the answer), then the loser states.
RuleState = closed_enum("RuleState", tuple(read.SPEAKERS) + tuple(read.LOSER_STATES))
SPEAKER_CODES = frozenset(code(RuleState(s)) for s in read.SPEAKERS)

#: Why a rule that took part lost (`read.LOSS_REASONS`, in its order); each maps to ONE loser state.
LossReason = closed_enum("LossReason", tuple(read.LOSS_REASONS))


def loss_state(reason: StrEnum) -> StrEnum:
    """The loser state a loss reason gives (`read.LOSS_REASONS`)."""
    return RuleState(read.LOSS_REASONS[LossReason(reason).value])


#: How a rule reaches a section: by its own extents, or by the tributary walk.
Via = closed_enum("Via", read.VIAS)

Authority = read.Authority
Scope = read.Scope

#: `rules.closure_grade`: None = not a closure.
ClosureGrade = closed_enum("ClosureGrade", bundle_rules.CLOSURE_GRADES)


# --------------------------------------------------------------------------------------------
# Waters and parts
# --------------------------------------------------------------------------------------------

def _water_kinds() -> Tuple[str, ...]:
    from pipeline.common.registry_kinds import WATER_KINDS
    return tuple(sorted(WATER_KINDS))


#: `item.kind` — the registry's, decided once (AGENTS 55).
WaterKind = closed_enum("WaterKind", _water_kinds())

#: `section_steelhead` codes 1 / 2 (`read.STEELHEAD_CODES`); None = neither.
SteelheadPresence = closed_enum("SteelheadPresence",
                                tuple(read.STEELHEAD_CODES[c] for c in sorted(read.STEELHEAD_CODES)))


def presence_code(p: Optional[str]) -> Optional[int]:
    """`section_steelhead.code` of a presence (1 known, 2 possible), None for None."""
    if p is None:
        return None
    want = SteelheadPresence(p).value
    return next(c for c, n in read.STEELHEAD_CODES.items() if n == want)


def _province_except_kinds() -> Tuple[str, ...]:
    from pipeline.deliver.bundle.licensing import PROVINCE_EXCEPT_KINDS
    return tuple(PROVINCE_EXCEPT_KINDS)


#: The families of area where a province-wide requirement stops (`province_except.area_kind`).
ProvinceExceptKind = closed_enum("ProvinceExceptKind", _province_except_kinds())


def province_except_values() -> Tuple[str, ...]:
    """Every value a part's `province_except` column may hold: '' or a sorted, comma-joined subset
    of `ProvinceExceptKind` (the SQL CHECK is generated from this, never typed)."""
    ks = sorted(k.value for k in ProvinceExceptKind)
    return tuple(",".join(c) for n in range(len(ks) + 1) for c in combinations(ks, n))


#: The book's regions and zones, as the bundle spells them (`section_home.region`).
REGIONS: Tuple[str, ...] = ("1", "2", "3", "4", "5", "6", "7a", "7b", "8")
Region = closed_enum("Region", REGIONS)


# --------------------------------------------------------------------------------------------
# The status index
# --------------------------------------------------------------------------------------------

#: The status index's wire code (`status_index`: 0 base · 1 own · 2 closed · 3 tidal · 4 outside).
StatusCode = closed_enum("StatusCode", ("base", "own", "closed", "tidal", "outside"))


# --------------------------------------------------------------------------------------------
# The answers' vocabularies (this module is their registry)
# --------------------------------------------------------------------------------------------

#: The page's 15-step rule kind (`display.kind_of`) plus `possession_cap` (rows F6).
DisplayKind = closed_enum("DisplayKind", (
    "gear", "conduct", "vessel", "anglerclosure", "duty", "exempt", "standing", "while",
    "possession", "annual", "possession_cap", "sizecap", "subcap", "gate", "pool", "size"))

#: The decided answer's status (`rows`), the card row's kind.
DecidedStatus = closed_enum("DecidedStatus", ("keep", "nolimit", "release", "closed"))

#: A rule's role in a decided answer (`rows.eval_sp`).
Role = closed_enum("Role", (
    "governs", "agrees", "contains", "also", "narrows", "limit", "floor", "season",
    "possession_cap", "duty", "possession", "falls", "moot", "replaced", "lifted"))

#: A line of a decided answer (`rows.eval_sp`, `rows.build_model`).
LineType = closed_enum("LineType", (
    "rel", "cap", "subcap", "also", "outer", "steel", "outercap", "outersize", "orphan", "partly",
    "caution", "tnote", "annual", "possession_cap", "duty", "record"))

#: A fact of a card row: a line, or one of the row's own facts.
FactType = closed_enum("FactType", tuple(LineType) + ("origin", "origin2", "exc", "xref"))

#: A condition on keeping (`rows.row_conds`).
CondType = closed_enum("CondType", (
    "origin", "size", "back", "group", "cap", "subcap", "outercap", "outersize", "origin2",
    "streamcap"))

#: Whose count a pool is (`rows.scope_of`, the card badge).
BadgeOf = closed_enum("BadgeOf", ("water", "area", "region", "bc"))

#: Consumer 5.6's presence line (`display.steelhead_line`); None = no line.
SteelheadLine = closed_enum("SteelheadLine",
                            ("possible_with_rules", "known_with_rules", "known_no_rules"))


# --------------------------------------------------------------------------------------------
# Ids
# --------------------------------------------------------------------------------------------

#: The index into the sorted list of `entry_id::rule_id` — the bundle's `rule_ix`, the export's
#: `rules` array. The string form appears only in error messages, the spec and `rule_ids`.
RuleIx = NewType("RuleIx", int)
SetId = NewType("SetId", int)
KeyIx = NewType("KeyIx", int)
ReadingIx = NewType("ReadingIx", int)
VerdictIx = NewType("VerdictIx", int)
ItemId = NewType("ItemId", str)


# --------------------------------------------------------------------------------------------
# Stage artifacts (DATAFLOW §1.3)
# --------------------------------------------------------------------------------------------

@dataclass(frozen=True, order=True, slots=True)
class RuleKey:
    """What a section contributes to `read.effective_rules_bound`: its rule set, whether a rainbow
    over 50 cm is a steelhead there, whether the steelhead rules apply there. Invariant:
    steelhead_water implies steelhead_rules (the bundle refuses otherwise)."""
    set_id: int
    steelhead_water: bool
    steelhead_rules: bool

    def __post_init__(self):
        if not isinstance(self.set_id, int) or isinstance(self.set_id, bool):
            raise TypeError(f"RuleKey.set_id {self.set_id!r} is not an int")
        if not isinstance(self.steelhead_water, bool) or not isinstance(self.steelhead_rules, bool):
            raise TypeError(f"RuleKey flags must be bool: {self!r}")

    def as_list(self) -> list:
        return [self.set_id, int(self.steelhead_water), int(self.steelhead_rules)]


@dataclass(frozen=True, slots=True)
class Part:
    """Every section of one named water carrying the same answer-relevant facts (the bundle's
    `part` table; `part_ix` is the export's `waters[item].parts` index)."""
    item_id: str
    part_ix: int
    set_id: Optional[int]                   # None <=> every section is outside B.C.
    key: Optional[int]                      # the rule key (`rule_key.key_ix`); None <=> set_id None
    licensing_set: Optional[int]            # None: no placed licensing record binds it
    province_except: Tuple[str, ...]        # sorted ProvinceExceptKind values; () = none
    steelhead_water: bool
    steelhead: Optional[str]                # SteelheadPresence value; None = neither
    steelhead_rules: bool
    home_regions: Tuple[str, ...]           # sorted Region values; () = no straddling section
    kind: str                               # WaterKind value
    tidal: bool
    sections: int
    rep_sid: int                            # the lowest section: a HANDLE, never leaves the bundle

    def __post_init__(self):
        if (self.set_id is None) != (self.key is None):
            raise ValueError(f"Part {self.item_id}#{self.part_ix}: set_id and key go together")
        for k in self.province_except:
            ProvinceExceptKind(k)
        if list(self.province_except) != sorted(self.province_except):
            raise ValueError(f"Part {self.item_id}#{self.part_ix}: province_except not sorted")
        if self.steelhead is not None:
            SteelheadPresence(self.steelhead)
        for r in self.home_regions:
            Region(r)
        WaterKind(self.kind)
        if self.steelhead_water and not self.steelhead_rules:
            raise ValueError(f"Part {self.item_id}#{self.part_ix}: steelhead water without the "
                             f"steelhead rules")
        if self.sections < 1:
            raise ValueError(f"Part {self.item_id}#{self.part_ix}: no sections")


@dataclass(frozen=True, slots=True)
class Reading:
    """One distinct reading of a key's year: the day the reader is asked on, and whether every
    game fish is under a speaking full closure then (the status predicate, stored once)."""
    key: int
    ix: int
    first_day: int
    closed: bool


@dataclass(frozen=True, slots=True)
class Segment:
    """A contiguous run of days of a key's year, from `start` to the day before the next start."""
    key: int
    start: int
    reading: int


class VerdictRow(NamedTuple):
    """One rule's line in one verdict (the traced reader's answer, stored as returned)."""
    rule: int                           # RuleIx
    state: StrEnum                      # RuleState
    reason: Optional[StrEnum]           # LossReason; None <=> a speaker state
    by: Optional[int]                   # RuleIx of the winner or lifter; None <=> a speaker state
    lifted_in_part_by: Tuple[int, ...]  # () unless partly lifted (speakers only)

    def check(self) -> "VerdictRow":
        st = RuleState(self.state)
        speaker = st.value in read.SPEAKER_STATES
        if speaker != (self.reason is None) or speaker != (self.by is None):
            raise ValueError(f"VerdictRow {self}: reason and by go with the loser states only")
        if self.reason is not None and loss_state(self.reason) is not st:
            raise ValueError(f"VerdictRow {self}: reason {self.reason} gives another state")
        if self.lifted_in_part_by and not speaker:
            raise ValueError(f"VerdictRow {self}: only a speaker is partly lifted")
        if list(self.lifted_in_part_by) != sorted(set(self.lifted_in_part_by)):
            raise ValueError(f"VerdictRow {self}: lifted_in_part_by not sorted and unique")
        return self


@dataclass(frozen=True, slots=True)
class Frame:
    """The address of one reader call, and the verdict it returned."""
    key: int
    reading: int
    fish: str
    origin: str
    verdict: int
