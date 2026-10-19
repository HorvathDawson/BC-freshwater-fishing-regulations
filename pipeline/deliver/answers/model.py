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


class SubcapLift(Model):
    """Kinds a water's own rule lifts out of a sub-cap (they still count toward the total)."""
    members: Tuple[Fish, ...] = Field(min_length=1)
    by: RuleIx


class Subcap(Model):
    """A count or size clause of a total holding some of its kinds here (rows F14)."""
    r: RuleIx
    take: Count
    water: Optional[Literal["stream", "lake"]]       # a stream (lake) share; None: any water
    over_cm: Optional[Cm]                            # a size clause's band
    under_cm: Optional[Cm]
    of: Tuple[Fish, ...] = Field(min_length=1)       # the kinds it holds here
    lifted: Tuple[SubcapLift, ...]


class Headline(Model):
    """The card's big number (answers 2.8, A1): this water's number with no lift — on a Region 2
    stream the stream share ("2 a day per angler from this stream", UR2 1), on a lake the total ("4 a day per
    angler")."""
    n: Optional[Count]                                # None: no daily limit
    unit: StrictStr = Field(min_length=1)
    says: StrictStr = Field(min_length=1)
    #: the lifted numbers said beside the big one, so "2" never reads as the whole limit (round 9
    #: persona): Vedder "4 if they're hatchery rainbow trout 50 cm or less"; every lifted number above
    #: the headline, highest first (ROUND10 W1)
    also: Tuple["HeadAlso", ...] = ()


class HeadAlso(Model):
    """A lifted kind's own number beside the headline."""
    n: Optional[Count]
    says: StrictStr = Field(min_length=1)


class CardLiftEach(Model):
    """One lifting rule of a card lift: its kinds, its number (None: any number), the rule (`by`, its
    Book page) and the words naming its fish ("hatchery rainbow trout 50 cm or less")."""
    kinds: Tuple[Fish, ...] = Field(min_length=1)
    n: Optional[Count]
    by: RuleIx
    fish: StrictStr = Field(min_length=1)


class CardLift(Model):
    """A water's rules lifting kinds out of the share, said positively in ONE line per lifting entry
    (ROUND10 W1: "… its own rule replaces Region 4’s 2-from-streams limit for rainbow trout (up to 5 a
    day) and Dolly Varden/bull trout (up to 2 a day)."); `each` every lifting rule, highest number
    first; `by`, `n` the first's (the page cites its Book page), `kinds` every kind lifted."""
    says: StrictStr = Field(min_length=1)
    by: RuleIx
    kinds: Tuple[Fish, ...] = Field(min_length=1)
    n: Optional[Count]
    each: Tuple[CardLiftEach, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _first(self):
        if (self.by, self.n) != (self.each[0].by, self.each[0].n) or \
                list(self.kinds) != list(dict.fromkeys(S for e in self.each for S in e.kinds)):
            raise ValueError("a lift's `by`, `n` are its first rule's and `kinds` every rule's")
        return self


class Presence(Model):
    """ROUND14 4: the steelhead presence line, said INSIDE the steelhead section (a steelhead kind row,
    a not-kept steelhead line, or the steelhead's own row) — `line` the frame's `steelhead_line` code,
    `says` its words ("Steelhead are known to be in this water."; UR2 4, where a rainbow over 50 cm is a steelhead:
    "Steelhead are known to be in this water, so a rainbow trout over 50 cm (20 in) counts as a steelhead.")."""
    line: Literal["possible_with_rules", "known_with_rules"]
    says: StrictStr = Field(min_length=1)


class CardKind(Model):
    """One kind row: its species (one picture each, P5), name, origin/size, number, notes (what the
    row's own cap counts toward — never a size cap, ROUND11 A1), duties (a year, record it, stop
    after); `presence` (ROUND14 4) the steelhead presence line on the steelhead kind row."""
    species: Tuple[Fish, ...] = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    detail: StrictStr
    n: Optional[Count]
    notes: Tuple[Annotated[StrictStr, Field(min_length=1)], ...]
    duties: Tuple[Annotated[StrictStr, Field(min_length=1)], ...]
    presence: Optional[Presence] = None

    @model_validator(mode="after")
    def _presence_on_steelhead(self):
        if self.presence is not None and "ST" not in self.species:
            raise ValueError("the steelhead presence line sits on the steelhead's own section")
        return self


class SubGroup(Model):
    """ROUND14 3: a count over SOME of a bucket's kinds, nested in it with those kinds and their own
    sizes — "Cutthroat trout or rainbow trout: 1 between them" (the Bella Coola mainstem, Apr 1-May 31);
    `by` the count's rule; the gate proves the kinds together keep exactly `n`."""
    says: StrictStr = Field(min_length=1)
    n: Count
    by: RuleIx
    kinds: Tuple[CardKind, ...] = Field(min_length=2)


class NoteThen(Model):
    """A cap nested in a bucket note (ROUND12 3): at most `n` of `kinds` over `over_cm` (gate-proved)."""
    n: Count
    over_cm: Cm
    kinds: Tuple[Fish, ...] = Field(min_length=1)


class BucketNote(Model):
    """ROUND11 A1 / ROUND12 3: a cap inside the group, said ONCE on the bucket it governs ("Only 1 of
    these may be over 50 cm, not counting steelhead"; a cap spanning buckets on the first: "Only 1 of
    the day’s trout and char may be over 50 cm"; a kind's own: "Only 1 rainbow trout may be over 90
    cm"; a count over two kind rows, `over_cm` 0: "Only 1 of these may be cutthroat trout or rainbow
    trout"). `kinds` the kinds it holds (the gate keeps them over `over_cm`: at most `n`),
    `not_counting` the kinds it names as outside it (still keepable over the size after those `n`),
    `then` the caps nested in it, in order ("…, and only 1 over 50 cm")."""
    says: StrictStr = Field(min_length=1)
    n: Count
    over_cm: Cm
    kinds: Tuple[Fish, ...] = Field(min_length=1)
    not_counting: Tuple[Fish, ...]
    then: Tuple[NoteThen, ...]


class SizeThen(Model):
    """A size cap nested in a size group (ROUND15 1): at most `n` of `species` over `over_cm` (gate-proved)."""
    n: Count
    over_cm: Cm
    species: Tuple[Fish, ...] = Field(min_length=1)


class SizeGroup(Model):
    """ROUND15 1 (owner): a size cap as a BOXED GROUP, like a count group, naming EXACTLY the kinds it can
    bind today — kept and able to pass the size — in the book's order: "Brown trout, Dolly Varden/bull trout,
    lake trout or brook trout: only 1 over 50 cm between them" (the Bella Coola mainstem, May 1 — never the
    cutthroat ≤33 cm, the rainbow ≤50 cm or a closed steelhead). It NAMES its kinds (`species`; each one's
    kind row stays in its bucket — a cap may span buckets or cut a kind row); `by` the cap's rule;
    `not_counting` kinds of the card it says stay outside it (still keepable over the size after its `n`);
    `then` the caps nested in it ("…, and only 1 over 50 cm"). The gate proves the number, that each kind
    named may pass the size here, and that no kind the rule binds over the size goes unnamed."""
    says: StrictStr = Field(min_length=1)
    n: Count
    over_cm: Cm
    by: RuleIx
    species: Tuple[Fish, ...] = Field(min_length=1)
    not_counting: Tuple[Fish, ...]
    then: Tuple[SizeThen, ...]


class Replaces(Model):
    """ROUND13 D: what a water's own-rule bucket replaces, generated from its lifts — `says` names each
    cap of the region the bucket's lifting rules lift for its kinds ("Replaces Region 2’s 2-from-streams
    limit and 30 cm minimum size for hatchery rainbow trout" — ROUND19 6: the fish by name), `rules` those caps in the
    order said."""
    says: StrictStr = Field(min_length=1)
    rules: Tuple[RuleIx, ...] = Field(min_length=1)


class Bucket(Model):
    """Kinds grouped under the cap they draw from: the stream (lake) share, a water's lifting rule,
    or the total; `by` the rule of that cap; `notes` the size caps said on it (ROUND11 A1);
    `replaces` (ROUND13 D) what a lifting bucket replaces, else None; `groups` (ROUND14 3) its nested
    count groups, each holding some of its kind rows (said before the notes)."""
    title: Optional[StrictStr]                        # None: the only bucket, the headline's
    cap: Optional[Count]
    by: RuleIx
    why: Literal["share", "rule", "total"]
    kinds: Tuple[CardKind, ...] = Field(min_length=1)
    groups: Tuple[SubGroup, ...]
    notes: Tuple[BucketNote, ...]
    replaces: Optional[Replaces]
    sizes: Tuple[SizeGroup, ...] = ()                 # ROUND15 1: the size caps, each a group
    between: Optional[StrictStr] = None               # ROUND15 5: "Up to 2 a day between them"

    @model_validator(mode="after")
    def _replaces_a_lift(self):
        if self.replaces is not None and self.why != "rule":
            raise ValueError("only a water's own-rule bucket replaces a cap")
        if any(n.over_cm for n in self.notes):
            raise ValueError("a size cap is a size group (ROUND15 1), never a note")
        if self.between is not None and self.why != "rule":
            raise ValueError("only a water's own-rule bucket says its shared cap")
        return self


class NotKept(Model):
    """ROUND13 S: fish the card's row holds that nobody may keep here, said with whose rule —
    "Steelhead: no fishing for them, not even catch and release — release any you hook by accident" (`status` closed,
    UR1 F: the p.4 legend), "Dolly Varden/bull trout: release every one"
    (release); `by` the deciding rule (its Book page), `whose` "this river’s rule". Gate-proved.
    ROUND14 6: one line per status and deciding row, fish in the book's order ("Rainbow trout and
    cutthroat trout: release every one"), a group named with its members; `rules` every deciding rule
    (`by` the first). ROUND14 4: `presence` the steelhead presence line on the steelhead's line."""
    species: Tuple[Fish, ...] = Field(min_length=1)
    status: Literal["closed", "release"]
    says: StrictStr = Field(min_length=1)
    by: RuleIx
    rules: Tuple[RuleIx, ...] = Field(min_length=1)
    whose: StrictStr = Field(min_length=1)
    presence: Optional[Presence] = None

    @model_validator(mode="after")
    def _by_first(self):
        if self.rules[0] != self.by:
            raise ValueError("a not-kept line's `by` is the first of its `rules`")
        if self.presence is not None and "ST" not in self.species:
            raise ValueError("the steelhead presence line sits on the steelhead's own section")
        return self


class Worked(Model):
    """One line of the card's ONE list (RULINGS Q23b; ROUND12 4 folds the kept-elsewhere examples in):
    after keeping `kept` of `species` (`origin`; the gate takes the first kind and origin that may be
    kept so) at `at` — "here" (a lifted kind past `share`: "If you keep 3 hatchery rainbow trout", UR2 2), on another "stream"
    / "lake" of the region ("Kept some at a lake?") or "elsewhere" (either), fish over
    `over_cm` when set — `here` more here, `other` more on another water of this kind, `away` more on
    the other kind (None: not claimed), counting the kinds `of` (None: every kind of the card) and only
    fish over `over_cm`; `most` "exactly" or "at_most" (a ceiling the answer states). Proved by the
    gate."""
    kept: Positive
    q: StrictStr = Field(min_length=1)
    a: StrictStr = Field(min_length=1)
    species: Tuple[Fish, ...]
    origin: Optional[Literal["hatchery", "wild"]]
    share: Optional[RuleIx]
    here: Optional[Count]
    other: Optional[Count]
    away: Optional[Count]
    at: Literal["here", "stream", "lake", "elsewhere"]
    over_cm: Optional[Cm]
    most: Literal["exactly", "at_most"]
    of: Optional[Tuple[Fish, ...]]

    @model_validator(mode="after")
    def _basket(self):
        if (self.at == "here") != (self.share is not None):
            raise ValueError("a keep-here line names the share its kind passes; an elsewhere line none")
        if self.here is not None and (not self.species or self.origin is None):
            raise ValueError("a line claiming a number names the kind and origin it starts from")
        return self


class CardState(Model):
    """Which cap a card line STATES (the gate: none twice)."""
    r: RuleIx
    line: StrictStr = Field(min_length=1)


class TableBand(Model):
    """ROUND16 1: one size band of a species' table row — from the rules' own cut points, merged where nothing
    kept here changes ("under 25 cm (10 in)", "25–33 cm (10–13 in)", "over 33 cm (13 in)"); `to` None: no top.
    ROUND19 5 (RULINGS P37, the boundary standard): `from_in` / `to_in` whether the band holds its ends — a fish
    exactly at a cut belongs to the band whose words include it ("X or less" / "X or longer" include X, "under X" /
    "over X" exclude it)."""
    from_: Cm = Field(alias="from")
    to: Optional[Cm]
    says: StrictStr = Field(min_length=1)
    from_in: StrictBool
    to_in: StrictBool


WhyLit = Literal["released", "closed", "too small", "too big", "protected size"]


class TableCell(Model):
    """ROUND16 1: the most of that fish alone at that size here today (None: no limit) — gate-proved
    against the engine; `why` when it is 0; `origin` when only one origin may be kept; `by_origin` when
    both may, in different numbers; `as` "ST" where a rainbow that size IS a steelhead (p.80; its number
    is the steelhead's), "RB" for a steelhead under it."""
    n: Optional[Count]
    why: Optional[WhyLit]
    origin: Optional[OriginLit]
    by_origin: Optional[Dict[OriginLit, Optional[Count]]]
    as_: Optional[Fish] = Field(alias="as")

    @model_validator(mode="after")
    def _why_when_none(self):
        if (self.n == 0) != (self.why is not None):
            raise ValueError("a cell says why exactly when it keeps none")
        if self.origin is not None and self.by_origin is not None:
            raise ValueError("a cell names one origin or both, not both ways")
        return self


class CardTable(Model):
    """ROUND16 1: the species × size table of a group — `bands` and `cells` per species, parallel;
    `steelhead_cm` the length over which a rainbow is a steelhead here (None: no such water)."""
    bands: Dict[Fish, Tuple[TableBand, ...]]
    cells: Dict[Fish, Tuple[TableCell, ...]]
    steelhead_cm: Optional[Cm]

    @model_validator(mode="after")
    def _parallel(self):
        if set(self.bands) != set(self.cells) or any(len(self.bands[S]) != len(self.cells[S])
                                                     for S in self.bands):
            raise ValueError("every table band has its cell")
        for S, bs in self.bands.items():
            at, held = 0, None
            for b in bs:
                if b.from_ != at:
                    raise ValueError(f"{S}'s bands cover every length once")
                # ROUND19 5: a cut belongs to exactly one of the two bands meeting at it
                if held is not None and held == b.from_in:
                    raise ValueError(f"{S}'s cut at {at} cm belongs to exactly one band")
                at, held = b.to, b.to_in
            if at is not None or held:
                raise ValueError(f"{S}'s last band has no top")
        return self


class CapKind(Model):
    """ROUND16 1: one kind of fish a basket counts — a species, a size band (`to` None: no top), an origin
    where the rules tell hatchery from wild (None: either). `id` "<species>.<band>[.h|.w]"."""
    id: Annotated[StrictStr, Field(pattern=r"^[A-Z_]+\.[0-9]+(\.[hw])?$")]
    species: Fish
    from_: Cm = Field(alias="from")
    to: Optional[Cm]
    origin: Optional[OriginLit]
    from_in: StrictBool          # ROUND19 5: the band holds its ends (the boundary standard, RULINGS P37)
    to_in: StrictBool


class CapPlaces(Model):
    """The kinds a cap counts, by where they were kept today: here, another stream, another lake."""
    here: Tuple[StrictStr, ...]
    stream: Tuple[StrictStr, ...]
    lake: Tuple[StrictStr, ...]


class Cap(Model):
    """ROUND16 1: one constraint of the group's CAP SYSTEM — a basket is legal iff for every cap the fish of
    its `kinds` (kept at each place) number at most `take`; `if_kept_here`: the cap holds only once the
    basket keeps one of its `here` kinds (a water's own number counting fish kept elsewhere, rows F9). A
    take-0 cap: none of those kinds may be kept. `by` the rule, `scope` and `says` its words."""
    take: Count
    kinds: CapPlaces
    scope: StrictStr = Field(min_length=1)
    by: RuleIx
    says: StrictStr = Field(min_length=1)
    if_kept_here: StrictBool

    @model_validator(mode="after")
    def _conditional(self):
        if self.if_kept_here and not (self.kinds.here and (self.kinds.stream or self.kinds.lake)):
            raise ValueError("only a cap counting fish kept elsewhere toward one kept here is conditional")
        return self


class TreeThen(Model):
    """A size cap nested in another (ROUND18 1): at most `n` over `over_cm`."""
    n: Count
    over_cm: Cm


class TreeExtra(Model):
    """RULINGS Q16 on a size tier (ROUND18 3, ROUND19 3): after its number, `n` more of `species` over the size may be
    kept — "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead" (UR1 B); `by` the cap letting
    them past (Region 2's "2 over 50 cm" with steelhead). The extra kinds sit INSIDE the tier."""
    n: Positive
    species: Tuple[Fish, ...] = Field(min_length=1)
    by: Optional[RuleIx] = None


class SizeNote(Model):
    """ROUND19 3: how a kind a size tier lets past its number counts toward it — the steelhead's "it counts toward
    the 1 over 50 cm (20 in), but once you keep one, a second fish over 50 cm (20 in) may be kept"; `tier` the size
    tier around it."""
    tier: Annotated[StrictStr, Field(pattern=r"^t[0-9]+$")]
    says: StrictStr = Field(min_length=1)


class TreeSizes(Model):
    """ROUND18 2: the lengths a kind may be kept at here, [from, to | None] each, in cm and in whole inches
    (the nearest; the cm is the book's)."""
    cm: Tuple[Tuple[Cm, Optional[Cm]], ...]
    in_: Tuple[Tuple[StrictInt, Optional[StrictInt]], ...] = Field(alias="in")


class AlsoCounts(Model):
    """ROUND18 1/7: a cap a kind's tier does not hold that still binds it, said on the kind — a size cap
    (`over_cm` > 0: "only 1 over 50 cm (20 in), if you haven’t already kept a Dolly Varden/bull trout or lake trout
    over 50 cm") or a count (`over_cm` 0: "no more than 3 counting any lake trout you keep"); `tier` the size tier
    it is (None: no tier holds it), `by` its rule (None: a count the card states as a note), `species` every kind it
    counts, `then` its nested caps. Gate-proved."""
    tier: Optional[StrictStr]
    by: Optional[RuleIx]
    n: Count
    over_cm: Cm
    then: Tuple[TreeThen, ...]
    species: Tuple[Fish, ...] = Field(min_length=1)
    says: StrictStr = Field(min_length=1)


class TreeKind(Model):
    """ROUND18 1/7: one kind in the card's tree — `says` "Up to 2 may be brook, brown or cutthroat trout", `detail`
    its sizes in cm (inches) ("30 cm (12 in) or longer"; ROUND19 1: `says` names the origin with the fish, "Up to 2
    may be hatchery brook, brown or cutthroat trout"; UR2 3/4: a kind held to fewer than its count tier "Only 1 of the 2
    may be …", a steelhead kind keeping hatchery fish only "… hatchery steelhead only — wild steelhead go back"),
    `sizes`, `also_counts`, `duties` (UR2 5: "after you keep 2, stop fishing for the day"), `sentence` the whole line,
    "of" its nearest count tier ("Of the 2 a day: up to 2 may be …, 30 cm (12 in) or longer — …"); `presence`
    the steelhead presence line. Every number gate-proved."""
    node: Literal["kind"]
    species: Tuple[Fish, ...] = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    n: Optional[Count]
    says: StrictStr = Field(min_length=1)
    detail: StrictStr
    sizes: TreeSizes
    also_counts: Tuple[AlsoCounts, ...]
    duties: Tuple[Annotated[StrictStr, Field(min_length=1)], ...]
    sentence: StrictStr = Field(min_length=1)
    presence: Optional[Presence] = None
    size_note: Optional[SizeNote] = None
    #: ROUND20b C1: the kind is the ONLY row of an own tier at the tier's number — the tier line names the fish ("This
    #: river’s rule: up to 4 hatchery rainbow trout a day") and the page draws no sentence under it, the number once
    folded: StrictBool = False


class TreeTier(Model):
    """ROUND18 1 (owner: the card as FOLDERS): a tier line holding what it limits — `tier` total ("4 a day per angler
    across Region 2, lakes included", UR1 A), own (a water's own rule: "This river’s rule: up to 4 a day", with `whose`,
    `replaces`, `why_here` — ROUND18 8, "its row covers its tributaries, so it applies here" where the rule reached
    this water by the tributary walk — and `line`), share ("2 a day per angler from this stream (part of the 4)"; UR3 4,
    where a water's own rule lifts fish out of it: "— your hatchery rainbow trout count toward the 4, not these 2"),
    count ("1 cutthroat or rainbow trout in all", UR1 D; UR3 3, inside an own tier or the share: "(part of the 2)") or size (ROUND19 3: HOLDING every kind it caps, "Only 1 of the fish in this box may
    be over 50 cm (20 in) — 2 if one is a hatchery steelhead", UR1 B: `over_cm`, `species` the kinds its cap
    names, `then`, `extra`); `children` its tiers and kinds. `id` names it for a kind's `also_counts`."""
    node: Literal["tier"]
    id: Annotated[StrictStr, Field(pattern=r"^t[0-9]+$")]
    tier: Literal["total", "own", "share", "count", "size"]
    n: Optional[Count]
    says: StrictStr = Field(min_length=1)
    by: RuleIx
    children: Tuple[Annotated[Union["TreeTier", TreeKind], Field(discriminator="node")], ...]
    whose: Optional[StrictStr] = None
    why_here: Optional[StrictStr] = None
    replaces: Optional[StrictStr] = None
    line: Optional[StrictStr] = None
    over_cm: Optional[Cm] = None
    species: Optional[Tuple[Fish, ...]] = None
    then: Optional[Tuple[TreeThen, ...]] = None
    extra: Optional[TreeExtra] = None

    @model_validator(mode="after")
    def _shape(self):
        if self.tier == "own" and self.whose is None or self.whose is not None and self.tier not in ("own", "total"):
            raise ValueError("a water's own-rule tier (or a water's own total) says whose rule it is")
        if (self.tier == "size") != (self.over_cm is not None):
            raise ValueError("a size tier, and only it, has a size")
        if self.extra is not None and self.tier != "size":
            raise ValueError("only a size tier lets extra kinds past its number")
        return self


TreeTier.model_rebuild()


class Header(Model):
    """ROUND18 4: the group's compact header. ROUND20 1 (owner: the group's top and the first tier are ONE block): the
    header IS the tree's root tier — `says` its line ("4 a day per angler across Region 2, lakes included"; a water's
    own total "This river’s rule: up to 1 a day per angler"), `tier` its id (the page draws the root's children at the
    top of the tree; the gate holds `says` to the root's words); `tag` None since ROUND19 1 (the kinds' names carry
    the origin)."""
    says: StrictStr = Field(min_length=1)
    tier: Optional[StrictStr] = None
    tag: Optional[StrictStr]


class GroupTotal(Model):
    """One total on today's card and its sub-caps (answers 2.5, rows F14) — the DECISION (`pool`,
    `total`, `kinds`, `subcaps`) — and, answers 2.8 (ROUND9 A1-A3), the CARD water-first, each fact
    once: `headline`, `origin`, `lifts`, `total_line`, `buckets`, `not_kept`, `worked`, `states` (`card.py`; every
    number proved by `card_check`)."""
    pool: RuleIx
    total: Union[Positive, Literal["unlimited"]]
    kinds: Tuple[Fish, ...] = Field(min_length=1)    # every kind on the card counting toward it
    subcaps: Tuple[Subcap, ...]
    headline: Headline
    origin: Optional[StrictStr]
    lifts: Tuple[CardLift, ...]
    total_line: Optional[StrictStr]
    buckets: Tuple[Bucket, ...] = Field(min_length=1)
    not_kept: Tuple[NotKept, ...]
    worked: Tuple[Worked, ...]
    states: Tuple[CardState, ...]
    table: CardTable                                  # ROUND16 1: species × size, gate-proved
    cap_kinds: Tuple[CapKind, ...]                    # ROUND16 1: the basket's vocabulary
    caps: Tuple[Cap, ...]                             # ROUND16 1: legal iff every cap holds
    tree: Tuple[TreeTier, ...] = Field(min_length=1, max_length=1)   # ROUND18 1: the card as folders
    header: Header                                    # ROUND18 4: one compact line

    @model_validator(mode="after")
    def _caps_name_kinds(self):
        ids = {k.id for k in self.cap_kinds}
        if len(ids) != len(self.cap_kinds):
            raise ValueError("two cap kinds share an id")
        if any(k not in ids for c in self.caps for k in (*c.kinds.here, *c.kinds.stream, *c.kinds.lake)):
            raise ValueError("a cap counts only the group's cap kinds")
        if not set(self.kinds) <= set(self.table.bands):
            raise ValueError("every kind of the group has its table row")
        return self

    @model_validator(mode="after")
    def _shares(self):
        if len(self.kinds) < 2 and not self.subcaps:
            raise ValueError("a group is a total several kinds share, or one with a sub-cap")
        for s in self.subcaps:
            if not set(s.of) <= set(self.kinds) or any(not set(L.members) <= set(self.kinds)
                                                       for L in s.lifted):
                raise ValueError("a sub-cap holds (or lifts) only kinds counting toward the total")
        every = [S for b in self.buckets for k in (*b.kinds, *(k for s in b.groups for k in s.kinds))
                 for S in k.species]
        if sorted(every) != sorted(self.kinds):
            raise ValueError("every kind of the total sits in exactly one bucket row")
        if set(S for x in self.not_kept for S in x.species) & set(self.kinds):
            raise ValueError("a fish the card keeps is never said not kept")
        rs = [x.r for x in self.states]
        if len(rs) != len(set(rs)):
            raise ValueError("a cap is stated once on the card")
        # ROUND18 1: every kind exactly once in the tree, under tiers; tier ids unique; a kind's tier refs resolve
        got, ids, refs = [], [], []

        def go(t):
            ids.append(t.id)
            for x in t.children:
                if isinstance(x, TreeTier):
                    go(x)
                else:
                    got.extend(x.species)
                    refs.extend(a.tier for a in x.also_counts if a.tier is not None)
        go(self.tree[0])
        if self.tree[0].tier != "total":
            raise ValueError("the tree is one total tier")
        if sorted(got) != sorted(self.kinds):
            raise ValueError("every kind of the total sits in the tree exactly once")
        if len(ids) != len(set(ids)) or not set(refs) <= set(ids):
            raise ValueError("tier ids are unique and every reference names one")
        return self


class Elsewhere(Model):
    """The kept-elsewhere line of a keep row (rows F9, F14): one sentence and worked examples
    [[question, answer]]."""
    says: StrictStr = Field(min_length=1)
    ask: Tuple[Tuple[StrictStr, StrictStr], ...]


class Week(Model):
    """ROUND14 5: a weekday window said as weekdays, the same on every moment's frame of the day —
    "Keep up to 5 on Saturdays and Sundays; release Monday to Friday" (Kootenay Lake's Lower West Arm
    kokanee); `species` the row's fish whose answer it is (several fish answering differently are each
    named: "Kokanee: …")."""
    species: Tuple[Fish, ...] = Field(min_length=1)
    says: StrictStr = Field(min_length=1)
    status: Optional[StrictStr] = None                # ROUND15 3: "Weekends: keep 5 · Weekdays: release"
    parts: Tuple["WeekPart", ...] = ()                # ROUND15 3: each day-part, how it was decided

    @model_validator(mode="after")
    def _one_today(self):
        for S in self.species:
            mine = [p for p in self.parts if S in p.species]
            if mine and sum(p.today for p in mine) != 1:
                raise ValueError("exactly one day-part of a fish is today's")
        return self


class WeekPart(Model):
    """ROUND15 3: one day-part of a weekday window — `days` "Sat–Sun", `when` "Saturday and Sunday",
    `weekdays` the days, `status` and `n` (keep: the number), `says` "keep up to 5", `rules` the rule(s)
    deciding it (how it was decided), `today` the frame's own moment is in it."""
    species: Tuple[Fish, ...] = Field(min_length=1)
    days: StrictStr = Field(min_length=1)
    when: StrictStr = Field(min_length=1)
    weekdays: Tuple[WeekdayLit, ...] = Field(min_length=1)
    status: Decided_
    n: Optional[Count]
    says: StrictStr = Field(min_length=1)
    rules: Tuple[RuleIx, ...] = Field(min_length=1)
    today: StrictBool


class Restated(Model):
    """ROUND15 3: a rule of the row restating its winner `of` (same fish, number, days and sizes) — one
    ladder line; its book words the note (`says`) — a quota two waters share in plain words (UR3 6): "Kokanee from
    the Lower and Upper West Arms together: no more than 5 a day.". ROUND16 Q2:
    also another row's identical statement, farther from the stream than the winner's (shown once, credited
    to the nearer row): "Zone B also prints this: “All from Williston Lake and its tributaries”."."""
    r: RuleIx
    of: RuleIx
    says: StrictStr = Field(min_length=1)


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
    group: Optional[GroupTotal] = None                # answers 2.5: on ONE row per total
    elsewhere: Optional[Elsewhere] = None             # answers 2.5: set <=> a keep row
    presence: Optional[Presence] = None               # ROUND14 4: on the steelhead's own row
    week: Optional[Week] = None                       # ROUND14 5: its fish's weekday window in words
    restates: Optional[Tuple[Restated, ...]] = None   # ROUND15 3: same-row restatements, as notes

    @model_validator(mode="after")
    def _pool_rows(self):
        has_pool = self.pool is not None
        if self.group is not None and not has_pool:
            raise ValueError("a group total only on a pool row")
        if (self.elsewhere is not None) != (self.kind == "keep"):
            raise ValueError("the kept-elsewhere line is set <=> a keep row")
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
        if self.presence is not None and "ST" not in self.all_members:
            raise ValueError("the steelhead presence line sits on the steelhead's own row")
        if self.week is not None and not set(self.week.species) <= set(self.all_members):
            raise ValueError("a row's weekday window names only its own fish")
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
    #: ROUND10 W4: every rule deciding the row (`by`'s, then each `also`'s), each cited for its Book page
    cites: Tuple[RuleIx, ...] = Field(min_length=1)
    #: ROUND10 W4: the Lines row in plain words ("1 line (rod)" — ROUND11 A2; "1 line (rod); 2 while
    #: alone in a boat"); `lines_per_angler` only
    says: Optional[StrictStr] = None


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


class WithAdult(Model):
    who: StrictStr = Field(min_length=1)
    counts: StrictBool


class BuyDoc(Model):
    doc: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    prices: Prices


class UnderSixteen(Model):
    """ROUND15 2 (owner): what an under-16 angler of one residency needs here (not guided, no status), read
    off the profiles' answers (`licence.under_16_summary`): fish with a licensed adult (`with_adult`), or
    buy `buy` (`own_limit`: a limit of their own), or nothing; `says` the sentence."""
    residency: Literal["resident", "non_resident", "non_resident_alien"]
    label: StrictStr = Field(min_length=1)
    none_needed: StrictBool
    with_adult: Optional[WithAdult]
    buy: Tuple[BuyDoc, ...]
    own_limit: StrictBool
    says: StrictStr = Field(min_length=1)


class LicenceFrame(Model):
    holds: Union[Holds, TidalHolds]
    profiles: Tuple[Union[ProfileAnswer, TidalProfile], ...]
    under_16: Optional[Tuple[UnderSixteen, ...]] = None     # ROUND15 2; absent on tidal water

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

class InheritedLift(Model):
    """ROUND11 A3: a lift this section relies on that ANOTHER water's ✱ row carries to its tributaries
    (RULINGS T13 / T3a), in words: "Open Jul 15–Aug 31: the Quinsam River’s exemption from the summer
    stream closure (MUs 1-1 to 1-6) covers its tributaries." — `by` the lifting rule (its Book page,
    its `applies_trib`), `lifts` the rules it lifts here today."""
    says: StrictStr = Field(min_length=1)
    by: RuleIx
    lifts: Tuple[RuleIx, ...] = Field(min_length=1)
    #: ROUND16 Q1 (RULINGS T15): a nearer row binding this stream prints no such exemption — said, naming
    #: both rows ("Trout Lake’s row, nearer this stream, prints no such exemption; the Duncan River’s is
    #: applied here because its row covers all its tributaries."), else None
    caution: Optional[StrictStr] = None


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
    #: answers 2.6: the OPEN-DAYS STRIP's code — the OR of `display.strip`'s bits (closed 1,
    #: release 2, fly_only 4, artificial_fly_only 8, bait_ban 16; 0 open), from this frame's status
    #: and the rows and gear frames at the same part key and segment (`display.strip_code`)
    strip: Annotated[StrictInt, Field(ge=0, le=31)]
    #: ROUND11 A3 (display 8): the lifts inherited by the tributary walk this answer relies on, said
    #: on the Keep side; absent where none is (a quota lift the card already names is not repeated)
    inherited: Optional[Tuple[InheritedLift, ...]] = Field(default=None, min_length=1)

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
        if (self.strip == 1) != (self.status == "closed"):
            raise ValueError("the strip says closed (alone) exactly where the status is closed")
        if self.strip & 4 and self.strip & 8:
            raise ValueError("fly fishing only and artificial flies only are one gear answer")
        return self


class StripState(Model):
    """One state of the open-days strip (`display.STRIP`): its bit in a frame's `strip`, the
    legend's label and its sentence."""
    state: Literal["open", "closed", "release", "fly_only", "artificial_fly_only", "bait_ban"]
    bit: Annotated[StrictInt, Field(ge=0, le=16)]
    label: StrictStr
    says: StrictStr


class ReasonWords(Model):
    """Why a rule lost, in words (`display.REASON_WORDS`): `lost` said of the loser ("{by}" the
    rule that beat it), `won` of the winner ("{rule}" the rule it beat)."""
    lost: StrictStr
    won: StrictStr


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
    #: ROUND16 3: every rule's title, its effect in words ("Release all steelhead", "Release lake trout,
    #: Oct 1–Nov 30") — never the raw list item; the verbatim stays the rule's own words
    title: StrictStr = Field(min_length=1)
    closure: Optional[Literal[True]] = None          # set <=> a gate that closes
    bands: Optional[Tuple[Tuple[Cm, Optional[Cm], Optional[Count]], ...]] = None
    plain: Optional[StrictStr] = None                # None: the page shows the label (UR3 2: a no-fishing place
    #                                                  rule's book words, distances in feet too: "23 m (75 ft)")
    #: Z12/Q41: a closure printed "unless opened", in the user's accepted words
    unless_opened: Optional[StrictStr] = None
    #: answers 2.8 (D2): the heading a standing no-fishing place rule shows under on the KEEP side,
    #: beside the open/closed strip ("Where you can’t fish": the fishway and fish-facility buffers)
    where: Optional[StrictStr] = None
    #: G3: the rule said for the fish subsets the ladders list it for
    subsets: Optional[Tuple[SubsetSay, ...]] = Field(default=None, min_length=1)
    #: answers 2.6, HOW THIS WAS DECIDED: [[printed page, PDF page]] of the rule's entry (open the
    #: official synopsis at `#page=<PDF page>`), why it applies (its scope, and what a lift lifts),
    #: and — for a water row's rule the tributary walk carries — the same said where it is inherited
    book: Tuple[Tuple[Positive, Positive], ...] = Field(min_length=1)
    applies: StrictStr = Field(min_length=1)
    applies_trib: Optional[StrictStr] = None

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
    #: answers 2.7 (A4): the part strip's short name, from the range's end labels ("Mouth → Vedder
    #: Crossing Bridge"); `label` stays the sheet's full name
    short: StrictStr = Field(min_length=1)
    runs: StrictStr
    place: Optional[StrictStr]                        # None: no entry heading
    hint: StrictStr
    km: Optional[Union[StrictInt, StrictFloat]]       # None: no run carries a measure
    closed_all_year: StrictBool
    paper_licence: Tuple[RuleIx, ...]


class LicensingFacts(Model):
    """answers 2.7 (A3): a licensing record's book pages [[printed page, PDF page]]."""
    book: Tuple[Tuple[Positive, Positive], ...] = Field(min_length=1)


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


# --------------------------------------------------------------------------------------------
# The top level: THE UNNAMED WATER (answers 2.4, S1 — every section has an answer)
# --------------------------------------------------------------------------------------------

RegionLit = _enum(T.Region, "Region")


class PlaceNames(Model):
    """The whole name of an unnamed answer in a region set, per water kind (`display.place_label`)."""
    lake: StrictStr
    stream: StrictStr
    wetland: StrictStr


class Place(Model):
    """A REGION SET sections lie in (the bundle's `region_place`, S1b), with its words ("Region 5")
    and the name of an unnamed answer there per water kind: "Unnamed stream · Region 5"."""
    regions: Tuple[RegionLit, ...] = Field(min_length=1)
    label: StrictStr
    names: PlaceNames


class Unnamed(Model):
    """The unnamed water's keys (S1b, keyed by content): every key from `first` on is a content
    only unnamed parts have; `parts[up]` the key (index into `keys`) of the bundle's unnamed part
    `up` — a named part's key where the content is the same."""
    first: Count
    parts: Tuple[Count, ...]


#: The top-level tables' models (not sections): `moments`, `glossary`, `places`, `unnamed`.
TOP: Dict[str, TypeAdapter] = {"moments": TypeAdapter(Tuple[Moment, ...]),
                               "glossary": TypeAdapter(Glossary),
                               "places": TypeAdapter(Tuple[Place, ...]),
                               "unnamed": TypeAdapter(Unnamed)}

STATICS: Dict[Tuple[str, str], TypeAdapter] = {
    ("display", "rules"): TypeAdapter(Tuple[RuleFacts, ...]),
    ("display", "waters"): TypeAdapter(Dict[StrictStr, WaterFacts]),
    ("display", "strip"): TypeAdapter(Tuple[StripState, ...]),
    ("gear", "count_nouns"): TypeAdapter(Dict[StrictStr, Tuple[StrictStr, StrictStr]]),
    ("display", "licensing"): TypeAdapter(Tuple[LicensingFacts, ...]),
    ("display", "reasons"): TypeAdapter(Dict[_enum(T.LossReason, "LossReason"), ReasonWords]),
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
