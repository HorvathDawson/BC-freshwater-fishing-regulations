"""The `rows` section's models: lines, facts, conditions, the decided answer and the card's rows
(`RowsFrame`)."""
from __future__ import annotations

from typing import Annotated, Any, Dict, Literal, Optional, Tuple, Union
from pydantic import Field, StrictBool, StrictInt, StrictStr, model_validator
from pipeline.deliver.answers.model.base import (
    BadgeLit, Cm, Count, Decided_, Fish, FishLit, Model, OriginLit, Positive, RoleLit, RuleIx, SteelLineLit
)
from pipeline.deliver.answers.model.top import WeekdayLit


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
    #: rows 31 (owner rule 2026-10-21, page v36 narrowAll): the row's narrowing clause binds every one of its fish
    #: (false where the row has none) — with `share`, the badge reads "All Region 2 streams", else "Region 2-wide"
    narrow_all: StrictBool
    #: T183 (owner 2026-10-24): on a piece in two regions, the row's caps are the stricter of TWO regions' tables —
    #: the regions in the book's words ("Regions 3 and 5"), said in place of the pool's own region. Absent elsewhere.
    both: Optional[StrictStr] = None

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
    "A rainbow trout over 50 cm (20 in) here counts as a steelhead.", ROUND20g 2)."""
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


class Stamp(Model):
    """rows 31 (owner rule 2026-10-21; the prototype's round UR7 stampsFor, `stamps.py`): a licence document this fish
    needs here, for the angler the Keep tab answers for (a B.C. resident 16 or older) — `doc` the licence answer's
    document, `act` what it is needed for (targeting: to fish for the fish, even to release them; retaining: to keep
    one, over its size where it names one), `name` the document's name ("Conservation Surcharge Stamp for Shuswap
    rainbow trout"), `when` the licence tab's sentence ("Only if you keep rainbow trout over 50 cm"). Draw "You need
    <name> › <when>" linking to the licence."""
    doc: StrictStr = Field(min_length=1)
    act: Literal["targeting", "retaining"]
    name: StrictStr = Field(min_length=1)
    when: StrictStr = Field(min_length=1)


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
    #: rows 31: the licence documents a released fish needs (to fish for it, even to release it); absent where none
    stamps: Optional[Tuple[Stamp, ...]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _by_first(self):
        if self.rules[0] != self.by:
            raise ValueError("a not-kept line's `by` is the first of its `rules`")
        if self.stamps is not None and (self.status != "release" or any(x.act != "targeting" for x in self.stamps)):
            raise ValueError("a not-kept fish needs a document only to fish for it, and only where it is released")
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
    kept — "Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead" (UR1 B, ROUND UR6 1); `by` the cap letting
    them past (Region 2's "2 over 50 cm" with steelhead). The extra kinds sit INSIDE the tier."""
    n: Positive
    species: Tuple[Fish, ...] = Field(min_length=1)
    by: Optional[RuleIx] = None


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
    `sizes`, `also_counts`, `duties` (ROUND20g 2: "stop fishing here after 2 a day"), `sentence` the whole line,
    "of" its nearest count tier ("Of the 2 a day: up to 2 may be …, 30 cm (12 in) or longer — …"); `presence`
    the steelhead presence line. T128: a kind line names the total it is part of — "Of your 2 trout and char a day in
    streams, up to 2 may be hatchery brook, brown or cutthroat trout" (`says`; its `sentence` the same with the detail and
    notes), unless it is that total's only kind at its number. Every number gate-proved."""
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
    #: ROUND20i 1: the size as the rule, "keep 30–50 cm (12–20 in); release the rest" (None: any size, none, a steelhead)
    size_rule: Optional[StrictStr] = None
    #: ROUND20b C1: the kind is the ONLY row of an own tier at the tier's number — the tier line names the fish ("This
    #: river’s rule: up to 4 hatchery rainbow trout a day") and the page draws no sentence under it, the number once
    folded: StrictBool = False
    #: rows 31: the licence documents this kind needs here (`stamps.py`); absent where none
    stamps: Optional[Tuple[Stamp, ...]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _keep_stamps_keep(self):
        if self.stamps is not None and self.n == 0 and any(x.act == "retaining" for x in self.stamps):
            raise ValueError("a kind keeping none needs no document to keep it")
        return self


class TreeTier(Model):
    """ROUND18 1 (owner: the card as FOLDERS): a tier line holding what it limits — `tier` total ("4 a day per angler
    across Region 2, lakes included", UR1 A), own (a water's own rule: "This river’s rule: up to 4 a day", with `whose`,
    `replaces`, `why_here` — ROUND18 8, "its row covers its tributaries, so it applies here" where the rule reached
    this water by the tributary walk — and `line`), share ("2 a day per angler from this stream (part of the 4)"; UR3 4,
    where a water's own rule lifts fish out of it: "— your hatchery rainbow trout count toward the 4, not these 2"),
    count ("1 cutthroat or rainbow trout in all", UR1 D; UR3 3, inside an own tier or the share: "(part of the 2)") or size (ROUND19 3: HOLDING every kind it caps, "Of these, only 1 may
    be over 50 cm (20 in) — 2 if one is a hatchery steelhead", UR1 B, ROUND UR6 1: `over_cm`, `species` the kinds its cap
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
    #: ROUND UR6 6 (owner): an own tier beside the share under a region total says how it stands to both ("Not part of the 2
    #: from streams; part of the 5 a day across Region 4")
    relation: Optional[StrictStr] = None
    over_cm: Optional[Cm] = None
    species: Optional[Tuple[Fish, ...]] = None
    then: Optional[Tuple[TreeThen, ...]] = None
    extra: Optional[TreeExtra] = None
    #: ROUND20g 1 (owner): the one-origin rule said on the share's tier where it holds on the share's water only ("From
    #: streams, keep hatchery fish only — wild trout and char go back.", Region 2 p.21); the group's `origin` is then None
    origin: Optional[StrictStr] = None

    @model_validator(mode="after")
    def _shape(self):
        if self.tier == "own" and self.whose is None or self.whose is not None and self.tier not in ("own", "total"):
            raise ValueError("a water's own-rule tier (or a water's own total) says whose rule it is")
        if (self.tier == "size") != (self.over_cm is not None):
            raise ValueError("a size tier, and only it, has a size")
        if self.extra is not None and self.tier != "size":
            raise ValueError("only a size tier lets extra kinds past its number")
        if self.relation is not None and self.tier != "own":
            raise ValueError("only a water's own-rule tier says how it stands to the share and the total")
        if self.origin is not None and self.tier != "share":
            raise ValueError("only the share's tier says an origin (the group says one that holds where its total does)")
        return self


TreeTier.model_rebuild()


class GroupPossession(Model):
    """ROUND20h 3 (owner): the possession quota on THIS group's total — `per_daily` the rule's multiplier, `of` the group's
    total (the daily quota it multiplies, never the stream share), `n` their product, `by` the possession rule; `example`
    "With 4 trout and char a day you may have 8: 2 days’ fish."; `doubles` "Possession doubles the 4 a day, not the 2
    from streams." where a share sits under the total (None otherwise)."""
    per_daily: StrictInt = Field(ge=1)
    of: Count
    n: Count
    by: RuleIx
    example: StrictStr = Field(min_length=1)
    doubles: Optional[StrictStr]


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
    possession: Optional[GroupPossession] = None      # ROUND20h 3: the possession quota on this total

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
        # T80 3 (rows.A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP): a total of ONE trout/char kind with no sub-cap is a
        # group too — only as its row's own pool, which the frame checks (`RowsFrame._one_kind_groups`)
        if self.one_kind() and not set(self.kinds) <= _trout_char():
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

    def one_kind(self) -> bool:
        # a total of one kind with no sub-cap: valid only as a trout/char row's own pool (T80 3)
        return len(self.kinds) < 2 and not self.subcaps


def _trout_char() -> frozenset:
    from pipeline.deliver.answers.common import trout_char   # the predicate rows.group_of makes the group by
    return trout_char()


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


YearStatus = Literal["keep", "release", "closed", "none"]
#: rows 30: one run of a row's year strip, [status, days] (`year.py`)
YearRun = Tuple[YearStatus, Positive]
BOOK_YEAR_DAYS = 365


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
    #: rows 30: THE YEAR STRIP — run-length [[status, days]] over the 365 book days (`year.py`: the most
    #: permissive of the members' leading answers over each day's moments outside an hours window; page v36
    #: ROUND14 5). Every row has one.
    year: Tuple[YearRun, ...] = Field(min_length=1)
    #: rows 31 (owner rule 2026-10-21, page v36 parentRow): the index, in this frame's `rows`, of the pooled row whose
    #: pool holds this pooled row's fish (it cross-references them: an `xref` fact; or, T139, it carries the group of
    #: this row's total, another rule's) — the card draws no article for this row; absent on every other row
    #: (`rows.parent_of`)
    parent: Optional[Annotated[StrictInt, Field(ge=0)]] = None
    #: rows 32 (owner rule 2026-10-21; the prototype's round UR7 stampsFor on an Other fish row): the licence documents
    #: the row's fish need by its answer (`stamps.row_stamps`); absent where none
    stamps: Optional[Tuple[Stamp, ...]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _row_stamps(self):
        if self.stamps is None:
            return self
        if self.kind == "closed":
            raise ValueError("a closed row's fish may not be fished: no document")
        if self.kind == "release" and any(x.act == "retaining" for x in self.stamps):
            raise ValueError("a release row keeps none: no document to keep one")
        return self

    @model_validator(mode="after")
    def _year(self):
        if sum(n for _s, n in self.year) != BOOK_YEAR_DAYS:
            raise ValueError(f"a row's year strip covers {sum(n for _s, n in self.year)} days, not 365")
        if any(a[0] == b[0] for a, b in zip(self.year, self.year[1:])):
            raise ValueError("a row's year strip repeats a status in two runs side by side")
        return self

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
        if self.parent is not None and not has_pool:
            raise ValueError("only a pooled row is held by another pool")
        if self.scope is not None and self.scope.narrow_all and self.narrow is None:
            raise ValueError("`narrow_all` says the row's narrowing clause binds every fish: the row has one")
        return self


class FishAnswer(Model):
    hatchery: Optional[Decided]                       # None = no rule in scope speaks
    wild: Optional[Decided]
    # T161 (rows 33): the origin that LEADS (`common.lead_name`): what a one-line view says — the app's `mainRes`
    # reads it and never picks; None exactly when neither origin answers
    lead: Optional[Literal["hatchery", "wild"]]

    @model_validator(mode="after")
    def _lead_is_the_decided_one(self):
        # proved against THE one function (no second copy): the shipped leader is `common.lead_name`'s pick
        from pipeline.deliver.answers import common
        h = None if self.hatchery is None else {"status": self.hatchery.status}
        w = None if self.wild is None else {"status": self.wild.status}
        if self.lead != common.lead_name(h, w):
            raise ValueError(f"`lead` {self.lead!r} is not the leading origin {common.lead_name(h, w)!r} "
                             "(common.lead_origin)")
        return self


class RowsFrame(Model):
    spp: Tuple[Fish, ...]
    fish: Dict[Fish, FishAnswer]
    rows: Tuple[Row, ...]
    steelhead_line: Optional[SteelLineLit]

    @model_validator(mode="after")
    def _parents(self):
        for i, r in enumerate(self.rows):
            if r.parent is None:
                continue
            if r.parent == i or r.parent >= len(self.rows) or self.rows[r.parent].pool is None:
                raise ValueError(f"row {i}'s `parent` {r.parent} is not another pooled row of the frame")
        return self

    @model_validator(mode="after")
    def _one_kind_groups(self):
        # T80 3: a one-kind trout/char group (no sub-cap) is its kind's OWN row's total — some row of the frame pools
        # that kind under the group's pool. A one-kind row inside another rule's total (Duncan River's DV inside
        # Region 4's 5) is no group: "5" over one kind that can never reach it.
        for i, r in enumerate(self.rows):
            g = r.group
            if g is None or not g.one_kind():
                continue
            if not any(o.pool == g.pool and set(g.kinds) <= set(o.members) for o in self.rows):
                raise ValueError(f"row {i}'s one-kind group (pool {g.pool}) is not its own row's pool: a group is a "
                                 f"total several kinds share, or one with a sub-cap, or a trout/char row's own total")
            # T80 follow-up (rows.A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW): only on a row drawn on its own — a row held
            # by another pool (Perry Creek's brook trout under the St. Mary's tributaries' 1) is said inside that pool
            if r.parent is not None:
                raise ValueError(f"row {i}'s one-kind group (pool {g.pool}) sits on a row held by another pool "
                                 f"(row {r.parent}): a one-kind trout/char group is a drawn row's own total")
        return self
