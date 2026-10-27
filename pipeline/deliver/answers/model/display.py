"""The `display` section's models: a segment's status (`StatusFrame`), the strip, and the statics (rule,
part and water facts)."""
from __future__ import annotations

from typing import Annotated, Literal, Optional, Tuple, Union
from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr, model_validator
from pipeline.deliver import types as T
from pipeline.deliver.answers.model.base import Cm, Count, FishLit, LicIx, Model, Positive, RuleIx, _enum


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
    #: display 18 (owner rule 2026-10-21): the closures that close the water on a closed day — page v36 closersAt's
    #: choice of `closing` (`display.closers_of`: those closing the most game fish, and any not wholly inside a
    #: closure of more fish), in `closing`'s order; set <=> status closed. The banner lists these; the app never
    #: chooses among `closing` itself
    closers: Optional[Tuple[RuleIx, ...]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _closing_closes(self):
        if any(not fs for _, fs in self.closing):
            raise ValueError("a closing rule closes at least one fish")
        if (self.closers is not None) != (self.status == "closed"):
            raise ValueError("`closers` is set <=> the status is closed")
        if self.closers is not None:
            order = [r for r, _ in self.closing]
            if not set(self.closers) <= set(order) or \
                    list(self.closers) != [r for r in order if r in set(self.closers)]:
                raise ValueError("`closers` are closing rules, in `closing`'s order")
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


class BookBox(Model):
    """T215: where a rule's entry sits on its synopsis PDF page: `pdf` the 1-based PDF page, `box` [x0, top, x1, bottom]
    and `size` [width, height] in points, origin top-left (the extractor's, `row_boxes.json`)."""
    pdf: Positive
    box: Tuple[StrictFloat, StrictFloat, StrictFloat, StrictFloat]
    size: Tuple[StrictFloat, StrictFloat]


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
    #: T215: the entry's row on the PDF page, to highlight (absent: the entry is no table row, or no sidecar)
    book_box: Optional[Tuple[BookBox, ...]] = Field(default=None, min_length=1)

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
