"""THE TROUT AND CHAR TABLE (trout table plan, contract 2.3; tickets F and I): the DECODED value of one cell, `TroutFrame`,
and the WIRE shapes the release stores it in.

These models are THE source of the shape (contract guarantee 1). `TroutFrame` is `FRAMES["trout"]` (`model/frames.py`);
the wire models are emitted beside it (`frames.TROUT_WIRE`, contract 2.3 "wire against decoded"): `TroutFrameWire`
(a `trout_frame` row: [table id, dates id | null, covers]), `TroutTableWire` / `BoxWire` / `TRowWire` (a `trout_table`
row: the body, each row's `sheet` a `trout_sheet` id), `DateSet` (a `trout_dates` row) and `FishSheet` (a `trout_sheet`
row), the same on both sides. The decoded and the wire table share their fields and validators (`_TRowFields`,
`_BoxFields`, `_TableFields`); only a row's `sheet` differs. ANY CHANGE of a field, a validator or the wire layout bumps
the `trout` section's version (`answers.SECTIONS`; contract 2.1), and the I0 pin then fails until the app moves too.

Every field is required; a nullable one has no default and is paired, in a validator, with the condition that
empties it (`model/__init__.py`). No number the app could count with: words, structure and rule indexes only (the one
exception: `YearBar.runs`, day counts drawn to scale, contract guarantee 2). Every text is a whole phrase drawn
unchanged (`Txt`): no leading or trailing space, no "{" or "}" except the one `{day}` slot in `TopLine.lead`.
"""
from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple

from pydantic import AfterValidator, Field, StrictBool, StrictInt, StrictStr, model_validator
from typing_extensions import TypeAliasType

from pipeline.deliver.answers.model.base import FishLit, Model, OriginLit, RuleIx
from pipeline.deliver.answers.model.rows import YearRun

#: the top line's one day slot (contract Q-D2): the app puts "today" or "on Jul 15" there
DAY_SLOT = "{day}"
#: the release and closed column words a reopening may sit under (contract 2.5, owner 15:36)
REOPENS = "reopens "


def _txt(v: str) -> str:
    if v != v.strip():
        raise ValueError(f"a table text has a leading or trailing space: {v!r}")
    if "{" in v.replace(DAY_SLOT, "") or "}" in v.replace(DAY_SLOT, ""):
        raise ValueError(f"a table text holds a brace: {v!r}")
    if DAY_SLOT in v:
        raise ValueError(f"{DAY_SLOT} is the top line's slot only: {v!r}")
    return v


def _lead(v: str) -> str:
    if v != v.strip():
        raise ValueError(f"the top line has a leading or trailing space: {v!r}")
    if v.count(DAY_SLOT) > 1:
        raise ValueError(f"the top line holds {DAY_SLOT} more than once: {v!r}")
    if "{" in v.replace(DAY_SLOT, "") or "}" in v.replace(DAY_SLOT, ""):
        raise ValueError(f"the top line holds a brace outside its slot: {v!r}")
    return v


#: a whole phrase, drawn unchanged
Txt = Annotated[StrictStr, Field(min_length=1), AfterValidator(_txt)]
#: rule indexes (`rule.rix`), each resolved at build
Src = Tuple[RuleIx, ...]
#: a table row: a fish (`common.trout_char()`), or one origin of a twin pair
RowKey = Annotated[StrictStr, Field(pattern=r"^(CT|DV|EB|GB|LT|RB|ST)(/(wild|hatchery))?$")]


class TopLine(Model):
    lead: Annotated[StrictStr, Field(min_length=1), AfterValidator(_lead)]
    sub: Optional[Txt]                  # (trout 4, owner 2026-11-18 01:28) the exception under the general number: "or 4 if
                                        # they’re hatchery rainbow trout 50 cm (20 in) or less" (the Vedder); null otherwise
    second: Optional[Txt]               # the cross-box limit (Lois, Khartoum: = TroutTable.together, same words) or the
                                        # lakes' own count (Echoes: "only 1 from both lakes together", together null)


class Fold(Model):
    value: Txt                          # "Up to 5" | "Up to 2 or 5" | "Up to 6 rainbow, 4 others" | "Release" | …
    tone: Literal["keep", "release", "closed", "none"]


class Band(Model):
    from_: StrictInt = Field(alias="from", ge=0)
    to: StrictInt = Field(ge=0)
    text: Txt
    #: "size" (T810 L, trout 2; owner 2026-11-18 on Region 4's cutthroat and rainbow): the box's number and a size cap the
    #: band's rows share, "counts toward the 2 · only 1 over 50 cm (20 in) between them" (two halves joined by " · ";
    #: drawn as given, its size in the words only)
    kind: Literal["upto", "own", "shared", "possession", "size"]

    @model_validator(mode="after")
    def _span(self):
        if self.to < self.from_:
            raise ValueError(f"a band runs backwards: {self.from_}..{self.to}")
        if self.kind in ("upto", "own", "possession") and self.to != self.from_:
            raise ValueError(f"a {self.kind!r} band is one row's ({self.from_}..{self.to})")
        if self.kind in ("shared", "size") and self.to == self.from_:
            raise ValueError(f"a {self.kind} band spans two rows or more")
        return self


class SheetLine(Model):
    t: Txt
    bold: StrictBool                    # the stop duty


class FishSheet(Model):
    head: Txt
    here: Tuple[SheetLine, ...]
    src: Src = Field(max_length=4)


class _TRowFields(Model):
    """A table row's fields, decoded or on the wire (only `sheet` differs: `TRow`, `TRowWire`)."""
    key: RowKey
    fish: FishLit
    name: Txt
    origin: Optional[OriginLit]         # set <=> one of a twin pair (wild with a quota or size of its own)
    label: Optional[Txt]                # "Hatchery only" beside the name; null when the box carries it
    split: StrictBool                   # (trout 5) a fish's hatchery and wild twins drawn as ONE row ("… if wild"): the
                                        # fish sheet's hatchery link shows as for a row with an origin or a label
    sub: Optional[Txt]
    lines: Tuple[Txt, ...]              # NOW, bold: size, "uses the 1 over …", possession, stop duty, weekday words
    quiet: Tuple[Txt, ...]              # NOW, regular weight
    src: Src
    sheet: Any                          # `TRow`: the FishSheet; `TRowWire`: its `trout_sheet` id

    @model_validator(mode="after")
    def _key(self):
        S, _, o = self.key.partition("/")
        if S != self.fish:
            raise ValueError(f"row {self.key}: its fish is {self.fish}")
        if (o or None) != self.origin:
            raise ValueError(f"row {self.key}: origin {self.origin!r} <=> the key's origin")
        if self.origin is not None and self.label is not None:
            raise ValueError(f"row {self.key}: a twin row carries its origin, never a label")
        if self.split and (self.origin is not None or self.label is not None):
            raise ValueError(f"row {self.key}: a merged hatchery and wild row has no origin and no label")
        return self


class TRow(_TRowFields):
    sheet: FishSheet                    # DECODED


class TRowWire(_TRowFields):
    sheet: Annotated[StrictInt, Field(ge=0)]      # WIRE: the `trout_sheet` id


class _BoxFields(Model):
    """A box's fields, decoded or on the wire (`Box`, `BoxWire`: their rows differ)."""
    kind: Literal["keep", "release", "closed"]
    head: Txt
    label: Optional[Txt]                # keep only: "Hatchery only" when it covers every row of the box
    strip: Optional[Txt]                # keep only
    possession: Optional[Txt]           # keep only: the possession every row shares, its own line under the strip
                                        # ("In your possession: no more than 2 trout and char in all"; T810 G2 Q4)
    tag: Optional[Txt]                  # keep only: "Lake’s rule"
    rows: Tuple[Any, ...] = Field(min_length=1)
    bands: Tuple[Band, ...]
    #: (trout 4, owner 2026-11-18 01:22, R4) a count some rows of one band share among themselves, drawn INSIDE that band
    #: with its cap's words only: "only 1 between them" (the Vedder's Dolly Varden and lake trout inside "2 total from
    #: these 6"), "only 1 over 50 cm (20 in) between them" (kind "size"). Each spans two rows or more of one band, never
    #: the band whole; they never overlap
    inner: Tuple[Band, ...]

    @model_validator(mode="after")
    def _bands(self):
        if self.kind != "keep":
            if self.bands or self.label or self.strip or self.tag or self.possession or self.inner:
                raise ValueError(f"a {self.kind} box has no bands, label, strip, possession or tag")
            return self
        taken: set = set()
        for x in self.inner:
            if x.kind not in ("shared", "size"):
                raise ValueError(f"a nested band is shared or size, not {x.kind!r}")
            out = next((b for b in self.bands if b.from_ <= x.from_ and x.to <= b.to), None)
            if out is None or (out.from_, out.to) == (x.from_, x.to):
                raise ValueError(f"a nested band {x.from_}..{x.to} lies inside no band, or is one whole")
            span = set(range(x.from_, x.to + 1))
            if span & taken:
                raise ValueError(f"nested bands overlap at rows {sorted(span & taken)}")
            taken |= span
        at = 0
        for b in self.bands:
            if b.from_ != at:
                raise ValueError(f"the bands leave a gap or overlap at row {at} (a band from {b.from_})")
            at = b.to + 1
        if at != len(self.rows):
            raise ValueError(f"the bands cover rows 0..{at - 1} of {len(self.rows)}")
        if self.label is not None and any(r.label is not None for r in self.rows):
            raise ValueError("a label on a box and on its rows")
        return self


class Box(_BoxFields):
    rows: Tuple[TRow, ...] = Field(min_length=1)


class BoxWire(_BoxFields):
    rows: Tuple[TRowWire, ...] = Field(min_length=1)


class YearBar(Model):
    title: Txt
    runs: Tuple[YearRun, ...] = Field(min_length=1)


class Together(Model):
    text: Txt
    src: Src = Field(min_length=1)


class _TableFields(Model):
    """The table body's fields, decoded or on the wire (`TroutTable`, `TroutTableWire`: their boxes differ)."""
    top: TopLine
    fold: Fold
    hatch_link: StrictBool
    boxes: Tuple[Any, ...] = Field(min_length=1)
    together: Optional[Together]
    year: Optional[YearBar]             # only when every fish shares one state today (all release or all closed)
    #: under "Fished elsewhere today? ›" (no link where empty), in order: each covered keep row's kept-elsewhere SENTENCE
    #: ("This lake’s rule: 2 a day here. …", "Your own limit, counted across …"; T810 L), then the worked lines ("Kept 2
    #: elsewhere? Keep 3 more here."). A worked line starts "Kept "; anything else is a sentence. Drawn as given.
    elsewhere: Tuple[Txt, ...]
    src: Src

    @model_validator(mode="after")
    def _whole(self):
        order = {"keep": 0, "release": 1, "closed": 2}
        ks = [order[b.kind] for b in self.boxes]
        if ks != sorted(ks) or ks.count(1) > 1 or ks.count(2) > 1:
            raise ValueError(f"boxes: keep boxes first, then at most one release and one closed ({ks})")
        keys = [r.key for b in self.boxes for r in b.rows]
        if len(keys) != len(set(keys)):
            raise ValueError(f"a row key twice: {sorted(k for k in keys if keys.count(k) > 1)}")
        fish_box: Dict[str, int] = {}
        for bi, b in enumerate(self.boxes):
            for r in b.rows:
                if fish_box.setdefault(r.fish, bi) != bi:
                    raise ValueError(f"{r.fish} sits in two boxes")
        marked = any(b.label for b in self.boxes) or any(r.label or r.origin or r.split for b in self.boxes
                                                         for r in b.rows)
        if self.hatch_link != marked:
            raise ValueError("hatch_link <=> some row or box carries a label, an origin or a hatchery/wild split")
        if self.together is not None and self.together.text != self.top.second:
            # T810 G2 (owner 15:41): a limit across two boxes is said under the top line AND under the table (Lois);
            # the lakes' own count (Echoes) only under the top line: `second` without `together`
            raise ValueError("together is set => top.second is, in the same words")
        if self.year is not None and (any(b.kind == "keep" for b in self.boxes) or len(self.boxes) != 1):
            raise ValueError("the year bar only on a table whose fish all share one state (all release or all closed)")
        cited = set(self.src)
        for b in self.boxes:
            for r in b.rows:
                if not set(r.src) <= cited or not set(getattr(r.sheet, "src", ())) <= cited:
                    raise ValueError(f"row {r.key} cites a rule the table's src leaves out")
        if self.together is not None and not set(self.together.src) <= cited:
            raise ValueError("together cites a rule the table's src leaves out")
        if len(self.src) != len(set(self.src)):
            raise ValueError("the table's src names a rule twice")
        return self

    @property
    def form(self) -> Optional[str]:
        """"closed" or "release" for a table in a closed or release-every-one form (no keep box, one kind); else None."""
        kinds = {b.kind for b in self.boxes}
        if kinds == {"closed"}:
            return "closed"
        if kinds == {"release"}:
            return "release"
        return None


class TroutTable(_TableFields):
    boxes: Tuple[Box, ...] = Field(min_length=1)


class TroutTableWire(_TableFields):
    boxes: Tuple[BoxWire, ...] = Field(min_length=1)


class DateSet(Model):
    top: Optional[Txt]
    card: Optional[Txt]
    fold: Optional[Txt]
    box: Dict[StrictStr, Txt]
    band: Dict[StrictStr, Txt]
    row: Dict[StrictStr, Txt]
    sheet: Dict[RowKey, Tuple[Txt, ...]]

    @model_validator(mode="after")
    def _some(self):
        if not (self.top or self.card or self.fold or self.box or self.band or self.row or self.sheet):
            raise ValueError("an empty date set is null")
        if (self.fold is not None) != (self.top is not None) or (self.fold is not None and self.fold != self.top):
            raise ValueError("the folded entry's date is the top line's, set <=> it is")
        return self


class TroutFrame(Model):
    """FRAMES["trout"] (ticket I): one cell's decoded value."""
    table: TroutTable
    dates: Optional[DateSet]
    covers: Tuple[Annotated[StrictInt, Field(ge=0)], ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _dates(self):
        if len(self.covers) != len(set(self.covers)) or list(self.covers) != sorted(self.covers):
            raise ValueError("covers: rows-frame row indexes, ascending, each once")
        d = self.dates
        if d is None:
            return self
        boxes = self.table.boxes
        # (T810 L, trout 3; owner 2026-11-18 00:52 "said once") a date holds every row under its level without a date of
        # its own: the most specific level wins (row, band, box, then the card line or top). A level never repeats the
        # date of the level above it (said once), and each level speaks once per row
        levels: Dict[Tuple[int, int], List[str]] = {}

        def box_of(k: str) -> int:
            b = int(k)
            if not 0 <= b < len(boxes):
                raise ValueError(f"a date names box {k}, which does not exist")
            return b
        whole = d.card if d.card is not None else d.top
        for b in range(len(boxes)):
            for r in range(len(boxes[b].rows)):
                levels[(b, r)] = [] if whole is None else [whole[:1].lower() + whole[1:]]
        for k, t in d.box.items():
            b = box_of(k)
            for r in range(len(boxes[b].rows)):
                levels[(b, r)].append(t)
        for k, t in d.band.items():
            b, _, i = k.partition(":")
            bi = box_of(b)
            if not i.isdigit() or int(i) >= len(boxes[bi].bands):
                raise ValueError(f"a date names band {k}, which does not exist")
            band = boxes[bi].bands[int(i)]
            for r in range(band.from_, band.to + 1):
                levels[(bi, r)].append(t)
        for k, t in d.row.items():
            b, _, i = k.partition(":")
            bi = box_of(b)
            if not i.isdigit() or int(i) >= len(boxes[bi].rows):
                raise ValueError(f"a date names row {k}, which does not exist")
            levels[(bi, int(i))].append(t)
        for (b, r), got in levels.items():
            if any(x == y for x, y in zip(got, got[1:])):
                raise ValueError(f"row {b}:{r} carries one date at two levels")
        keys = {r.key for b in boxes for r in b.rows}
        for k in d.sheet:
            if k not in keys:
                raise ValueError(f"a sheet date names row {k}, which the table does not hold")
        form = self.table.form
        if d.top is not None and form is None:
            raise ValueError("a top-line date only on a closed or release-every-one table")
        if d.card is not None and d.top is not None:
            raise ValueError("a card line and a top-line date at once")
        if d.card is not None and REOPENS in d.card.lower():
            raise ValueError("a reopening date on the card line (only something closed today reopens)")
        if d.top is not None and d.top.startswith(REOPENS) and form != "closed":
            raise ValueError("a reopening date under the top line of a table that is not closed today")
        for k, t in list(d.box.items()) + [(k.partition(":")[0], t) for k, t in d.band.items()] + \
                [(k.partition(":")[0], t) for k, t in d.row.items()]:
            if t.startswith(REOPENS) and boxes[int(k)].kind != "closed":
                raise ValueError(f"a reopening date in a {boxes[int(k)].kind} box: only a fish closed today reopens")
        return self


#: A `trout_frame` row on the wire (contract 2.1): [the `trout_table` id, the `trout_dates` id or null (no date set),
#: covers]. `TroutCodec` (`answers/encode.py`) writes and reads it.
TroutFrameWire = TypeAliasType("TroutFrameWire", Tuple[Annotated[StrictInt, Field(ge=0)],
                                                       Optional[Annotated[StrictInt, Field(ge=0)]],
                                                       Tuple[Annotated[StrictInt, Field(ge=0)], ...]])
