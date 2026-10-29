"""THE IN-SEASON MATCHES (T226, owner 2026-10-28): `data/curated/inseason/matches.json` — AUTHORED.

One entry per in-season row id (the T225 feed's `rows[].id`, `is-` + 12 hex: a hash of region + water as printed + its
order among that water's rows, so a reworded row keeps its id) saying WHERE the change applies, in the extent
vocabulary of the catalogue (`parsing.entry_models.Extent`), narrowed to what a banner needs:

    {"op": "whole",         "item_id": "gnis:…"}                       the whole water (a lake's closure area is its
                                                                         own item)
    {"op": "upstream_of",   "item_id": "gnis:…", "splits": [s]}         a part of the water, by an existing named split
    {"op": "downstream_of", "item_id": "gnis:…", "splits": [s]}
    {"op": "between",       "item_id": "gnis:…", "splits": [a, b]}

`extents` is a list (a UNION: "Chilliwack Lake, Upper Pitt River"). `effective` overrides the feed's dates ({"from",
"to"}, ISO; either may be omitted), `note` is the curator's. `history` is append-only: one row per edit (set / clear)
with its time, who and the note.

INFORMATIONAL ONLY. A match puts a banner on the water in the app; it never changes a computed answer or a status.
This file is never a mark (AGENTS 66): the review app writes it through its one in-season route, `writes.commit`.

    load(path=None) -> Matches       raises on a missing file (AGENTS 37) and on an invalid one
    problems(doc, feed_ids=…, split_ids=…) -> [str]   every reason the doc is refused, [] when it is not
    set_match / clear_match          the edit + its history row (pure: the caller commits the text)
    dumps(doc) -> str                the file's text (stable key order)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

FORMAT = "inseason-matches/1"
ID_RE = re.compile(r"^is-[0-9a-f]{12}$")
ITEM_RE = re.compile(r"^(gnis|wbk|blk|wsc):[\w.\-]+$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
#: how many splits each op names
SPLITS_FOR = {"whole": 0, "upstream_of": 1, "downstream_of": 1, "between": 2}


class InSeasonExtent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    op: Literal["whole", "upstream_of", "downstream_of", "between"]
    item_id: str
    splits: List[str] = Field(default_factory=list)

    @field_validator("item_id")
    @classmethod
    def _item(cls, v: str) -> str:
        if not ITEM_RE.match(v):
            raise ValueError(f"item_id {v!r} is not a registry id (gnis:…, wbk:…)")
        return v

    def model_post_init(self, _ctx) -> None:
        n = SPLITS_FOR[self.op]
        if len(self.splits) != n:
            raise ValueError(f"op {self.op} names {n} split(s), got {len(self.splits)}")
        if len(set(self.splits)) != len(self.splits):
            raise ValueError(f"op {self.op} names the same split twice")


class Effective(BaseModel):
    # `from` is a keyword: the field is `from_`, the JSON key `from`
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    @field_validator("from_", "to")
    @classmethod
    def _date(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not DATE_RE.match(v):
            raise ValueError(f"date {v!r} is not YYYY-MM-DD")
        return v

    def model_post_init(self, _ctx) -> None:
        if self.from_ and self.to and self.to < self.from_:
            raise ValueError(f"effective to {self.to} is before from {self.from_}")


class Match(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    #: the row as printed when matched (for a reader of this file; the feed is the authority)
    region: str = ""
    water: str = ""
    extents: List[InSeasonExtent]
    effective: Optional[Effective] = None
    note: str = ""
    by: str = ""
    at: str = ""
    #: owner add-on 2026-10-28: "auto" = written by `pipeline.regs.inseason.auto` (unambiguous only), "owner" = a
    #: person (the review tab, by hand). An owner match always wins: auto never overwrites one.
    source: Literal["owner", "auto"] = "owner"
    #: auto only: what matched (water name → item, boundary words → split ids) and how sure
    evidence: Optional[dict] = None
    confidence: Optional[Literal["high", "medium", "low"]] = None

    def model_post_init(self, _ctx) -> None:
        if not self.extents:
            raise ValueError("a match names at least one extent (clear the match instead)")


class HistoryRow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    at: str
    id: str
    action: Literal["set", "clear", "auto"]
    by: str = ""
    note: str = ""
    #: the match as set (absent on a clear)
    extents: Optional[List[InSeasonExtent]] = None


class Matches(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    comment: str = Field(default="", alias="$comment")
    format: Literal["inseason-matches/1"]
    matches: Dict[str, Match]
    history: List[HistoryRow] = Field(default_factory=list)
    #: auto's BEST GUESS for a row it would not match (ambiguous): pre-fills the review tab's editor, never shown in
    #: the app. {id: {"extents": [...]?, "why": "...", "evidence": {...}}}
    guesses: Dict[str, dict] = Field(default_factory=dict)

    @field_validator("matches")
    @classmethod
    def _ids(cls, v: Dict[str, Match]) -> Dict[str, Match]:
        bad = [k for k in v if not ID_RE.match(k)]
        if bad:
            raise ValueError(f"not an in-season row id (is-<12 hex>): {bad[:5]}")
        return v


def default_path() -> Path:
    from pipeline.common.curated import CURATED
    return CURATED.inseason.matches


def parse(text: str) -> Matches:
    return Matches.model_validate(json.loads(text))


def load(path: Path | str | None = None, *, split_ids: Iterable[str] | None = None) -> Matches:
    """The curated matches, validated. A missing file RAISES (AGENTS 37); so does an unknown split when `split_ids`
    is given (default: every id in the curated splits.json)."""
    p = Path(path) if path else default_path()
    doc = parse(p.read_text(encoding="utf-8"))
    errs = problems(doc, split_ids=all_split_ids() if split_ids is None else split_ids)
    if errs:
        raise ValueError(f"{p}: " + "; ".join(errs))
    return doc


def all_split_ids() -> set[str]:
    from pipeline.atlas.splits.splits import split_index
    from pipeline.common.curated import CURATED
    return set(split_index(json.loads(Path(CURATED.waters.splits).read_text(encoding="utf-8"))))


def split_labels() -> dict[str, str]:
    """split id -> its label, the name the release's `part_mark.label` carries where two parts meet."""
    from pipeline.atlas.splits.splits import split_index
    from pipeline.common.curated import CURATED
    idx = split_index(json.loads(Path(CURATED.waters.splits).read_text(encoding="utf-8")))
    return {k: str(v.get("label") or "") for k, v in idx.items()}


def problems(doc: Matches, *, feed_ids: Iterable[str] | None = None,
             split_ids: Iterable[str] | None = None) -> list[str]:
    """Every reason `doc` is refused: a split that is not in splits.json, a match for a row the feed does not hold
    (only when `feed_ids` is given: the feed's rows AND its `removed`; the backend checks a new match against it)."""
    out: list[str] = []
    known = set(split_ids) if split_ids is not None else None
    feed = set(feed_ids) if feed_ids is not None else None
    for rid, m in sorted(doc.matches.items()):
        if feed is not None and rid not in feed:
            out.append(f"{rid}: no such in-season row in the feed")
        for e in m.extents:
            for s in e.splits:
                if known is not None and s not in known:
                    out.append(f"{rid}: unknown split {s!r}")
    for h in doc.history:
        if not ID_RE.match(h.id):
            out.append(f"history: {h.id!r} is not an in-season row id")
    return out


def _dump_match(m: Match) -> dict:
    return m.model_dump(by_alias=True, exclude_none=True, exclude_defaults=False)


def dumps(doc: Matches) -> str:
    """The file's text: `$comment`, format, matches by id, history in order."""
    out = {"$comment": doc.comment, "format": doc.format,
           "matches": {k: _dump_match(doc.matches[k]) for k in sorted(doc.matches)},
           "history": [h.model_dump(exclude_none=True) for h in doc.history],
           **({"guesses": {k: doc.guesses[k] for k in sorted(doc.guesses)}} if doc.guesses else {})}
    return json.dumps(out, indent=2, ensure_ascii=False) + "\n"


def set_match(doc: Matches, rid: str, match: Match, *, at: str, by: str = "", note: str = "") -> Matches:
    """`doc` with `rid` matched to `match` and a `set` history row."""
    if not ID_RE.match(rid):
        raise ValueError(f"{rid!r} is not an in-season row id")
    m = match.model_copy(update={"at": at, "by": by or match.by, "note": note or match.note})
    guesses = {k: v for k, v in doc.guesses.items() if k != rid}
    hist = HistoryRow(at=at, id=rid, action="set", by=by, note=note or match.note, extents=list(match.extents))
    return doc.model_copy(update={"matches": {**doc.matches, rid: m}, "history": [*doc.history, hist],
                                  "guesses": guesses})


def clear_match(doc: Matches, rid: str, *, at: str, by: str = "", note: str = "") -> Matches:
    """`doc` with `rid` unmatched and a `clear` history row. Raises KeyError for a row not matched."""
    if rid not in doc.matches:
        raise KeyError(rid)
    rest = {k: v for k, v in doc.matches.items() if k != rid}
    return doc.model_copy(update={"matches": rest,
                                  "history": [*doc.history, HistoryRow(at=at, id=rid, action="clear", by=by, note=note)]})


__all__ = ["FORMAT", "InSeasonExtent", "Effective", "Match", "HistoryRow", "Matches", "ValidationError",
           "load", "parse", "problems", "dumps", "set_match", "clear_match", "split_labels", "all_split_ids",
           "default_path"]
