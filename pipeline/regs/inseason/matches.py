"""THE IN-SEASON MATCHES (T226, owner 2026-10-28): `data/curated/inseason/notice_waters.json` — AUTHORED.

One entry per in-season row id (the T225 feed's `rows[].id`, `is-` + 12 hex: (T520) a hash of region + water + the
change's normalised text — never its position, so a row inserted above another cannot move a match onto it; an amended
notice is a NEW id, unmatched until re-matched) saying WHERE the change applies, in the extent vocabulary of the
catalogue (`parsing.entry_models.Extent`), narrowed to what a banner needs:

    {"op": "whole",         "item_id": "gnis:…"}                       the whole water (a lake's closure area is its
                                                                         own item)
    {"op": "upstream_of",   "item_id": "gnis:…", "splits": [s]}         a part of the water, by an existing named split
    {"op": "downstream_of", "item_id": "gnis:…", "splits": [s]}
    {"op": "between",       "item_id": "gnis:…", "splits": [a, b]}

`extents` is a list (a UNION: "Chilliwack Lake, Upper Pitt River"). `effective` overrides the feed's dates ({"from",
"to"}, ISO; either may be omitted), `note` is the curator's. `history` is append-only: one row per edit (set / clear)
with its time, who and the note.

(T520) `waters` is the match RESOLVED on the release the file names (`release`), by `pipeline.regs.inseason.resolve`
where the atlas is (the review app on save; the release promote, `upload.sh`, re-resolves every match): per water it
touches, `{"all": true}` (every section of it) or `{"runs": [[blk, m0, m1], …], "lakes"?: true, "ends": [[lo, hi], …]}`
— FWA stream key + metre ranges (never a section id or a part index, AGENTS 5). (T673) They are RELEASE-INDEPENDENT:
a release places a range on every section of the water whose line [from, to) it touches (`resolve.touched`), so any
release, older or newer, reads the same file; `lakes: true` = every lake piece of the river (no line to range).
`release` stays as where it was resolved and proved (the stamp). Lakes and wetlands are whole-item only. `unresolved` says why a re-resolve could not place the match (publish refuses it).
`publish` only formats this — no atlas.

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
from typing import Dict, Iterable, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

FORMAT = "inseason-matches/2"
ID_RE = re.compile(r"^is-[0-9a-f]{12}$")
ITEM_RE = re.compile(r"^(gnis|wbk|blk|wsc):[\w.\-]+$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
#: how many splits each op names
SPLITS_FOR = {"whole": 0, "upstream_of": 1, "downstream_of": 1, "between": 2}


class InSeasonExtent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    op: Literal["whole", "upstream_of", "downstream_of", "between"]
    item_id: str
    #: the place(s) cutting the water: a splits.json id, an atlas boundary id (`area:…`, `length:…`), or a derived
    #: pick-point (`points.DERIVED_RE`: `mouth`, `lake:<wbk>:inlet|outlet`, `trib:<item>`, `region:A|B`, `mu:A|B`)
    splits: List[str] = Field(default_factory=list)
    #: owner 2026-10-29: "include" adds the regs' tributary walk; "only" is the walk without the stretch itself
    tributaries: Optional[Literal["include", "only"]] = None

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


class Water(BaseModel):
    """(T520) one water of a resolved match: the whole of it, or FWA ranges + their ends in words."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    all: Optional[Literal[True]] = None
    runs: List[Tuple[int, int, int]] = Field(default_factory=list)
    #: (T673) a partly covered STREAM water's lake pieces (a lake the river flows through: no line in any release, so
    #: no range can name one): true = EVERY lake piece of the water, on whatever release reads it. A match covering
    #: some but not all of them is unresolved (`resolve.resolve_match`), never widened or narrowed
    lakes: bool = False
    #: each covered stretch of the water's MAIN LINE as (low, high) end TOKENS, mouth upward — a split id, a
    #: pick-point id (`points`), `mouth` or `source`; words are the file's `words` (core composes the sentence)
    ends: List[Tuple[str, str]] = Field(default_factory=list)

    def model_post_init(self, _ctx) -> None:
        if bool(self.all) == bool(self.runs or self.lakes):
            raise ValueError("a resolved water is either {all: true} or {runs, lakes?, ends}")
        if self.all and self.ends:
            raise ValueError("a whole water has no ends")
        for blk, m0, m1 in self.runs:
            if not 0 <= m0 < m1:
                raise ValueError(f"run {[blk, m0, m1]} is not a range")


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
    #: owner add-on 2026-10-28: "auto" = written by the auto matcher (removed T520; its matches keep the label),
    #: "owner" = a person (the review tab, by hand), "claude" = an agent
    source: Literal["owner", "auto", "claude"] = "owner"
    #: auto only: what matched (water name → item, boundary words → split ids) and how sure
    evidence: Optional[dict] = None
    confidence: Optional[Literal["high", "medium", "low"]] = None
    #: (T520) resolved on the file's `release`; empty only with `unresolved`
    waters: Dict[str, Water] = Field(default_factory=dict)
    unresolved: Optional[str] = None
    #: `stamp_of(extents, release, waters)` when resolved: publish refuses a match whose stamp no longer fits (a hand
    #: edit of its extents or ranges that was not re-resolved)
    stamp: Optional[str] = None

    def model_post_init(self, _ctx) -> None:
        if not self.extents:
            raise ValueError("a match names at least one extent (clear the match instead)")


class HistoryRow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    at: str
    id: str
    #: "auto": rows the removed auto matcher wrote (history is append-only: kept, never written again)
    action: Literal["set", "clear", "auto", "rekey"]
    by: str = ""
    note: str = ""
    #: the match as set (absent on a clear)
    extents: Optional[List[InSeasonExtent]] = None


class Matches(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    comment: str = Field(default="", alias="$comment")
    format: Literal["inseason-matches/2"]
    #: (T520) the data release every match's `waters` is resolved on
    release: str = ""
    #: (T520, INTERIM until T517 ships a token -> words table in the release) the words of every end token the
    #: matches use (`resolve.interim_words`: a place's name, never a sentence)
    words: Dict[str, str] = Field(default_factory=dict)
    matches: Dict[str, Match]
    history: List[HistoryRow] = Field(default_factory=list)

    @field_validator("matches")
    @classmethod
    def _ids(cls, v: Dict[str, Match]) -> Dict[str, Match]:
        bad = [k for k in v if not ID_RE.match(k)]
        if bad:
            raise ValueError(f"not an in-season row id (is-<12 hex>): {bad[:5]}")
        return v


def end_tokens(doc: "Matches") -> List[str]:
    """(T517 review finding 1) every end token the curated in-season matches ship (`mouth` / `source` aside: the
    sentence's shape says them) — what the release's `display.ends` must hold words for."""
    return sorted({t for m in doc.matches.values() for w in (m.waters or {}).values() for p in w.ends for t in p}
                  - {"mouth", "source"})


def default_path() -> Path:
    from pipeline.common.curated import CURATED
    return CURATED.inseason.notice_waters


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
    return set(split_index(json.loads(Path(CURATED.waters.cuts).read_text(encoding="utf-8"))))


def split_labels() -> dict[str, str]:
    """split id -> its label, the name the release's `part_mark.label` carries where two parts meet."""
    from pipeline.atlas.splits.splits import split_index
    from pipeline.common.curated import CURATED
    idx = split_index(json.loads(Path(CURATED.waters.cuts).read_text(encoding="utf-8")))
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
                if known is not None and s not in known and not place_id_shape(s):
                    out.append(f"{rid}: unknown split {s!r}")
    for h in doc.history:
        if not ID_RE.match(h.id):
            out.append(f"history: {h.id!r} is not an in-season row id")
    return out


def span(row: dict, effective: Optional[Effective] = None) -> tuple[Optional[str], Optional[str]]:
    """WHEN A ROW IS IN FORCE — `@app/core` `inSeasonSpan`, the one rule (T520): the curated override, else the feed's
    dates, else — ONLY for a one-off window (T548): a change printing ONE date range, `parsed.window`, that OPENS on
    the row's `from` day, not a rescind / clarification — that range's end in the `from` year, rolling on over the new
    year. Any other printed window is a yearly season: no end is inferred; the row is in force until the page drops it.
    ONE case file pins both: `app/packages/core/src/inseason-span.parity.json`, read by `inseason.test.ts` and
    `test_inseason_publish.py`."""
    frm = (effective.from_ if effective and effective.from_ else None) or row.get("from") or None
    to = (effective.to if effective and effective.to else None) or row.get("to") or None
    parsed = row.get("parsed") or {}
    w = parsed.get("window") or {}
    kinds = parsed.get("kinds") or []
    if not to and frm and w.get("to_month") and w.get("to_day") and w.get("from_month") and w.get("from_day") \
            and frm[5:] == f"{w['from_month']:02d}-{w['from_day']:02d}" \
            and "rescind" not in kinds and "clarification" not in kinds:
        y = int(frm[:4])
        end = f"{y}-{w['to_month']:02d}-{w['to_day']:02d}"
        to = f"{y + 1}-{end[5:]}" if end < frm else end
    return frm, to


def stamp_of(m: Match, release: str) -> str:
    """(T520) the hash a resolved match carries: its extents, the release and its waters, as dumped."""
    import hashlib
    body = json.dumps({"extents": [e.model_dump(exclude_none=True) for e in m.extents], "release": release,
                       "waters": {k: m.waters[k].model_dump(exclude_none=True, exclude_defaults=True)
                                  for k in sorted(m.waters)}}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]


def stamped(m: Match, release: str) -> Match:
    return m.model_copy(update={"stamp": stamp_of(m, release)})


def place_id_shape(pid: str) -> bool:
    """A pick-point id that is not a splits.json id but has a valid shape (a derived point or an atlas boundary);
    whether it is ON the water is the review backend's check (`points.points`)."""
    from pipeline.regs.inseason import points as P
    return P.is_derived(pid) or bool(P.ATLAS_ID_RE.match(pid))


def _dump_match(m: Match) -> dict:
    d = m.model_dump(by_alias=True, exclude_none=True, exclude_defaults=False)
    d["waters"] = {k: {kk: (list(map(list, vv)) if kk in ("runs", "ends") else vv) for kk, vv in w.items()
                       if vv not in ([], "", None, False)}
                   for k, w in sorted(d["waters"].items())}
    return d


def dumps(doc: Matches) -> str:
    """The file's text: `$comment`, format, matches by id, history in order."""
    out = {"$comment": doc.comment, "format": doc.format, "release": doc.release,
           "words": {k: doc.words[k] for k in sorted(doc.words)},
           "matches": {k: _dump_match(doc.matches[k]) for k in sorted(doc.matches)},
           "history": [h.model_dump(exclude_none=True) for h in doc.history]}
    text = json.dumps(out, indent=2, ensure_ascii=False) + "\n"
    # a range or an end pair on one line (the file is read by people too)
    return re.sub(r"\[\s+(-?\d+|\"[^\"\n]*\"),\s+(-?\d+|\"[^\"\n]*\")(?:,\s+(-?\d+))?\s+\]",
                  lambda g: "[" + ", ".join(x for x in g.groups() if x is not None) + "]", text)


def set_match(doc: Matches, rid: str, match: Match, *, at: str, by: str = "", note: str = "") -> Matches:
    """`doc` with `rid` matched to `match` and a `set` history row."""
    if not ID_RE.match(rid):
        raise ValueError(f"{rid!r} is not an in-season row id")
    m = match.model_copy(update={"at": at, "by": by or match.by, "note": note or match.note})
    hist = HistoryRow(at=at, id=rid, action="set", by=by, note=note or match.note, extents=list(match.extents))
    return doc.model_copy(update={"matches": {**doc.matches, rid: m}, "history": [*doc.history, hist]})


def clear_match(doc: Matches, rid: str, *, at: str, by: str = "", note: str = "") -> Matches:
    """`doc` with `rid` unmatched and a `clear` history row. Raises KeyError for a row not matched."""
    if rid not in doc.matches:
        raise KeyError(rid)
    rest = {k: v for k, v in doc.matches.items() if k != rid}
    return doc.model_copy(update={"matches": rest,
                                  "history": [*doc.history, HistoryRow(at=at, id=rid, action="clear", by=by, note=note)]})


__all__ = ["FORMAT", "InSeasonExtent", "Effective", "Match", "HistoryRow", "Matches", "ValidationError",
           "Water", "span", "stamp_of", "stamped", "load", "parse", "problems", "dumps", "set_match", "clear_match", "split_labels", "all_split_ids",
           "default_path"]
