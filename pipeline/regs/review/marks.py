"""A reviewer's MARK, the append-only store that holds them, and the one staleness decision.

    data/curated/regulations/review/marks.jsonl      (`CURATED.regulations.review.marks`)

One JSON object per line, each a `Mark` (pydantic, `extra=forbid`). APPEND-ONLY: a later mark on
the same target supersedes an earlier one (`latest`), so the file is the history and git holds it.
It is REVIEWED curated data (AGENTS 35): no program regenerates it, a migration only APPENDS
re-stamped copies (`carry.carry_marks`).

A mark is taken against the fingerprints of its target AS IT WAS (`fingerprint`), and
`stale_reasons` compares them with the target NOW. The answer is a list of typed reasons
(`StaleReason`), empty while the mark still holds — never one undifferentiated "changed".

RULE IDENTITY (PLAN §4): a record of the mark follows the current record with the same key AND
the same meaning; failing that, a record of the same kind (rule / licensing) with the same
meaning that nothing else claimed (a re-parse renumbered the rules); failing that it is "rule
changed". A mark never lands silently on another rule.
"""
from __future__ import annotations

import datetime as dt
import json
from enum import Enum
from pathlib import Path
from typing import Iterable, Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StaleReason(str, Enum):
    """Why a mark no longer holds — which of the reviewer's questions a change reopens."""
    book_text_changed = "book text changed"
    rule_changed = "rule changed"
    binding_moved = "binding moved"
    answer_changed = "answer changed"
    awaiting_build = "awaiting build"


#: the order reasons are reported in (and the order of the enum)
STALE_REASONS: tuple[StaleReason, ...] = tuple(StaleReason)

Scope = Literal["entry", "rule", "split"]
Verdict = Literal["verified", "wrong", "unsure", "deferred"]
Answer = Literal["y", "n", "?"]
#: the checks each scope answers (PLAN §2: T text, R rules, P place, A answer; a split is a place)
CHECKS: dict[str, tuple[str, ...]] = {"entry": ("T", "R", "P", "A"),
                                      "rule": ("T", "R", "P", "A"),
                                      "split": ("P",)}


class RecordPrint(BaseModel):
    """One record's fingerprints (`fingerprint.entry_records`)."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    key: str = Field(..., pattern=r"^(entry|rule:.+|lic:.+|split:.+)$")
    meaning: str
    binding: str
    reach: Optional[str] = None
    answer: Optional[str] = None


class Prints(BaseModel):
    """What a mark is held against: the book text (None for a split) and every record's prints."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    text: Optional[str] = None
    records: tuple[RecordPrint, ...]


class Mark(BaseModel):
    """One reviewer's call on one target. `reviewer` is required; `pdf_sha256` is the book the
    reviewer read (the repo copy's bytes, which differ from gov.bc.ca's); `pages` are PRINTED page
    numbers. `carried_by` names the migration that re-stamped a copy of an earlier mark."""
    model_config = ConfigDict(extra="forbid", frozen=True)

    scope: Scope
    target: str = Field(..., min_length=1, description="entry_id, or a split id")
    rule_id: Optional[str] = Field(default=None, description="scope rule: the rule marked")
    verdict: Verdict
    answers: dict[Literal["T", "R", "P", "A"], Answer]
    problem: Optional[str] = None
    note: str = ""
    reviewer: str = Field(..., min_length=1)
    date: str = Field(..., description="ISO date the mark was made (carried: the original's)")
    pdf_sha256: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    pages: list[int] = Field(default_factory=list, description="printed pages read")
    prints: Prints
    carried_by: Optional[str] = None

    @field_validator("reviewer")
    @classmethod
    def _reviewer_named(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("a mark needs the reviewer's name")
        return v.strip()

    @field_validator("date")
    @classmethod
    def _iso_date(cls, v: str) -> str:
        dt.date.fromisoformat(v)
        return v

    @model_validator(mode="after")
    def _consistent(self) -> "Mark":
        need = CHECKS[self.scope]
        if (self.scope == "rule") != (self.rule_id is not None):
            raise ValueError("a rule mark names its rule_id; an entry or split mark does not")
        extra = sorted(set(self.answers) - set(need))
        if extra:
            raise ValueError(f"a {self.scope} mark answers {list(need)}, not {extra}")
        got = [self.answers.get(k) for k in need]
        if self.verdict == "verified" and got != ["y"] * len(need):
            raise ValueError(f"verified needs every check {list(need)} answered y")
        if self.verdict == "wrong":
            if "n" not in got:
                raise ValueError("wrong needs a check answered n")
            if not (self.problem or "").strip() or not self.note.strip():
                raise ValueError("wrong needs a problem category and a note")
        if self.verdict == "unsure" and ("?" not in got or "n" in got):
            raise ValueError("unsure needs a check answered ? and none answered n")
        if self.verdict == "deferred" and not self.note.strip():
            raise ValueError("deferred needs a note saying why")
        keys = [r.key for r in self.prints.records]
        if self.scope == "split":
            if keys != [f"split:{self.target}"] or self.prints.text is not None:
                raise ValueError("a split mark holds exactly its split's prints")
        else:
            if self.prints.text is None or "entry" not in keys:
                raise ValueError("an entry or rule mark holds the text and the entry record")
            if self.scope == "rule" and sorted(keys) != sorted(["entry", f"rule:{self.rule_id}"]):
                raise ValueError("a rule mark holds the entry record and its own rule's")
        return self

    @property
    def key(self) -> tuple:
        """What a later mark supersedes this one on."""
        return (self.scope, self.target, self.rule_id)


# --------------------------------------------------------------------------------------------
# The store
# --------------------------------------------------------------------------------------------

def line(mark: Mark) -> str:
    """THE ONE SERIALISATION of a mark: one compact JSON line, keys in model order."""
    return json.dumps(json.loads(mark.model_dump_json()), ensure_ascii=False,
                      separators=(",", ":")) + "\n"


def load(path: Path) -> list[Mark]:
    """Every mark in the file, in order. A MISSING FILE RAISES (AGENTS 37: absent curated data is a
    bug, never an empty set) — the store is checked in empty. A bad line raises naming its number."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"no marks store at {p} — it is checked in (empty) under "
                                f"data/curated/regulations/review/")
    out: list[Mark] = []
    for n, text in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
        if not text.strip():
            continue
        try:
            out.append(Mark.model_validate_json(text))
        except ValueError as ex:
            raise ValueError(f"{p}:{n}: not a mark: {ex}") from ex
    return out


def appended(path: Path, marks: Sequence[Mark]) -> str:
    """The store's text with `marks` appended — what a writer commits (the review app through its
    backed-up `writes.commit`, `append` through an atomic write)."""
    p = Path(path)
    before = p.read_text(encoding="utf-8") if p.exists() else ""
    if before and not before.endswith("\n"):
        before += "\n"
    return before + "".join(line(m) for m in marks)


def append(path: Path, marks: Sequence[Mark]) -> None:
    """Append `marks` to the store (atomic). A reviewer's NEW mark asks the gate first
    (`refuse_on_gate`; the review app's `post_mark`)."""
    from pipeline.regs.parsing.io import atomic_write
    if not Path(path).exists():
        load(path)                                             # raises: the store is checked in
    atomic_write(Path(path), appended(path, marks))


def refuse_on_gate(target: str, gate_errors: Sequence) -> None:
    """MARKS ARE REFUSED ON AN ENTRY THAT FAILS THE GATE (PLAN §4 phase 1): what the reviewer would
    sign off is not what ingest accepts. `gate_errors` is the save gate's answer for the entry as
    it is on disk (the review app's `check_entry`, `validate_catalogue` beneath it)."""
    if gate_errors:
        msgs = [e.get("msg", str(e)) if isinstance(e, Mapping) else str(e) for e in gate_errors]
        raise ValueError(f"{target} fails the gate, so it cannot be marked: " + "; ".join(msgs[:5]))


def latest(marks: Iterable[Mark]) -> dict[tuple, Mark]:
    """The mark in force per target: the LAST one appended (the store is the history)."""
    out: dict[tuple, Mark] = {}
    for m in marks:
        out[m.key] = m
    return out


# --------------------------------------------------------------------------------------------
# The one staleness decision
# --------------------------------------------------------------------------------------------

def _kind(key: str) -> str:
    return key.split(":", 1)[0]


def match_records(marked: Sequence[RecordPrint], now: Sequence[RecordPrint]
                  ) -> tuple[list[tuple[RecordPrint, RecordPrint]], list[RecordPrint], list[RecordPrint]]:
    """RULE IDENTITY: pair each marked record with the current record it IS — same key and same
    meaning; else same kind and same meaning, unclaimed (a re-parse renumbered it). Never by id
    alone: a record whose meaning changed has no pair — it is "rule changed", and its id may now
    be another rule's. Returns (pairs, marked records with no current one, current records no
    mark holds)."""
    free = list(now)
    pairs: list[tuple[RecordPrint, RecordPrint]] = []
    left: list[RecordPrint] = []
    for rule in (lambda m, c: c.key == m.key and c.meaning == m.meaning,
                 lambda m, c: _kind(c.key) == _kind(m.key) and c.meaning == m.meaning):
        left = []
        for m in marked:
            if any(p[0] is m for p in pairs):
                continue
            hit = next((c for c in free if rule(m, c)), None)
            if hit is None:
                left.append(m)
            else:
                pairs.append((m, hit))
                free.remove(hit)
    return pairs, left, free


def stale_reasons(mark: Mark, now: Prints | Mapping | None) -> list[StaleReason]:
    """Why `mark` no longer holds on its target as it is NOW (`now`: the target's prints from
    `fingerprint`; None when the target is gone). Empty = the mark holds. Reasons in
    `STALE_REASONS` order, each once."""
    if now is None:
        return [StaleReason.rule_changed]
    if not isinstance(now, Prints):
        now = Prints.model_validate(now)
    got: set[StaleReason] = set()
    if mark.prints.text != now.text:
        got.add(StaleReason.book_text_changed)
    current = list(now.records)
    if mark.scope == "rule":
        # a rule mark is held against its entry's record and ITS rule — the entry's other rules
        # are not what it answered for; its rule may have been renumbered (`match_records`)
        current = [r for r in current
                   if r.key == "entry" or _kind(r.key) == "rule"]
    pairs, gone, new = match_records(mark.prints.records, current)
    if gone:
        got.add(StaleReason.rule_changed)
    if new and mark.scope != "rule":
        got.add(StaleReason.rule_changed)
    for was, cur in pairs:
        if was.meaning != cur.meaning:
            got.add(StaleReason.rule_changed)
        if was.binding != cur.binding:
            got.add(StaleReason.binding_moved)
        for field, moved in (("reach", StaleReason.binding_moved),
                             ("answer", StaleReason.answer_changed)):
            a, b = getattr(was, field), getattr(cur, field)
            if a is not None and b is None:
                got.add(StaleReason.awaiting_build)
            elif a is not None and a != b:
                got.add(moved)
    return [r for r in STALE_REASONS if r in got]


def split_states(marks: Iterable[Mark], ctx) -> dict[str, dict]:
    """Every split in splits.json (`ctx`, a `fingerprint.Context`) -> its `status` now. A split
    nobody has confirmed is `unmarked`, so a split ADDED mid-review queues itself for its own
    `split` mark (owner 2026-10-23); a moved one reads stale, "binding moved"."""
    from pipeline.regs.review import fingerprint as F
    held = {m.target: m for m in latest(marks).values() if m.scope == "split"}
    return {sid: status(held.get(sid), Prints.model_validate(F.split_fingerprints(sid, ctx)))
            for sid in sorted(ctx.split_index)}


def status(mark: Mark | None, now: Prints | Mapping | None) -> dict:
    """What the queue shows for one target: {status, verdict, reasons}. `status` is `unmarked`, the
    verdict while the mark holds, or `stale`."""
    if mark is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    reasons = stale_reasons(mark, now)
    return {"status": "stale" if reasons else mark.verdict, "verdict": mark.verdict,
            "reasons": [r.value for r in reasons]}


