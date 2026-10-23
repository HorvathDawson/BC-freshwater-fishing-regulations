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
import fcntl
import json
import os
from contextlib import contextmanager
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

#: THE PROBLEM CATEGORIES a "wrong" mark names (PLAN §2), grouped by the check that finds them.
#: The review screen offers exactly these; the store keeps any string (an old mark stays valid).
PROBLEMS: dict[str, tuple[str, ...]] = {
    "T": ("text differs from the book", "wrong page"),
    "R": ("clause missing", "rule invented", "species", "quota", "size", "dates", "gear",
          "tributary scope"),
    "P": ("wrong water", "split misplaced", "ambiguous cut pick", "tributary walk over-reaches",
          "undrawn part / unresolved place"),
    "A": ("app answer differs", "open/closed wrong"),
}


def verdict_from_answers(scope: str, answers: Mapping[str, str]) -> Optional[str]:
    """THE VERDICT THE ANSWERS GIVE (PLAN §2): any n -> wrong; else any ? -> unsure; every check of
    the scope y -> verified; otherwise none yet (a check unanswered). `deferred` is never derived:
    it is the reviewer's own call, with a note. `Mark._consistent` refuses any other verdict."""
    got = [answers.get(k) for k in CHECKS[scope]]
    if "n" in got:
        return "wrong"
    if "?" in got:
        return "unsure"
    if got == ["y"] * len(got):
        return "verified"
    return None


#: THE REVIEWER'S WORDS for a refused mark (T142): the model's message (pydantic prefixes it
#: "Value error, ") -> what the person at the review screen is told to do. Matched by the model's
#: own phrase, so a reworded model message falls through to itself, never to a wrong instruction.
REFUSAL_WORDS: tuple[tuple[str, str], ...] = (
    ("wrong needs a problem category and a note",
     "A wrong mark needs a problem (pick one from the list under the checklist) and a note "
     "saying what is wrong."),
    ("wrong needs a check answered n", "A wrong mark needs a check answered n."),
    ("verified needs every check", "Verified needs every check answered y."),
    ("unsure needs a check answered ?", "Unsure needs a check answered ? and none answered n."),
    ("deferred needs a note saying why", "To defer, write a note saying why."),
    ("a mark needs the reviewer's name", "Type your name first: every mark names its reviewer."),
)


def unanswered_words(scope: str, *, awaiting: bool = False) -> str:
    """What a reviewer is told when the answers give no verdict yet (a check left unanswered).
    `awaiting`: the target awaits its build (`vintage.entry_state`), so only T and R can be
    answered (`vintage.CHECKS_WHILE_AWAITING`) and T y R y is no verdict (Q96, not built) —
    "answer every check" would ask for what the screen will not let them answer."""
    if awaiting:
        from pipeline.regs.review.vintage import CHECKS_WHILE_AWAITING
        can = [c for c in CHECKS[scope] if c in CHECKS_WHILE_AWAITING]
        wait = [c for c in CHECKS[scope] if c not in CHECKS_WHILE_AWAITING]
        return (f"Until it is built only {' and '.join(can)} can be answered and "
                f"{' and '.join(wait)} wait, so \"all fine\" waits for the build: record it "
                f"wrong (answer n) or unsure (answer ?), or defer it with a note.")
    return (f"Answer every check ({' '.join(CHECKS[scope])}) with y, n or ? before recording "
            f"the mark, or defer it with a note.")


def refusal_words(msg: str, field: str | None = None) -> str:
    """A refused mark's message in the reviewer's words (`REFUSAL_WORDS`); `field` is the payload
    field the refusal names, if any (an empty `reviewer` is refused by its length, not a phrase).
    A message no phrase matches (the gate's, awaiting build's — already sentences) is passed through
    as it is, less pydantic's "Value error, " prefix."""
    m = str(msg).removeprefix("Value error, ")
    if field == "reviewer":
        return dict(REFUSAL_WORDS)["a mark needs the reviewer's name"]
    for phrase, words in REFUSAL_WORDS:
        if phrase in m:
            return words
    return m


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
    #: THE REVIEWER'S TIME on the target before this mark, by the review screen's clock (seconds
    #: from the target opening — or the previous mark of a zone entry's rules — to this one): the
    #: T136 pilot's "time per entry". None for a mark made outside the screen (a migration's copy,
    #: a script).
    seconds: Optional[float] = Field(default=None, ge=0)

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


@contextmanager
def store_lock(path: Path):
    """THE STORE'S LOCK ACROSS PROCESSES: an exclusive `flock` on the store's DIRECTORY, held for a
    whole read-append-write. A writer reads the file, appends and replaces it (`atomic_write`), so
    two processes doing that at once (the review app and a migration's `append`) dropped one's
    lines; a lock on the file itself would not do, since the replace gives the next writer a new
    inode. The review app's thread lock (`writes.LOCK`) serialises its own threads only."""
    d = Path(path).parent
    fd = os.open(d, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def append(path: Path, marks: Sequence[Mark]) -> None:
    """Append `marks` to the store (atomic, under `store_lock`). A reviewer's NEW mark asks the
    gate first (`refuse_on_gate`; the review app's `post_mark`)."""
    from pipeline.regs.parsing.io import atomic_write
    if not Path(path).exists():
        load(path)                                             # raises: the store is checked in
    with store_lock(path):
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
    """RULE IDENTITY: pair each marked record with the current record it IS, in passes, each
    taking only records no earlier pass claimed:

      1. same key, meaning and binding        (nothing moved)
      2. same kind, meaning and binding       (a re-parse renumbered it)
      3. same key and meaning                 (its binding moved)
      4. same kind and meaning — ONLY when the marked key is gone from `now` (renumbered AND moved)

    Binding before key: 38 rules in 16 entries share their meaning with a sibling ("No fishing" on
    two reaches), and pairing on meaning alone sent a mark to the sibling — a renumber read as
    "binding moved", a changed rule as "binding moved" instead of "rule changed". Pass 4 never
    runs while the key still names a record: that record IS the rule the mark was on, changed.
    Never by id alone: a record whose meaning changed has no pair — "rule changed".
    Returns (pairs, marked records with no current one, current records no mark holds)."""
    free = list(now)
    keys_now = {c.key for c in now}
    pairs: list[tuple[RecordPrint, RecordPrint]] = []
    left: list[RecordPrint] = list(marked)
    for same in (lambda m, c: c.key == m.key and c.meaning == m.meaning and c.binding == m.binding,
                 lambda m, c: (_kind(c.key) == _kind(m.key) and c.meaning == m.meaning
                               and c.binding == m.binding),
                 lambda m, c: c.key == m.key and c.meaning == m.meaning,
                 lambda m, c: (m.key not in keys_now and _kind(c.key) == _kind(m.key)
                               and c.meaning == m.meaning)):
        still: list[RecordPrint] = []
        for m in left:
            hit = next((c for c in free if same(m, c)), None)
            if hit is None:
                still.append(m)
            else:
                pairs.append((m, hit))
                free.remove(hit)
        left = still
    return pairs, left, free


def held_rule(mark: Mark, now: Prints | Mapping | None) -> str | None:
    """The rule_id a RULE mark is on NOW (`match_records`: renumbered rules are followed), or None
    when its rule has no pair (changed or gone) or `mark` is not a rule mark."""
    if mark.scope != "rule" or now is None:
        return None
    if not isinstance(now, Prints):
        now = Prints.model_validate(now)
    pairs, _, _ = match_records(mark.prints.records, _held_against(mark, now))
    return next((cur.key.split(":", 1)[1] for was, cur in pairs
                 if was.key == f"rule:{mark.rule_id}"), None)


def _held_against(mark: Mark, now: Prints) -> list[RecordPrint]:
    """The current records a mark is compared with: a rule mark, its entry's record and the
    entry's rules (its own may have been renumbered); any other mark, every record."""
    if mark.scope == "rule":
        return [r for r in now.records if r.key == "entry" or _kind(r.key) == "rule"]
    return list(now.records)


#: WHICH PART OF A MARK'S PRINTS MOVED (`moved`) -> the reason it gives. `records`: a record the
#: mark held is gone, or (not a rule mark) a new one appeared; `unbuilt`: a bundle half the mark
#: had is not built now.
MOVED_REASONS: dict[str, StaleReason] = {
    "text": StaleReason.book_text_changed,
    "records": StaleReason.rule_changed,
    "meaning": StaleReason.rule_changed,
    "binding": StaleReason.binding_moved,
    "reach": StaleReason.binding_moved,
    "answer": StaleReason.answer_changed,
    "unbuilt": StaleReason.awaiting_build,
}
#: the parts a BUILD writes (the bundle's half) — every other part is read from the curated files.
#: A mark gone stale with only these moved was moved by a rebuild alone, not by an edit: across a
#: rebuild of unchanged inputs that is a FALSE STALE (the T136 gate: 0).
BUILT_PARTS = frozenset({"reach", "answer", "unbuilt"})


def moved(mark: Mark, now: Prints | Mapping | None) -> frozenset[str]:
    """THE ONE COMPARISON of a mark's prints with its target's prints NOW (`now`: from
    `fingerprint`; None when the target is gone): which parts moved (`MOVED_REASONS`' keys).
    Empty = the mark holds. `stale_reasons` is this, named; the T136 gate asks it which moves a
    rebuild alone made (`BUILT_PARTS`)."""
    if now is None:
        return frozenset({"records"})
    if not isinstance(now, Prints):
        now = Prints.model_validate(now)
    got: set[str] = set()
    if mark.prints.text != now.text:
        got.add("text")
    # a rule mark is held against its entry's record and ITS rule — the entry's other rules are
    # not what it answered for; its rule may have been renumbered (`match_records`)
    pairs, gone, new = match_records(mark.prints.records, _held_against(mark, now))
    if gone or (new and mark.scope != "rule"):
        got.add("records")
    for was, cur in pairs:
        if was.meaning != cur.meaning:
            got.add("meaning")
        if was.binding != cur.binding:
            got.add("binding")
        for part in ("reach", "answer"):
            a, b = getattr(was, part), getattr(cur, part)
            if a is not None and b is None:
                got.add("unbuilt")
            elif a is not None and a != b:
                got.add(part)
    return frozenset(got)


def stale_reasons(mark: Mark, now: Prints | Mapping | None) -> list[StaleReason]:
    """Why `mark` no longer holds on its target as it is NOW (`now`: the target's prints from
    `fingerprint`; None when the target is gone). Empty = the mark holds. Reasons in
    `STALE_REASONS` order, each once — `moved`, named by `MOVED_REASONS`."""
    got = {MOVED_REASONS[p] for p in moved(mark, now)}
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


