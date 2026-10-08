"""`verdicts.sqlite` — the reader's every answer, typed, interned, checked (DATAFLOW §1.3 D, U1).

A sidecar beside `bundle.sqlite`, stamped with the bundle's digests (`section_handles`,
`reach_digest`, `rule_ids_sha256`); never shipped to the app. Every code is a closed enum's
(`pipeline.deliver.types`), and the file CARRIES ITS ENUM TABLES: `VerdictStore.open` refuses a
file whose tables differ from the Python enums, so a state or a loss reason renamed or added in
the reader is a refusal until both move together.

  meta          format 'verdicts/1' and the bundle's digests
  enum_*        the codes: rule state, loss reason (with the ONE state it gives), fish, origin
  key_meta      per rule key: `own` — a water table's row binds the set (the status floor)
  key_fish      per rule key: the fish asked (every game fish + the extras a member rule names)
  reading       per rule key: each distinct reading of its year, the day it is asked on, `closed`
  segment       per rule key: the runs of days, each to a reading (cover 1..366, start on 1)
  verdict_rule  one rule's line in an interned verdict: state; a loser's reason and `by`
  verdict_lifter  the rules lifting a speaker in part
  frame         (key, reading, fish, origin) -> verdict: the address of one reader call
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver import types as T
from pipeline.deliver.bundle import read

FORMAT = "verdicts/1"

#: The digests a verdicts file must share with its bundle (meta keys of both).
DIGESTS = ("section_handles", "reach_digest", "rule_ids_sha256")

SCHEMA = """
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE enum_state  (code INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                          speaker INTEGER NOT NULL CHECK (speaker IN (0, 1)));
CREATE TABLE enum_reason (code INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                          state INTEGER NOT NULL REFERENCES enum_state(code),
                          UNIQUE (code, state));
CREATE TABLE enum_fish   (code INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                          game INTEGER NOT NULL CHECK (game IN (0, 1)));
CREATE TABLE enum_origin (code INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE key_meta (key_ix INTEGER PRIMARY KEY CHECK (key_ix >= 0),
                       own INTEGER NOT NULL CHECK (own IN (0, 1)));
CREATE TABLE key_fish (key_ix INTEGER NOT NULL REFERENCES key_meta(key_ix),
                       fish INTEGER NOT NULL REFERENCES enum_fish(code),
                       PRIMARY KEY (key_ix, fish)) WITHOUT ROWID;
CREATE TABLE reading (key_ix INTEGER NOT NULL REFERENCES key_meta(key_ix),
                      reading INTEGER NOT NULL CHECK (reading >= 0),
                      first_day INTEGER NOT NULL CHECK (first_day BETWEEN 1 AND 366),
                      closed INTEGER NOT NULL CHECK (closed IN (0, 1)),
                      PRIMARY KEY (key_ix, reading)) WITHOUT ROWID;
CREATE TABLE segment (key_ix INTEGER NOT NULL,
                      start INTEGER NOT NULL CHECK (start BETWEEN 1 AND 366),
                      reading INTEGER NOT NULL,
                      PRIMARY KEY (key_ix, start),
                      FOREIGN KEY (key_ix, reading) REFERENCES reading(key_ix, reading))
                      WITHOUT ROWID;
CREATE TABLE verdict_rule (
  verdict INTEGER NOT NULL CHECK (verdict >= 0),
  rule    INTEGER NOT NULL CHECK (rule >= 0),
  state   INTEGER NOT NULL REFERENCES enum_state(code),
  reason  INTEGER,
  by_rule INTEGER CHECK (by_rule >= 0),
  PRIMARY KEY (verdict, rule),
  FOREIGN KEY (reason, state) REFERENCES enum_reason(code, state),
  CHECK ((state <= 3) = (reason IS NULL)),
  CHECK ((state <= 3) = (by_rule IS NULL))) WITHOUT ROWID;
CREATE TABLE verdict_lifter (
  verdict INTEGER NOT NULL, rule INTEGER NOT NULL, lifter INTEGER NOT NULL CHECK (lifter >= 0),
  PRIMARY KEY (verdict, rule, lifter),
  FOREIGN KEY (verdict, rule) REFERENCES verdict_rule(verdict, rule)) WITHOUT ROWID;
CREATE TABLE frame (key_ix  INTEGER NOT NULL,
                    reading INTEGER NOT NULL,
                    fish    INTEGER NOT NULL,
                    origin  INTEGER NOT NULL REFERENCES enum_origin(code),
                    verdict INTEGER NOT NULL CHECK (verdict >= 0),
                    PRIMARY KEY (key_ix, reading, fish, origin),
                    FOREIGN KEY (key_ix, fish) REFERENCES key_fish(key_ix, fish),
                    FOREIGN KEY (key_ix, reading) REFERENCES reading(key_ix, reading))
                    WITHOUT ROWID;
"""


class VerdictsError(SystemExit):
    """A verdicts file that cannot be written, opened or trusted: named, never defaulted."""


def enum_rows() -> Dict[str, List[tuple]]:
    """The enum tables a verdicts file carries, from the Python enums (the one source)."""
    return {
        "enum_state": [(T.code(s), s.value, int(s.value in read.SPEAKER_STATES))
                       for s in T.RuleState],
        "enum_reason": [(T.code(r), r.value, T.code(T.loss_state(r))) for r in T.LossReason],
        "enum_fish": [(T.code(f), f.value, int(f.value in T.GAME_FISH)) for f in T.FishCode],
        "enum_origin": [(T.code(o), o.value) for o in T.AskOrigin],
    }


def create(path: Path, bundle_meta: Dict[str, str]) -> sqlite3.Connection:
    """A new, empty verdicts file at `path`: schema, enum tables, the bundle's digests."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    db = sqlite3.connect(path)
    db.execute("PRAGMA page_size = 4096")
    db.execute("PRAGMA journal_mode = OFF")
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA)
    for table, rows in enum_rows().items():
        db.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(rows[0]))})", rows)
    missing = [k for k in DIGESTS if not bundle_meta.get(k)]
    if missing:
        raise VerdictsError(f"verdicts: the bundle states no {missing} — rebuild it")
    db.executemany("INSERT INTO meta (k, v) VALUES (?, ?)",
                   [("format", FORMAT)] + [(k, bundle_meta[k]) for k in DIGESTS])
    return db


def bundle_meta(bundle: str) -> Dict[str, str]:
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        return dict(b.execute("SELECT k, v FROM meta"))
    finally:
        b.close()


Row = Tuple[int, int, Optional[int], Optional[int], Tuple[int, ...]]


@dataclass
class VerdictStore:
    """An opened, checked verdicts file. Lookups only; every answer is the reader's, stored."""
    path: str
    db: sqlite3.Connection
    rule_ids: List[str]                                  # RuleIx -> "entry::rule"
    grade: Dict[int, Optional[str]]                      # RuleIx -> rule.closure_grade
    _fish_code: Dict[str, int] = field(default_factory=dict)
    _segments: Optional[Dict[int, List[Tuple[int, int]]]] = None
    _readings: Optional[Dict[int, List[T.Reading]]] = None
    _own: Optional[Dict[int, bool]] = None
    _fish: Optional[Dict[int, Tuple[str, ...]]] = None
    _frames: Dict[int, Dict[Tuple[int, str, str], int]] = field(default_factory=dict)
    _rows: Dict[int, Tuple[Row, ...]] = field(default_factory=dict)

    @classmethod
    def open(cls, path, bundle) -> "VerdictStore":
        """Refuses another format, a digest not the bundle's, an enum table not the Python
        enum's."""
        path, bundle = str(path), str(bundle)
        if not Path(path).is_file():
            raise VerdictsError(f"verdicts: no file at {path} — run `python -m pipeline.deliver "
                                f"verdicts --bundle {bundle} --out {path}`")
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        meta = dict(db.execute("SELECT k, v FROM meta"))
        if meta.get("format") != FORMAT:
            raise VerdictsError(f"verdicts: {path} is format {meta.get('format')!r}, not {FORMAT}")
        bm = bundle_meta(bundle)
        for k in DIGESTS:
            if meta.get(k) != bm.get(k):
                raise VerdictsError(f"verdicts: {path} carries {k} {meta.get(k)!r}, the bundle "
                                    f"{bm.get(k)!r} — not this bundle's verdicts")
        for table, rows in enum_rows().items():
            got = [tuple(r) for r in db.execute(f"SELECT * FROM {table} ORDER BY code")]
            if got != rows:
                raise VerdictsError(f"verdicts: {path} `{table}` is not the Python enum's — the "
                                    f"reader's vocabulary moved; rebuild the verdicts")
        b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
        try:
            ix = b.execute("SELECT x.ix, x.entry_id, x.rule_id, r.closure_grade FROM rule_ix x "
                           "JOIN rule r ON r.entry_id = x.entry_id AND r.rule_id = x.rule_id "
                           "ORDER BY x.ix").fetchall()
        finally:
            b.close()
        if [i for i, *_ in ix] != list(range(len(ix))):
            raise VerdictsError("verdicts: the bundle's rule_ix is not 0..n-1")
        return cls(path=path, db=db, rule_ids=[f"{e}::{r}" for _, e, r, _ in ix],
                   grade={i: g for i, _, _, g in ix},
                   _fish_code={f.value: T.code(f) for f in T.FishCode})

    # ---- per key ------------------------------------------------------------------------
    def _load(self) -> None:
        if self._segments is not None:
            return
        seg: Dict[int, List[Tuple[int, int]]] = {}
        for k, start, r in self.db.execute("SELECT key_ix, start, reading FROM segment "
                                           "ORDER BY key_ix, start"):
            seg.setdefault(k, []).append((start, r))
        rd: Dict[int, List[T.Reading]] = {}
        for k, r, first, closed in self.db.execute("SELECT key_ix, reading, first_day, closed "
                                                   "FROM reading ORDER BY key_ix, reading"):
            rd.setdefault(k, []).append(T.Reading(k, r, first, bool(closed)))
        fish: Dict[int, List[str]] = {}
        for k, f in self.db.execute("SELECT key_ix, fish FROM key_fish ORDER BY key_ix, fish"):
            fish.setdefault(k, []).append(T.by_code(T.FishCode, f).value)
        self._own = {k: bool(o) for k, o in self.db.execute("SELECT key_ix, own FROM key_meta")}
        self._segments, self._readings = seg, rd
        self._fish = {k: tuple(v) for k, v in fish.items()}

    @property
    def keys(self) -> List[int]:
        self._load()
        return sorted(self._own)

    def runs(self, key: int) -> List[Tuple[int, int]]:
        """[(start day, reading)] covering 1..366."""
        self._load()
        return self._segments[key]

    def readings(self, key: int) -> List[T.Reading]:
        self._load()
        return self._readings[key]

    def reading_of(self, key: int, day: int) -> int:
        from pipeline.deliver.calendar import reading_of
        return reading_of(self.runs(key), day)

    def closed(self, key: int, reading: int) -> bool:
        return self.readings(key)[reading].closed

    def own(self, key: int) -> bool:
        self._load()
        return self._own[key]

    def fish(self, key: int) -> Tuple[str, ...]:
        """The fish asked on this key (FishCode order)."""
        self._load()
        return self._fish[key]

    # ---- frames and verdicts ------------------------------------------------------------
    def frames(self, key: int) -> Dict[Tuple[int, str, str], int]:
        """{(reading, fish, origin): verdict id} for one key."""
        got = self._frames.get(key)
        if got is None:
            got = {(r, T.by_code(T.FishCode, f).value, T.by_code(T.AskOrigin, o).value): v
                   for r, f, o, v in self.db.execute(
                       "SELECT reading, fish, origin, verdict FROM frame WHERE key_ix = ?", (key,))}
            if not got:
                raise VerdictsError(f"verdicts: no frame for rule key {key}")
            self._frames[key] = got
        return got

    def verdict_id(self, key: int, reading: int, fish: str, origin: str) -> int:
        fr = self.frames(key)
        v = fr.get((reading, T.FishCode(fish).value, T.AskOrigin(origin).value))
        if v is None:
            raise VerdictsError(f"verdicts: rule key {key} reading {reading} was not asked "
                                f"{fish!r} ({origin}) — asked: {self.fish(key)}")
        return v

    def rows(self, verdict: int) -> Tuple[Row, ...]:
        """One verdict's rows, by rule: (rule, state code, reason code | None, by | None,
        lifters)."""
        got = self._rows.get(verdict)
        if got is None:
            lift: Dict[int, List[int]] = {}
            for r, l in self.db.execute("SELECT rule, lifter FROM verdict_lifter WHERE verdict = ? "
                                        "ORDER BY rule, lifter", (verdict,)):
                lift.setdefault(r, []).append(l)
            got = tuple((r, s, rs, b, tuple(lift.get(r, ())))
                        for r, s, rs, b in self.db.execute(
                            "SELECT rule, state, reason, by_rule FROM verdict_rule "
                            "WHERE verdict = ? ORDER BY rule", (verdict,)))
            self._rows[verdict] = got
        return got

    def verdict(self, key: int, reading: int, fish: str, origin: str) -> Tuple[T.VerdictRow, ...]:
        """The reader's traced answer, typed."""
        return tuple(T.VerdictRow(r, T.by_code(T.RuleState, s),
                                  None if rs is None else T.by_code(T.LossReason, rs), b, l)
                     for r, s, rs, b, l in self.rows(self.verdict_id(key, reading, fish, origin)))

    def on_day(self, key: int, day: int, fish: str, origin: str = "none"):
        """The verdict for a key on a day (1..366)."""
        return self.verdict(key, self.reading_of(key, day), fish, origin)
