"""THE STATUS INDEX — what colour every section and every water is, on any day of the year.

    python -m pipeline.deliver.status_index [--bundle FILE] [--out FILE]

The map and the search list colour water by ONE three-valued answer (`app/packages/core/src/
status.ts` WATER_STATUS): closed / has its own regulations / base regulations only. Deciding it
from the bundle on the device means shipping the ladder to the app and running it per section,
per day; this file is that answer precomputed, small enough to fetch at app start.

READ ONLY FROM THE BUNDLE (`bundle-is-the-only-source`), through the reference reader
`read.effective_rules_bound` — the same function `read.effective_rules` runs — so the index
cannot disagree with the reader. `pipeline/tests/test_status_index.py` samples (section, day)
pairs and holds the file to `effective_rules` itself.

WHAT EACH STATUS MEANS (decided here, once):

  closed   On that day, for EVERY game fish (the book's closed list, p.80, minus crayfish —
           `GAME_FISH`), `effective_rules` returns a rule that SPEAKS and is an unconditional
           closure: take 0 and may not fish for it (`may_target: 0`), with no length band, no
           origin, no `while`, no `when_targeting`, no `side`, and not partly lifted. A "beside"
           closure (some hours or weekdays, an unreadable season, one half of the channel) and a
           `not_yet_mapped` note never close a section. A closure of SOME species only is not
           `closed`: the section's other answer stands (own when it has a row, see below).
  own      Not closed, and the section is bound to a rule beyond the base: any rule of a water
           table's row (`r<n>:` entries — a named water, a cut piece of it, a water table's area
           row such as the CVWMA waters or the Liard River watershed, or such a row reaching it
           by the tributary walk). A zone or provincial table's rule is base even when it names
           waters (the Skeena/Nass winter closure, the white sturgeon licence waters).
           Independent of the day: a water with its own row "has its own regulations" all year,
           which is what the synopsis says about it. A `not_yet_mapped` note and a `side` rule
           count — the water's panel shows them; they never make it closed.
  base     Neither: only zone, area-of-zone, provincial and superior (federal / parks) rules bind
           it. NOT IN THE FILE — absence means base, on every day.

Two sections are not freshwater-regulation answers at all, and carry their own codes:
  tidal    (`tidal` table — Nitinat Lake): tidal water is its own case; the app shows no
           freshwater status there (the regulations panel points to the tidal rules).
  outside  (`outside_bc`): past the border, no ruleset. Absence would read "base" — "open under
           the general rules" — which is exactly what the bundle schema says they must never
           read, so they are listed.

THE FORMAT (version 2). All integers are unsigned LEB128 varints unless stated.

    "BCSI"          4 bytes magic
    version         1 byte  (= 2)
    handles         8 bytes: the bundle's `meta.section_handles` (16 hex digits) as raw bytes —
                    the app refuses an index whose digest differs from its tiles' and bundle's
    reach           8 bytes: the bundle's `meta.reach_digest` (16 hex digits) as raw bytes — WHICH
                    rule bindings this file was cut from. Two bundles from one atlas and two reach
                    runs carry the same handles and different rules; version 1 could not tell
                    their indexes apart. A reader holding a bundle refuses an index whose reach
                    digest differs from the bundle's.
    nProfiles       then each profile: nRuns, then nRuns x varint(length << 3 | code); the
                    lengths sum to 366 and cover day 1 (Jan 1) .. 366 (Dec 31) in order, on the
                    catalogue's leap calendar (`catalogue._day_index`: Feb 29 is day 60, Mar 1 is
                    61 in every year). A season crossing New Year is just two runs.
    nRuns           then THREE COLUMNS of nRuns varints each — gap[], count[], profile[] —
                    run i covers sids [start_i, start_i + count_i) with start_i = end_{i-1} +
                    gap_i (end_0 = 0, end_i = start_i + count_i); all carry profile_i. Sids in no
                    run are base. Columns, not interleaved triples: each column is one kind of
                    number, which gzip models far better (interleaved: 453 KB gzipped; columns: 421 KB).
    nItems          then FOUR COLUMNS, items sorted by item_id: shared[] (bytes shared with the
                    previous id), suffixLen[], then the suffixes' UTF-8 bytes concatenated, then
                    profile[]. The water's rollup over its parts (`item_section`); items whose
                    every part is base are absent.

    code: 0 base · 1 own · 2 closed · 3 tidal · 4 outside

A WATER'S ROLLUP, per day, over its parts (`item_section`): closed when every part is closed;
tidal / outside when every part is; own when any part carries a row's rule (its FLOOR is own,
whether or not that part is closed that day); base otherwise — a water with no row of its own,
part of it under a zone closure, is "base regulations only", which is true: a zone closure is
the base. Outside parts are ignored when any part is in B.C.

Deterministic: sorted iteration, no clocks. Section handles are the tiles' feature ids — the
same integers, under the same digest — so the map colours from this file with no bundle query.
They never leave this vintage-locked set (AGENTS 5): the file is refused by any reader holding a
different digest, exactly as a mixed tiles/bundle pair is.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.common.curated import GENERATED
from pipeline.deliver.bundle import read
from pipeline.deliver.bundle.rules import closure_grade
from pipeline.deliver.calendar import DAYS, day_of, month_day
from pipeline.regs.parsing.catalogue import GAME_FISH

MAGIC = b"BCSI"
VERSION = 2

BASE, OWN, CLOSED, TIDAL, OUTSIDE = 0, 1, 2, 3, 4
CODE_NAMES = {BASE: "base", OWN: "own", CLOSED: "closed", TIDAL: "tidal", OUTSIDE: "outside"}

DEFAULT_OUT = GENERATED.bundle / "status_index.bin"




# --------------------------------------------------------------------------------------------
# What a closure is, and what "own" is
# --------------------------------------------------------------------------------------------

def is_full_closure(x: dict) -> bool:
    """An unconditional "no fishing": take 0, may not fish for it, at every size, for every
    origin, whatever the means or target, across the whole channel, and drawn (not a note) — the
    one closure predicate's "full" grade (`rules.closure_grade`)."""
    return closure_grade(x) == "full"


def closes(rows: Iterable[dict]) -> bool:
    """Does an `effective_rules` answer (for one fish) close the water to that fish?"""
    return any(r.get("state") == "speaks" and is_full_closure(r) and not r.get("partly_lifted")
               for r in rows)


#: Is a bound rule more than the base — a rule of a water table's row (`r<n>:` entries)? The ONE
#: definition is the verdicts stage's (`verdicts.project.beyond_base`, stored as `key_meta.own`);
#: the oracle below reads it from there.
from pipeline.deliver.verdicts.project import beyond_base  # noqa: E402


# --------------------------------------------------------------------------------------------
# One rule key's year: a PROJECTION of the stored verdicts (DATAFLOW P4)
# --------------------------------------------------------------------------------------------

def moment_profiles(store, key: int) -> List[Tuple[int, ...]]:
    """Per moment of the key (`store.moments`), its status code on each day 1..366: CLOSED on a
    day whose reading at that moment is closed (`reading.closed`, the predicate computed once by
    the verdicts from the reader's stored answers), else the key's floor — OWN when a water
    table's row binds it (`key_meta.own`), BASE otherwise. No reader call."""
    floor = OWN if store.own(key) else BASE
    closed = {r.ix: r.closed for r in store.readings(key)}
    out: List[Tuple[int, ...]] = []
    for m in range(len(store.moments(key))):
        codes: List[int] = []
        runs = store.runs(key, m)
        for i, (start, reading) in enumerate(runs):
            end = runs[i + 1][0] if i + 1 < len(runs) else DAYS + 1
            codes += [CLOSED if closed[reading] else floor] * (end - start)
        out.append(tuple(codes))
    return out


def key_profile(store, key: int) -> Tuple[int, ...]:
    """The status code of a section carrying this rule key, on each day 1..366: CLOSED on a day
    closed at EVERY moment of the key (`moment_profiles`), else the key's floor. A night closure
    closes its hours, never the day; a rule of some weekdays closes those weekdays, never the
    day (answers 2.1: the map colours a day, and a day open at some moment is open — the answers
    file carries each moment's own status)."""
    profs = moment_profiles(store, key)
    if len(profs) == 1:
        return profs[0]
    return tuple(CLOSED if all(p[d] == CLOSED for p in profs) else
                 next(p[d] for p in profs if p[d] != CLOSED) for d in range(DAYS))


def runs_of(profile: Sequence[int]) -> List[Tuple[int, int]]:
    """[(length, code), …] covering days 1..366."""
    out: List[Tuple[int, int]] = []
    for c in profile:
        if out and out[-1][1] == c:
            out[-1] = (out[-1][0] + 1, c)
        else:
            out.append((1, c))
    return out


def rollup(parts: Sequence[Tuple[Tuple[int, ...], int]]) -> Tuple[int, ...]:
    """A water's code per day from its parts' `(profile, floor)` (see the module docstring).

    The FLOOR, not the day's code, says whether a part has its own regulations: a part its own
    row closes reads CLOSED on those days, and the water still has its own regulations then."""
    out = []
    for day in range(DAYS):
        here = [(p[day], f) for p, f in parts if p[day] != OUTSIDE] or \
            [(p[day], f) for p, f in parts]
        codes = [c for c, _ in here]
        if all(c == CLOSED for c in codes):
            out.append(CLOSED)
        elif all(c == TIDAL for c in codes):
            out.append(TIDAL)
        elif all(c == OUTSIDE for c in codes):
            out.append(OUTSIDE)
        elif any(f == OWN for c, f in here if c != TIDAL):
            out.append(OWN)
        else:
            out.append(BASE)
    return tuple(out)


# --------------------------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------------------------

def compute(path: str, verdicts: Optional[str] = None, log=print) -> dict:
    """Everything the file holds, as plain Python: `profiles` (tuples of 366 codes),
    `sections` {sid: profile index}, `items` {item_id: profile index}, `handles`. A projection of
    the verdicts (`verdicts.sqlite` beside the bundle unless named): no reader runs here."""
    from pipeline.deliver.verdicts.store import VerdictStore
    t0 = time.time()
    store = VerdictStore.open(verdicts or str(Path(path).with_name("verdicts.sqlite")), path)
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        meta = dict(db.execute("SELECT k, v FROM meta WHERE k IN ('section_handles', "
                               "'reach_digest')"))
        for k in ("section_handles", "reach_digest"):
            if not meta.get(k):
                raise SystemExit(f"status_index: {path} records no meta.{k} — rebuild the bundle")
        handles, reach = meta["section_handles"], meta["reach_digest"]
        tidal = {s for (s,) in db.execute("SELECT sid FROM tidal")}
        outside = {s for (s,) in db.execute("SELECT sid FROM outside_bc")}
        by_key: Dict[int, Tuple[Tuple[int, ...], int]] = {}
        sec: Dict[int, Tuple[Tuple[int, ...], int]] = {}       # sid -> (profile, floor), not base
        base = (BASE,) * DAYS
        n = 0
        for sid, key in db.execute("SELECT sid, key_ix FROM section_ruleset ORDER BY sid"):
            n += 1
            got = by_key.get(key)
            if got is None:
                got = by_key[key] = (key_profile(store, key), OWN if store.own(key) else BASE)
            if sid in tidal:
                got = ((TIDAL,) * DAYS, got[1])
            if got[0] != base:
                sec[sid] = got
        for sid in outside:
            sec.setdefault(sid, ((OUTSIDE,) * DAYS, BASE))
        items = db.execute("SELECT i.item_id, s.sid FROM item i JOIN item_section s "
                           "ON s.ord = i.ord ORDER BY i.item_id, s.sid").fetchall()
    finally:
        db.close()
    total = n + len(outside)            # outside B.C. carries no rule set (the bundle proves it)
    log(f"  {len(by_key)} (ruleset, steelhead, steelhead rules) keys projected in {time.time() - t0:.1f}s")

    parts: Dict[str, set] = defaultdict(set)
    for item_id, sid in items:
        # a section absent from `sec` reads base every day, so its floor is BASE too
        parts[item_id].add(sec.get(sid, (base, BASE)))
    item_profile: Dict[str, Tuple[int, ...]] = {}
    memo: Dict[frozenset, Tuple[int, ...]] = {}
    for item_id in sorted(parts):
        k = frozenset(parts[item_id])
        p = memo.get(k)
        if p is None:
            p = memo[k] = rollup(sorted(k))
        if p != base:
            item_profile[item_id] = p

    kept = {sid: p for sid, (p, _) in sec.items()}
    profiles = sorted(set(kept.values()) | set(item_profile.values()),
                      key=lambda p: (runs_of(p), p))
    idx = {p: i for i, p in enumerate(profiles)}
    return {
        "handles": handles,
        "reach_digest": reach,
        "profiles": profiles,
        "sections": {sid: idx[p] for sid, p in sorted(kept.items())},
        "items": {i: idx[p] for i, p in sorted(item_profile.items())},
        "total_sections": total,
        "total_items": len(parts),
    }


def _varint(n: int) -> bytes:
    if n < 0:
        raise ValueError(n)
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def encode(idx: dict) -> bytes:
    out = bytearray(MAGIC)
    out.append(VERSION)
    for key in ("handles", "reach_digest"):
        h = bytes.fromhex(idx[key])
        if len(h) != 8:
            raise ValueError(f"{key} {idx[key]!r} is not 16 hex digits")
        out += h
    out += _varint(len(idx["profiles"]))
    for p in idx["profiles"]:
        rs = runs_of(p)
        out += _varint(len(rs))
        for length, code in rs:
            out += _varint(length << 3 | code)
    # section runs: consecutive sids sharing a profile
    runs: List[List[int]] = []
    for sid, pi in idx["sections"].items():
        if runs and runs[-1][0] + runs[-1][1] == sid and runs[-1][2] == pi:
            runs[-1][1] += 1
        else:
            runs.append([sid, 1, pi])
    out += _varint(len(runs))
    gaps, end = bytearray(), 0
    for start, count, _ in runs:
        gaps += _varint(start - end)
        end = start + count
    out += gaps
    out += b"".join(_varint(c) for _, c, _ in runs)
    out += b"".join(_varint(pi) for _, _, pi in runs)
    out += _varint(len(idx["items"]))
    shared, lens, tails, profs = bytearray(), bytearray(), bytearray(), bytearray()
    prev = b""
    for item_id, pi in idx["items"].items():
        b = item_id.encode("utf-8")
        n = 0
        while n < min(len(b), len(prev)) and b[n] == prev[n]:
            n += 1
        shared += _varint(n)
        lens += _varint(len(b) - n)
        tails += b[n:]
        profs += _varint(pi)
        prev = b
    out += shared + lens + tails + profs
    return bytes(out)


class Index:
    """The file read back — the Python twin of the app's decoder, for tests and tools."""

    def __init__(self, data: bytes):
        if data[:4] != MAGIC:
            raise ValueError("not a status index")
        if data[4] != VERSION:
            raise ValueError(f"status index version {data[4]}, expected {VERSION}")
        self.handles = data[5:13].hex()
        self.reach_digest = data[13:21].hex()
        pos = 21

        def v() -> int:
            nonlocal pos
            n = shift = 0
            while True:
                b = data[pos]
                pos += 1
                n |= (b & 0x7F) << shift
                shift += 7
                if not b & 0x80:
                    return n
        self.profiles: List[Tuple[int, ...]] = []
        for _ in range(v()):
            days: List[int] = []
            for _ in range(v()):
                x = v()
                days += [x & 7] * (x >> 3)
            if len(days) != DAYS:
                raise ValueError("a profile does not cover 366 days")
            self.profiles.append(tuple(days))
        self.sections: Dict[int, int] = {}
        n = v()
        gaps = [v() for _ in range(n)]
        counts = [v() for _ in range(n)]
        profs = [v() for _ in range(n)]
        end = 0
        for gap, count, pi in zip(gaps, counts, profs):
            start = end + gap
            for s in range(start, start + count):
                self.sections[s] = pi
            end = start + count
        self.items: Dict[str, int] = {}
        n = v()
        shared = [v() for _ in range(n)]
        lens = [v() for _ in range(n)]
        prev = b""
        ids = []
        for k, m in zip(shared, lens):
            b = prev[:k] + data[pos:pos + m]
            pos += m
            ids.append(b.decode("utf-8"))
            prev = b
        for item_id in ids:
            self.items[item_id] = v()
        if pos != len(data):
            raise ValueError("trailing bytes in status index")

    def code(self, sid: int, on) -> int:
        pi = self.sections.get(sid)
        return BASE if pi is None else self.profiles[pi][day_of(on) - 1]

    def item_code(self, item_id: str, on) -> int:
        pi = self.items.get(item_id)
        return BASE if pi is None else self.profiles[pi][day_of(on) - 1]


# --------------------------------------------------------------------------------------------
# The slow, direct reading — what the tests hold the file to
# --------------------------------------------------------------------------------------------

def status_by_reader(sid: int, on, path: str) -> int:
    """One section's code on one day, straight from `read.effective_rules` (one bundle query per
    fish) and the section's own bindings — no rulesets shared, no days skipped, no memo."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if db.execute("SELECT 1 FROM tidal WHERE sid = ?", (sid,)).fetchone():
            return TIDAL
        if db.execute("SELECT 1 FROM outside_bc WHERE sid = ?", (sid,)).fetchone():
            return OUTSIDE
        bound = db.execute("SELECT r.entry_id, r.rule_id, r.via FROM section_ruleset s "
                           "JOIN ruleset r ON r.set_id = s.set_id WHERE s.sid = ?",
                           (sid,)).fetchall()
    finally:
        db.close()
    if all(closes(read.effective_rules(sid, on, f, path)) for f in GAME_FISH):
        return CLOSED
    every = read._rules_of(path)
    return OWN if any(beyond_base(every[(e, r)], v) for e, r, v in bound
                      if (e, r) in every) else BASE


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.deliver.status_index",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--verdicts", type=Path, default=None,
                    help="default: verdicts.sqlite beside the bundle")
    a = ap.parse_args(argv)
    if not a.bundle.exists():
        raise SystemExit(f"status_index: no bundle at {a.bundle} — build it first "
                         f"(`python -m pipeline.deliver.bundle`)")
    print(f"status index: {a.bundle} -> {a.out}")
    idx = compute(str(a.bundle), str(a.verdicts) if a.verdicts else None)
    data = encode(idx)
    if Index(data).sections != idx["sections"]:
        raise SystemExit("status_index: the encoded file does not read back")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_bytes(data)
    import gzip
    gz = len(gzip.compress(data, 9, mtime=0))
    counts = defaultdict(int)
    for pi in idx["sections"].values():
        for c in set(idx["profiles"][pi]):
            counts[CODE_NAMES[c]] += 1
    print(f"  handles {idx['handles']}  ·  rules {idx['reach_digest']}  ·  "
          f"{len(idx['profiles'])} profiles  ·  "
          f"{len(idx['sections']):,} of {idx['total_sections']:,} sections  ·  "
          f"{len(idx['items']):,} of {idx['total_items']:,} waters")
    print(f"  sections ever: " + ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))
    print(f"  {len(data):,} bytes ({gz:,} gzipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
