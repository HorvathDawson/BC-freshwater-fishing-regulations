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

  closed   On that day, for EVERY game fish (the book's closed list, p.86, minus crayfish —
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

THE FORMAT (version 1). All integers are unsigned LEB128 varints unless stated.

    "BCSI"          4 bytes magic
    version         1 byte  (= 1)
    handles         8 bytes: the bundle's `meta.section_handles` (16 hex digits) as raw bytes —
                    the app refuses an index whose digest differs from its tiles' and bundle's
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

MAGIC = b"BCSI"
VERSION = 1
DAYS = 366

BASE, OWN, CLOSED, TIDAL, OUTSIDE = 0, 1, 2, 3, 4
CODE_NAMES = {BASE: "base", OWN: "own", CLOSED: "closed", TIDAL: "tidal", OUTSIDE: "outside"}

DEFAULT_OUT = GENERATED.bundle / "status_index.bin"


def game_fish() -> Tuple[str, ...]:
    """The book's closed list of freshwater game fish (p.86) — every fish a "no fishing" must
    hold for before a section is called closed. Crayfish are on the list but are trapped, not
    angled, and a fin-fish closure leaves them (`read.speaks_for`), so they are left out."""
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES
    return tuple(f for f in BOOK_SPECIES if f != "CRA")


GAME_FISH = game_fish()


def day_of(on) -> int:
    """`datetime.date` or `(month, day)` -> 1..366 on the catalogue's leap calendar."""
    return read._day(on)


def month_day(day: int) -> Tuple[int, int]:
    """1..366 -> (month, day), the inverse of `day_of` (day 60 is Feb 29)."""
    from pipeline.regs.parsing.catalogue import _LAST_DAY
    m = 1
    while day > _LAST_DAY[m]:
        day -= _LAST_DAY[m]
        m += 1
    return m, day


# --------------------------------------------------------------------------------------------
# What a closure is, and what "own" is
# --------------------------------------------------------------------------------------------

def is_full_closure(x: dict) -> bool:
    """An unconditional "no fishing": take 0, may not fish for it, at every size, for every
    origin, whatever the means or target, across the whole channel, and drawn (not a note)."""
    return (x.get("take") == 0 and x.get("may_target") == 0
            and not (x.get("lengths") or x.get("origin") or x.get("while")
                     or x.get("when_targeting") or x.get("side")
                     or read.not_yet_mapped(x)))


def closes(rows: Iterable[dict]) -> bool:
    """Does an `effective_rules` answer (for one fish) close the water to that fish?"""
    return any(r.get("state") == "speaks" and is_full_closure(r) and not r.get("partly_lifted")
               for r in rows)


def beyond_base(rule: dict, via: str) -> bool:
    """Is this bound rule MORE than the base — a rule of a water table's row (`r<n>:` entries)?
    Everything a zone or provincial table writes (`z<n>:` / `zp:`) is the base, including the
    rules those tables write about named waters ("streams of the Skeena and Nass watersheds",
    the white sturgeon licence waters): the base is the TABLES, not the scope (`source_of` rank
    0 on a zone entry is still the zone's table). `via` is accepted for symmetry and unused —
    a row reaching a tributary by the walk is the row's rule there too."""
    return not str(rule["entry"]).startswith("z")


# --------------------------------------------------------------------------------------------
# One ruleset's year
# --------------------------------------------------------------------------------------------

def set_floor(bound: Sequence[tuple], path: str) -> int:
    """OWN when any bound rule is beyond the base (`beyond_base`), else BASE — the answer on
    every day the section is not closed."""
    every = read._rules_of(path)
    return OWN if any(beyond_base(every[(e, r)], v) for e, r, v in bound
                      if (e, r) in every) else BASE


def set_profile(bound: Sequence[tuple], steelhead: bool, path: str) -> Tuple[int, ...]:
    """The status code of a section carrying these bindings, on each day 1..366.

    Faithful shortcut through `effective_rules_bound`, in two steps that change no answer:
      * a day on which, for some game fish, NO bound full closure speaking for it is in force
        cannot be closed (effective_rules returns only bound, in-force rules) — skipped;
      * days on which every bound rule's `when` (and every lift's `when`) reads the same give
        the same answer — the reader is asked once per distinct reading."""
    every = read._rules_of(path)
    keys = [(e, r) for e, r, _ in bound if (e, r) in every]
    floor = set_floor(bound, path)

    shut = [k for k in keys if is_full_closure(every[k])]
    if not shut:
        return (floor,) * DAYS
    whens = [every[k].get("when") for k in keys]
    lifts = [x.get("when") for k in keys for x in (every[k].get("exempts") or []) if "when" in x]
    covers_fish = {k: frozenset(f for f in GAME_FISH if read.speaks_for(every[k], f))
                   for k in shut}

    memo: Dict[tuple, int] = {}
    out: List[int] = []
    for day in range(1, DAYS + 1):
        md = month_day(day)
        held = set()
        for k in shut:
            if read.in_force(every[k].get("when"), md) == "yes":
                held |= covers_fish[k]
        if len(held) < len(GAME_FISH):
            out.append(floor)
            continue
        sig = (tuple(read.in_force(w, md) for w in whens),
               tuple(read.in_force(w, md) for w in lifts))
        got = memo.get(sig)
        if got is None:
            got = CLOSED if all(
                closes(read.effective_rules_bound(bound, steelhead, md, f, path))
                for f in GAME_FISH) else floor
            memo[sig] = got
        out.append(got)
    return tuple(out)


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

def compute(path: str, log=print) -> dict:
    """Everything the file holds, as plain Python: `profiles` (tuples of 366 codes),
    `sections` {sid: profile index}, `items` {item_id: profile index}, `handles`."""
    t0 = time.time()
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        handles = db.execute("SELECT v FROM meta WHERE k = 'section_handles'").fetchone()
        if not handles or not handles[0]:
            raise SystemExit(f"status_index: {path} records no meta.section_handles — rebuild "
                             f"the bundle")
        handles = handles[0]
        sets: Dict[int, list] = defaultdict(list)
        for set_id, e, r, via in db.execute(
                "SELECT set_id, entry_id, rule_id, via FROM ruleset "
                "ORDER BY set_id, entry_id, rule_id"):
            sets[set_id].append((e, r, via))
        sid_set = db.execute("SELECT sid, set_id FROM section_ruleset ORDER BY sid").fetchall()
        steel = {s for (s,) in db.execute("SELECT DISTINCT sid FROM steelhead_water")}
        tidal = {s for (s,) in db.execute("SELECT sid FROM tidal")}
        outside = {s for (s,) in db.execute("SELECT sid FROM outside_bc")}
        items = db.execute("SELECT i.item_id, s.sid FROM item i JOIN item_section s "
                           "ON s.ord = i.ord ORDER BY i.item_id, s.sid").fetchall()
    finally:
        db.close()

    by_key: Dict[tuple, Tuple[Tuple[int, ...], int]] = {}
    sec: Dict[int, Tuple[Tuple[int, ...], int]] = {}       # sid -> (profile, floor)
    for sid, set_id in sid_set:
        key = (set_id, sid in steel)
        got = by_key.get(key)
        if got is None:
            bound = sets.get(set_id, [])
            got = by_key[key] = (set_profile(bound, key[1], path), set_floor(bound, path))
        sec[sid] = ((TIDAL,) * DAYS, got[1]) if sid in tidal else got
    for sid in outside:
        sec.setdefault(sid, ((OUTSIDE,) * DAYS, BASE))
    log(f"  {len(by_key)} (ruleset, steelhead) keys evaluated in {time.time() - t0:.1f}s")

    base = (BASE,) * DAYS
    parts: Dict[str, set] = defaultdict(set)
    for item_id, sid in items:
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

    sec_profile = {sid: p for sid, (p, _) in sec.items()}
    kept = {sid: p for sid, p in sec_profile.items() if p != base}
    profiles = sorted(set(kept.values()) | set(item_profile.values()),
                      key=lambda p: (runs_of(p), p))
    idx = {p: i for i, p in enumerate(profiles)}
    return {
        "handles": handles,
        "profiles": profiles,
        "sections": {sid: idx[p] for sid, p in sorted(kept.items())},
        "items": {i: idx[p] for i, p in sorted(item_profile.items())},
        "total_sections": len(sec_profile),
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
    h = bytes.fromhex(idx["handles"])
    if len(h) != 8:
        raise ValueError(f"section_handles {idx['handles']!r} is not 16 hex digits")
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
        pos = 13

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
    a = ap.parse_args(argv)
    if not a.bundle.exists():
        raise SystemExit(f"status_index: no bundle at {a.bundle} — build it first "
                         f"(`python -m pipeline.deliver.bundle`)")
    print(f"status index: {a.bundle} -> {a.out}")
    idx = compute(str(a.bundle))
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
    print(f"  handles {idx['handles']}  ·  {len(idx['profiles'])} profiles  ·  "
          f"{len(idx['sections']):,} of {idx['total_sections']:,} sections  ·  "
          f"{len(idx['items']):,} of {idx['total_items']:,} waters")
    print(f"  sections ever: " + ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))
    print(f"  {len(data):,} bytes ({gz:,} gzipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
