"""THE METHOD FILE (T213): which sections are BAIT BAN or FLY FISHING ONLY on each day of the year — the map's "Bait &
fly" colouring, a third choice beside Standard and Salmon.

    python -m pipeline.deliver.method [--store FILE] [--out FILE]

ONE SOURCE, NOTHING DECIDED HERE. The answers already say it per part key and segment: the display frame's `strip`
(answers 2.6, `answers.display.strip`) is the OR of closed 1 · release 2 · fly_only 4 · artificial_fly_only 8 ·
bait_ban 16, composed from the verdicts' status and the gear answer (`gear.fly`, `gear.bait_ban`) — the bits the water
page's open-days strip draws. This file is a PROJECTION of those strips through the regs store (`regs.sqlite`), so the
map and the strip cannot disagree: no rule is read here.

A SECTION'S CLASS ON A DAY (codes, in the order the map's legend lists them):

  0 none      open, and neither bait ban nor fly only — the map draws the plain "other" colour
  1 bait      bait ban (strip bit 16), not fly only
  2 fly       fly fishing only or artificial flies only (bits 4 / 8) — a fly-only water bans bait as well

CLOSED IS NOT HERE: a day closed at every moment is 0. The status index already says closed, day for day, and the map
loads it first; the Bait & fly colouring paints a section the status index closes in the closed colour before reading
this file (one source for "closed"). Carrying it again cost 125 kB gzipped and pushed the release over its total gate.

A day with several MOMENTS (weekday classes, an hours window: answers 2.1) has one segment per moment. The day is
closed when every moment is (the status index's rule: a night closure never closes the day); otherwise its gear bits
are those held at EVERY open moment — the map paints what holds all day, never a restriction of some hours.
A tidal frame has no strip: 0. A section with no akey (outside B.C.) is in no run: 0.

THE FORMAT (`method/1`), varints unsigned LEB128 (`pipeline.deliver.binary`):

    "BCMT"      4 bytes magic
    version     1 byte (= 1)
    handles     8 bytes: the regs store's `meta.section_handles` (16 hex digits) as raw bytes
    nProfiles   then each profile: nRuns, then nRuns x varint(length << 2 | code); lengths sum to 366 and cover day 1
                (Jan 1) .. 366 (Dec 31) on the catalogue's leap calendar (Feb 29 is day 60) — the status index's days
    nRuns       then THREE COLUMNS of nRuns varints — gap[], count[], profile[]: run i covers sids
                [start_i, start_i + count_i), start_i = end_{i-1} + gap_i (end_0 = 0). A sid in no run is 0 every day.

`Index` is the reference decoder the tests (and the app's `@app/core` `decodeMethod`) are held to.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.common.curated import GENERATED
from pipeline.deliver.binary import read_varint, varint

FORMAT = "method/1"
MAGIC = b"BCMT"
VERSION = 1
DAYS = 366

NONE, BAIT, FLY = 0, 1, 2
CODE_NAMES = {NONE: "none", BAIT: "bait", FLY: "fly"}

#: the strip's bits (`answers.display.strip.STRIP`), held equal to it by test_method_file.py
STRIP_CLOSED, STRIP_FLY, STRIP_ARTIFICIAL_FLY, STRIP_BAIT_BAN = 1, 4, 8, 16
GEAR_BITS = STRIP_FLY | STRIP_ARTIFICIAL_FLY | STRIP_BAIT_BAN

DEFAULT_OUT = GENERATED.bundle / "method.bin"


class MethodError(SystemExit):
    pass


def day_code(strips: Sequence[Optional[int]]) -> int:
    """One day's class from the strips of its segments (one per moment; None: a tidal frame, no strip)."""
    got = [s or 0 for s in strips]
    if got and all(s == STRIP_CLOSED for s in got):
        return NONE                     # closed: the status index's answer, not this file's
    bits = GEAR_BITS
    for s in got:
        if s != STRIP_CLOSED:
            bits &= s
    if bits & (STRIP_FLY | STRIP_ARTIFICIAL_FLY):
        return FLY
    return BAIT if bits & STRIP_BAIT_BAN else NONE


def key_profile(starts: Sequence[int], strips: Sequence[Optional[int]]) -> Tuple[int, ...]:
    """A key's class on each day 1..366 from its segments' start days (repeated once per moment group) and strips."""
    if not starts or starts[0] != 1 or len(starts) != len(strips):
        raise MethodError(f"method: segments {list(starts)!r} do not start on day 1 or miss their frames")
    out: List[int] = []
    distinct = sorted(set(starts))
    for i, start in enumerate(distinct):
        end = distinct[i + 1] if i + 1 < len(distinct) else DAYS + 1
        out += [day_code([strips[s] for s, x in enumerate(starts) if x == start])] * (end - start)
    return tuple(out)


def runs_of(profile: Sequence[int]) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    for c in profile:
        if out and out[-1][1] == c:
            out[-1] = (out[-1][0] + 1, c)
        else:
            out.append((1, c))
    return out


def compute(store_path) -> dict:
    """{handles, profiles, sections {sid: profile index}} from a regs store (a path, or an open connection)."""
    import sqlite3
    from pipeline.deliver.store.decode import Store
    con = store_path if isinstance(store_path, sqlite3.Connection) else None
    st = Store(None if con else store_path, verify=False, con=con)
    handles = st.meta.get("section_handles")
    if not handles:
        raise MethodError("method: the regs store records no meta.section_handles")
    segs = {s: json.loads(t) for s, t in st.con.execute("SELECT seg, starts FROM seglist")}
    strip = {i: st.val("display_frame", b).get("strip") for i, b in st.con.execute("SELECT id, j FROM display_frame")}
    cells: Dict[int, List[int]] = defaultdict(list)
    for a, s, d in st.con.execute("SELECT akey, s, display FROM cell ORDER BY akey, s"):
        if s != len(cells[a]):
            raise MethodError(f"method: akey {a}'s cells are out of sequence")
        cells[a].append(d)
    by_key = {a: key_profile(segs[seg], [strip[d] for d in cells[a]])
              for a, seg in st.con.execute("SELECT akey, seg FROM akey ORDER BY akey")}
    none = (NONE,) * DAYS
    kept = {sid: by_key[a] for sid, a in sorted(st.section_akeys().items()) if by_key[a] != none}
    profiles = sorted(set(kept.values()), key=lambda p: (runs_of(p), p))
    idx = {p: i for i, p in enumerate(profiles)}
    return {"handles": handles, "profiles": profiles, "sections": {sid: idx[p] for sid, p in kept.items()}}


def encode(m: dict) -> bytes:
    out = bytearray(MAGIC)
    out.append(VERSION)
    h = bytes.fromhex(m["handles"])
    if len(h) != 8:
        raise ValueError(f"handles {m['handles']!r} is not 16 hex digits")
    out += h
    out += varint(len(m["profiles"]))
    for p in m["profiles"]:
        rs = runs_of(p)
        if sum(n for n, _ in rs) != DAYS:
            raise ValueError("a profile does not cover 366 days")
        out += varint(len(rs))
        for length, code in rs:
            out += varint(length << 2 | code)
    runs: List[List[int]] = []
    for sid, pi in sorted(m["sections"].items()):
        if runs and runs[-1][0] + runs[-1][1] == sid and runs[-1][2] == pi:
            runs[-1][1] += 1
        else:
            runs.append([sid, 1, pi])
    out += varint(len(runs))
    end = 0
    for start, count, _ in runs:
        out += varint(start - end)
        end = start + count
    out += b"".join(varint(c) for _, c, _ in runs)
    out += b"".join(varint(pi) for _, _, pi in runs)
    return bytes(out)


class Index:
    """The file read back — the Python twin of the app's decoder."""

    def __init__(self, data: bytes):
        if data[:4] != MAGIC:
            raise ValueError("not a method file")
        if data[4] != VERSION:
            raise ValueError(f"method file version {data[4]}, expected {VERSION}")
        self.handles = data[5:13].hex()
        pos = 13

        def v() -> int:
            nonlocal pos
            n, pos = read_varint(data, pos)
            return n
        self.profiles: List[Tuple[int, ...]] = []
        for _ in range(v()):
            days: List[int] = []
            for _ in range(v()):
                x = v()
                days += [x & 3] * (x >> 2)
            if len(days) != DAYS:
                raise ValueError("a profile does not cover 366 days")
            self.profiles.append(tuple(days))
        n = v()
        gaps = [v() for _ in range(n)]
        counts = [v() for _ in range(n)]
        profs = [v() for _ in range(n)]
        if pos != len(data):
            raise ValueError("trailing bytes in method file")
        self.sections: Dict[int, int] = {}
        end = 0
        for gap, count, pi in zip(gaps, counts, profs):
            start = end + gap
            for s in range(start, start + count):
                self.sections[s] = pi
            end = start + count

    def code(self, sid: int, day: int) -> int:
        """The class of a section on day 1..366."""
        pi = self.sections.get(sid)
        return NONE if pi is None else self.profiles[pi][day - 1]


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.deliver.method", description=__doc__.split("\n\n")[0])
    ap.add_argument("--store", type=Path, default=GENERATED.bundle / "regs.sqlite")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    a = ap.parse_args(argv)
    m = compute(a.store)
    data = encode(m)
    if Index(data).sections != m["sections"]:
        raise MethodError("method: the encoded file does not read back")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_bytes(data)
    import gzip
    ever: Dict[str, int] = defaultdict(int)
    for pi in m["sections"].values():
        for c in set(m["profiles"][pi]):
            ever[CODE_NAMES[c]] += 1
    print(f"method: {len(m['profiles'])} profiles · {len(m['sections']):,} sections · "
          + ", ".join(f"{k} {v:,}" for k, v in sorted(ever.items()))
          + f" · {len(data):,} bytes ({len(gzip.compress(data, 9, mtime=0)):,} gzipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
