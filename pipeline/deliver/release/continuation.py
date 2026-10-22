"""THE CONTINUATION LINK (owner 2026-10-21: "it should be able to navigate up if one stream comes directly from another,
like vedder from chilliwack"): `water_continue (ord, part, next_ord, next_part)` in the release's water file — water
`ord` CONTINUES AS water `next_ord`: the same watercourse under another name, `part` the part of `ord` at its mouth and
`next_part` the part of `next_ord` at its top. The app's water map steps across it (▲ from the Vedder's top part into
the Chilliwack's lowest, ▼ back). Topology, not a regulation.

THE RULE (stated, pinned in `test_water_continue.py` and on the real Chilliwack / Vedder in `test_release.py`). A
continues as B when, both streams and `item_down` already saying A flows into B:
  - A's MAIN LINE (the FWA blue line holding most of A's sections; ties: the higher top measure, then the key) BEGINS
    there — the section is A's lowest measure on that line — and flows (`section_down`) into
  - B's main line at its TOP — B's highest measure on its main line;
  - and it is the watercourse there: the only section flowing into B's top, or on B's own blue line.

WHY NOT "THE SAME BLUE LINE" ALONE. That was the ask, and the atlas says otherwise: the Chilliwack is FWA blue line
380887781 from measure 0, and it flows into the Vedder's line 356363467 at measure 12723, the top of that line — two
blue lines, one watercourse, a name change at Vedder Crossing. A blue line that ENDS where another begins, with nothing
else flowing in, is the same water going on. What it shuts out is what must stay out: a tributary meets the receiver
part way up (the receiver's own line goes on above it), or at a confluence (two inflows at the top, neither the
receiver's line); the Vedder's sloughs (Stewart, Street, Lewis, McGillivray) reach it through no section of their own
at all. A braid piece or a side channel is never a water's main line, so it cannot carry the link.

The section ids come from the atlas's `section_handles.txt` (`{blue_line}:{measure}`; `lake:…` is no line), the one
the bundle was built on — `rows` is handed them by the release builder, which checks the digest.
"""
from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

Line = Tuple[str, int]


class ContinuationError(SystemExit):
    pass


def line_of(section_id: str) -> Optional[Line]:
    """`'380887781:0'` -> `('380887781', 0)`; a section on no blue line (`lake:…`) -> None."""
    head, _, tail = section_id.rpartition(":")
    if not head or head == "lake":
        return None
    try:
        return head, int(tail)
    except ValueError:
        return None


def _main(sids: Iterable[int], line: Callable[[int], Optional[Line]]) -> Optional[str]:
    n: Counter = Counter()
    top: Dict[str, int] = {}
    for s in sids:
        ln = line(s)
        if ln is None:
            continue
        n[ln[0]] += 1
        top[ln[0]] = max(top.get(ln[0], ln[1]), ln[1])
    return max(n, key=lambda k: (n[k], top[k], k)) if n else None


def continuations(streams: Set[int], secs: Dict[int, Set[int]], item_down: Iterable[Tuple[int, int]],
                  down: Dict[int, int], line: Callable[[int], Optional[Line]]) -> List[Tuple[int, int, int, int]]:
    """[(A ord, A's mouth section, B ord, B's top section)] for every A that continues as B, by A's ord."""
    ups: Dict[int, List[int]] = defaultdict(list)
    for a, b in down.items():
        ups[b].append(a)

    def end(o: int, lowest: bool) -> Optional[Tuple[str, int]]:
        m = _main(secs.get(o, ()), line)
        if m is None:
            return None
        on = [(line(s)[1], s) for s in secs[o] if line(s) is not None and line(s)[0] == m]  # type: ignore[index]
        return m, (min(on) if lowest else max(on))[1]

    out = []
    for a_ord, b_ord in sorted(set(item_down)):
        if a_ord not in streams or b_ord not in streams or a_ord == b_ord:
            continue
        ae, be = end(a_ord, True), end(b_ord, False)
        if ae is None or be is None:
            continue
        (a_line, a), (b_line, b) = ae, be
        if down.get(a) != b:
            continue
        if a_line == b_line or ups[b] == [a]:
            out.append((a_ord, a, b_ord, b))
    seen: Dict[int, int] = {}
    for a_ord, _, b_ord, _ in out:
        if b_ord in seen:
            raise ContinuationError(f"water_continue: water ord {b_ord} is continued by two waters "
                                    f"({seen[b_ord]} and {a_ord}); the atlas's topology is wrong there")
        seen[b_ord] = a_ord
    return out


def rows(b: sqlite3.Connection, ids: Sequence[str]) -> List[Tuple[int, int, int, int]]:
    """`water_continue` rows `(ord, part, next_ord, next_part)` from the bundle `b` and the atlas's section ids (`ids[h-1]`
    names handle `h`)."""
    def line(sid: int) -> Optional[Line]:
        return line_of(ids[sid - 1]) if 0 < sid <= len(ids) else None

    streams = {o for (o,) in b.execute("SELECT ord FROM item WHERE kind = 'stream'")}
    secs: Dict[int, Set[int]] = defaultdict(set)
    for o, s in b.execute("SELECT ord, sid FROM item_section"):
        secs[o].add(s)
    down = dict(b.execute("SELECT sid, down_sid FROM section_down"))
    part = {(o, s): p for o, s, p in b.execute("SELECT ord, sid, part_ix FROM part_section")}
    out = []
    for a_ord, a, b_ord, bs in continuations(streams, secs, b.execute("SELECT ord, down_ord FROM item_down"), down,
                                             line):
        pa, pb = part.get((a_ord, a)), part.get((b_ord, bs))
        if pa is None or pb is None:
            raise ContinuationError(f"water_continue: section {a if pa is None else bs} has no part")
        out.append((a_ord, pa, b_ord, pb))
    return out
