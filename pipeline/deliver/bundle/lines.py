"""A WATER'S MAIN LINE — the one place the bundle decides it (T118, owner 2026-10-22: "unify checks that decide the same
thing").

THE DECISION. "Which sections are this water's main line?" A named stream is made of pieces of one or more FWA blue
lines: its own line, and the side channels, braids and lower lines the registry gave the same name. Its MAIN LINE is
the blue line carrying the most of its LENGTH (metres of its stream pieces on that line); a tie goes to the lowest
blue-line key, so the choice depends on the pieces and nothing else. Its polygons and lakes are on no line.

WHO ASKS. `spans.compute` (the main stem `section_span` measures along, `off_stem` for the rest),
`continuation.links` / `candidates` (the line whose lowest piece is a water's mouth and whose highest is its top, for
`water_continue` and the span end words) and `topology.mouth_downs` (the mouth `water_exit` walks down from). Each
used to decide it on its own: spans by length, continuation and topology by SECTION COUNT (ties: the higher top
measure, then the higher key). Where a water's pieces are short on its true stem and many on a side line (Ecstall
River: 37 of its 42 sections off the length stem), the two named different lines, so the mouth `water_exit` walked
from and the stem the spans measured were not on one line. No module of `deliver/bundle/` picks a stem but this one
(`test_lines.py`, the grep pin).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple


def piece(node) -> Optional[Tuple[str, float]]:
    """(blue line, length in metres) of an atlas graph node that is a piece of a blue line — a STREAM node; None for
    anything else (a lake, a wetland, a river's polygon), which lies on no line."""
    if node is None or getattr(node.kind, "value", node.kind) != "stream":
        return None
    return str(node.blk), float(node.length_m)


def main_line(pieces: Iterable[Tuple[str, float]]) -> Optional[str]:
    """THE water's main line from its pieces `(blue line, length_m)`: the line carrying the most length (to the
    millimetre); a tie goes to the lowest line key. None for no pieces."""
    total: dict[str, float] = defaultdict(float)
    for blk, length in pieces:
        total[str(blk)] += float(length)
    if not total:
        return None
    return min(total.items(), key=lambda kv: (-round(kv[1], 3), kv[0]))[0]


def main_line_of(nodes: Mapping[str, object], node_ids: Iterable[str]) -> Optional[str]:
    """`main_line` of the graph nodes `node_ids` (ids of `nodes`, the atlas graph's node dict): their stream pieces.
    An id the graph does not hold is no piece."""
    return main_line(p for p in (piece(nodes.get(n)) for n in node_ids) if p is not None)


def handle_lengths(nodes: Mapping[str, object], ids: Sequence[str], sids: Iterable[int]) -> Dict[int, float]:
    """{section handle: length in metres} of the handles in `sids` that are stream pieces (`piece`) — what `line_ends`
    weighs by. `ids[h-1]` names handle `h` (the atlas's `section_handles.txt`); `nodes` is the graph's node dict."""
    out: Dict[int, float] = {}
    for h in sids:
        p = piece(nodes.get(ids[h - 1])) if 0 < h <= len(ids) else None
        if p is not None:
            out[h] = p[1]
    return out


def line_ends(sids: Iterable[int], line: Callable[[int], Optional[Tuple[str, int]]],
              length: Mapping[int, float]) -> Optional[Tuple[str, int, int]]:
    """(main line, its LOWEST section, its HIGHEST section) of a water by section handle: the water's mouth piece and
    top piece on its main line. `sids` are its handles, `line(h)` the (blue line, measure) a handle names
    (`continuation.line_of`) or None, `length` {handle: metres} of its stream pieces (`handle_lengths`); a handle with
    no length is no piece. None: the water has no piece on a line (a lake)."""
    on = [(ln, s) for s in sorted(set(sids)) if s in length and (ln := line(s)) is not None]
    m = main_line((ln[0], length[s]) for ln, s in on)
    if m is None:
        return None
    ms = sorted((ln[1], s) for ln, s in on if ln[0] == m)
    return m, ms[0][1], ms[-1][1]
