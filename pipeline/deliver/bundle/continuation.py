"""THE CONTINUATION LINK (owner 2026-10-21: "it should be able to navigate up if one stream comes directly from another,
like vedder from chilliwack"): `water_continue (ord, part, next_ord, next_part)`, written by the BUNDLE (`links` while it
holds the atlas graph, `write` once the parts exist) and copied into the release's water file — water
`ord` CONTINUES AS water `next_ord`: the same watercourse under another name, `part` the part of `ord` at its mouth and
`next_part` the part of `next_ord` at its top. The app's water map steps across it (▲ from the Vedder's top part into
the Chilliwack's lowest, ▼ back). Topology, not a regulation.

THE RULE (stated, pinned in `test_water_continue.py` and on the real waters in `test_release.py`). A continues as B
when both are streams, `item_down` already says A flows into B, and in the ATLAS GRAPH (`graph.pkl`: every inflow, with
the measure on the receiving line where it joins):
  - A's MAIN LINE (the FWA blue line holding most of A's sections; ties: the higher top measure, then the key) BEGINS
    there — the piece is A's lowest measure on that line — and flows into
  - B's main line's TOP piece (B's highest measure on its main line);
  - and A is the watercourse there. Either A's mouth piece lies on B's own blue line (the FWA main stem), or:
      · A joins within `STUB_M` of the top of that piece — B's line does not go on above A's mouth (the Chilliwack
        joins 25 m below the Vedder line's end, the atlas's snapping), and
      · nothing else joins B there: no other stream and no lake outlet within `CONFLUENCE_M` of A's mouth or above it
        — two rivers meeting make a third, and neither is it (the Sproat and the Stamp 64 m apart make the Somass;
        the North and South Thompson 145 m apart the Thompson; Cowichan Lake's outlet sits 635 m above Oliver Creek,
        at the Cowichan's top), and
      · no piece of B itself comes in lower down that piece (the upper Cottonwood joins the Cottonwood's top piece
        448 m below the Swift: the Cottonwood goes on as itself);
  - and the FWA WATERSHED CODES do not say the flow is backwards: B's code is never a descendant of A's (B's code
    extending A's — `100-1` and `100-1-2` — says B is a TRIBUTARY of A, so "A flows into B" is the data reversed). The
    owner's case: the added streams say Eagle Trib.5 (`…-757900`) flows into Eagle Trib.5-3 (`…-757900-384000`), an
    added-streams orientation defect (owner 2026-10-21 late: "we could just trim out those bad ones"). A missing code
    is no evidence; A's code extending B's (a tributary that is all there is above B's top) is kept, as the owner kept
    the municipal ones.

WHY THE GRAPH, NOT THE BUNDLE'S `section_down` (review 2026-10-21). `section_down` stores gauge-shed members only, so
"the only inflow at B's top" read from it was blind to most inflows: the first cut shipped 92 rows, 89 of them
tributaries or confluences, and missed every continuation outside a shed.

WHY NOT "THE SAME BLUE LINE" ALONE. That was the ask, and the atlas says otherwise: the Chilliwack is FWA blue line
380887781 from measure 0, and it flows into the Vedder's line 356363467 at the top of that line — two blue lines, one
watercourse, a name change at Vedder Crossing. A braid piece or a side channel is never a water's main line, so it
cannot carry the link.

The section ids come from the atlas's `section_handles.txt` (`{blue_line}:{measure}`; `lake:…` is no line), the one
the bundle is built on — the bundle build hands `links` them and the graph it already holds (audit F07, owner
2026-10-22 call 6: the release used to unpickle graph.pkl again just for this).
"""
from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from pipeline.common.utils.wsc import trim_wsc

Line = Tuple[str, int]

#: How far B's line may go on above A's mouth and still be "A's mouth is B's top": the atlas's snapping, not a reach of
#: B's own (the Chilliwack: 25 m; the real tributaries the first cut shipped joined 238 m and more below).
STUB_M = 100.0
#: Another inflow this close to A's mouth (or above it) makes it a confluence: the Somass's two rivers 64 m apart, the
#: Taku's 90, the Fort Nelson's 77, the Thompson's 145.
CONFLUENCE_M = 200.0


class ContinuationError(SystemExit):
    pass


class Flow:
    """The atlas graph in section HANDLES: `edges` (from, to, at — the measure on `to`'s line where `from` joins), each
    piece's top measure (`top`), which waters own each section (`owners`) and each piece's FWA watershed code (`wsc`,
    where known). `from` may be 0: an inflow the handle table does not name still comes in."""

    def __init__(self, edges: Iterable[Tuple[int, int, float]], top: Mapping[int, float],
                 owners: Mapping[int, Set[int]], wsc: Optional[Mapping[int, str]] = None):
        self._out: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        self._in: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        for f, t, at in edges:
            self._out[f].append((t, at))
            self._in[t].append((f, at))
        self.top, self.owners = top, owners
        self.wsc: Mapping[int, str] = wsc or {}

    def out(self, sid: int) -> List[Tuple[int, float]]:
        return self._out.get(sid, [])

    def into(self, sid: int) -> List[Tuple[int, float]]:
        return self._in.get(sid, [])


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


def drains_into(code: str, into: str) -> bool:
    """Whether the FWA watershed code `code` names a tributary (at any depth) of the water coded `into`: `into`'s groups
    are a proper prefix of `code`'s, after the `-000000` padding is trimmed. An empty code says nothing."""
    c, i = trim_wsc(code or ""), trim_wsc(into or "")
    return bool(c and i) and c.startswith(i + "-")


def _goes_on(a_ord: int, a: int, b_ord: int, b: int, same_line: bool, flow: Flow) -> bool:
    """Whether A's mouth piece `a` is the watercourse at B's top piece `b` (THE RULE, above)."""
    joins = [at for to, at in flow.out(a) if to == b]
    if not joins:
        return False
    if drains_into(flow.wsc.get(b, ""), flow.wsc.get(a, "")):
        return False
    if same_line:
        return True
    at = max(joins)
    if flow.top.get(b, at) - at > STUB_M:
        return False
    others = [(f, x) for f, x in flow.into(b) if f != a]
    if any(x >= at - CONFLUENCE_M for _, x in others):
        return False
    return not any(b_ord in flow.owners.get(f, ()) for f, _ in others)


def continuations(streams: Set[int], secs: Dict[int, Set[int]], item_down: Iterable[Tuple[int, int]],
                  line: Callable[[int], Optional[Line]], flow: Flow) -> List[Tuple[int, int, int, int]]:
    """[(A ord, A's mouth section, B ord, B's top section)] for every A that continues as B, by A's ord."""
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
        if _goes_on(a_ord, a, b_ord, b, a_line == b_line, flow):
            out.append((a_ord, a, b_ord, b))
    for i, what in ((2, "is continued by two waters"), (0, "continues as two waters")):
        seen: Dict[int, int] = {}
        for r in out:
            if r[i] in seen:
                raise ContinuationError(f"water_continue: water ord {r[i]} {what} ({seen[r[i]]} and {r[2 - i]}); "
                                        f"the atlas's topology is wrong there")
            seen[r[i]] = r[2 - i]
    return out


def graph_flow(graph, ids: Sequence[str], owners: Mapping[int, Set[int]], tops: Iterable[int]) -> Flow:
    """The `Flow` of the atlas graph (`StreamGraph`) for the pieces that matter: the edges into the candidate top pieces
    `tops` (handles) and out of every piece flowing into them, in handles (`ids[h-1]` names handle `h`), with each of
    those pieces' FWA watershed code."""
    hid = {s: h for h, s in enumerate(ids, 1)}
    edges, top, wsc = [], {}, {}
    for h in tops:
        node = ids[h - 1]
        n = graph.nodes.get(node)
        if n is None:
            continue
        top[h], wsc[h] = float(n.up_m), n.wsc
        for ix in graph.up_adj.get(node, ()):
            e = graph.edges[ix]
            f = hid.get(e.from_node, 0)
            edges.append((f, h, float(e.at_measure)))
            fn = graph.nodes.get(e.from_node)
            if f and fn is not None:
                wsc[f] = fn.wsc
    return Flow(edges, top, owners, wsc)


def links(b: sqlite3.Connection, ids: Sequence[str], graph) -> List[Tuple[int, int, int, int]]:
    """[(A ord, A's mouth section, B ord, B's top section)] from the bundle `b` as far as it is built when the graph is
    loaded (`item`, `item_section`, `item_down`), the atlas's section ids (`ids[h-1]` names handle `h`) and its graph
    (`graph.pkl`, a `StreamGraph`). The bundle build calls this while it holds the graph; `write` turns the sections
    into parts once `part_section` exists."""
    def line(sid: int) -> Optional[Line]:
        return line_of(ids[sid - 1]) if 0 < sid <= len(ids) else None

    streams = {o for (o,) in b.execute("SELECT ord FROM item WHERE kind = 'stream'")}
    secs: Dict[int, Set[int]] = defaultdict(set)
    owners: Dict[int, Set[int]] = defaultdict(set)
    for o, s in b.execute("SELECT ord, sid FROM item_section"):
        secs[o].add(s)
        owners[s].add(o)
    tops = set()
    for o in streams:
        m = _main(secs.get(o, ()), line)
        if m is not None:
            tops.add(max((line(s)[1], s) for s in secs[o] if line(s) is not None and line(s)[0] == m)[1])  # type: ignore[index]
    flow = graph_flow(graph, ids, owners, tops)
    return continuations(streams, secs, b.execute("SELECT ord, down_ord FROM item_down"), line, flow)


def write(b: sqlite3.Connection, found: Iterable[Tuple[int, int, int, int]], cov) -> int:
    """The bundle's `water_continue (ord, part, next_ord, next_part)` from `links`' rows, once `part_section` exists. A
    linked section with no part stops the build."""
    part = {(o, s): p for o, s, p in b.execute("SELECT ord, sid, part_ix FROM part_section")}
    out = []
    for a_ord, a, b_ord, bs in found:
        pa, pb = part.get((a_ord, a)), part.get((b_ord, bs))
        if pa is None or pb is None:
            raise ContinuationError(f"water_continue: section {a if pa is None else bs} has no part")
        out.append((a_ord, pa, b_ord, pb))
    b.executemany("INSERT INTO water_continue (ord, part, next_ord, next_part) VALUES (?,?,?,?)", sorted(out))
    cov.filled("water_continue", len(out))
    return len(out)
