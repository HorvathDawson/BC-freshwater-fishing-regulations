"""THE CONTINUATION LINK (owner 2026-10-21: "it should be able to navigate up if one stream comes directly from another,
like vedder from chilliwack"): `water_continue (ord, part, next_ord, next_part)`, written by the BUNDLE (`links` while it
holds the atlas graph, `write` once the parts exist) and copied into the release's water file — water
`ord` CONTINUES AS water `next_ord`: the same watercourse under another name, `part` the part of `ord` at its mouth and
`next_part` the part of `next_ord` at its top. The app's water map steps across it (▲ from the Vedder's top part into
the Chilliwack's lowest, ▼ back). Topology, not a regulation.

THE RULE (stated, pinned in `test_water_continue.py` and on the real waters in `test_release.py`). A continues as B
when both are streams, `item_down` already says A flows into B, and in the ATLAS GRAPH (`graph.pkl`: every inflow, with
the measure on the receiving line where it joins):
  - A's MAIN LINE (the FWA blue line carrying most of A's length, `lines.main_line` — the one the spans measure along;
    T118: it was the line holding most of A's SECTIONS, and the two differed) BEGINS
    there — the piece is A's lowest measure on that line — and flows into
  - B's main line's TOP piece (B's highest measure on its main line);
  - and A is the watercourse there. Either A's mouth piece lies on B's own blue line (the FWA main stem), or:
      · A joins within `STUB_M` of the top of that piece — B's line does not go on above A's mouth (the Chilliwack
        joins 25 m below the Vedder line's end, the atlas's snapping), and
      · nothing else joins B there: no other stream within `CONFLUENCE_M` of A's mouth or above it — two rivers
        meeting make a third, and neither is it (the Sproat and the Stamp 64 m apart make the Somass; the North and
        South Thompson 145 m apart the Thompson) — and no lake outlet (Cowichan Lake's outlet sits 635 m above Oliver
        Creek, at the Cowichan's top: B begins at the lake, T108), and
      · no piece of B itself comes in within `OWN_M` below (the upper Cottonwood joins the Cottonwood's top piece
        448 m below the Swift: the Cottonwood goes on as itself);
  - TWO RIVERS MEET (T59, owner 2026-10-22: "think of things like this as one system; bella coola atnarko is another
    example"): where every gate above holds except that another inflow joins within `CONFLUENCE_M`, or B's own side
    channel rejoins its top piece farther down than `OWN_M`, the atlas cannot say which river goes on — the BOOK can.
    A synopsis row whose heading names A and B together as one water ("ATNARKO/BELLA COOLA RIVERS", matched to both;
    `heading_pairs`) makes A the one that goes on, and the other river stays a tributary; a heading pairing two rivers
    meeting there with B names neither (`settle`). Without a pairing nothing changes: the Somass and the Thompson
    stay confluences. Not built: a fallback on upstream length / Strahler order where no heading pairs them;
  - and the FWA WATERSHED CODES do not say the flow is backwards: B's code is never a descendant of A's (B's code
    extending A's — `100-1` and `100-1-2` — says B is a TRIBUTARY of A, so "A flows into B" is the data reversed). The
    owner's case: the added streams say Eagle Trib.5 (`…-757900`) flows into Eagle Trib.5-3 (`…-757900-384000`), an
    added-streams orientation defect (owner 2026-10-21 late: "we could just trim out those bad ones"). A missing code
    is no evidence; A's code extending B's (a tributary that is all there is above B's top) is kept, as the owner kept
    the municipal ones.

ONE DECISION (T108, owner 2026-10-22: "unify checks that decide the same thing"). `goes_on` is the only place that
answers "at its lower end, does A go on as B, meet B at a confluence, or end?", with one constant set (`STUB_M`,
`CONFLUENCE_M`, `OWN_M`). The bundle's span end words read it (`lower_ends` -> `spans.compute`): a `continues:<B>` end
is exactly a `water_continue` link. `spans.py` used to decide it again (`lower_end_into`: 100 / 250 m and a magnitude
test) and disagreed on 11 of 17 links. Folded in from there: a water never continues into ITSELF (two items of one
name), a creek flowing into a LAKE ends at the lake, a LAKE draining in near B's top is where B begins (A ends: it was
a confluence here, which only a book pairing could have turned into a link), and two rivers meeting a little below
B's top — one within `STUB_M`, A within `CONFLUENCE_M` (the Nakina and the Inklin making the Taku) — are a
confluence, not a tributary. Kept from there, for the WORDS only: a confluence names the other river only when it is
of comparable rank (RULINGS P10, `lower_ends`).

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
from pipeline.common.utils import wsc as _wsc
from pipeline.common import entry_id as EID
from pipeline.common import tolerances as _tol
from pipeline.deliver.bundle.lines import handle_lengths, line_ends

Line = Tuple[str, int]

#: How far B's line may go on above A's mouth and still be "A's mouth is B's top": the atlas's snapping, not a reach of
#: B's own (the Chilliwack: 25 m; the real tributaries the first cut shipped joined 238 m and more below).
STUB_M = _tol.STUB_M                # 100 m (pipeline/common/tolerances.py, as are the two below)
#: Another inflow this close to A's mouth (or above it) makes it a confluence: the Somass's two rivers 64 m apart, the
#: Taku's 90, the Fort Nelson's 77, the Thompson's 145.
CONFLUENCE_M = _tol.CONFLUENCE_M    # 200 m
#: A piece of B itself joining B's top piece this close below A's mouth (or above it) is B going on as itself (the upper
#: Cottonwood, 448 m below the Swift). Farther down it is B's own side channel rejoining a long top piece (the Bella
#: Coola's braids join its 14 km top piece 7 km and more below the Atnarko): that leaves only a book pairing to decide.
OWN_M = _tol.OWN_M                  # 1000 m


class ContinuationError(SystemExit):
    pass


class Flow:
    """The atlas graph in section HANDLES: `edges` (from, to, at — the measure on `to`'s line where `from` joins), each
    piece's top measure (`top`), which waters own each section (`owners`) and each piece's FWA watershed code (`wsc`,
    where known). `from` may be 0: an inflow the handle table does not name still comes in.

    `length` {section: metres} of every stream piece of the waters (`lines.handle_lengths`): what a water's MAIN LINE
    is weighed by (`lines.line_ends`, T118). `goes_on` alone needs none; `candidates` refuses a Flow without it, which
    would have no main lines at all and so no links, silently.

    And what only the lower-end words need (T108, all optional): `lakes` {top piece: [measure a LAKE drains in at]} (a
    lake outlet is not a stream edge), `into_lake` the pieces that flow into a lake (a creek ends at the lake) and
    `names` {ord: name} (no water continues into itself)."""

    def __init__(self, edges: Iterable[Tuple[int, int, float]], top: Mapping[int, float],
                 owners: Mapping[int, Set[int]], wsc: Optional[Mapping[int, str]] = None, *,
                 length: Optional[Mapping[int, float]] = None,
                 lakes: Optional[Mapping[int, Sequence[float]]] = None, into_lake: Iterable[int] = (),
                 names: Optional[Mapping[int, str]] = None):
        self._out: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        self._in: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        for f, t, at in edges:
            self._out[f].append((t, at))
            self._in[t].append((f, at))
        self.top, self.owners = top, owners
        self.length: Optional[Mapping[int, float]] = length
        self.wsc: Mapping[int, str] = wsc or {}
        self.lakes: Mapping[int, Sequence[float]] = lakes or {}
        self.into_lake: Set[int] = set(into_lake)
        self.names: Mapping[int, str] = names or {}

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


def drains_into(code: str, into: str) -> bool:
    """Whether the FWA watershed code `code` names a tributary (at any depth) of the water coded `into`: `into`'s groups
    are a proper prefix of `code`'s, after the `-000000` padding is trimmed. An empty code says nothing."""
    return _wsc.is_strictly_below(trim_wsc(code or ""), trim_wsc(into or ""))


#: `goes_on`'s answers: A goes on as B; A goes on as B only if the book pairs them (two rivers meet there); A does not.
YES, CONFLUENCE, NO = "yes", "confluence", None


def _same_name(a: str, b: str) -> bool:
    return bool(a) and " ".join(a.lower().split()) == " ".join(b.lower().split())


def goes_on(a_ord: int, a: int, b_ord: int, b: int, same_line: bool, flow: Flow) -> Optional[str]:
    """AT ITS LOWER END, DOES WATER A GO ON AS WATER B, MEET B AT A CONFLUENCE, OR END? (T108: the one place this is
    decided — `water_continue` and the span end words both read it.) A's mouth piece `a`, B's top piece `b` (THE RULE,
    above): `YES`, `CONFLUENCE` (two rivers meet at B's top: only a book pairing decides, `settle`; the span end names
    the other river, `lower_ends`) or `NO` (A ends: a tributary's mouth, or a lake).

    In order: A must flow into `b`; a water never goes on as ITSELF (two items of one name: Jeperson Side Channel); a
    creek flowing into a LAKE ends at the lake (Still Creek into Burnaby Lake, though FWA also links it to the
    Brunette's top); the FWA codes must not say the flow is backwards; on B's own blue line it goes on. Off it: a LAKE
    draining into B within `CONFLUENCE_M` of A's mouth or above is where B begins (Cowichan Lake above Oliver Creek): A
    ends. B's own piece within `OWN_M`: B goes on as itself. B's line going on more than `STUB_M` above A's mouth: A is a
    tributary — unless another river joins within `STUB_M` of B's top and A within `CONFLUENCE_M` of it: two rivers
    meet a little below the top (the Nakina 65 m below the Taku's, the Inklin 154 m; the Fontas 53 m below the Fort
    Nelson's, the Sikanni Chief 130 m). Else any other inflow within `CONFLUENCE_M` (or B's own side channel farther
    down) makes it a confluence; nothing else: YES."""
    joins = [at for to, at in flow.out(a) if to == b]
    if not joins:
        return NO
    if _same_name(flow.names.get(a_ord, ""), flow.names.get(b_ord, "")):
        return NO
    if a in flow.into_lake:
        return NO
    if drains_into(flow.wsc.get(b, ""), flow.wsc.get(a, "")):
        return NO
    if same_line:
        return YES
    at = max(joins)
    if any(x >= at - CONFLUENCE_M for x in flow.lakes.get(b, ())):
        return NO
    others = [(f, x) for f, x in flow.into(b) if f != a]
    own = [x for f, x in others if b_ord in flow.owners.get(f, ())]
    if any(x >= at - OWN_M for x in own):
        return NO
    top = flow.top.get(b, at)
    if top - at > STUB_M:
        meet = top - at <= CONFLUENCE_M and any(x >= top - STUB_M for _, x in others)
        return CONFLUENCE if meet else NO
    return CONFLUENCE if own or any(x >= at - CONFLUENCE_M for _, x in others) else YES


#: A link: (A ord, A's mouth section, B ord, B's top section, made at a confluence).
Link = Tuple[int, int, int, int, bool]


def candidates(streams: Set[int], secs: Dict[int, Set[int]], item_down: Iterable[Tuple[int, int]],
               line: Callable[[int], Optional[Line]], flow: Flow) -> List[Link]:
    """Every A whose mouth meets B's top as THE RULE says, by A's ord; the last field says it was made at a confluence
    (two rivers meet there), which only a book pairing keeps (`settle`)."""
    if flow.length is None:
        raise ContinuationError("continuation.candidates: the Flow carries no piece lengths, so no water has a main "
                                "line — build it with `length` (`lines.handle_lengths`)")

    def end(o: int, lowest: bool) -> Optional[Tuple[str, int]]:
        got = line_ends(secs.get(o, ()), line, flow.length)
        if got is None:
            return None
        m, low, high = got
        return m, (low if lowest else high)

    out = []
    for a_ord, b_ord in sorted(set(item_down)):
        if a_ord not in streams or b_ord not in streams or a_ord == b_ord:
            continue
        ae, be = end(a_ord, True), end(b_ord, False)
        if ae is None or be is None:
            continue
        (a_line, a), (b_line, b) = ae, be
        how = goes_on(a_ord, a, b_ord, b, a_line == b_line, flow)
        if how is not NO:
            out.append((a_ord, a, b_ord, b, how == CONFLUENCE))
    return out


def settle(found: Iterable[Link], paired: Set[Tuple[int, int]]) -> List[Tuple[int, int, int, int]]:
    """[(A ord, A's mouth section, B ord, B's top section)]: every link not made at a confluence, and a confluence's
    link only when the book pairs A with B (`paired`, either order: `heading_pairs`) and pairs no other river meeting
    there with B — "ATNARKO/BELLA COOLA RIVERS" names the Atnarko; a heading pairing both would name neither. A water
    continued twice or continuing twice stops the build."""
    found = sorted(set(found))
    pair = lambda a, b: (a, b) in paired or (b, a) in paired  # noqa: E731
    named = Counter(r[2] for r in found if r[4] and pair(r[0], r[2]))
    out = [r[:4] for r in found if not r[4] or (pair(r[0], r[2]) and named[r[2]] == 1)]
    for i, what in ((2, "is continued by two waters"), (0, "continues as two waters")):
        seen: Dict[int, int] = {}
        for r in out:
            if r[i] in seen:
                raise ContinuationError(f"water_continue: water ord {r[i]} {what} ({seen[r[i]]} and {r[2 - i]}); "
                                        f"the atlas's topology is wrong there")
            seen[r[i]] = r[2 - i]
    return out


def continuations(streams: Set[int], secs: Dict[int, Set[int]], item_down: Iterable[Tuple[int, int]],
                  line: Callable[[int], Optional[Line]], flow: Flow,
                  paired: Iterable[Tuple[int, int]] = ()) -> List[Tuple[int, int, int, int]]:
    """[(A ord, A's mouth section, B ord, B's top section)] for every A that continues as B, by A's ord: `candidates`,
    then `settle` with the book's pairings `paired` (ord pairs)."""
    return settle(candidates(streams, secs, item_down, line, flow), set(paired))


#: Plural kind words a heading naming two waters ends on ("ATNARKO/BELLA COOLA RIVERS"): each segment is one of them.
_PLURAL_KINDS = frozenset({"rivers", "creeks", "sloughs", "channels"})


def heading_pairs(heading: str, names: Mapping[str, str]) -> Set[frozenset]:
    """THE BOOK'S PAIRING (T59): the pairs of waters `names` ({item_id: registry name}, the entry's matched waters) that
    ONE heading names together as one row — "ATNARKO/BELLA COOLA RIVERS", "CHILLIWACK / Vedder Rivers". The heading's
    name (`place_names.heading_name`: no notes, no "[Includes Tributaries] …" tail) split on "/"; a water is named by a
    segment when the words saying which water it is are the SAME (`place_names._which_water`, the segments' plural
    kind word dropped): "South Atnarko River" is not "Atnarko", and "KISPIOX RIVER CONFLUENCE" names a place."""
    from pipeline.deliver.bundle.place_names import _which_water, heading_name
    segs = [frozenset(_which_water(s) - _PLURAL_KINDS) for s in heading_name(heading or "", quotes=False).split("/")]
    if len(segs) < 2:
        return set()
    at: Dict[int, List[str]] = defaultdict(list)
    for item, name in sorted(names.items()):
        w = _which_water(name)
        for k, s in enumerate(segs):
            if s and w == s:
                at[k].append(item)
    return {frozenset((x, y)) for i in at for j in at if i < j for x in at[i] for y in at[j] if x != y}


def pairs_of(b: sqlite3.Connection, heads: Iterable[Tuple[str, str, Sequence[str]]]) -> Set[Tuple[int, int]]:
    """{(ord, ord)} both orders, of every two STREAMS (the bundle's `item`) one water row's heading pairs
    (`heading_pairs`) among its matched waters. `heads` is `(entry_id, heading, matched item ids)` per entry; only water
    rows (`r…`) whose heading has a "/" can pair. One rule for both readers: `book_pairs` (the `entry` table) and
    `catalogue_heads` (the catalogue, read before `entry` exists — the donor panels need the links then, T76)."""
    ordof = {i: (o, n) for o, i, n in b.execute("SELECT ord, item_id, name FROM item WHERE kind = 'stream'")}
    out: Set[Tuple[int, int]] = set()
    for eid, full, matched in sorted(heads, key=lambda h: h[0]):
        if not EID.is_water_row(eid.lower()) or "/" not in (full or ""):
            continue
        names = {i: ordof[i][1] for i in matched or () if i in ordof}
        for p in heading_pairs(full, names):
            x, y = sorted(ordof[i][0] for i in p)
            out |= {(x, y), (y, x)}
    return out


def book_pairs(b: sqlite3.Connection) -> Set[Tuple[int, int]]:
    """`pairs_of` the bundle's `entry` table (`entry.full_name`, `entry.matched`)."""
    import json
    return pairs_of(b, [(e, f, json.loads(m or "[]"))
                        for e, f, m in b.execute("SELECT entry_id, full_name, matched FROM entry")])


def catalogue_heads(entries_dir, registry=None) -> List[Tuple[str, str, List[str]]]:
    """`(entry_id, heading, matched)` of every entry of every entry SOURCE under `entries_dir`, read as `rules.write`
    reads them (`rules.read_entries`): the rows `entry` will hold, before it is written."""
    from pipeline.deliver.bundle.rules import read_entries
    return [(e["entry_id"], e.get("name") or "", list(e.get("matched") or ()))
            for e in read_entries(entries_dir, registry)]


def item_pairs(b: sqlite3.Connection, settled: Iterable[Tuple[int, int, int, int]]) -> List[Tuple[str, str]]:
    """`settle`'s links as `(water item, the water item it goes on as)` — what the donor panels take (T76)."""
    item = dict(b.execute("SELECT ord, item_id FROM item"))
    return sorted((item[a], item[c]) for a, _s, c, _t in settled)


#: A section's FWA rank: (Strahler stream order, Shreve stream magnitude) — `ranks`.
Rank = Tuple[int, int]


def strahler(a: int, b: int) -> int:
    """The Strahler order below two streams of orders a and b meeting."""
    return a + 1 if a == b else max(a, b)


def lower_ends(found: Iterable[Link], settled: Iterable[Tuple[int, int, int, int]], item_of: Mapping[int, str],
               rank: Mapping[int, Rank]) -> Dict[int, str]:
    """THE SPAN END WORDS OF A WATER'S NATURAL LOWER END (T108), from `goes_on`'s answers — the relation is never decided
    again: {A's mouth section: token}. A settled link (`water_continue`) is `continues:<B>`. A link made at a
    confluence the book did not settle is `confluence:<X>`, X the other river A meets at B's top — another candidate
    into the same top piece of COMPARABLE RANK (RULINGS P10: Strahler orders within 1 that together make B's order;
    `rank` by section, `ranks`), the largest by magnitude, then the lowest item id: the Sproat and the Stamp make the
    Somass, the Talchako meets the Atnarko. With no such river, or no link, the section keeps its `mouth` (Sixtyone
    Creek, order 3, beside Fiftynine Creek, order 5: a tributary). `item_of` is {ord: item_id}."""
    found = sorted(set(found))
    went = {(a_ord, a): b_ord for a_ord, a, b_ord, _b in settled}
    at_top: Dict[Tuple[int, int], List[Tuple[int, int]]] = defaultdict(list)
    for a_ord, a, b_ord, b, _c in found:
        at_top[(b_ord, b)].append((a_ord, a))

    def meets(a: int, x: int, b: int) -> bool:
        if a not in rank or x not in rank or b not in rank:
            return False
        oa, ox = rank[a][0], rank[x][0]
        return abs(oa - ox) <= 1 and strahler(oa, ox) == rank[b][0]

    out: Dict[int, str] = {}
    for a_ord, a, b_ord, b, conf in found:
        if (a_ord, a) in went:
            out[a] = f"continues:{item_of[went[(a_ord, a)]]}"
        elif conf:
            partners = sorted((-rank[x][1], item_of[xo]) for xo, x in at_top[(b_ord, b)]
                              if xo != a_ord and meets(a, x, b))
            if partners:
                out[a] = f"confluence:{partners[0][1]}"
    return out


def ranks(graph, ids: Sequence[str], sids: Iterable[int]) -> Dict[int, Rank]:
    """{section handle: (FWA stream order, stream magnitude)} from the atlas graph, for `lower_ends`; a piece the FWA
    gives no order has no rank."""
    out = {}
    for h in sids:
        n = graph.nodes.get(ids[h - 1]) if 0 < h <= len(ids) else None
        if n is not None and getattr(n, "stream_order", None) is not None:
            out[h] = (int(n.stream_order), int(getattr(n, "stream_magnitude", None) or 0))
    return out


def _kind(x) -> str:
    k = getattr(x, "kind", None)
    return str(getattr(k, "value", k) or "")


def graph_flow(graph, ids: Sequence[str], owners: Mapping[int, Set[int]], tops: Iterable[int],
               names: Optional[Mapping[int, str]] = None,
               length: Optional[Mapping[int, float]] = None) -> Flow:
    """The `Flow` of the atlas graph (`StreamGraph`) for the pieces that matter: the edges into the candidate top pieces
    `tops` (handles) and out of every piece flowing into them, in handles (`ids[h-1]` names handle `h`), with each of
    those pieces' FWA watershed code; a LAKE draining into a top piece is kept apart (`Flow.lakes`), and a piece flowing
    into it that also flows into a lake is marked (`Flow.into_lake`). `length` is `lines.handle_lengths` of the
    sections in `owners`, computed here when the caller has not already."""
    hid = {s: h for h, s in enumerate(ids, 1)}
    down = getattr(graph, "down_adj", {})
    edges, top, wsc, lakes, into_lake = [], {}, {}, defaultdict(list), set()
    for h in tops:
        node = ids[h - 1]
        n = graph.nodes.get(node)
        if n is None:
            continue
        top[h], wsc[h] = float(n.up_m), n.wsc
        for ix in graph.up_adj.get(node, ()):
            e = graph.edges[ix]
            fn = graph.nodes.get(e.from_node)
            if _kind(e) == "lake_out" or (fn is not None and _kind(fn) == "lake"):
                lakes[h].append(float(e.at_measure))
                continue
            f = hid.get(e.from_node, 0)
            edges.append((f, h, float(e.at_measure)))
            if f and fn is not None:
                wsc[f] = fn.wsc
                if any(_kind(graph.edges[j]) == "lake_in" or _kind(graph.nodes.get(graph.edges[j].to_node)) == "lake"
                       for j in down.get(e.from_node, ())):
                    into_lake.add(f)
    if length is None:
        length = handle_lengths(graph.nodes, ids, owners)
    return Flow(edges, top, owners, wsc, length=length, lakes=lakes, into_lake=into_lake, names=names)


def links(b: sqlite3.Connection, ids: Sequence[str], graph) -> List[Link]:
    """The `candidates` [(A ord, A's mouth section, B ord, B's top section, at a confluence)] from the bundle `b` as far
    as it is built when the graph is loaded (`item`, `item_section`, `item_down`), the atlas's section ids (`ids[h-1]`
    names handle `h`) and its graph (`graph.pkl`, a `StreamGraph`). The bundle build calls this while it holds the
    graph; `write` settles them with the book's pairings and turns the sections into parts once `entry` and
    `part_section` exist."""
    def line(sid: int) -> Optional[Line]:
        return line_of(ids[sid - 1]) if 0 < sid <= len(ids) else None

    streams = {o for (o,) in b.execute("SELECT ord FROM item WHERE kind = 'stream'")}
    secs: Dict[int, Set[int]] = defaultdict(set)
    owners: Dict[int, Set[int]] = defaultdict(set)
    for o, s in b.execute("SELECT ord, sid FROM item_section"):
        secs[o].add(s)
        owners[s].add(o)
    length = handle_lengths(graph.nodes, ids, owners)
    tops = set()
    for o in streams:
        got = line_ends(secs.get(o, ()), line, length)
        if got is not None:
            tops.add(got[2])
    flow = graph_flow(graph, ids, owners, tops,
                      names=dict(b.execute("SELECT ord, name FROM item WHERE kind = 'stream'")), length=length)
    return candidates(streams, secs, b.execute("SELECT ord, down_ord FROM item_down"), line, flow)


def write(b: sqlite3.Connection, found: Iterable[Link], cov,
          built_with: Optional[Iterable[Tuple[int, int, int, int]]] = None) -> int:
    """The bundle's `water_continue (ord, part, next_ord, next_part)` from `links`' candidates, settled with the book's
    pairings (`book_pairs`, from `entry`), once `part_section` exists. A linked section with no part stops the build.

    `built_with`: the links the donor panels were built with (settled earlier from `catalogue_heads`, T76). The table
    must be exactly those — or the Vedder's panel and its ▲ would disagree about what one system is — else the build
    stops."""
    part = {(o, s): p for o, s, p in b.execute("SELECT ord, sid, part_ix FROM part_section")}
    settled = settle(found, book_pairs(b))
    if built_with is not None and sorted(settled) != sorted(built_with):
        raise ContinuationError(f"water_continue: the panels were built with {sorted(built_with)}, the entry table "
                                f"settles {sorted(settled)} — the catalogue read and `entry` disagree")
    out = []
    for a_ord, a, b_ord, bs in settled:
        pa, pb = part.get((a_ord, a)), part.get((b_ord, bs))
        if pa is None or pb is None:
            raise ContinuationError(f"water_continue: section {a if pa is None else bs} has no part")
        out.append((a_ord, pa, b_ord, pb))
    b.executemany("INSERT INTO water_continue (ord, part, next_ord, next_part) VALUES (?,?,?,?)", sorted(out))
    cov.filled("water_continue", len(out))
    return len(out)
