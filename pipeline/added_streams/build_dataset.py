"""One-time batch: municipal sources -> the minted "additional streams" dataset.

    clean -> merge -> fwa_match -> resolve receiver -> underlake -> mint -> validate -> write

Duplicates are dropped (favour FWA); extensions keep only the novel tail on the FWA stream's own
blk+wsc; novel streams mint a negative blk with a WSC that prefix-descends their receiver (FWA stream,
another added stream, or a 900 tidal root). `resolve_and_mint` is the pure core (inject FWA chains +
lake/tidal geometry) so it is hermetically testable; `build` is the gpkg-loading wrapper.

Output: `pipeline/added_streams/added_streams.build.json` (+ name-variant candidates + a report).
Nothing here edits existing pipeline code; graph wiring is a later Phase B.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.ops import substring, unary_union
from shapely.strtree import STRtree

from pipeline.added_streams import coastal
from pipeline.added_streams.fwa_match import _FwaIndex, classify, NameCandidate
from pipeline.added_streams.ingest import ConnectorSpec, added_fidrows
from pipeline.added_streams.merge import merge_channels
from pipeline.added_streams.underlake import LakeIndex, assign_under_lake
from pipeline.added_streams.validate import validate_propagation
from pipeline.added_streams.wsc import mint_wsc
from pipeline.utils.wsc import trim_wsc
from pipeline.models import BlkChain

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_CONNECT_TOL = 80.0        # a novel mouth must be within this of its receiver / the tidal boundary
_CONFLUENCE_GAP = 1.0
_GAP_TOL_M = 15.0          # merge same-name sections of one creek across gaps this small (one channel)
_TOUCH_TOL = 20.0          # two municipal channels connect where an endpoint of one meets the other
_SAME_NAME_TOL = 400.0     # bridge FREE dangling ends of ONE named creek this far apart (a mouth reach
                           # digitised well away from the body) so a fragmented creek reaches its outlet
_HINT_TOL = 800.0          # honour an explicit receiver hint unless the named FWA is absurdly far
_LAKE_TOL = 500.0          # a mouth this close to a lake polygon drains into it


def _albers(line: LineString) -> LineString:
    return LineString([_TO_ALBERS.transform(x, y) for x, y in line.coords])


def _orient_mouth_first(line: LineString, toward) -> LineString:
    """Orient so coords[0] is the end nearest ``toward`` (a geometry) — the mouth."""
    c = list(line.coords)
    return line if Point(c[0]).distance(toward) <= Point(c[-1]).distance(toward) else LineString(c[::-1])


def _resolve_topology(channels, geom, cls, index, fwa_by_blk, fwa_chains, lake_index,
                      tidal_union, connect_tol, ext_clip=None, dem_mouth=None, dem_down=None,
                      dem_outlet=None, connector_tol=250.0):
    """Topology-first receiver resolution (dataset-agnostic).

    Phase 1 builds the municipal CONNECTION GRAPH purely from geometry — two channels join where an
    endpoint of one lies on the other (a mouth meeting a stream). Phase 2 finds each channel's EXTERNAL
    ANCHOR (where an endpoint reaches an FWA line / a lake it drains into / the tidal boundary, or an
    explicit source hint), roots every connected component at its anchored channels (the outlets), and
    multi-source-BFS orients the tree so leaves are headwaters and flow runs to the nearest outlet. A
    novel's receiver is its parent toward the outlet — an FWA line when the parent is a duplicate or
    extension (favour FWA), else the parent added stream; an outlet's receiver is its anchor.
    Duplicates/extensions are FWA and act only as anchors (never minted here). No trimming — a novel's
    geometry is kept whole and merely oriented mouth-first. Mutates ``geom`` (orientation only).
    Returns (receiver: blk -> (kind, receiver_blk), unresolved: list[str])."""
    import heapq
    blks = [ch.blk for ch in channels]
    ch_by_blk = {ch.blk: ch for ch in channels}
    ntree = STRtree([geom[b] for b in blks]) if blks else None

    def _ends(b):
        c = geom[b].coords
        return (Point(c[0]), Point(c[-1]))

    def _lake_mainstem(poly):
        best = None
        for i in (index.tree.query(poly) if index.tree else []):
            g = index.geoms[int(i)]
            if not g.intersects(poly):
                continue
            c = fwa_by_blk.get(index.blk[int(i)])
            key = ((c.stream_magnitude or 0) if c else 0, g.length)
            if best is None or key > best[0]:
                best = (key, index.blk[int(i)], g)
        return (best[1], best[2]) if best else None

    # Phase 1 — municipal connection graph. adj[b][o] = the gap (connector length) between b and o, so
    # an endpoint of one channel lying on another (a mouth meeting a stream) makes a near-zero edge.
    adj: dict[int, dict[int, float]] = {b: {} for b in blks}
    def _link(a, c, gap):
        if gap < adj[a].get(c, 1e18):
            adj[a][c] = gap; adj[c][a] = gap
    for b in blks:
        for e in _ends(b):
            for i in (ntree.query(e.buffer(_TOUCH_TOL)) if ntree else []):
                o = blks[int(i)]
                d = e.distance(geom[o])
                if o != b and d <= _TOUCH_TOL:
                    _link(b, o, d)
    # Same-name gap bridge — a FRAGMENTED creek (municipal data leaves gaps, e.g. Suter Brook's mouth
    # reach digitised 362 m from its body) must connect into ONE component so the whole creek reaches its
    # outlet instead of stranding upstream pieces as unresolved. Per named creek, take the pieces already
    # joined by touches (Phase 1) as sub-components and add a MINIMUM SPANNING TREE of the smallest gap
    # bridges (<= _SAME_NAME_TOL) needed to join those sub-components. A spanning TREE adds no cycles, so
    # Dijkstra (rooted at the outlet: tidal/FWA/lake) still orients flow correctly — no uphill fans.
    byname: dict[str, list[int]] = {}
    for b in blks:
        cn = _canon_name(ch_by_blk[b].name)
        if cn:
            byname.setdefault(cn, []).append(b)
    for grp in byname.values():
        if len(grp) < 2:
            continue
        uf = {b: b for b in grp}
        def _find(z):
            while uf[z] != z:
                uf[z] = uf[uf[z]]; z = uf[z]
            return z
        for b in grp:                                        # seed with existing touch-connectivity
            for o in adj[b]:
                if o in uf:
                    uf[_find(b)] = _find(o)
        edges = []
        for x in range(len(grp)):
            for y in range(x + 1, len(grp)):
                a, c = grp[x], grp[y]
                gap = min(min(p.distance(geom[c]) for p in _ends(a)),
                          min(q.distance(geom[a]) for q in _ends(c)))
                if gap <= _SAME_NAME_TOL:
                    edges.append((gap, a, c))
        for gap, a, c in sorted(edges):                      # Kruskal: fewest, smallest gap bridges
            if _find(a) != _find(c):
                uf[_find(a)] = _find(c); _link(a, c, gap)

    # Phase 2 — external anchor for each channel (kind, receiver_blk, anchor_geom, gap) or None.
    # ``only`` restricts the search to a single point (the dem-given mouth) instead of both ends.
    # ``reach`` widens the outlet search (a dem-directed CONNECTOR to a nearby FWA/tidal/lake outlet).
    def _anchor(b, only=None, reach=None):
        ch, m = ch_by_blk[b], cls[b]
        r = reach if reach is not None else connect_tol
        ends = (only,) if only is not None else _ends(b)
        if only is None and m.klass in ("duplicate", "extension") and m.fwa_blk in fwa_by_blk:
            return ("fwa", m.fwa_blk, fwa_by_blk[m.fwa_blk].geometry, 0.0)   # it IS FWA — favour FWA
        hint = (ch.overrides.get("connect_to_hint") or "").strip().lower()
        if hint:                                             # explicit source field (not a parsed name)
            named = [c for c in fwa_chains if (c.gnis_name or "").strip().lower() == hint]
            if named:
                bb = min(named, key=lambda c: min(e.distance(c.geometry) for e in ends))
                hd = min(e.distance(bb.geometry) for e in ends)
                if hd <= _HINT_TOL:
                    return ("fwa", str(bb.blk), bb.geometry, hd)
        best = None
        for i in (index.tree.query(geom[b].buffer(r)) if index.tree else []):
            g = index.geoms[int(i)]; d = min(e.distance(g) for e in ends)
            if d <= r and (best is None or d < best[3]):
                best = ("fwa", index.blk[int(i)], g, d)
        for i in (lake_index.tree.query(geom[b].buffer(max(_LAKE_TOL, r))) if lake_index.tree else []):
            poly = lake_index.geoms[int(i)]; d = min(e.distance(poly) for e in ends)
            if d <= max(_LAKE_TOL, r) and (best is None or d < best[3]):
                ms = _lake_mainstem(poly)
                if ms:
                    best = ("fwa", str(ms[0]), ms[1], d)
        if tidal_union is not None:
            d = min(e.distance(tidal_union) for e in ends)
            # A 900- COASTAL FWA reach is open sea like the tidal boundary itself — prefer a clean tidal
            # terminus over a connector to it (Dynamite/Heron drain to Burrard Inlet). Only a real INLAND
            # route (e.g. the 100- Fraser) the stream reaches beats tidal (Sanctuary Slough); and tidal wins
            # when nothing else was found at all.
            bc = fwa_by_blk.get(best[1]) if (best is not None and best[0] == "fwa") else None
            coastal_best = best is None or (bc is not None and trim_wsc(bc.fwa_watershed_code or "").startswith("900"))
            if d <= r and coastal_best and (best is None or d < best[3]):
                best = ("tidal", "", tidal_union, d)
        return best

    anchor = {b: _anchor(b) for b in blks}

    # Phase 2 — least-cost tree to an outlet. Dijkstra seeded at every anchored channel with cost = its
    # anchor gap; an edge costs its connector length (near 0 for touching channels). Each channel's
    # parent is the neighbour on its cheapest path to an outlet — so a channel prefers reaching a good
    # outlet THROUGH a stream it touches over rooting at its own distant anchor (a lake it merely nears).
    INF = float("inf")
    cost = {b: INF for b in blks}
    parent: dict[int, Optional[int]] = {}
    pq = []
    for b in blks:
        a = anchor[b]
        if a is not None:
            cost[b] = a[3]; parent[b] = None; heapq.heappush(pq, (a[3], b))
    while pq:
        c0, x = heapq.heappop(pq)
        if c0 > cost[x]:
            continue
        for y, g in adj[x].items():
            nc = c0 + max(g, 1.0)                            # +1/hop tiebreak toward fewer, shorter links
            if nc < cost[y]:
                cost[y] = nc; parent[y] = x; heapq.heappush(pq, (nc, y))

    receiver: dict[int, tuple[str, str]] = {}
    unresolved: list[str] = []
    for b in blks:
        if cls[b].klass != "novel":                         # duplicates/extensions are FWA, not minted here
            continue
        if cost[b] == INF:                                  # its whole component never reaches the network
            unresolved.append(f"{ch_by_blk[b].source}:{ch_by_blk[b].name or b}")
        elif parent[b] is None:                             # an outlet — its receiver is its external anchor
            kind, rblk, ag, _gap = anchor[b]
            geom[b] = _orient_mouth_first(geom[b], ag)
            receiver[b] = (kind, rblk)
        else:                                               # flows into its parent toward the outlet
            p = parent[b]; mp = cls[p]
            if mp.klass == "extension" and ext_clip and p in ext_clip:
                receiver[b] = ("ext", str(p)); toward = ext_clip[p]   # joins the extension's clip
            elif mp.klass in ("duplicate", "extension"):
                receiver[b] = ("fwa", mp.fwa_blk); toward = fwa_by_blk[mp.fwa_blk].geometry
            else:
                receiver[b] = ("added", str(p)); toward = geom[p]
            geom[b] = _orient_mouth_first(geom[b], toward)

    # DEM FLOW DIRECTION (authoritative): when dem_mouth is supplied, EVERY novel is oriented mouth-first
    # at its dem mouth (so resolved flow == dem-raw), and its receiver is re-derived at that MOUTH end —
    # an external outlet (FWA/lake/tidal) if one is within tol, else the channel it flows INTO there
    # (a novel, or an FWA-duplicate/extension neighbour → that FWA). The Dijkstra receiver above is only
    # the fallback for channels dem left untouched. A grounding pass drops danglers/cycles to unresolved.
    if dem_mouth is not None:
        for b in blks:                                             # orient EVERY novel mouth-first at its
            if cls[b].klass == "novel" and dem_mouth.get(b) is not None:
                geom[b] = _orient_mouth_first(geom[b], dem_mouth[b])   # dem mouth (coords[0] = downstream)
        # APPROVED-LAKE NODES: an approved lake whose under-lake FWA reach is excluded is still a drainage
        # node. Find each lake's OUTFLOW novel (source end at the lake, mouth end away) so an inflow (mouth
        # at the lake) resolves THROUGH the lake to wherever the outflow drains (inflow -> outflow -> FWA).
        lgeoms = lake_index.geoms; ltree = lake_index.tree
        lake_outflow: dict[int, int] = {}
        if ltree is not None:
            for b in blks:
                if cls[b].klass != "novel":
                    continue
                src, mouth = Point(geom[b].coords[-1]), Point(geom[b].coords[0])
                for i in ltree.query(src.buffer(_LAKE_TOL)):
                    poly = lgeoms[int(i)]
                    if src.distance(poly) <= _LAKE_TOL and mouth.distance(poly) > _LAKE_TOL:
                        cur = lake_outflow.get(int(i))
                        if cur is None or geom[b].length > geom[cur].length:
                            lake_outflow[int(i)] = b               # the lake's outflow (prefer the longest)
        def _recv_of_channel(o):                                   # map a downstream CHANNEL to a receiver tuple
            mo = cls[o]
            if mo.klass == "extension" and ext_clip and o in ext_clip:
                return ("ext", str(o))
            if mo.klass in ("duplicate", "extension") and mo.fwa_blk in fwa_by_blk:
                return ("fwa", mo.fwa_blk)                         # a channel that IS an FWA line
            return ("added", str(o))

        dem_receiver: dict[int, tuple[str, str]] = {}
        for b in blks:
            if cls[b].klass != "novel":
                continue
            mouth = Point(geom[b].coords[0])
            op = dem_outlet.get(b) if dem_outlet else None        # b is a component SINK draining to a real
            if op is not None:                                     # FWA/tidal/lake outlet: root it THERE first —
                a = _anchor(b, only=op, reach=connector_tol)      # authoritative OVER dem_down, which for a sink
                if a is not None:                                  # can be a spurious cross-channel cycle (Eagle
                    dem_receiver[b] = (a[0], a[1]); continue       # Creek 154 <-> its short reach: 198<->199)
            dd = dem_down.get(b) if dem_down else None            # else the channel dem routes b's mouth INTO
            if dd is not None and dd != b and dd in ch_by_blk:    # authoritative: mirror the dem tree exactly,
                alt = _touched_receiver_channel(mouth, dd, adj[b], geom)   # but a trib joins the stream it
                tgt = alt if (alt is not None and alt in ch_by_blk) else dd  # TOUCHES, not a farther one dem
                dem_receiver[b] = _recv_of_channel(tgt); continue           # bridged its mouth to (Magnolia Trib 1)
            a = _anchor(b, only=mouth)                             # else an outlet AT the mouth end?
            if a is not None:
                dem_receiver[b] = (a[0], a[1]); continue
            best = None                                            # else the channel it flows INTO at the mouth
            for o in adj[b]:
                if o == b or mouth.distance(geom[o]) > max(_TOUCH_TOL, connect_tol):
                    continue
                if best is None or geom[o].length > best[0]:       # prefer the longer (mainstem) neighbour
                    best = (geom[o].length, _recv_of_channel(o))
            if best is not None:
                dem_receiver[b] = best[1]; continue
            a = _anchor(b, only=mouth, reach=connector_tol)        # a CONNECTOR to a nearby FWA/tidal/lake
            if a is not None:                                      # outlet the mouth points at (dem-directed)
                dem_receiver[b] = (a[0], a[1]); continue
            if ltree is not None:                                  # LAKE NODE: mouth at an approved lake ->
                for i in ltree.query(mouth.buffer(_LAKE_TOL)):     # drain through the lake's outflow novel
                    if mouth.distance(lgeoms[int(i)]) <= _LAKE_TOL:
                        of = lake_outflow.get(int(i))
                        if of is not None and of != b:
                            dem_receiver[b] = ("added", str(of)); break
        receiver = dem_receiver
        unresolved = [b for b in blks if cls[b].klass == "novel" and b not in receiver]
        # GROUND: an "added" receiver is valid only if its chain reaches an external outlet; drop
        # danglers/cycles so no receiver points at a non-minted blk (breaks _merge_by_name / WSC).
        grounded: dict[int, bool] = {}
        def _grounds(b, seen):
            if b in grounded:
                return grounded[b]
            if b not in receiver:
                return False
            k, rb = receiver[b]
            if k != "added":
                grounded[b] = True; return True
            rbi = int(rb)
            g = rbi not in seen and _grounds(rbi, seen | {b})
            grounded[b] = g; return g
        for b in list(receiver):
            if not _grounds(b, set()):
                del receiver[b]; unresolved.append(b)
        unresolved = [f"{ch_by_blk[b].source}:{ch_by_blk[b].name or b}" for b in unresolved]
    return receiver, unresolved


_CONT_PROP = 0.85         # a novel joining above this fraction of an FWA line's length is at its UP-end
_CONT_GAP = 5.0           # ...and a piece merges as a continuation only if it TOUCHES within this
_MAINSTEM_GAP = 120.0     # ...but a SAME-NAME mainstem section that continues at the up-end folds into one
                          # blk across a larger digitizing gap (so a split mainstem is ONE blk, not two + a
                          # connector), bridged as part of the mainstem geometry. Municipal data leaves
                          # ~tens-of-metres gaps where a mainstem crosses a tributary confluence.

_BLUELINE_TRUNC_TOL = 1.0     # a receiver whose loaded chain starts more than this far up its blue line
                              # (mouth_measure) is only PARTIALLY loaded — its length_m understates the true
                              # blue-line length, so proj/length_m mis-mints (see _blue_line_total).
_CHILD_TOUCH_TOL = 30.0       # an FWA child must touch its parent blue line within this to fix the scale


_SHORT_TRIB_M = 50.0      # drop a SHORT leaf tributary of another added stream (a stub with nothing flowing
                          # into it); iterated so a stream left with only pruned inflows becomes prunable too.
_OVERSHOOT_TOL = 40.0     # walk a channel mouth that overshoots PAST its receiver back to the crossing when
                          # the overshoot is at most this; a larger gap is a genuine connector, left alone.


def _prune_short_leaf_tribs(minted, receiver, geom, max_len: float = _SHORT_TRIB_M):
    """Filter out short municipal stub tributaries: a stream is dropped when it flows into ANOTHER ADDED
    stream, is at most ``max_len`` long, and has nothing (still kept) flowing into it. Iterated, so a short
    stream whose only tributaries were themselves pruned becomes a leaf and is pruned in turn (Buena Vista
    Trib.3, 48 m, and its like). Mainstems, FWA/tidal-attached streams, and anything with a surviving
    tributary are kept. Returns the surviving ``minted`` list."""
    kept = {ch.blk for ch in minted}
    changed = True
    while changed:
        changed = False
        has_inflow = set()
        for ch in minted:
            if ch.blk not in kept:
                continue
            k, rb = receiver[ch.blk]
            if k == "added" and int(rb) in kept:
                has_inflow.add(int(rb))
        for ch in minted:
            if ch.blk not in kept:
                continue
            k, rb = receiver[ch.blk]
            if k == "added" and geom[ch.blk].length <= max_len and ch.blk not in has_inflow:
                kept.discard(ch.blk); changed = True
    return [ch for ch in minted if ch.blk in kept]


def _clip_receiver_overshoot(line: LineString, rgeom) -> LineString:
    """Walk a channel mouth back to its receiver when it OVERSHOOTS. A municipal line drawn a few m past the
    river it drains into crosses ``rgeom`` then dangles beyond it, so the mouth sits on the far side and the
    connector doubles BACK to the river (the Brunette screenshot). If the mouth-side of the line crosses the
    receiver within ``_OVERSHOOT_TOL``, trim that overshoot so the mouth lands ON the crossing (a zero-gap
    confluence). A mouth already on the receiver, a non-crossing gap (a real connector), or an overshoot
    longer than the tol is left unchanged. ``line`` is mouth-first; the result stays mouth-first."""
    if Point(line.coords[0]).distance(rgeom) <= _CONFLUENCE_GAP:
        return line                                          # already touching — nothing to trim
    inter = line.intersection(rgeom)
    if inter.is_empty:
        return line                                          # doesn't cross — a genuine gap (real connector)
    xs = ([inter] if inter.geom_type == "Point"
          else [g for g in getattr(inter, "geoms", []) if g.geom_type == "Point"])
    if not xs:
        return line                                          # overlapping/parallel — not an overshoot
    d = min(line.project(p) for p in xs)                     # first crossing from the mouth
    if not (0.0 < d <= _OVERSHOOT_TOL):
        return line                                          # crossing at the mouth (0) or too far up — leave it
    return substring(line, d, line.length)                   # drop the mouth-side overshoot [0, d]


def _keep_novel(name: str, members, dem_kept_src) -> bool:
    """Keep a novel channel iff the DEM kept at least one of its member features — dropping loops/braids the
    DEM eliminated (every feature was a loop-closer it removed), not just nameless noise. So a NAMED braid
    that is gone in dem-raw is gone in resolved too (Little Stawamus side channels). Without a DEM pass
    (``dem_kept_src is None``, hermetic tests) keep it iff it is named."""
    if dem_kept_src is None:
        return bool((name or "").strip())
    return any(s in dem_kept_src for s in members)


def _touched_receiver_channel(mouth: Point, dd, cand, geom) -> Optional[int]:
    """A tributary joins the stream it PHYSICALLY touches. If the DEM routed a mouth into a channel ``dd`` it
    does NOT touch (needs a connector, > _TOUCH_TOL away) while the mouth sits ON another candidate channel
    (<= _CONFLUENCE_GAP), return that touched channel so the trib nests under it — Magnolia Trib 1 joins
    Magnolia Creek 24 m above the Magnolia -> Little Stawamus confluence, so the DEM bridged its mouth
    straight to Little Stawamus (the near-mouth T was inside the noding end_buf). Else None (keep ``dd``)."""
    if geom[dd].distance(mouth) <= _TOUCH_TOL:
        return None                                    # dem's receiver is already touched — nothing to prefer
    best = min((o for o in cand if o != dd), key=lambda o: geom[o].distance(mouth), default=None)
    if best is not None and geom[best].distance(mouth) <= _CONFLUENCE_GAP:
        return best
    return None


def _mouth_end(e0: Point, e1: Point, own_mouths: list, global_dist) -> Point:
    """Which END of a merged channel is its dem mouth: the endpoint nearest one of the channel's OWN reach
    mouths. Scoping to own reaches stops a FOREIGN creek whose mouth touches this channel's HEADWATER from
    flipping it (the Stoney Creek reversal — a trib mouth landed exactly on Stoney's high end, so a global
    scan chose the wrong end). A channel with none of its own reaches falls back to the global nearest-mouth
    distance (unchanged behaviour)."""
    if own_mouths:
        d = lambda p: min(p.distance(q) for q in own_mouths)
        return e0 if d(e0) <= d(e1) else e1
    return e0 if global_dist(e0) <= global_dist(e1) else e1


def _blue_line_total(rc: BlkChain, fwa_chains: list[BlkChain]) -> Optional[float]:
    """Effective full route-length of ``rc``'s blue line, recovered from its already-coded FWA children.

    Our FWA extract is regional, so a big river's blue line is only partially loaded: ``rc.length_m`` (the
    loaded span) understates the true length, and minting a tributary's segment as proj/length_m over-counts
    (Sanctuary Slough got 100-567200 on the Fraser, whose real FWA neighbours sit near 100-0123xx). But every
    existing FWA tributary carries its OWN local code = (its route measure up rc) / (rc's TRUE total length);
    inverting that over rc's touching DIRECT children and taking the median recovers the true total —
    name-agnostic, using only loaded data. ``None`` when rc has no usable coded children (mint falls back to
    length_m)."""
    base = trim_wsc(rc.fwa_watershed_code)
    if rc.geometry is None or not base:
        return None
    g = rc.geometry
    ests: list[float] = []
    for c in fwa_chains:
        cw = trim_wsc(c.fwa_watershed_code)
        if c.geometry is None or not cw.startswith(base + "-"):
            continue
        tail = cw[len(base) + 1:]
        if "-" in tail or not tail.isdigit():
            continue                                       # a DIRECT child only (one segment beyond rc)
        seg = int(tail)
        if seg <= 0:
            continue
        m0, m1 = Point(c.geometry.coords[0]), Point(c.geometry.coords[-1])
        mouth = m0 if m0.distance(g) <= m1.distance(g) else m1
        if mouth.distance(g) > _CHILD_TOUCH_TOL:
            continue                                       # mouth doesn't touch rc (a clip/grandchild off-line)
        ests.append((rc.mouth_measure + g.project(mouth)) / (seg / 1e6))
    if not ests:
        return None
    ests.sort()
    return ests[len(ests) // 2]                            # median (robust to a stray off-line child)


def _canon_name(s: str) -> str:
    """Canonical name key: lowercase, punctuation/space stripped ("Logger's Lane" == "loggerslane")."""
    import re
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _deloop(coords: list, tol: float = 12.0, max_loop: float = 400.0) -> list:
    """Remove small self-loops from a stitched line: when the path returns within ``tol`` metres of an
    earlier vertex (and the intervening excursion is shorter than ``max_loop``), snap back to that vertex,
    dropping the loop. Kills the little triangles left when the trunk traverses overlapping/duplicate
    source fragments near a junction. A real river's meanders stay well apart, so genuine geometry is
    kept; only true crossings under ``max_loop`` are cut."""
    from math import hypot
    out = [tuple(coords[0])]
    for p in coords[1:]:
        p = tuple(p)
        cut = None
        for j in range(len(out) - 2, -1, -1):               # nearest earlier vertex (scan back)
            if hypot(p[0] - out[j][0], p[1] - out[j][1]) <= tol:
                loop_len = sum(hypot(out[k + 1][0] - out[k][0], out[k + 1][1] - out[k][1])
                               for k in range(j, len(out) - 1))
                if loop_len <= max_loop:
                    cut = j
                break
        if cut is not None:
            del out[cut + 1:]                               # snap back, dropping the excursion
        else:
            out.append(p)
    return out


def _deloop_line(g: LineString) -> Optional[LineString]:
    """De-loop a LineString; returns None if it collapses to < 2 points (a degenerate all-loop fragment
    that should just be dropped)."""
    dl = _deloop(list(g.coords))
    return LineString(dl) if len(dl) >= 2 else None


def _is_name_tributary(child: str, parent: str) -> bool:
    """True when ``child``'s name marks it a tributary OF ``parent`` (e.g. "X Trib 2" / "X Tributary" /
    "X Branch" is a tributary of "X"). Used to enforce flow direction from the names: a mainstem must
    never flow INTO its own tributary."""
    c, p = _canon_name(child), _canon_name(parent)
    if not p or c == p or not c.startswith(p):
        return False
    return c[len(p):].startswith(("trib", "tributary", "branch", "fork"))


def _merge_by_name(minted, receiver, geom, ch_by_blk_all):
    """Merge same-name pieces into ONE named stream = one blk = one WSC (FWA's model: one blue line,
    many fids). Pieces of the same name that chain together (a piece whose receiver is another piece of
    the SAME name) are folded onto their downstream-most member; its LONGEST path (mouth->source) becomes
    the trunk geometry (for tributary projection + wsc) and the rest ride along as extra SEGMENTS of the
    same blk. So a fragmented trunk collapses to one wsc (900-102882-XXXXXX), same-name braids share that
    wsc, and their internal connectors vanish (same blk — no uphill edge). Mutates ``geom`` (rep gets the
    trunk line); returns (minted, receiver, folded, extra_geoms[rep]=[folded piece geoms as segments])."""
    from pipeline.added_streams.merge import _greedy_chain
    blks = {ch.blk for ch in minted}
    ch_by_blk = {ch.blk: ch for ch in minted}

    import re
    def nm(b):                                              # canonical: ignore punctuation ("Logger's"=="Loggers")
        return re.sub(r"[^a-z0-9]", "", (ch_by_blk_all[b].name or "").lower())

    def cont(b):                                             # (proportion, gap) of b's mouth on receiver
        k, rb = receiver[b]
        if k != "added" or int(rb) not in geom or geom[int(rb)].length <= 0:
            return (-1.0, 1e18)
        rg = geom[int(rb)]; mouth = Point(geom[b].coords[0])
        return (rg.project(mouth) / rg.length, mouth.distance(rg))

    uf = {b: b for b in blks}
    def find(x):
        while uf[x] != x:
            uf[x] = uf[uf[x]]; x = uf[x]
        return x
    # Fold ONLY genuine collinear UP-END continuations into one blk (a creek digitised as a few in-line
    # sections). A same-name BRAID/SLOUGH that rejoins mid-line is NOT folded — it keeps its own blk and
    # is tied to the mainstem's wsc later (same name => same wsc, different blk; the user's model). This
    # also keeps `_longest`+append from ever stitching a straight jump across a fork.
    same_parent = {}                                         # child -> the receiver it continues at up-end
    for b in blks:
        k, rb = receiver[b]
        if k != "added" or int(rb) not in blks:
            continue
        name_ok = (not nm(b)) or (not nm(int(rb))) or nm(b) == nm(int(rb))   # same-name or an unnamed frag
        ci = cont(b)
        gap_tol = _MAINSTEM_GAP if nm(b) and nm(b) == nm(int(rb)) else _CONT_GAP   # mainstem folds wider
        touches_upend = ci[0] >= _CONT_PROP and ci[1] <= gap_tol      # a real end-to-end continuation
        if touches_upend and name_ok:
            same_parent[b] = int(rb); uf[find(b)] = find(int(rb))
    groups: dict[int, list[int]] = {}
    for b in blks:
        groups.setdefault(find(b), []).append(b)

    rep_of = {}                                              # group -> its downstream-most member (mouth)
    for g, members in groups.items():
        ms = set(members)
        rep_of[g] = next((b for b in members if receiver[b][0] != "added"
                          or int(receiver[b][1]) not in ms), members[0])
    children: dict[int, list[int]] = {}
    for c, p in same_parent.items():
        children.setdefault(p, []).append(c)

    def _longest(b):                                         # longest path (blks) from b upstream by length
        best, best_len = [b], geom[b].length
        for c in children.get(b, ()):
            sub = _longest(c)
            ln = geom[b].length + sum(geom[x].length for x in sub)
            if ln > best_len:
                best_len, best = ln, [b] + sub
        return best

    new_receiver, keep, extra_geoms, folded = {}, [], {}, set()
    for g, members in groups.items():
        rep = rep_of[g]
        if len(members) > 1:
            trunk = _longest(rep)                            # mouth->source main path -> the trunk geometry
            coords = list(geom[rep].coords)                  # rep is mouth-first; KEEP mouth at coords[0]
            extras = [geom[b] for b in members if b not in set(trunk)]
            for i, b in enumerate(trunk[1:], 1):
                bc = list(geom[b].coords)
                if Point(bc[-1]).distance(Point(coords[-1])) < Point(bc[0]).distance(Point(coords[-1])):
                    bc = bc[::-1]                            # connect its near end to the growing up-end
                if Point(bc[0]).distance(Point(coords[-1])) > _MAINSTEM_GAP:   # doesn't continue up-end:
                    extras += [geom[x] for x in trunk[i:]]  # never fabricate a big jump — ride as segments
                    break
                coords += bc[1:] if coords[-1] == bc[0] else bc
            geom[rep] = _deloop_line(LineString(coords)) or LineString(coords)
            extra_geoms[rep] = [x for x in (_deloop_line(e) for e in extras) if x is not None]
            folded |= {b for b in members if b != rep}
        else:
            geom[rep] = _deloop_line(geom[rep]) or geom[rep]          # single novels: de-loop too
        new_receiver[rep] = receiver[rep]
        keep.append(ch_by_blk[rep])
    for ch in keep:                                          # re-point tributaries onto the kept trunk blk
        k, rb = new_receiver[ch.blk]
        if k == "added":
            new_receiver[ch.blk] = (k, str(rep_of[find(int(rb))]))
    return keep, new_receiver, folded, extra_geoms


def _name_candidate(muni_name: str, fwa_name: str, fwa_blk: str, fwa_gnis: str) -> Optional[NameCandidate]:
    muni = (muni_name or "").strip()
    if not muni or (fwa_name and fwa_name.strip().lower() == muni.lower()):
        return None
    return NameCandidate(target_gnis=fwa_gnis, target_blk=fwa_blk, name=muni,
                         display=not fwa_name, conflict=bool(fwa_name))


# FWA reaches the municipal geojson supersedes: wsc-code PREFIXES to drop (the reach + everything
# upstream). The muni lines there then classify as novels and mint their own codes. Keyed by source.
FWA_EXCLUDE_BY_SOURCE: dict[str, tuple] = {
    "squamish": ("900-105574-087851", "900-103611", "900-102882-190726"),  # each reach + all tribs -> municipal
    "burnaby": ("100-019698-",),                 # everything UPSTREAM of Brunette River (Deer + Burnaby
    #                                              Lake, Still/Stoney/etc.) -> municipal version + lake-node
    #                                              graph. Trailing '-' keeps Brunette River (100-019698) itself.
}

# Only these lakes (by GNIS name) form drainage NODES per source — every stream touching one connects to
# it and drains through its outflow (user: burnaby is only Deer + Burnaby Lake). All OTHER lakes (small
# unnamed ponds, far hill lakes) are ignored for node-forming so they can never merge unrelated creeks
# (the squamish Little Stawamus / Valleycliffe over-merge). Under-lake fid tagging still uses every lake.
APPROVED_LAKE_NAMES_BY_SOURCE: dict[str, tuple] = {
    "burnaby": ("Deer Lake", "Burnaby Lake"),
}

# Sources whose OWN municipal line direction is trustworthy (drawn upstream-first). ALL sources are now
# DEM-oriented: burnaby was trusted only because its line direction was cleaner, but T-junction noding +
# the headwater loop-closer fix made DEM correct for its tricky cases (e.g. Ramsay Trib.6 drawn backwards),
# so DEM gives the right flow AND source markers there too. Shared by build() and mapcheck so the dataset
# matches the validation view. (trust_source is still supported per-source; the set is just empty for now.)
RELIABLE_SOURCES: set = set()


def approved_lake_polys(gpkg, bbox, source: str) -> list:
    """FWA lake/manmade polygons (EPSG:3005) whose GNIS name is approved for ``source`` as a drainage
    node. Empty when the source has no approved lakes (squamish/port_moody) — no lake nodes are formed."""
    names = {n.strip().lower() for n in APPROVED_LAKE_NAMES_BY_SOURCE.get(source, ())}
    if not names:
        return []
    from data.data_extractor import FWADataAccessor
    fwa = FWADataAccessor(gpkg)
    out = []
    for lyr in ("lakes", "manmade"):
        gdf = fwa.get_layer(lyr, columns=["GNIS_NAME_1", "GNIS_NAME_2", "geometry"], bbox=bbox)
        for g, n1, n2 in zip(gdf.geometry, gdf["GNIS_NAME_1"], gdf["GNIS_NAME_2"]):
            if g is not None and {str(n1 or "").strip().lower(),
                                  str(n2 or "").strip().lower()} & names:
                out.append(g)
    return out


def resolve_and_mint(features: list[dict], fwa_chains: list[BlkChain], lake_index: LakeIndex,
                     tidal_geoms: list, *, tol: float = 30.0, connect_tol: float = _CONNECT_TOL,
                     drop_unconfirmed: bool = True, exclude_wsc: tuple = (),
                     orient_sampler=None, trust_source: bool = False,
                     approved_lakes: Optional[list] = None
                     ) -> tuple[list[dict], list[NameCandidate], dict]:
    """Pure core. Returns (stream records, name candidates, report). Raises ValueError on a
    propagation violation (so a bad dataset is never written).

    ``exclude_wsc``: FWA watershed-code PREFIXES to drop before matching — a stream whose (trimmed) wsc
    starts with any prefix is removed along with everything upstream of it (its tributaries share the
    prefix). Used to delete FWA reaches the municipal geojson supersedes (the muni lines there then
    classify as novels and mint their own codes) — e.g. Squamish drops ``900-105574-087851``."""
    if exclude_wsc:
        fwa_chains = [c for c in fwa_chains
                      if not any(trim_wsc(c.fwa_watershed_code).startswith(p) for p in exclude_wsc)]
    if drop_unconfirmed:      # e.g. Squamish 'Unconfirmed' watercourses — too noisy to mint
        features = [f for f in features
                    if not str(f.get("properties", {}).get("ftype", "")).lower().startswith("unconfirmed")]
    channels = merge_channels(features, mint_missing_blk=True, gap_tol_m=_GAP_TOL_M)
    geom = {ch.blk: _albers(ch.geometry) for ch in channels}
    ch_by_blk = {ch.blk: ch for ch in channels}
    index = _FwaIndex(fwa_chains)
    fwa_by_blk = {str(c.blk): c for c in fwa_chains}
    coastal_streams = [(Point(c.geometry.coords[0]), c.fwa_watershed_code)
                       for c in fwa_chains if coastal.is_coastal_wsc(c.fwa_watershed_code)]
    tidal_union = unary_union(tidal_geoms) if tidal_geoms else None

    # 1. classify (name-aware: a same-named FWA stream nearby is a duplicate even if offset)
    cls = {ch.blk: classify(geom[ch.blk], index, tol=tol, muni_name=ch.name) for ch in channels}
    candidates: list[NameCandidate] = []
    for ch in channels:
        m = cls[ch.blk]
        if m.klass in ("duplicate", "extension"):
            cand = _name_candidate(ch.name, m.fwa_name, m.fwa_blk,
                                   fwa_by_blk[m.fwa_blk].gnis_id if m.fwa_blk in fwa_by_blk else "")
            if cand:
                candidates.append(cand)

    # Nameless novels are dropped UNLESS dem kept them: dem already peels unnamed stubs and drops pure-
    # unnamed components, so a nameless piece dem KEPT is a real connector/outlet reach inside a NAMED
    # drainage — it must be minted or the named streams above it cannot reach their outlet (the port_moody
    # Kyle Creek bug: its component's outlet reach is unnamed). The dem-aware drop is applied AFTER the dem
    # pass below (which needs to see every piece); here we keep them all. Without a dem pass (hermetic
    # tests) the fallback drop still removes all nameless novels.
    novels = [ch for ch in channels if cls[ch.blk].klass == "novel"]
    extensions = [ch for ch in channels if cls[ch.blk].klass == "extension"]

    # An extension continues its FWA blue line PAST the line's end. Pre-compute each extension's clip
    # (oriented mouth-first at the FWA end) so that a novel joining the extension attaches to the CLIP —
    # part of the extended blue line — not to a far point on the original FWA centreline.
    ext_meta: dict[int, dict] = {}
    ext_clip: dict[int, LineString] = {}
    for ch in extensions:
        m = cls[ch.blk]; fwa = fwa_by_blk[m.fwa_blk]
        clip = _orient_mouth_first(m.clip3005, fwa.geometry)
        ext_clip[ch.blk] = clip
        ext_meta[ch.blk] = {"clip": clip, "fwa_blk": m.fwa_blk, "fwa_wsc": m.fwa_wsc,
                            "base_measure": fwa.mouth_measure + fwa.length_m, "fwa": fwa}

    # 2. resolve receiver — TOPOLOGY FIRST (dataset-agnostic). Build the municipal connection graph from
    #    geometry, root each connected component at its outlet(s) (where it meets FWA / a lake / tidal),
    #    and orient the tree so leaves are headwaters flowing to the outlet. Duplicates/extensions are
    #    FWA (favour FWA) and serve only as topology anchors; novels keep their whole geometry (no trim).
    # Flow direction from dem_flow (opt-in): accepted-raw for reliable sources (trust_source), else
    # dem-corrected. Run dem on the SAME per-FEATURE geometry the dem-raw view uses (not the merged
    # channels — a channel's stitched vertex order can break the trust_source convention), so resolved
    # flow matches dem-raw exactly. Each channel's mouth = the channel endpoint that coincides with a
    # feature dem-mouth (the outlet feature's downstream end); the other end is the headwater.
    # APPROVED lakes only form drainage NODES (user: burnaby is only Deer + Burnaby Lake). Every other
    # lake polygon (small unnamed ponds, far hill lakes) must NOT merge unrelated creeks — that caused the
    # squamish Little Stawamus / Valleycliffe over-merge. The full lake_index still tags under-lake fids.
    approved_lake_index = LakeIndex([(g, "") for g in approved_lakes]) if approved_lakes else LakeIndex([])
    dem_mouth = None
    dem_down = None
    dem_outlet = None
    if orient_sampler is not None:
        from pipeline.added_streams.dem import dem_flow
        fwa_out = [c.geometry for c in fwa_chains if c.geometry is not None]   # kept FWA (exclusions already
        dem_out, _, _ = dem_flow(features, orient_sampler, trust_source=trust_source,  # applied) + tidal so dem
                                 lakes=approved_lakes, fwa=fwa_out, tidal=tidal_union)  # computes outlet connectors
        def _reaches(v):                                            # a T-noded feature has >1 reach (an apex-
            return v["reaches"] if v.get("reaches") else [v]       # draining loop's two arms); each carries its
        #                                                            own {coords, down, outlet} — treat each as a flow unit
        mouth_pts = [Point(_TO_ALBERS.transform(*r["coords"][0])) for v in dem_out.values() for r in _reaches(v)]
        mtree = STRtree(mouth_pts) if mouth_pts else None
        def _dist_to_mouth(p):
            if mtree is None:
                return 1e18
            hits = [mouth_pts[int(j)] for j in mtree.query(p.buffer(50.0))]
            return min((p.distance(q) for q in hits), default=1e18)
        # each channel's OWN reach mouths (via merge provenance), so the mouth-end pick is scoped to this
        # channel — a foreign trib whose mouth touches this channel's headwater can't flip it (Stoney Creek).
        src_to_blk: dict = {}                                   # EXACT feature -> channel via merge provenance
        for ch in channels:                                     # (geometry-nearest mis-maps a trib to the
            for sid in ch.members:                              #  mainstem it runs beside -> its `down` looked
                src_to_blk[sid] = ch.blk                        #  internal and it never got a receiver)
        own_mouths: dict = {}
        for i in dem_out:
            b = src_to_blk.get(features[i].get("properties", {}).get("src_id"))
            for r in _reaches(dem_out[i]):
                own_mouths.setdefault(b, []).append(Point(_TO_ALBERS.transform(*r["coords"][0])))
        dem_mouth = {}
        for ch in channels:
            c = list(geom[ch.blk].coords)
            dem_mouth[ch.blk] = _mouth_end(Point(c[0]), Point(c[-1]),
                                           own_mouths.get(ch.blk, []), _dist_to_mouth)

        # DEM TREE -> per-channel downstream receiver. dem_flow already routes every piece to its
        # component sink (good direction + bridges); mirror that exactly: map each dem FEATURE to the
        # channel that carries it (nearest channel geometry), then each channel's dem-downstream = the
        # channel its mouth feature flows INTO (`down`). This replaces the resolver's own longest-neighbour
        # guess, so resolved flow == dem-raw. A channel whose mouth feature has no `down` drains to an
        # external outlet (handled by the anchor fallback in _resolve_topology). `src_to_blk` (built above
        # for own_mouths) maps each EXACT feature to its channel via merge provenance — geometry-nearest
        # would mis-map a trib to the mainstem it runs beside, so its `down` looked internal and it never
        # got a receiver.
        fblk = {i: src_to_blk.get(features[i].get("properties", {}).get("src_id")) for i in dem_out}
        cand: dict[int, tuple] = {}                              # channel -> (dist-of-exit-feature, recv chan)
        dem_outlet: dict[int, Point] = {}                        # channel -> dem's outlet POINT (on FWA/tidal)
        for i in dem_out:
            b = fblk.get(i)
            for r in _reaches(dem_out[i]):                       # each reach of feature i is a flow unit
                d, ot = r.get("down"), r.get("outlet")
                if b is not None and ot is not None:            # this reach is its component's sink: it
                    dem_outlet[b] = Point(_TO_ALBERS.transform(*ot))  # drains to dem's outlet (grounds the chain)
                if b is None or d is None:
                    continue
                rb = fblk.get(d)
                if rb is None or rb == b:                        # `down` stays inside the same channel
                    continue
                fm = Point(_TO_ALBERS.transform(*r["coords"][0]))   # this reach's mouth
                dist = fm.distance(dem_mouth[b]) if b in dem_mouth else 0.0
                if b not in cand or dist < cand[b][0]:          # the mouth-most exit wins
                    cand[b] = (dist, rb)
        dem_down = {b: v[1] for b, v in cand.items()}

    # dem-aware novel drop (deferred from above): keep a novel only if dem kept any of its features (a real
    # connector/outlet reach). This drops loop/braid channels the dem eliminated — even NAMED ones, so a
    # braid gone in dem-raw is gone in resolved too — as well as nameless noise.
    dem_kept_src = ({features[i].get("properties", {}).get("src_id") for i in dem_out}
                    if orient_sampler is not None else None)
    def _keep(ch):
        if cls[ch.blk].klass != "novel":
            return True
        return _keep_novel(ch.name, ch.members, dem_kept_src)
    channels = [ch for ch in channels if _keep(ch)]
    novels = [ch for ch in channels if cls[ch.blk].klass == "novel"]

    receiver, unresolved = _resolve_topology(channels, geom, cls, index, fwa_by_blk, fwa_chains,
                                             approved_lake_index, tidal_union, connect_tol, ext_clip,
                                             dem_mouth, dem_down=dem_down, dem_outlet=dem_outlet)

    # Flow direction from the NAMES: a mainstem must never flow INTO its own tributary. If topology routed
    # a piece named "X" into a piece named "X Trib N" (e.g. a Little Stawamus mainstem section rooting
    # through Little Stawamus Creek Trib 1), bypass the trib so the piece flows past it toward the outlet
    # the trib itself drains to. Run BEFORE the name-merge so a mainstem section then resolves to the
    # mainstem (not its trib) and folds into one blk. Iterated for trib chains; tree stays acyclic.
    def _bypass_own_tribs():
        for _ in range(len(receiver) + 1):
            changed = False
            for b in list(receiver):
                kind, rb = receiver[b]
                if kind == "added" and int(rb) in ch_by_blk \
                        and _is_name_tributary(ch_by_blk[int(rb)].name, ch_by_blk[b].name):
                    receiver[b] = receiver[int(rb)]      # flow past my own tributary, not into it
                    changed = True
            if not changed:
                break
    _bypass_own_tribs()

    minted = [ch for ch in novels if ch.blk in receiver]
    minted, receiver, folded, extra_geoms = _merge_by_name(minted, receiver, geom, ch_by_blk)

    # a novel that TOUCHES an FWA line at its UP-end continues that blue line — reclassify it as an
    # extension so it (and its tributaries) nest under the FWA wsc directly, never piling up at 999999.
    # Only when it genuinely touches (<= connect_tol): a hint-anchored stream sitting far from the FWA's
    # source is left as-is — turning it into an extension would sprout a long fabricated connector.
    for ch in list(minted):
        kind, rb = receiver[ch.blk]
        if kind != "fwa" or rb not in fwa_by_blk:
            continue
        fwa = fwa_by_blk[rb]; g = geom[ch.blk]; mouth = Point(g.coords[0])
        if not fwa.length_m:
            continue
        prop = fwa.geometry.project(mouth) / fwa.length_m    # match mint_wsc's measure (rc.length_m)
        if prop >= _CONT_PROP and mouth.distance(fwa.geometry) <= connect_tol:
            ext_meta[ch.blk] = {"clip": g, "fwa_blk": rb, "fwa_wsc": fwa.fwa_watershed_code,
                                "base_measure": fwa.mouth_measure + fwa.length_m, "fwa": fwa}
            extensions.append(ch); minted.remove(ch)
            for other in minted:                            # its tributaries now join the extension clip
                k2, rb2 = receiver[other.blk]
                if k2 == "added" and int(rb2) == ch.blk:
                    receiver[other.blk] = ("ext", str(ch.blk))
    _bypass_own_tribs()                              # again: reclassification may have re-pointed receivers
    minted = _prune_short_leaf_tribs(minted, receiver, geom)   # drop short municipal stub tributaries
    kept_blks = {ch.blk for ch in minted}

    # 3. topo order (receiver-first over added edges) for top-down WSC
    order_topo = _toposort(minted, receiver)
    wsc_of: dict[int, str] = {}
    proj_of: dict[int, float] = {}
    rlen_of: dict[int, float] = {}
    connector_of: dict[int, Optional[dict]] = {}
    used_coastal: set[str] = set()
    bl_total: dict[str, float] = {}                               # FWA blk -> true blue-line length (cached)
    add_geom_wsc: dict[str, tuple[LineString, str, float]] = {}   # added blk -> (geom, wsc, length)
    for ch in order_topo:
        kind, rblk = receiver[ch.blk]
        line = geom[ch.blk]
        mouth = Point(line.coords[0])
        if kind == "tidal":
            wsc = coastal.estimate_coastal_wsc(mouth, coastal_streams, used_coastal)
            proj_of[ch.blk] = 0.0; connector_of[ch.blk] = None
        else:
            to_blk = rblk
            if kind == "fwa":
                rc = fwa_by_blk[rblk]; rgeom, rwsc, rmm, rlen = (rc.geometry, rc.fwa_watershed_code,
                                                                 rc.mouth_measure, rc.length_m)
            elif kind == "ext":
                # an extension IS the FWA blue line continued past its end, so a tributary attaches to the
                # WHOLE line (FWA + clip) and connects to the NEAREST point — not always the clip, which
                # can be far up (the 218 m Logger's Lane Trib 2 connector reached the extension when the
                # orange FWA was 60 m away). Build FWA(mouth->up) + clip so the near point wins.
                em = ext_meta[int(rblk)]; clip = em["clip"]; attach = Point(clip.coords[0])
                fc = list(em["fwa"].geometry.coords)
                if Point(fc[0]).distance(attach) < Point(fc[-1]).distance(attach):
                    fc = fc[::-1]                                # FWA oriented mouth-first (clip end last)
                rgeom = LineString(fc + list(clip.coords)[1:])
                rwsc = em["fwa_wsc"]; rmm = em["fwa"].mouth_measure; rlen = rgeom.length
                to_blk = em["fwa_blk"]
            else:
                rgeom, rwsc, rlen = add_geom_wsc[rblk]; rmm = 0.0
            clipped = _clip_receiver_overshoot(line, rgeom)   # a mouth that overshot PAST the receiver is
            if clipped is not line:                           # walked back to the crossing (no backwards
                geom[ch.blk] = line = clipped                 # connector); the trimmed geometry is the output
                mouth = Point(line.coords[0])
            proj = rgeom.project(mouth); confl = rgeom.interpolate(proj)
            mint_d, mint_len = proj, rlen
            if kind == "fwa" and rmm > _BLUELINE_TRUNC_TOL:       # partially-loaded big river: mint against the
                total = bl_total.get(rblk)                        # ABSOLUTE route measure over the TRUE blue-line
                if total is None:                                 # length (rlen is only the loaded span)
                    total = bl_total[rblk] = _blue_line_total(fwa_by_blk[rblk], fwa_chains) or rlen
                mint_d, mint_len = rmm + proj, total
            wsc = str(ch.overrides.get("wsc") or mint_wsc(rwsc, mint_d, mint_len))
            proj_of[ch.blk] = proj; rlen_of[ch.blk] = rlen
            gap = mouth.distance(confl)
            connector_of[ch.blk] = {"to_blk": to_blk, "to_fwa": kind in ("fwa", "ext"),
                                    "at_measure": rmm + proj, "x": confl.x, "y": confl.y,
                                    "kind": "confluence" if gap <= _CONFLUENCE_GAP else "connector"}
        wsc_of[ch.blk] = wsc
        add_geom_wsc[str(ch.blk)] = (line, wsc, line.length)

    # same name => same wsc. Collapse each same-name lineage to its SENIOR code (the shallowest same-name
    # code that is a prefix-ancestor — a braid/slough is a different blk on the same blue-line lineage;
    # extensions seed their FWA code as the anchor). `pin[b]` = the senior for a NON-anchor member (one that
    # collapses strictly UP); anchors and unique-name tributaries are left to re-mint. Namesakes in another
    # watershed mint DIVERGENT codes (no ancestor relation) and stay distinct — the primary is never
    # overwritten.
    anchors: dict[str, list[str]] = {}                           # canonical name -> candidate senior wscs
    for ch in extensions:
        cn = _canon_name(ch.name)
        if cn:
            anchors.setdefault(cn, []).append(ext_meta[ch.blk]["fwa_wsc"])
    for b in wsc_of:
        cn = _canon_name(ch_by_blk[b].name)
        if cn:
            anchors.setdefault(cn, []).append(wsc_of[b])
    pin: dict[int, str] = {}
    for b in wsc_of:
        cn = _canon_name(ch_by_blk[b].name)
        tw = trim_wsc(wsc_of[b])
        seniors = [w for w in anchors.get(cn, ()) if tw.startswith(trim_wsc(w))]   # my lineage ancestors
        senior = min(seniors, key=lambda w: len(trim_wsc(w))) if seniors else wsc_of[b]
        if trim_wsc(senior) != tw and tw.startswith(trim_wsc(senior)):            # collapses strictly UP
            pin[b] = senior
    # same-name SIBLINGS under the same parent (a fragmented tributary that joins the mainstem at a couple
    # of nearby points) share ONE code — the mouth-most (shortest, then lexicographically smallest). Keyed
    # by (name, parent) so namesakes in a different drainage/parent stay distinct.
    sib: dict[tuple, list[int]] = {}
    for b in wsc_of:
        cn = _canon_name(ch_by_blk[b].name)
        if not cn:
            continue
        tw = trim_wsc(pin.get(b, wsc_of[b]))
        parent = tw.rsplit("-", 1)[0] if "-" in tw else tw
        sib.setdefault((cn, parent), []).append(b)
    for bs in sib.values():
        if len(bs) > 1:
            shared = min((pin.get(b, wsc_of[b]) for b in bs),
                         key=lambda w: (len(trim_wsc(w)), trim_wsc(w)))
            for b in bs:
                pin[b] = shared

    # Re-mint in receiver-first order so the collapse PROPAGATES: a pinned same-name piece takes the senior
    # code; every other added stream re-mints off its receiver's (now-collapsed) code — so a tributary that
    # flows straight into the mainstem lands exactly ONE level below it, not several (its old receiver was a
    # deep braid code that has since collapsed). FWA/ext/tidal receivers are fixed, so those keep their code.
    for ch in order_topo:
        b = ch.blk
        if b in pin:
            wsc_of[b] = pin[b]
        else:
            kind, rblk = receiver[b]
            if kind == "added":
                wsc_of[b] = str(ch.overrides.get("wsc")
                                or mint_wsc(wsc_of[int(rblk)], proj_of[b], rlen_of[b]))
        add_geom_wsc[str(b)] = (geom[b], wsc_of[b], geom[b].length)

    # 4. Strahler order + Shreve magnitude over the novel added network (leaves first)
    order_v, mag_v = _order_magnitude(minted, receiver, proj_of)

    # 5. build stream records
    streams: list[dict] = []
    for ch in minted:
        kind, rblk = receiver[ch.blk]
        line = geom[ch.blk]
        segs = assign_under_lake(line, lake_index)
        for g in extra_geoms.get(ch.blk, ()):               # same-name braids ride along as extra fids
            segs += assign_under_lake(g, lake_index)
        if kind == "ext":                                   # attaches to the extended FWA blue line
            em = ext_meta[int(rblk)]; rk, rb, rwsc = "fwa", em["fwa_blk"], em["fwa_wsc"]
        elif kind == "tidal":
            rk, rb, rwsc = kind, rblk, ""
        elif kind == "fwa":
            rk, rb, rwsc = kind, rblk, fwa_by_blk[rblk].fwa_watershed_code
        else:
            rk, rb, rwsc = kind, rblk, add_geom_wsc[rblk][1]
        streams.append(_record(ch, str(ch.blk), wsc_of[ch.blk], "novel", rk, rb, rwsc,
                               order_v[ch.blk], mag_v[ch.blk], segs, base_measure=0.0,
                               connector=connector_of[ch.blk]))
    for ch in extensions:                                       # classified + synthetic (up-end) extensions
        em = ext_meta[ch.blk]; fwa = em["fwa"]; clip = em["clip"]
        segs = assign_under_lake(clip, lake_index)
        # an extension continues its FWA blue line past the line's END: connect the extension's mouth to
        # that end (a connector if the source left a gap, else a zero-gap confluence), then extend.
        mouth = Point(clip.coords[0])
        end_pt = min((Point(fwa.geometry.coords[0]), Point(fwa.geometry.coords[-1])),
                     key=lambda p: p.distance(mouth))            # the FWA endpoint it diverges from
        gap = mouth.distance(end_pt)
        conn = {"to_blk": em["fwa_blk"], "to_fwa": True, "at_measure": em["base_measure"],
                "x": end_pt.x, "y": end_pt.y,
                "kind": "confluence" if gap <= _CONFLUENCE_GAP else "connector"}
        streams.append(_record(ch, em["fwa_blk"], em["fwa_wsc"], "extension", "fwa", em["fwa_blk"],
                               em["fwa_wsc"], fwa.stream_order or 1, fwa.stream_magnitude or 1, segs,
                               base_measure=em["base_measure"], connector=conn))

    violations = validate_propagation(streams)
    if violations:
        raise ValueError("added-streams propagation violations:\n  " + "\n  ".join(violations))

    # connector bridge lines (mouth -> receiver confluence) so the map shows continuity. Only a real
    # GAP is a connector; a zero-gap confluence (the stream already touches its receiver) draws nothing.
    conn_diags = []
    for st in streams:                                       # novels AND extensions (mouth -> receiver)
        c = st.get("connector")
        if not c or c["kind"] != "connector" or not st["segments"]:
            continue
        mx, my = _TO_LONLAT.transform(*st["segments"][0]["coords3005"][0])
        cx, cy = _TO_LONLAT.transform(c["x"], c["y"])
        conn_diags.append({"klass": "connector", "blk": st["blk"], "wsc": "",
                           "name": st["name"], "ftype": c["kind"], "fish": "",
                           "coords": [[round(mx, 6), round(my, 6)], [round(cx, 6), round(cy, 6)]]})

    report = {"counts": {k: sum(1 for ch in channels if cls[ch.blk].klass == k)
                         for k in ("duplicate", "extension", "novel")},
              "minted": len(streams), "unresolved": unresolved,
              "name_conflicts": [asdict(c) for c in candidates if c.conflict],
              "diagnostics": _diagnostics(channels, cls, kept_blks, wsc_of, ch_by_blk, geom,
                                          folded, ext_meta) + conn_diags}
    return streams, candidates, report


_TO_LONLAT = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)


def _diagnostics(channels, cls, kept_blks, wsc_of, ch_by_blk, geom, folded=frozenset(),
                 ext_meta=None) -> list[dict]:
    """Per-channel visualization records (lon/lat): final class, blk, wsc, name, type. For mapcheck.
    A minted novel uses its FINAL oriented geometry (``geom``) so the drawn line and its connector share
    the same mouth. Runs folded into a trunk (``folded``) are skipped — the trunk carries their line."""
    ext_meta = ext_meta or {}
    diags = []
    for ch in channels:
        m = cls[ch.blk]
        if ch.blk in folded:
            continue                                        # merged into a trunk blk; drawn there
        if ch.blk in ext_meta:                              # extension (classified or up-end reclassified)
            em = ext_meta[ch.blk]
            klass, blk, wsc = "extension", em["fwa_blk"], em["fwa_wsc"]
            coords = [_TO_LONLAT.transform(x, y) for x, y in em["clip"].coords]
        elif m.klass == "duplicate":
            klass, blk, wsc, coords = "duplicate", m.fwa_blk, m.fwa_wsc, list(ch.geometry.coords)
        elif ch.blk in kept_blks:
            klass, blk, wsc = "novel", str(ch.blk), wsc_of[ch.blk]
            coords = [_TO_LONLAT.transform(x, y) for x, y in geom[ch.blk].coords]   # trimmed/oriented
        else:
            klass, blk, wsc, coords = "unresolved", str(ch.blk), "", list(ch.geometry.coords)
        diags.append({"klass": klass, "blk": str(blk), "wsc": wsc, "name": ch.name,
                      "ftype": ch.overrides.get("ftype", ""), "fish": ch.overrides.get("fish", ""),
                      "coords": [[round(x, 6), round(y, 6)] for x, y in coords]})
    return diags


def _record(ch, blk, wsc, klass, rkind, rblk, rwsc, order, mag, segs, *, base_measure, connector):
    return {
        "blk": blk, "wsc": wsc, "name": ch.name, "source": ch.source,
        "src_id": ch.overrides.get("src_id"), "klass": klass,
        "ftype": ch.overrides.get("ftype", ""), "fish": ch.overrides.get("fish", ""),
        "species": ch.overrides.get("species", ""),
        "stream_order": order, "stream_magnitude": mag,
        "receiver_kind": rkind, "receiver_blk": rblk, "receiver_wsc": rwsc,
        "base_measure": base_measure, "connector": connector,
        "segments": [{"coords3005": list(g.coords), "wbk": w} for g, w in segs],
    }


def _toposort(channels, receiver) -> list:
    by_blk = {ch.blk: ch for ch in channels}
    out, state = [], {}
    def visit(ch):
        s = state.get(ch.blk)
        if s == 1:
            return
        if s == 0:
            raise ValueError(f"added streams: connect_to cycle at blk {ch.blk}")
        state[ch.blk] = 0
        kind, rblk = receiver[ch.blk]
        if kind == "added" and int(rblk) in by_blk:
            visit(by_blk[int(rblk)])
        state[ch.blk] = 1
        out.append(ch)
    for ch in channels:
        visit(ch)
    return out


def _order_magnitude(channels, receiver, proj_of):
    upstream: dict[int, list[int]] = {}
    for ch in channels:
        kind, rblk = receiver[ch.blk]
        if kind == "added":
            upstream.setdefault(int(rblk), []).append(ch.blk)
    order, mag = {}, {}
    for ch in reversed(_toposort(channels, receiver)):        # leaves first
        kids = upstream.get(ch.blk, [])
        mag[ch.blk] = 1 + sum(mag[k] for k in kids)
        cur = 1
        for k in sorted(kids, key=lambda k: proj_of.get(k, 0.0), reverse=True):
            o = order[k]
            cur = cur + 1 if o == cur else max(cur, o)
        order[ch.blk] = cur
    return order, mag


# ---------------------------------------------------------------- graph inputs / (de)serialize

def to_graph_inputs(streams: list[dict]) -> tuple[list, list[ConnectorSpec]]:
    """Rebuild (added FidRows, ConnectorSpecs) from stream records — for the harness / Phase B.
    Extension fids reuse the FWA blk (they merge into that blue line when combined with FWA fids)."""
    fids, specs = [], []
    for s in streams:
        segs = [(LineString(seg["coords3005"]), seg["wbk"]) for seg in s["segments"]]
        fids += added_fidrows(s["blk"], s["wsc"], segs, s["stream_order"], s["stream_magnitude"],
                              base_measure=s.get("base_measure", 0.0))
        c = s.get("connector")
        if c:
            specs.append(ConnectorSpec(from_node=f"{s['blk']}:0", to_blk=str(c["to_blk"]),
                                       to_fwa=c["to_fwa"], at_measure=c["at_measure"],
                                       x=c["x"], y=c["y"], kind=c["kind"]))
    return fids, specs


def load_build(path: str | Path) -> list[dict]:
    return json.loads(Path(path).read_text(encoding="utf-8"))["streams"]


def write(streams, candidates, report, out_dir: Path, name: str = "added_streams") -> Path:
    out = Path(out_dir) / "added_streams.build.json"
    out.write_text(json.dumps({"_about": "Minted additional-streams dataset (see added_streams).",
                               "streams": streams}, indent=1), encoding="utf-8")
    from pipeline.added_streams import __file__ as _pkg
    outputs = Path(_pkg).resolve().parents[2] / "output"
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "added_name_variant_candidates.json").write_text(
        json.dumps([asdict(c) for c in candidates], indent=1), encoding="utf-8")
    (outputs / "added_streams_report.md").write_text(_report_md(report), encoding="utf-8")
    return out


def _report_md(report: dict) -> str:
    c = report["counts"]
    lines = [f"# Added-streams batch report", "",
             f"- duplicates dropped (favour FWA): **{c['duplicate']}**",
             f"- extensions kept: **{c['extension']}**",
             f"- novel kept: **{c['novel']}**",
             f"- minted streams: **{report['minted']}**",
             f"- unresolved (skipped): **{len(report['unresolved'])}**", ""]
    if report["name_conflicts"]:
        lines += ["## Name conflicts (FWA name is boss; add the municipal name as a variant)", ""]
        for nc in report["name_conflicts"]:
            lines.append(f"- FWA blk {nc['target_blk']} (gnis {nc['target_gnis'] or '—'}) also called "
                         f"**{nc['name']}** in a municipal layer")
    if report["unresolved"]:
        lines += ["", "## Unresolved (no FWA/added/tidal receiver within tolerance)", ""]
        lines += [f"- {u}" for u in report["unresolved"][:50]]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- gpkg-loading wrapper + CLI

def _load_fwa(gpkg, bbox):
    from data.data_extractor import FWADataAccessor
    from pipeline.build import get_lake_wbk_kind
    from pipeline.graph.blk_chains import build_blk_chains, load_stream_fids
    fwa = FWADataAccessor(gpkg)
    fids = load_stream_fids(gpkg, bbox=bbox)
    lake_kind = get_lake_wbk_kind(fwa, bbox=bbox)
    chains = build_blk_chains(fids, lake_kind)
    lakes = _polys(fwa, ("lakes", "manmade"), bbox)
    tidal = _geoms(fwa, "tidal_boundary", bbox)
    return chains, LakeIndex(lakes), tidal, fids, lake_kind


def _polys(fwa, layers, bbox):
    out = []
    for lyr in layers:
        gdf = fwa.get_layer(lyr, columns=["WATERBODY_KEY", "geometry"], bbox=bbox)
        out += [(g, str(w)) for g, w in zip(gdf.geometry, gdf["WATERBODY_KEY"]) if g is not None]
    return out


def _geoms(fwa, layer, bbox):
    try:
        gdf = fwa.get_layer(layer, columns=["geometry"], bbox=bbox)
    except Exception:                                            # noqa: BLE001 — layer optional
        return []
    return [g for g in gdf.geometry if g is not None]


def build(sources: list[str], gpkg: str, pad: float = 3000.0, out_dir: Optional[Path] = None) -> Path:
    from pipeline.added_streams.clean import clean_source
    features = [f for s in sources for f in clean_source(s)]
    pts = [_TO_ALBERS.transform(x, y) for f in features for x, y in f["geometry"]["coordinates"]]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    bbox = (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)
    chains, lake_index, tidal, _, _ = _load_fwa(gpkg, bbox)
    from pipeline.added_streams.dem import ElevationSampler
    source = sources[0] if len(sources) == 1 else ""
    streams, candidates, report = resolve_and_mint(
        features, chains, lake_index, tidal,
        exclude_wsc=FWA_EXCLUDE_BY_SOURCE.get(source, ()),
        approved_lakes=approved_lake_polys(gpkg, bbox, source),
        orient_sampler=ElevationSampler(), trust_source=source in RELIABLE_SOURCES)
    out_dir = out_dir or (Path(__file__).resolve().parent)
    out = write(streams, candidates, report, out_dir)
    print(f"  sources={sources}  {report['counts']}  minted={report['minted']}  "
          f"unresolved={len(report['unresolved'])}  -> {out}")
    return out


def main() -> None:
    import argparse
    from project_config import get_config
    ap = argparse.ArgumentParser(description="Build the minted additional-streams dataset.")
    ap.add_argument("sources", nargs="*", default=["burnaby"],
                    help="municipal sources (default: burnaby)")
    ap.add_argument("--gpkg", default=None)
    args = ap.parse_args()
    build(args.sources or ["burnaby"], args.gpkg or get_config().fwa_data_gpkg)


if __name__ == "__main__":
    main()
