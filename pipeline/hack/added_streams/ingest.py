"""Turn curated added-stream features into synthetic FWA records + connector specs.

Pipeline (source-agnostic — OSM or hand-drawn both arrive as GeoJSON features):

  merge_channels           -> one Channel per stream (shared minted BLK, stitched geometry)
  resolve receivers        -> each channel joins an FWA stream or another added channel
  topological WSC          -> mint each channel's WSC top-down from the FWA root outward
  synthesize FidRow+BlkChain per channel (every FWA attribute the build reads)
  ConnectorSpec per channel -> a real flow edge at the receiver's confluence measure

`ingest` returns plain objects; `attach_connectors` adds the edges to an already-built graph. Neither
touches build.py — the standalone harness (and, later, build.py) wires them together.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.ops import transform as shp_transform

from pipeline.hack.added_streams.merge import Channel, merge_channels
from pipeline.hack.added_streams.wsc import mint_wsc
from pipeline.graph import cutting
from pipeline.graph.blk_chains import FidRow
from pipeline.models import BlkChain, FidSpan, FlowEdge, NodeKind, StreamGraph

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_CONNECT_TOL_M = 250.0      # auto receiver must be within this of the channel
_CONFLUENCE_GAP_M = 1.0     # mouth↔receiver closer than this => a shared vertex (kind "confluence")
_DEFAULT_EDGE_TYPE = "1000" # normal stream; NEVER "2300" (that would make it a barrier)


@dataclass(frozen=True)
class ConnectorSpec:
    """A flow edge to add after the graph is built: the added channel (``from_node``) flows into its
    receiver at ``at_measure`` on the receiver's line. ``to_blk`` is the RECEIVING MAINSTEM's blk —
    the connector is the mainstem picking up the tributary, so the connector carries this blk (not
    the tributary's, not a fresh one). ``to_fwa`` picks the resolver in ``attach_connectors``: an FWA
    blk needs a node-by-measure lookup; an added blk is ``{blk}:0``."""
    from_node: str
    to_blk: str        # the receiving MAINSTEM blk (== the connector's blk)
    to_fwa: bool
    at_measure: float
    x: float
    y: float
    kind: str          # "connector" (a mouth↔receiver gap) | "confluence" (shared vertex)


def load_features(path: str | Path) -> list[dict]:
    """Read a GeoJSON FeatureCollection (or a bare feature list) into feature dicts."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        return list(data.get("features", []))
    return list(data)


def _to_albers(line_lonlat: LineString) -> LineString:
    return shp_transform(lambda xs, ys, z=None: _TO_ALBERS.transform(xs, ys), line_lonlat)


class _Receiver:
    """Minimal receiver interface shared by an FWA BlkChain and a just-synthesized added BlkChain."""
    __slots__ = ("blk", "geometry", "mouth_measure", "length_m", "wsc", "is_fwa")

    def __init__(self, blk: str, geometry, mouth_measure: float, length_m: float, wsc: str, is_fwa: bool):
        self.blk, self.geometry, self.mouth_measure = blk, geometry, mouth_measure
        self.length_m, self.wsc, self.is_fwa = length_m, wsc, is_fwa

    @classmethod
    def from_chain(cls, ch: BlkChain, is_fwa: bool) -> "_Receiver":
        return cls(str(ch.blk), ch.geometry, ch.mouth_measure, ch.length_m,
                   ch.fwa_watershed_code, is_fwa)


def _resolve_receiver_blk(ch: Channel, geom3005: LineString, fwa_chains: list[BlkChain],
                          added_geoms: dict[int, LineString]) -> tuple[str, bool]:
    """Return (receiver_blk, is_fwa) for a channel. Uses explicit ``connect_to`` when present, else
    the nearest FWA blue line / other added channel within ``_CONNECT_TOL_M``."""
    ct = ch.connect_to or {}
    if ct.get("blk") not in (None, ""):
        b = int(ct["blk"])
        return (str(b), b > 0)
    if ct.get("gnis_id") not in (None, ""):
        gid = str(ct["gnis_id"])
        cands = [c for c in fwa_chains if str(c.gnis_id) == gid]
        if not cands:
            raise ValueError(f"added stream blk {ch.blk}: connect_to gnis_id {gid} not in FWA data")
        best = min(cands, key=lambda c: c.geometry.distance(geom3005))
        return (str(best.blk), True)
    if ct.get("coord"):
        p = Point(*_TO_ALBERS.transform(ct["coord"][0], ct["coord"][1]))
        best = min(fwa_chains, key=lambda c: c.geometry.distance(p))
        return (str(best.blk), True)
    # auto: nearest FWA chain vs nearest other added channel
    fwa_best = min(fwa_chains, key=lambda c: c.geometry.distance(geom3005), default=None)
    fwa_d = fwa_best.geometry.distance(geom3005) if fwa_best else float("inf")
    add_best_blk, add_d = None, float("inf")
    for ablk, ag in added_geoms.items():
        if ablk == ch.blk:
            continue
        dd = ag.distance(geom3005)
        if dd < add_d:
            add_best_blk, add_d = ablk, dd
    if min(fwa_d, add_d) > _CONNECT_TOL_M:
        raise ValueError(f"added stream blk {ch.blk}: no FWA/added receiver within {_CONNECT_TOL_M} m "
                         f"(nearest FWA {fwa_d:.0f} m, added {add_d:.0f} m) — add an explicit connect_to")
    if add_d < fwa_d:
        return (str(add_best_blk), False)
    return (str(fwa_best.blk), True)


def _oriented_mouth_first(geom3005: LineString, receiver_geom) -> LineString:
    """Orient a channel so coords[0] is the MOUTH (the end nearest its receiver)."""
    coords = list(geom3005.coords)
    d0 = Point(coords[0]).distance(receiver_geom)
    d1 = Point(coords[-1]).distance(receiver_geom)
    return geom3005 if d0 <= d1 else LineString(coords[::-1])


def _synth_records(ch: Channel, geom3005: LineString, wsc: str,
                   order: int, mag: int) -> tuple[FidRow, BlkChain]:
    """Build the synthetic FidRow + BlkChain (all FWA attributes the build reads). ``order``/``mag``
    are the computed Strahler order / Shreve magnitude (an override in props takes precedence)."""
    ov = ch.overrides
    blk = str(ch.blk)
    length = geom3005.length
    order = int(ov["stream_order"]) if ov.get("stream_order") not in (None, "") else order
    mag = int(ov["stream_magnitude"]) if ov.get("stream_magnitude") not in (None, "") else mag
    edge_type = str(ov.get("edge_type") or _DEFAULT_EDGE_TYPE)
    gnis_id = str(ov.get("gnis_id") or "")
    fid = f"add:{blk}"
    down_node, up_node = cutting.blk_endpoints(geom3005)
    row = FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=edge_type, wbk="",
                 gnis_id=gnis_id, gnis_name="", stream_order=order, stream_magnitude=mag,
                 down_m=0.0, up_m=length, geometry=geom3005, down_node=down_node, up_node=up_node)
    chain = BlkChain(blk=blk, fwa_watershed_code=wsc, fids=(FidSpan(fid, 0.0, length),),
                     geometry=geom3005, mouth_measure=0.0, length_m=length, name_tuples=(),
                     gnis_id=gnis_id, gnis_name="", stream_order=order, stream_magnitude=mag,
                     waterbody_runs=(), edge_types=(edge_type,))
    return row, chain


def added_fidrows(blk: str, wsc: str, segments: list[tuple[LineString, str]], order: int, mag: int,
                  edge_type: str = "1000", base_measure: float = 0.0, gnis_id: str = "") -> list[FidRow]:
    """One synthetic `FidRow` per (geometry, wbk) segment of an added stream, ordered mouth→source with
    accumulating route measures from ``base_measure``. An under-lake segment carries the FWA lake
    `wbk`, so `build_blk_chains` turns it into a `WaterbodyRun` that ties into the existing lake node.
    ``base_measure`` > 0 lets an EXTENSION continue past the FWA stream's up_m on the same blk."""
    rows: list[FidRow] = []
    m = base_measure
    for i, (geom, wbk) in enumerate(segments):
        length = geom.length
        dn, up = cutting.blk_endpoints(geom)
        rows.append(FidRow(
            fid=f"add:{blk}:{i}", blk=str(blk), wsc=wsc, edge_type=edge_type, wbk=str(wbk or ""),
            gnis_id=gnis_id, gnis_name="", stream_order=order, stream_magnitude=mag,
            down_m=m, up_m=m + length, geometry=geom, down_node=dn, up_node=up))
        m += length
    return rows


def _toposort(channels: list[Channel], receiver_of: dict[int, tuple[str, bool]]) -> list[Channel]:
    """Order channels so an added receiver is built before its dependents. Raises on a cycle."""
    by_blk = {c.blk: c for c in channels}
    order: list[Channel] = []
    state: dict[int, int] = {}          # 0=visiting, 1=done

    def visit(c: Channel) -> None:
        s = state.get(c.blk)
        if s == 1:
            return
        if s == 0:
            raise ValueError(f"added streams: connect_to cycle involving blk {c.blk}")
        state[c.blk] = 0
        rblk, is_fwa = receiver_of[c.blk]
        if not is_fwa:
            dep = by_blk.get(int(rblk))
            if dep is None:
                raise ValueError(f"added stream blk {c.blk}: connect_to added blk {rblk} not found")
            visit(dep)
        state[c.blk] = 1
        order.append(c)

    for c in channels:
        visit(c)
    return order


def _strahler_shreve(topo: list[Channel], receiver_of: dict[int, tuple[str, bool]],
                     join_measure: dict[int, float]) -> tuple[dict[int, int], dict[int, int]]:
    """Compute Strahler order + Shreve magnitude over the ADDED network (FWA side excluded — a
    tributary joining a larger FWA stream doesn't change its own order). Walked leaves-first.

    Shreve (additive):   magnitude(C) = 1 + sum(magnitude of added tributaries of C).
    Strahler (Horton):   fold tributaries source->mouth: equal order => +1, else keep the max.
    """
    upstream: dict[int, list[int]] = {}                 # added receiver blk -> child channel blks
    for c in topo:
        rblk, is_fwa = receiver_of[c.blk]
        if not is_fwa:
            upstream.setdefault(int(rblk), []).append(c.blk)
    order: dict[int, int] = {}
    mag: dict[int, int] = {}
    for c in reversed(topo):                             # leaves first (topo is receiver-first)
        kids = upstream.get(c.blk, [])
        mag[c.blk] = 1 + sum(mag[k] for k in kids)
        cur = 1                                          # this channel's own headwater
        for k in sorted(kids, key=lambda k: join_measure[k], reverse=True):   # source -> mouth
            o = order[k]
            cur = cur + 1 if o == cur else max(cur, o)
        order[c.blk] = cur
    return order, mag


def ingest(features: list[dict], fwa_chains: list[BlkChain]
           ) -> tuple[list[FidRow], list[BlkChain], list[ConnectorSpec]]:
    """Curated features + FWA chains -> (added FidRows, added BlkChains, ConnectorSpecs)."""
    channels = merge_channels(features)
    geom_of = {c.blk: _to_albers(c.geometry) for c in channels}
    receiver_of = {c.blk: _resolve_receiver_blk(c, geom_of[c.blk], fwa_chains, geom_of)
                   for c in channels}
    topo = _toposort(channels, receiver_of)             # receiver-first (mainstem before tributary)

    fwa_by_blk = {str(c.blk): c for c in fwa_chains}
    receivers: dict[str, _Receiver] = {}                # added blk -> built receiver (chained tribs)

    # pass 1 (receiver-first): orient geometry, place the confluence, mint WSC top-down.
    geom3005_of: dict[int, LineString] = {}
    wsc_of: dict[int, str] = {}
    confl_of: dict[int, tuple[float, float, float, float]] = {}   # blk -> (at_measure, x, y, gap)
    join_measure: dict[int, float] = {}                 # blk -> distance up its receiver (for order fold)
    for ch in topo:
        rblk, is_fwa = receiver_of[ch.blk]
        rc = _Receiver.from_chain(fwa_by_blk[rblk], True) if is_fwa else receivers[rblk]
        geom3005 = _oriented_mouth_first(geom_of[ch.blk], rc.geometry)
        mouth = Point(geom3005.coords[0])
        proj = rc.geometry.project(mouth)               # distance up the receiver from its mouth
        confl = rc.geometry.interpolate(proj)
        wsc = str(ch.overrides.get("wsc") or mint_wsc(rc.wsc, proj, rc.length_m))
        geom3005_of[ch.blk] = geom3005
        wsc_of[ch.blk] = wsc
        join_measure[ch.blk] = proj
        confl_of[ch.blk] = (rc.mouth_measure + proj, confl.x, confl.y, mouth.distance(confl))
        receivers[str(ch.blk)] = _Receiver(str(ch.blk), geom3005, 0.0, geom3005.length, wsc, False)

    # pass 2 (leaves-first): Strahler order + Shreve magnitude over the added network.
    order_of, mag_of = _strahler_shreve(topo, receiver_of, join_measure)

    add_fids: list[FidRow] = []
    add_chains: list[BlkChain] = []
    specs: list[ConnectorSpec] = []
    for ch in topo:
        rblk, is_fwa = receiver_of[ch.blk]
        at_measure, x, y, gap = confl_of[ch.blk]
        row, chain = _synth_records(ch, geom3005_of[ch.blk], wsc_of[ch.blk],
                                    order_of[ch.blk], mag_of[ch.blk])
        add_fids.append(row)
        add_chains.append(chain)
        specs.append(ConnectorSpec(
            from_node=f"{ch.blk}:0", to_blk=rblk, to_fwa=is_fwa, at_measure=at_measure,
            x=x, y=y, kind="confluence" if gap <= _CONFLUENCE_GAP_M else "connector"))
    return add_fids, add_chains, specs


def _fwa_node_for_measure(graph: StreamGraph, blk: str, measure: float) -> Optional[str]:
    """The FWA stream node on ``blk`` whose route span contains ``measure`` (nearest by span if none)."""
    cands = [(nid, n) for nid, n in graph.nodes.items()
             if n.kind == NodeKind.stream and str(n.blk) == blk]
    if not cands:
        return None
    for nid, n in cands:
        if n.down_m - 1e-6 <= measure <= n.up_m + 1e-6:
            return nid
    return min(cands, key=lambda kv: min(abs(kv[1].down_m - measure), abs(kv[1].up_m - measure)))[0]


def connector_geom_id(mainstem_blk: str, from_node: str) -> str:
    """Geometry-sidecar key for a connector bridge — keyed by the RECEIVING MAINSTEM blk so the
    connector shares the mainstem's blk (the mainstem reaching out to pick up the tributary)."""
    return f"connector:{mainstem_blk}:{from_node}"


def attach_connectors(graph: StreamGraph, geoms: dict, specs: list[ConnectorSpec]) -> dict:
    """Add each ConnectorSpec as a real flow edge (added channel -> receiver) and a short bridge
    LineString into ``geoms`` for rendering the mouth↔receiver gap. The bridge is keyed under the
    RECEIVING MAINSTEM's blk (``connector_geom_id``) so the connector carries the mainstem's blk.
    Mutates ``graph``/``geoms``. Returns {added, skipped, connectors:[{blk,from_node,to_node,kind,geom_id}]}."""
    existing = {(e.from_node, e.to_node) for e in graph.edges}
    connectors: list[dict] = []
    skipped = 0
    for s in specs:
        to_node = f"{s.to_blk}:0" if not s.to_fwa else _fwa_node_for_measure(graph, s.to_blk, s.at_measure)
        if s.from_node not in graph.nodes or not to_node or to_node not in graph.nodes:
            skipped += 1
            continue
        if (s.from_node, to_node) in existing:
            skipped += 1
            continue
        ei = len(graph.edges)
        graph.edges.append(FlowEdge(from_node=s.from_node, to_node=to_node, at_measure=s.at_measure,
                                    x=s.x, y=s.y, kind=s.kind))
        graph.up_adj.setdefault(to_node, []).append(ei)
        graph.down_adj.setdefault(s.from_node, []).append(ei)
        existing.add((s.from_node, to_node))
        geom_id = connector_geom_id(s.to_blk, s.from_node)          # connector's blk == mainstem blk
        g = geoms.get(s.from_node)
        if g is not None and hasattr(g, "coords"):
            geoms[geom_id] = LineString([g.coords[0], (s.x, s.y)])
        connectors.append({"blk": s.to_blk, "from_node": s.from_node, "to_node": to_node,
                           "kind": s.kind, "geom_id": geom_id})
    return {"added": len(connectors), "skipped": skipped, "connectors": connectors}
