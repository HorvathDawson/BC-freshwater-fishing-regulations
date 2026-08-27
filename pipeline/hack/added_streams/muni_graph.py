"""Municipal stream GRAPH from the geojson ALONE — no FWA, no WSC (those are a later overlay).

The job here is to get the *structure* right first: which municipal lines form one stream, how streams
branch into tributaries, and which way they flow. Pipeline:

  1. ``merge_channels`` -> decomposed runs (segments between junctions), each with a provisional blk.
  2. connection graph: an endpoint of one run lying on another (a mouth meeting a stream); larger gaps
     bridged only within one named creek.
  3. per connected component, choose the OUTLET (root) — the leaf run that maximises Strahler order,
     i.e. where the most tributaries have accumulated (the mouth). FWA-free + dataset-agnostic.
  4. orient the tree from the outlet (leaves = headwaters flowing down to it) and TRACE MAINSTEMS: at
     each confluence the branch with the largest upstream length keeps the blk; others start new blks.
     So each real stream = ONE blk, its tributaries = their own blks.
  5. Strahler order + Shreve magnitude over the stream tree.

``build_graph`` returns the streams; ``graph_map`` writes a Leaflet HTML that draws the flow (arrows
point downstream) coloured by blk so the branching/flow can be eyeballed before FWA is brought in.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.setrecursionlimit(50000)      # trace/Strahler recurse on tree depth; deep networks need headroom

from pyproj import Transformer
from shapely.geometry import LineString, Point

from pipeline.hack.added_streams.build_dataset import _albers
from pipeline.hack.added_streams.merge import _greedy_chain, merge_channels

_TO_LONLAT = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)
_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_TOUCH_TOL = 20.0          # two runs connect where an endpoint of one meets the other
_SAME_NAME_TOL = 250.0     # bridge an endpoint gap this large within ONE named creek


# ---------------------------------------------------------------- connection graph

def _ends(g: LineString) -> tuple:
    c = g.coords
    return (Point(c[0]), Point(c[-1]))


def _conn_graph(blks: list[int], geom: dict, name: dict) -> dict:
    """adj[b][o] = gap (connector length) between runs b and o (near 0 when they touch).

    Streams connect where a channel really joins another, so we are strict: same-name runs link only at
    a SHARED ENDPOINT (a junction — decompose already split them there); a run's endpoint links to a
    DIFFERENT-named run's line (a tributary mouth on a mainstem). This avoids a dense blob where every
    parallel run within tol is joined. Genuine mid-creek GAPS in one named creek are then bridged
    across components, closest-first (MST-like), so a creek chopped by a gap is one component."""
    from shapely.strtree import STRtree
    from pipeline.hack.added_streams.merge import _UF
    tree = STRtree([geom[b] for b in blks]) if blks else None
    adj: dict[int, dict[int, float]] = {b: {} for b in blks}

    def link(a, c, gap):
        if gap < adj[a].get(c, 1e18):
            adj[a][c] = gap; adj[c][a] = gap

    for b in blks:
        nb = (name[b] or "").strip().lower()
        for e in _ends(geom[b]):
            for i in (tree.query(e.buffer(_TOUCH_TOL)) if tree else []):
                o = blks[int(i)]
                if o == b:
                    continue
                ee = min(e.distance(oe) for oe in _ends(geom[o]))    # endpoint-to-endpoint (junction)
                if ee <= _TOUCH_TOL:
                    link(b, o, ee)
                elif (name[o] or "").strip().lower() != nb and e.distance(geom[o]) <= _TOUCH_TOL:
                    link(b, o, e.distance(geom[o]))                  # tributary mouth on a mainstem

    idx = {b: i for i, b in enumerate(blks)}
    uf = _UF(len(blks))
    for b in blks:
        for o in adj[b]:
            uf.union(idx[b], idx[o])
    by_name: dict[str, list[int]] = {}
    for b in blks:
        nm = (name[b] or "").strip().lower()
        if nm:
            by_name.setdefault(nm, []).append(b)
    cands = []
    for grp in by_name.values():
        for x in range(len(grp)):
            for y in range(x + 1, len(grp)):
                a, c = grp[x], grp[y]
                if uf.find(idx[a]) != uf.find(idx[c]):
                    gap = min(pa.distance(pc) for pa in _ends(geom[a]) for pc in _ends(geom[c]))
                    if gap <= _SAME_NAME_TOL:
                        cands.append((gap, a, c))
    for gap, a, c in sorted(cands):                                  # closest gaps first, only if merging
        if uf.find(idx[a]) != uf.find(idx[c]):
            link(a, c, gap); uf.union(idx[a], idx[c])
    return adj


def _components(blks: list[int], adj: dict) -> list[list[int]]:
    seen, out = set(), []
    for b in blks:
        if b in seen:
            continue
        stack, comp = [b], []
        seen.add(b)
        while stack:
            x = stack.pop(); comp.append(x)
            for y in adj[x]:
                if y not in seen:
                    seen.add(y); stack.append(y)
        out.append(comp)
    return out


# ---------------------------------------------------------------- orient + trace mainstems

def _rooted_tree(root: int, adj: dict, comp: list[int]) -> tuple[dict, dict]:
    """BFS from ``root``; return (parent, children) over the component's spanning tree."""
    from collections import deque
    parent = {root: None}
    children: dict[int, list[int]] = {b: [] for b in comp}
    dq = deque([root])
    while dq:
        x = dq.popleft()
        for y in adj[x]:
            if y not in parent:
                parent[y] = x; children[x].append(y); dq.append(y)
    return parent, children


def _strahler(root: int, children: dict) -> dict:
    """Strahler order over a rooted tree (leaves = 1; equal max children -> +1, else max)."""
    order = {}
    def post(n):
        kids = children[n]
        if not kids:
            order[n] = 1; return 1
        vals = sorted((post(k) for k in kids), reverse=True)
        order[n] = vals[0] + 1 if len(vals) > 1 and vals[0] == vals[1] else vals[0]
        return order[n]
    post(root)
    return order


def _root_order(leaf: int, adj: dict, comp: list[int]) -> tuple:
    _, children = _rooted_tree(leaf, adj, comp)
    order = _strahler(leaf, children)
    return order[leaf]


def _accumulate(root: int, children: dict, geom: dict) -> dict:
    acc = {}
    def post(n):
        acc[n] = geom[n].length + sum(post(k) for k in children[n])
        return acc[n]
    post(root)
    return acc


def _resolve_component(comp: list[int], adj: dict, geom: dict, name: dict) -> list[dict]:
    if len(comp) == 1:
        b = comp[0]
        return [_stream(b, [b], None, geom, name, order=1, mag=1)]
    deg = {b: len(adj[b]) for b in comp}
    leaves = [b for b in comp if deg[b] <= 1] or comp
    root = max(leaves, key=lambda L: _root_order(L, adj, comp))     # the outlet = max-order mouth
    parent, children = _rooted_tree(root, adj, comp)
    acc = _accumulate(root, children, geom)

    # trace mainstems: each run gets a STREAM blk; the largest-accumulation child keeps the blk, the
    # rest each start a new stream (a tributary). blk id = the mouth-most run of that stream.
    blk_of: dict[int, int] = {}
    def trace(run, stream_blk):
        blk_of[run] = stream_blk
        kids = sorted(children[run], key=lambda k: acc[k], reverse=True)
        for i, k in enumerate(kids):
            trace(k, stream_blk if i == 0 else k)
    trace(root, root)

    # group runs per stream, order mouth->source, build geometry + receiver
    runs_of: dict[int, list[int]] = {}
    for run, sb in blk_of.items():
        runs_of.setdefault(sb, []).append(run)
    depth = {}
    def setdepth(n, d):
        depth[n] = d
        for k in children[n]:
            setdepth(k, d + 1)
    setdepth(root, 0)

    # stream -> receiver stream (the stream its mouth-run's parent belongs to)
    recv_of: dict[int, int | None] = {}
    for sb, runs in runs_of.items():
        mouth_run = min(runs, key=lambda r: depth[r])
        p = parent[mouth_run]
        recv_of[sb] = blk_of[p] if p is not None else None

    # Strahler/Shreve over the stream tree
    up: dict[int, list[int]] = {sb: [] for sb in runs_of}
    for sb, rb in recv_of.items():
        if rb is not None:
            up[rb].append(sb)
    order, mag = {}, {}
    def so(sb):
        kids = up[sb]
        if not kids:
            order[sb] = 1; mag[sb] = 1; return
        for k in kids:
            so(k)
        vals = sorted((order[k] for k in kids), reverse=True)
        order[sb] = vals[0] + 1 if len(vals) > 1 and vals[0] == vals[1] else vals[0]
        mag[sb] = 1 + sum(mag[k] for k in kids)
    roots = [sb for sb, rb in recv_of.items() if rb is None]
    for r in roots:
        so(r)

    out = []
    for sb, runs in runs_of.items():
        out.append(_stream(sb, runs, recv_of[sb], geom, name, order[sb], mag[sb],
                           depth=depth, parent=parent, children=children))
    return out


def _stream(blk, runs, receiver, geom, name, order, mag, *, depth=None, parent=None,
            children=None) -> dict:
    """Build one stream record: runs chained mouth-first, receiver = the blk it flows into."""
    if len(runs) == 1:
        g = geom[runs[0]]
    else:
        ordered = sorted(runs, key=lambda r: depth[r])          # mouth-most run first
        g = LineString(_greedy_chain([list(geom[r].coords) for r in ordered]))
    nm = next((name[r] for r in runs if name[r]), "")
    return {"blk": int(blk), "receiver_blk": None if receiver is None else int(receiver),
            "name": nm, "order": order, "magnitude": mag,
            "coords3005": [list(c) for c in g.coords]}


def _orient_mouth_first(records: list[dict]) -> None:
    """Orient each stream so coords[0] is the MOUTH (nearest its receiver); outlets nearest a child."""
    by_blk = {r["blk"]: r for r in records}
    for r in records:
        rb = r["receiver_blk"]
        if rb is not None and rb in by_blk:
            toward = LineString(by_blk[rb]["coords3005"])
        else:                                                    # outlet: mouth = end far from any child
            kids = [c for c in records if c["receiver_blk"] == r["blk"]]
            toward = LineString(kids[0]["coords3005"]) if kids else None
        if toward is None:
            continue
        c = r["coords3005"]
        if Point(c[0]).distance(toward) > Point(c[-1]).distance(toward):
            r["coords3005"] = c[::-1]


# ---------------------------------------------------------------- public API

def build_graph(features: list[dict], gap_tol_m: float = 15.0, drop_unconfirmed: bool = True) -> list[dict]:
    if drop_unconfirmed:
        features = [f for f in features
                    if not str(f.get("properties", {}).get("ftype", "")).lower().startswith("unconfirmed")]
    channels = merge_channels(features, mint_missing_blk=True, gap_tol_m=gap_tol_m)
    geom = {c.blk: _albers(c.geometry) for c in channels}
    name = {c.blk: c.name for c in channels}
    blks = [c.blk for c in channels]
    adj = _conn_graph(blks, geom, name)
    streams: list[dict] = []
    for comp in _components(blks, adj):
        streams += _resolve_component(comp, adj, geom, name)
    _orient_mouth_first(streams)
    return streams


def graph_map(source: str, out_dir: Path) -> Path:
    from pipeline.hack.added_streams.clean import clean_source
    streams = build_graph(clean_source(source))
    feats = []
    for s in streams:
        coords = [[round(x, 6), round(y, 6)] for x, y in
                  (_TO_LONLAT.transform(px, py) for px, py in s["coords3005"])]
        if len(coords) < 2:
            continue
        feats.append({"type": "Feature",
                      "properties": {"blk": s["blk"], "name": s["name"], "order": s["order"],
                                     "magnitude": s["magnitude"], "receiver": s["receiver_blk"],
                                     "outlet": s["receiver_blk"] is None},
                      "geometry": {"type": "LineString", "coordinates": coords}})
    out = out_dir / f"muni_graph_{source}.html"
    n_out = sum(1 for s in streams if s["receiver_blk"] is None)
    out.write_text(_html(source, feats, len(streams), n_out), encoding="utf-8")
    return out


_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>muni graph — %(title)s</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet-polylinedecorator@1.6.0/dist/leaflet.polylineDecorator.js"></script>
<style>html,body,#map{height:100%%;margin:0}.panel{position:absolute;z-index:1000;top:8px;right:8px;
background:#fff;padding:8px 10px;font:12px sans-serif;border-radius:4px;box-shadow:0 1px 4px #0006}
.panel button{font:12px sans-serif;margin:2px}</style></head>
<body><div id="map"></div>
<div class="panel"><b>%(title)s</b> — streams %(n)d · outlets %(o)d<br>
colour by: <button onclick="setColor('blk')">blk</button>
<button onclick="setColor('order')">order</button>
<button onclick="setColor('outlet')">outlet</button>
<div style="margin-top:4px"><i>arrows point downstream (toward the outlet)</i></div></div>
<script>
const S=%(streams)s;
let colorBy='blk';
function hash(s){let h=0;s=String(s);for(let i=0;i<s.length;i++)h=(h*31+s.charCodeAt(i))|0;
  return 'hsl('+((h%%360)+360)%%360+',70%%,45%%)';}
function colorOf(p){if(colorBy==='outlet')return p.outlet?'#d62728':'#1f77b4';
  if(colorBy==='order')return ['#9ecae1','#6baed6','#3182bd','#08519c','#08306b','#041f4a'][Math.min(p.order-1,5)];
  return hash(p.blk);}
const map=L.map('map');
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);
let layer,deco=[];
function draw(){if(layer)map.removeLayer(layer);deco.forEach(d=>map.removeLayer(d));deco=[];
  layer=L.geoJSON(S,{style:f=>({color:colorOf(f.properties),weight:2+Math.min(f.properties.order,5),opacity:.85}),
    onEachFeature:(f,l)=>{const p=f.properties;
      l.bindTooltip('blk '+p.blk+' · '+(p.name||'(unnamed)')+'<br>order '+p.order+' · mag '+p.magnitude+
        ' · '+(p.outlet?'OUTLET':'→ '+p.receiver));
      // arrowheads point downstream: coords are stored mouth-first, so reverse for the decorator
      const latlngs=l.getLatLngs().slice().reverse();
      const d=L.polylineDecorator(latlngs,{patterns:[{offset:12,repeat:80,
        symbol:L.Symbol.arrowHead({pixelSize:8,pathOptions:{color:colorOf(p),fillOpacity:.9,weight:0}})}]});
      d.addTo(map);deco.push(d);}}).addTo(map);
  try{map.fitBounds(layer.getBounds(),{padding:[20,20]});}catch(e){map.setView([49.3,-123],12);}}
function setColor(c){colorBy=c;draw();}
draw();
</script></body></html>"""


def _html(title, streams, n, o) -> str:
    return _HTML % {"title": title, "streams": json.dumps(streams), "n": n, "o": o}


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Municipal stream graph (geojson only) -> flow HTML.")
    ap.add_argument("sources", nargs="*", default=["squamish"])
    ap.add_argument("--out", default="output")
    args = ap.parse_args()
    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    for src in (args.sources or ["squamish"]):
        print(f"  {src:12} -> {graph_map(src, out_dir)}")


if __name__ == "__main__":
    main()
