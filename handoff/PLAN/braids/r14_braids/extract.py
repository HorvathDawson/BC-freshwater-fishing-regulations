"""R14 item 7 — HEAVY step (run via heavy.sh): cut a small sub-atlas out of atlas/full so the census and
the review page can iterate without loading graph.pkl / geometries.pkl again. Read-only.

Writes <out>/sub.pkl:
  nodes   {nid: dict(attrs)}         every node within RADIUS of any bindable cut point (curated / gauge /
                                      length, not area/mu), every node of each cut's host blk, every node of
                                      the Serpentine's WSC, and every lake node with its in/out neighbours
  geoms   {nid: LineString}           stream geometry for the nodes above (EPSG:3005)
  edges   [dict]                      every edge touching a kept node
  cuts    [dict]                      the resolved splits (all of them; the census filters)
  lakes   {lake nid: {...}}           EVERY lake node: in/out stream neighbours with their owners (for the
                                      canal census) — attributes only
"""
import json, pickle, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from shapely.strtree import STRtree
from shapely.geometry import LineString
from pipeline.common.io.serialize import read_artifact

OUT = Path(sys.argv[1]); OUT.mkdir(parents=True, exist_ok=True)
B = Path("data/generated/atlas/full")
RADIUS = 4000.0
t0 = time.time()
g = read_artifact(str(B / "graph.pkl")); print("graph", round(time.time() - t0), flush=True)
geo = read_artifact(str(B / "geometries.pkl")); print("geoms", round(time.time() - t0), flush=True)
res = json.load(open(B / "splits.resolved.json"))

by_blk = defaultdict(list)
for nid, n in g.nodes.items():
    if n.blk:
        by_blk[n.blk].append(nid)
for b in by_blk:
    by_blk[b].sort(key=lambda s: g.nodes[s].down_m)


def as_line(ln):
    if ln.geom_type == "LineString":
        return ln if len(ln.coords) > 1 else None
    if ln.geom_type == "MultiLineString":
        cs = [c for part in ln.geoms for c in part.coords]
        return LineString(cs) if len(cs) > 1 else None
    return None


def point_at(blk, m):
    best = None
    for s in by_blk[blk]:
        n = g.nodes[s]; ln = geo.get(s)
        if ln is None or ln.is_empty:
            continue
        ln = as_line(ln)
        if ln is None:
            continue
        if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
            f = (m - n.down_m) / (n.up_m - n.down_m) if n.up_m > n.down_m else 0.0
            return ln.interpolate(f * ln.length)
        for mm, e in ((n.down_m, 0.0), (n.up_m, 1.0)):
            if best is None or abs(m - mm) < best[0]:
                best = (abs(m - mm), s, e)
    if best is None:
        return None
    ln = as_line(geo[best[1]])
    return ln.interpolate(best[2], normalized=True)


ids = [nid for nid in g.nodes if geo.get(nid) is not None and not geo[nid].is_empty]
tree = STRtree([geo[i] for i in ids]); print("tree", round(time.time() - t0), flush=True)

keep = set()
for r in res:
    if r.get("anchor_type") in ("area_boundary", "mu_boundary"):
        continue
    blk = r["blk"]
    if blk not in by_blk:
        continue
    keep.update(by_blk[blk])
    P = point_at(blk, r["route_measure"])
    if P is None:
        continue
    for k in tree.query(P.buffer(RADIUS)):
        keep.add(ids[int(k)])
print("near cuts", len(keep), round(time.time() - t0), flush=True)

SERP = "900-005473"
for nid, n in g.nodes.items():
    if (n.wsc or "").startswith(SERP):
        keep.add(nid)

# lakes: every lake node, its neighbours (attributes only)
lakes = {}
for nid, n in g.nodes.items():
    if n.blk:
        continue
    ins = [(g.edges[e].from_node, g.edges[e].kind, g.edges[e].x, g.edges[e].y) for e in g.up_adj.get(nid, [])]
    outs = [(g.edges[e].to_node, g.edges[e].kind, g.edges[e].x, g.edges[e].y) for e in g.down_adj.get(nid, [])]
    lakes[nid] = {"wbk": n.wbk, "wsc": n.wsc, "gnis": n.gnis_id, "name": n.display_name,
                  "through": list(n.through_names), "ins": ins, "outs": outs}

# expand by one hop so every kept node's neighbours have attributes
nb = set()
for nid in keep:
    for e in g.up_adj.get(nid, []):
        nb.add(g.edges[e].from_node)
    for e in g.down_adj.get(nid, []):
        nb.add(g.edges[e].to_node)
attr_nodes = keep | nb
for l in lakes.values():
    for x in l["ins"] + l["outs"]:
        attr_nodes.add(x[0])


def attrs(n):
    return {"blk": n.blk, "wbk": n.wbk, "wsc": n.wsc, "gnis": n.gnis_id, "name": n.display_name,
            "kind": str(getattr(n.kind, "value", n.kind)), "down": n.down_m, "up": n.up_m,
            "len": n.length_m, "order": n.stream_order, "et": list(n.edge_types),
            "lo": n.lower_bound.label if n.lower_bound else "", "hi": n.upper_bound.label if n.upper_bound else "",
            "through": list(n.through_names)}


nodes = {nid: attrs(g.nodes[nid]) for nid in attr_nodes if nid in g.nodes}
geoms = {nid: geo[nid] for nid in keep if geo.get(nid) is not None}
eidx = set()
for nid in keep:
    eidx.update(g.up_adj.get(nid, [])); eidx.update(g.down_adj.get(nid, []))
edges = []
for i in sorted(eidx):
    e = g.edges[i]
    edges.append({"f": e.from_node, "t": e.to_node, "m": e.at_measure, "x": e.x, "y": e.y, "k": e.kind})
pickle.dump({"nodes": nodes, "geoms": geoms, "edges": edges, "cuts": res, "lakes": lakes,
             "keep": sorted(keep)}, open(OUT / "sub.pkl", "wb"), protocol=5)
# global topology, attributes only (for the canal census)
topo_nodes = {nid: (n.blk, n.wbk, n.wsc, n.gnis_id, n.display_name, n.down_m, n.up_m, n.length_m,
                    tuple(n.edge_types), n.stream_order) for nid, n in g.nodes.items()}
topo_edges = [(e.from_node, e.to_node, e.at_measure, e.kind, round(e.x, 1), round(e.y, 1)) for e in g.edges]
pickle.dump({"nodes": topo_nodes, "edges": topo_edges}, open(OUT / "topo.pkl", "wb"), protocol=5)
print("done nodes", len(nodes), "geoms", len(geoms), "edges", len(edges), "lakes", len(lakes),
      round(time.time() - t0), flush=True)
