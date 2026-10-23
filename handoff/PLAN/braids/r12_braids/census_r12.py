"""R12 item 5 — census: every side channel of a named river that a cut on its main line would split
(topologically: its off-line component touches main-line pieces on both sides of the cut; and
geometrically: its own projected measure range spans the cut). Read-only on atlas/full."""
import json, math, sys, time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, ".")
import numpy as np
from shapely.geometry import LineString, Point
from shapely.strtree import STRtree
from pipeline.common.io.serialize import read_artifact
from pipeline.atlas.registry import load_registry

OUT = Path(sys.argv[1])
B = Path("data/generated/atlas/full")
t0 = time.time()
reg = load_registry(str(B / "registry.json"))
g = read_artifact(str(B / "graph.pkl"))
geo = read_artifact(str(B / "geometries.pkl"))
print("loaded", round(time.time() - t0), flush=True)


def bkind(b):
    if b is None:
        return "end"
    i = b.boundary_id
    if i.startswith("split:gauge__"):
        return "gauge"
    if i.startswith("split:length:"):
        return "length"
    if i.startswith("lake:"):
        return "lake"
    k = getattr(b.kind, "value", str(b.kind))
    if k in ("mu", "area", "border"):
        return k
    return "curated"


rows = []
seen_pairs = set()
for iid, it in sorted(reg.items()):
    if getattr(it, "kind", "") != "stream":
        continue
    secs = [s for s in it.section_ids if s in g.nodes and g.nodes[s].blk and s in geo]
    if len(secs) < 2:
        continue
    bl = defaultdict(float)
    for s in secs:
        bl[g.nodes[s].blk] += g.nodes[s].length_m or 0
    main = max(bl, key=lambda b: (bl[b], b))
    on = sorted((s for s in secs if g.nodes[s].blk == main), key=lambda s: g.nodes[s].down_m)
    off = [s for s in secs if g.nodes[s].blk != main]
    if not off or len(on) < 2:
        continue
    cuts = {}
    for a, b in zip(on, on[1:]):
        na, nb = g.nodes[a], g.nodes[b]
        if abs(na.up_m - nb.down_m) < 0.01:
            cuts[na.up_m] = bkind(na.upper_bound)
    if not cuts:
        continue
    tree = STRtree([geo[s] for s in on])

    def measure(pt):
        i = int(tree.nearest(pt))
        s = on[i]
        n, ln = g.nodes[s], geo[s]
        return n.down_m + ln.project(pt) / ln.length * (n.up_m - n.down_m), ln.distance(pt)

    offset = set(off)
    side_of = {}
    for s in on:
        side_of[s] = (g.nodes[s].down_m, g.nodes[s].up_m)
    # components of off-line pieces (inside this item) and the main-line pieces they touch
    comp_of, comps = {}, []
    for s in sorted(off):
        if s in comp_of:
            continue
        comp, stack = {s}, [s]
        while stack:
            x = stack.pop()
            nb = {g.edges[i].to_node for i in g.down_adj.get(x, [])} | {g.edges[i].from_node for i in g.up_adj.get(x, [])}
            for y in nb & offset:
                if y not in comp:
                    comp.add(y); stack.append(y)
        for x in comp:
            comp_of[x] = len(comps)
        touch = set()
        for x in comp:
            nb = {g.edges[i].to_node for i in g.down_adj.get(x, [])} | {g.edges[i].from_node for i in g.up_adj.get(x, [])}
            touch |= {y for y in nb if y in side_of}
        comps.append((comp, touch))
    prof = {}
    for s in off:
        ln = geo[s]
        if not ln.length:
            continue
        k = max(2, int(ln.length // 10))
        ms, ds = [], []
        for j in range(k):
            m, d = measure(ln.interpolate((j + 0.5) * ln.length / k))
            ms.append(m); ds.append(d)
        prof[s] = (np.array(ms), max(ds), ln.length)
    for ci, (comp, touch) in enumerate(comps):
        for c, kind in cuts.items():
            below = any(side_of[y][1] <= c + 0.01 for y in touch)
            above = any(side_of[y][0] >= c - 0.01 for y in touch)
            topo = below and above
            for s in sorted(comp):
                if s not in prof:
                    continue
                ms, mo, L = prof[s]
                a, b = ms.min(), ms.max()
                geom = a + 5 < c < b - 5
                if not (topo or geom):
                    continue
                if not (a - 50 < c < b + 50):
                    continue
                share_above = float(np.mean(ms > c))
                rows.append({"item": iid, "name": it.name, "piece": s, "cut": c, "kind": kind, "topo": topo,
                             "geom": geom, "len": L, "max_off": float(mo),
                             "share_above": share_above, "snap": float(min(c - a, b - c)) if geom else 0.0,
                             "comp": len(comp)})
print("rows", len(rows), round(time.time() - t0), flush=True)
json.dump(rows, open(OUT / "census.json", "w"), default=lambda o: o.item())
