"""Why do 6 channels touch host pieces on both sides of a cut the line does not cross?
For each edge between the channel and its host line: the edge's stored coordinate, the channel end it
should be (by at_measure), and both projected onto the host line (measure) vs the cut."""
import sys, json
sys.path.insert(0, ".")
from shapely.geometry import Point
from pipeline.common.io.serialize import read_artifact
g = read_artifact("data/generated/atlas/full/graph.pkl")
geo = read_artifact("data/generated/atlas/full/geometries.pkl")
CASES = [("360224406", "360879335", 58206.0), ("354087999", "354152994", 36656.37), ("359002030", "359571076", 20884.0),
         ("355992048", "356364114", 111136.98), ("360220099", "360887278", 123961.24), ("360225339", "360879335", 19630.0),
         ("360245483", "360881231", 19397.0)]
by = {}
for nid, n in g.nodes.items():
    if n.blk:
        by.setdefault(n.blk, []).append(nid)
def host_measure(hblk, pt):
    best = None
    for s in by[hblk]:
        ln = geo.get(s)
        if ln is None or not ln.length: continue
        d = ln.distance(pt)
        if best is None or d < best[0]:
            n = g.nodes[s]
            best = (d, s, n.down_m + ln.project(pt) / ln.length * (n.up_m - n.down_m))
    return round(best[2], 1), round(best[0], 1)
for ch, hblk, M in CASES:
    print("==", ch, "host", hblk, "cut", M)
    hp = set(by[hblk])
    for s in sorted(by[ch], key=lambda x: g.nodes[x].down_m):
        n = g.nodes[s]; ln = geo.get(s)
        if ln is None: continue
        e0, e1 = Point(ln.coords[0]), Point(ln.coords[-1])
        print("  piece", s, round(n.down_m), round(n.up_m), "end0->host", host_measure(hblk, e0), "end1->host", host_measure(hblk, e1))
        for adj, frm in ((g.down_adj, False), (g.up_adj, True)):
            for i in adj.get(s, []):
                e = g.edges[i]
                other = e.from_node if frm else e.to_node
                if other not in hp: continue
                o = g.nodes[other]
                xy = host_measure(hblk, Point(e.x, e.y)) if (e.x or e.y) else None
                side = "below" if o.up_m <= M + .01 else "above"
                print("    ", "in-from" if frm else "out-to", other, side, "kind", e.kind, "at", round(e.at_measure, 1), "edge xy->host", xy)
