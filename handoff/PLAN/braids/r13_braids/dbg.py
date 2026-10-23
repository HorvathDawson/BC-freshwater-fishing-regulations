import pickle, math, sys
sys.path.insert(0, ".")
from shapely.geometry import Point, LineString
D = pickle.load(open("data/generated/scratch_r10/braids_sub.pkl", "rb"))
g, geo = D["graph"], D["geoms"]
def host_ids(blk): return sorted([s for s in g.nodes if g.nodes[s].blk == blk and geo.get(s) is not None], key=lambda s: g.nodes[s].down_m)
def pt(ids, m):
    for s in ids:
        n = g.nodes[s]
        if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
            ln = geo[s]; return ln.interpolate((m - n.down_m) / max(n.up_m - n.down_m, 1e-9) * ln.length)
def hostm(ids, p):
    s = min(ids, key=lambda s: geo[s].distance(p)); n = g.nodes[s]; ln = geo[s]
    return round(n.down_m + ln.project(p) / ln.length * (n.up_m - n.down_m)), round(ln.distance(p))
for name, hblk, M, pieces in [("Morice Lamprey", "360885316", 54533.0, ["360213355:609", "360213355:0", "360213357:0", "360213358:0", "360213366:0", "360213368:0"]),
                              ("Peace Halfway d5000", "359572348", 1632426.0, ["359012853:0", "359012862:0"]),
                              ("Ash", None, None, ["354088143:161"])]:
    print("==", name)
    if hblk:
        ids = host_ids(hblk)
        a, b, c = pt(ids, M - 100), pt(ids, M + 100), pt(ids, M)
        dx, dy = b.x - a.x, b.y - a.y; h = math.hypot(dx, dy); t = (dx / h, dy / h)
        cl = LineString([(c.x + 1500 * t[1], c.y - 1500 * t[0]), (c.x - 1500 * t[1], c.y + 1500 * t[0])])
    for p in pieces:
        if p not in g.nodes: print(p, "missing"); continue
        n = g.nodes[p]; ln = geo.get(p)
        info = ""
        if hblk and ln is not None:
            e0, e1 = Point(ln.coords[0]), Point(ln.coords[-1])
            sd = lambda q: round((q.x - c.x) * t[0] + (q.y - c.y) * t[1])
            info = f"ends->host {hostm(ids, e0)} {hostm(ids, e1)} side {sd(e0)} {sd(e1)} hits {len(list(getattr(cl.intersection(ln), 'geoms', [cl.intersection(ln)]))) if not cl.intersection(ln).is_empty else 0}"
        print(p, n.blk, round(n.down_m), round(n.up_m), n.wsc, info)
        for i in g.down_adj.get(p, []):
            e = g.edges[i]; o = g.nodes.get(e.to_node)
            print("    out-to", e.to_node, o.blk if o else "", round(o.down_m) if o else "", round(o.up_m) if o else "", "at", round(e.at_measure), e.kind)
        for i in g.up_adj.get(p, []):
            e = g.edges[i]; o = g.nodes.get(e.from_node)
            print("    in-from", e.from_node, o.blk if o else "", round(o.down_m) if o else "", round(o.up_m) if o else "", "at", round(e.at_measure), e.kind)
