"""R12 item 5 — braid research (read-only). Reads the R10 braid subgraph dump (atlas/full vintage
2026-10-10, same as live reach run + bundle) and the live straddle.jsonl + bundle section_straddle.
Writes JSON + a sketch-geometry pickle to OUT (argv[1]). No pipeline state touched."""
import json, math, pickle, sqlite3, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
import numpy as np
from shapely.geometry import LineString, Point, MultiPoint
from shapely.strtree import STRtree

OUT = Path(sys.argv[1]); OUT.mkdir(parents=True, exist_ok=True)
D = pickle.load(open("data/generated/scratch_r10/braids_sub.pkl", "rb"))
g, reg, cases, geo = D["graph"], D["reg"], D["cases"], D["geoms"]
STEP = 5.0
WINDOWS = (50.0, 100.0, 200.0)

# ---------------------------------------------------------------- placements (live)
rows = [json.loads(l) for l in open("data/generated/reaches/full/straddle.jsonl")]
db = sqlite3.connect("data/generated/bundle/bundle.sqlite")
bundle = db.execute("select sid, entry_id, rule_id, how, side, share from section_straddle").fetchall()
labels = {(e, r): (t, lab, w) for e, r, t, lab, w in
          db.execute("select entry_id, rule_id, type, label, when_ from rule")}
# sid <-> section id: the bundle's sid for a straddle row is found by (entry, rule, share) match
case_by = {}
for c in cases:
    case_by[(c["entry"], c["rule"] or "", c["seq"], c["piece"])] = c
    case_by.setdefault((c["entry"], c["rule"] or "", None, c["piece"]), c)


class Line:
    """The scoped water's pieces on the cut's blue line, with measure <-> point."""

    def __init__(self, ids):
        self.ids = sorted(i for i in ids if i in geo and geo[i] is not None and geo[i].length)
        self.tree = STRtree([geo[i] for i in self.ids])
        self.by_m = sorted(self.ids, key=lambda i: g.nodes[i].down_m)

    def measure(self, pt):
        near = geo[self.ids[int(self.tree.nearest(pt))]].distance(pt)
        hit = min(self.ids[int(i)] for i in self.tree.query(pt, predicate="dwithin", distance=near + 1e-6)
                  if geo[self.ids[int(i)]].distance(pt) <= near + 1e-6)
        n, ln = g.nodes[hit], geo[hit]
        return n.down_m + ln.project(pt) / ln.length * (n.up_m - n.down_m), near

    def point(self, m):
        best = None
        for i in self.by_m:
            n = g.nodes[i]
            if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
                ln = geo[i]
                f = (m - n.down_m) / (n.up_m - n.down_m) if n.up_m > n.down_m else 0
                return ln.interpolate(f * ln.length), m
            d = min(abs(m - n.down_m), abs(m - n.up_m))
            if best is None or d < best[0]:
                best = (d, i, n.down_m if abs(m - n.down_m) < abs(m - n.up_m) else n.up_m)
        # m falls in a gap (lake) or off the line: nearest available end
        _, i, mm = best
        return self.point(mm)

    def geom(self, lo, hi):
        parts = []
        for i in self.by_m:
            n = g.nodes[i]
            if n.up_m >= lo and n.down_m <= hi:
                parts.append(geo[i])
        return parts


def samples(line, step=STEP):
    k = max(1, int(round(line.length / step)))
    return [line.interpolate((j + 0.5) * line.length / k) for j in range(k)], line.length / k


def n_points(x):
    if x.is_empty:
        return 0, False
    if isinstance(x, Point):
        return 1, False
    if isinstance(x, MultiPoint):
        return len(x.geoms), False
    if hasattr(x, "geoms"):
        pts = sum(1 for q in x.geoms if isinstance(q, Point))
        return pts + sum(1 for q in x.geoms if not isinstance(q, Point)), True
    return 1, True  # a LineString overlap


def tangent_split(line_obj, braid, c, W, R):
    P, _ = line_obj.point(c)
    a, ma = line_obj.point(c - W)
    b, mb = line_obj.point(c + W)
    tx, ty = b.x - a.x, b.y - a.y
    nrm = math.hypot(tx, ty)
    if nrm == 0:
        return None
    tx, ty = tx / nrm, ty / nrm
    nx, ny = -ty, tx
    cutl = LineString([(P.x - R * nx, P.y - R * ny), (P.x + R * nx, P.y + R * ny)])
    x = cutl.intersection(braid)
    k, overlap = n_points(x)
    pts = []
    if not x.is_empty:
        geoms = getattr(x, "geoms", [x])
        for q in geoms:
            if isinstance(q, Point):
                pts.append(q)
    s_at = sorted(braid.project(q) for q in pts)
    # half-plane share (upstream side = +t), over the braid's samples
    sm, w = samples(braid)
    up = sum(1 for q in sm if (q.x - P.x) * tx + (q.y - P.y) * ty > 0) / len(sm)
    dist_P = min((q.distance(P) for q in pts), default=None)
    return {"k": k, "overlap": overlap, "s_at": s_at, "up_share": up, "dist_from_cut_pt": dist_P,
            "t": (tx, ty), "P": (P.x, P.y), "cutline": list(cutl.coords)}


splits_at = {sid: json.loads(at) for sid, at in db.execute("select split_id, at from split")}


def window_of(c, r, uni):
    """A multi-line `between` has no single window: find the split whose cut falls inside the braid's
    projected range, and orient it so the recorded (side, share) is reproduced."""
    piece = r["section_id"]
    best = None
    for sp in c["ex"].get("splits", []):
        for item, km in splits_at.get(sp, []):
            if item not in reg:
                continue
            secs = [s for s in reg[item].section_ids if s in g.nodes and g.nodes[s].blk]
            bl = defaultdict(float)
            for s in secs:
                bl[g.nodes[s].blk] += g.nodes[s].length_m
            blk = max(bl, key=bl.get)
            ids = {s for s in uni | set(secs) if g.nodes.get(s) and g.nodes[s].blk == blk}
            bounds = sorted({g.nodes[s].down_m for s in ids} | {g.nodes[s].up_m for s in ids})
            cut = min(bounds, key=lambda b: abs(b - km * 1000))
            L = Line(ids)
            sm, _ = samples(geo[piece])
            ms = np.array([L.measure(q)[0] for q in sm])
            if not (ms.min() < cut < ms.max()):
                continue
            above = float(np.mean(ms > cut))
            want = r["share"] if r["side"] == "in" else 1 - r["share"]
            # in = above the cut, or in = below
            lo_hi = (cut, math.inf) if abs(above - want) <= abs((1 - above) - want) else (-math.inf, cut)
            best = (blk, *lo_hi)
    if best is None:
        print(f"no window for {c['entry']} {c['rule']} {piece} share {r['share']}")
        return None
    print("window_of", c["rule"], piece, best)
    return best


# ------------------------------------------------------------- one analysis per (piece, cut)
results = {}
sketch = {}
place_out = []
for r in rows:
    if r["how"] != "projected":
        continue
    rid = r.get("rule_id") or ""
    c = case_by.get((r["entry_id"], rid, r.get("extent"), r["section_id"])) or \
        case_by.get((r["entry_id"], rid, None, r["section_id"]))
    if c is None:
        c0 = next((x for x in cases if x["entry"] == r["entry_id"] and not x["rule"]
                   and x["piece"] == r["section_id"] and x["seq"] == r.get("extent")), None)
        if c0 is None:
            print("NO CASE", r)
            continue
        c = c0
    uni = set()
    for it in (c["scope"] or c["covered"]):
        uni |= set(reg[it].section_ids)
    if c["window"] is not None:
        blk, lo, hi = c["window"]
    else:
        wnd = window_of(c, r, uni)
        if wnd is None:
            continue
        blk, lo, hi = wnd
    line_ids = {s for s in uni if g.nodes.get(s) and g.nodes[s].blk == blk}
    if not line_ids:
        line_ids = {s for s in g.nodes if g.nodes[s].blk == blk and s in geo}
    L = Line(line_ids)
    piece = r["section_id"]
    braid = geo[piece]
    sm, w = samples(braid)
    ms, ds = zip(*(L.measure(q) for q in sm))
    ms, ds = np.array(ms), np.array(ds)
    share_in = float(np.mean((ms >= lo) & (ms <= hi)))
    cuts = [x for x in (lo, hi) if math.isfinite(x) and ms.min() - 2 < x < ms.max() + 2]
    key = (piece, blk, tuple(round(x, 1) for x in cuts))
    if key not in results:
        res = {"piece": piece, "blk": blk, "len": braid.length, "cuts": cuts,
               "name": g.nodes[piece].display_name, "wsc": g.nodes[piece].wsc,
               "main_wsc": g.nodes[sorted(line_ids)[0]].wsc,
               "names": [t.name + "/" + t.source.value for t in g.nodes[piece].name_tuples],
               "m_range": (float(ms.min()), float(ms.max())), "max_off": float(ds.max()),
               "mean_off": float(ds.mean())}
        # endpoints: FWA lines run mouth -> upward, coords[0] the downstream end
        e0, e1 = Point(braid.coords[0]), Point(braid.coords[-1])
        res["end_down"] = L.measure(e0)
        res["end_up"] = L.measure(e1)
        # foreign inflow into the braid (why the prune kept it), and the off-line component
        ins = [g.edges[i].from_node for i in g.up_adj.get(piece, [])]
        res["inflows"] = [(x, g.nodes[x].display_name, g.nodes[x].wsc, g.nodes[x].blk) for x in ins if x in g.nodes]
        outs = [g.edges[i].to_node for i in g.down_adj.get(piece, [])]
        res["outflows"] = [(x, g.nodes[x].display_name, g.nodes[x].blk) for x in outs if x in g.nodes]
        comp, stack = {piece}, [piece]
        while stack:
            x = stack.pop()
            nb = {g.edges[i].to_node for i in g.down_adj.get(x, [])} | {g.edges[i].from_node for i in g.up_adj.get(x, [])}
            for y in nb & uni:
                if y not in comp and g.nodes[y].blk != blk:
                    comp.add(y); stack.append(y)
        res["component"] = sorted(comp)
        # component junctions with the main line: component piece endpoints within 15 m of it
        js = []
        crange = [math.inf, -math.inf]
        for x in comp:
            if x not in geo:
                continue
            for pt in (Point(geo[x].coords[0]), Point(geo[x].coords[-1])):
                m, d = L.measure(pt)
                crange = [min(crange[0], m), max(crange[1], m)]
                if d < 15:
                    js.append(m)
        res["junctions"] = sorted(js)
        res["comp_range"] = crange
        per_cut = []
        for cut in cuts:
            pc = {"c": cut, "is": "lo" if cut == lo else "hi"}
            # A4 projection-crossing split
            sgn = np.sign(ms - cut)
            sgn = sgn[sgn != 0]
            pc["proj_crossings"] = int(np.sum(sgn[1:] != sgn[:-1]))
            first = int(np.argmax(np.diff(np.sign(ms - cut)) != 0)) if pc["proj_crossings"] else None
            pc["proj_split_s"] = (first + 1) * w if first is not None else None
            pc["share_above_proj"] = float(np.mean(ms > cut))
            # A1 snap: shift needed to put the piece / the component wholly on one side
            lo_p, hi_p = float(ms.min()), float(ms.max())
            pc["snap_piece"] = min(cut - lo_p, hi_p - cut)
            pc["snap_piece_dir"] = "down" if cut - lo_p <= hi_p - cut else "up"
            jl = [j for j in js] or [crange[0], crange[1]]
            jlo, jhi = min(min(jl), crange[0]), max(max(jl), crange[1])
            pc["snap_comp"] = min(cut - jlo, jhi - cut) if jlo < cut < jhi else 0.0
            pc["snap_comp_dir"] = "down" if cut - jlo <= jhi - cut else "up"
            pc["junction_nearest"] = min((abs(j - cut) for j in js), default=None)
            R = max(3000.0, 3 * res["max_off"])
            pc["tangent"] = {}
            for W in WINDOWS:
                t = tangent_split(L, braid, cut, W, R)
                pc["tangent"][W] = t
            per_cut.append(pc)
        res["per_cut"] = per_cut
        results[key] = res
        sketch[piece] = {"braid": list(braid.coords), "comp": {x: list(geo[x].coords) for x in comp if x in geo},
                         "main": [list(q.coords) for q in L.geom(res["m_range"][0] - 1500, res["m_range"][1] + 1500)],
                         "cuts": per_cut}
    res = results[key]
    which = ("bundle" if any(b[1] == r["entry_id"] and (b[2] or "") == rid and b[3] == "projected"
                              and abs((b[5] or 0) - r["share"]) < 1e-6 for b in bundle) else "share1")
    t, lab, wn = labels.get((r["entry_id"], rid), ("(entry scope)", "the entry's own water (which row applies)", None))
    place_out.append({"entry": r["entry_id"], "rule": rid, "extent": r.get("extent"), "piece": piece,
                      "side": r["side"], "share_rec": r["share"], "share_in_recomputed": share_in,
                      "window": (lo, hi), "key": [piece, blk, list(key[2])], "set": which,
                      "type": t, "label": lab, "when": wn, "splits": c["ex"].get("splits")})

json.dump({"braids": [dict(v, key=[k[0], k[1], list(k[2])]) for k, v in results.items()],
           "placements": place_out}, open(OUT / "braids_r12.json", "w"), indent=1, default=str)
pickle.dump(sketch, open(OUT / "sketch.pkl", "wb"))
print("braid-cuts", len(results), "placements", len(place_out),
      "bundle", sum(p["set"] == "bundle" for p in place_out))
