"""R13 item R — census of the UNIFORM FULL-SPLIT sweep (read-only on atlas/full, 2026-10-10 vintage).

For every cut a rule can bind (curated, non-area; and gauge), take the host channel's cut measure,
draw the perpendicular to the averaged tangent (±50 m = today's anchors._PERP_WINDOW_M, ±100 m = the
r12 proposal), and test EVERY other blue line with the host's WSC (the side channels; today's sweep
only tests the split's gnis/wsc target scope, which leaves unnamed side channels out).
Per (split, host, channel) it records: crossing count, the measure on the channel, distance to the
channel's ends and to its existing boundaries (sliver / pickup), whether it is already cut by that
split, and, for channels the line does NOT cross, whether the channel nevertheless touches host pieces
on both sides of the cut (a junction-at-cut "false straddle"). Writes census_r13.json to argv[1]."""
import json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from shapely.geometry import LineString, Point
from pipeline.common.io.serialize import read_artifact

OUT = Path(sys.argv[1]); OUT.mkdir(parents=True, exist_ok=True)
B = Path("data/generated/atlas/full")
t0 = time.time()
g = read_artifact(str(B / "graph.pkl"))
print("graph", round(time.time() - t0), flush=True)
geo = read_artifact(str(B / "geometries.pkl"))
print("geoms", round(time.time() - t0), flush=True)
res = json.load(open(B / "splits.resolved.json"))
bound = set()
for l in open("data/generated/reaches/full/rule_extent.jsonl"):
    bound |= set(json.loads(l).get("splits") or [])

by_blk = defaultdict(list)
by_wsc = defaultdict(set)
for nid, n in g.nodes.items():
    if n.blk and nid in geo and geo[nid] is not None and not geo[nid].is_empty:
        by_blk[n.blk].append(nid)
        by_wsc[n.wsc].add(n.blk)
for b in by_blk:
    by_blk[b].sort(key=lambda s: g.nodes[s].down_m)
print("indexed", len(by_blk), round(time.time() - t0), flush=True)


def point_at(blk, m):
    best = None
    for s in by_blk[blk]:
        n = g.nodes[s]
        if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
            ln = geo[s]
            f = (m - n.down_m) / (n.up_m - n.down_m) if n.up_m > n.down_m else 0.0
            return ln.interpolate(f * ln.length)
        for mm, end in ((n.down_m, 0.0), (n.up_m, 1.0)):
            d = abs(m - mm)
            if best is None or d < best[0]:
                best = (d, s, end)
    _, s, end = best
    return geo[s].interpolate(end, normalized=True)


def cutline(blk, m, W, R=1500.0):
    a, b, c = point_at(blk, m - W), point_at(blk, m + W), point_at(blk, m)
    dx, dy = b.x - a.x, b.y - a.y
    h = math.hypot(dx, dy)
    if h == 0:
        return None, None, c
    tx, ty = dx / h, dy / h
    nx, ny = -ty, tx
    return LineString([(c.x - R * nx, c.y - R * ny), (c.x + R * nx, c.y + R * ny)]), (tx, ty), c


def side(t, c, p):
    return (p.x - c.x) * t[0] + (p.y - c.y) * t[1]


def pts(x):
    if x.is_empty:
        return []
    if x.geom_type == "Point":
        return [x]
    if hasattr(x, "geoms"):
        o = []
        for q in x.geoms:
            o += pts(q)
        return o
    if x.geom_type == "LineString":
        return [Point(x.coords[0]), Point(x.coords[-1])]
    return []


hosts = {}
already = defaultdict(set)
for r in res:
    sid = r["split_id"]
    braid = "braid cut" in (r.get("concern") or "")
    src = r.get("source")
    at = r.get("anchor_type")
    if at in ("area_boundary", "mu_boundary"):
        continue
    if braid:
        already[sid].add(r["blk"])
        continue
    if src not in ("curated", "gauge"):
        continue
    hosts[(sid, r["blk"], round(r["route_measure"], 2))] = (src, at)
print("hosts", len(hosts), flush=True)

rows = []
for (sid, hblk, M), (src, at) in sorted(hosts.items()):
    if hblk not in by_blk:
        continue
    hwsc = g.nodes[by_blk[hblk][0]].wsc
    hpieces = set(by_blk[hblk])
    lines = {}
    for W in (50.0, 100.0):
        lines[W] = cutline(hblk, M, W)
    P = lines[100.0][2]
    for oblk in sorted(by_wsc.get(hwsc, ()) - {hblk}):
        ps = by_blk[oblk]
        first, last = ps[0], ps[-1]
        e0 = geo[first].interpolate(0.0)
        e1 = geo[last].interpolate(1.0, normalized=True)
        near = min(e0.distance(P), e1.distance(P))
        # cheap gate: the channel must come within 1.6 km of the cut point
        if min(geo[s].distance(P) for s in ps) > 1600:
            continue
        rec = {"split": sid, "src": src, "anchor": at, "bound": sid in bound, "host": hblk, "M": M,
               "ch": oblk, "ch_len": sum(g.nodes[s].length_m or 0 for s in ps), "n_pieces": len(ps),
               "already": oblk in already.get(sid, ()), "end_dist_to_P": near}
        for W, (cl, t, c) in lines.items():
            if cl is None:
                rec[f"w{int(W)}"] = None
                continue
            s0, s1 = side(t, c, e0), side(t, c, e1)
            spans = (s0 > 0 > s1) or (s0 < 0 < s1)
            hits = []
            for s in ps:
                for q in pts(cl.intersection(geo[s])):
                    n, ln = g.nodes[s], geo[s]
                    mm = n.down_m + (ln.project(q) / ln.length) * (n.up_m - n.down_m) if ln.length else n.down_m
                    hits.append((round(mm, 1), round(q.distance(P), 1)))
            hits = sorted(set(hits))
            # distance from the nearest hit to the channel's ends and to its existing piece bounds
            bnds = sorted({g.nodes[s].down_m for s in ps} | {g.nodes[s].up_m for s in ps})
            lo_end, hi_end = g.nodes[first].down_m, g.nodes[last].up_m
            hit_dm = []
            for s in ps:
                for q in pts(cl.intersection(geo[s])):
                    h = min(by_blk[hblk], key=lambda x: geo[x].distance(q)); hn = g.nodes[h]; hl = geo[h]
                    hit_dm.append(round(hn.down_m + hl.project(q) / hl.length * (hn.up_m - hn.down_m) - M, 1))
            if hits:
                best = min(hits, key=lambda h: h[1])
                to_end = min(best[0] - lo_end, hi_end - best[0])
                to_bnd = min(abs(best[0] - b) for b in bnds)
            else:
                best, to_end, to_bnd = None, None, None
            # which side of the line the channel's body lies on (5 m samples)
            up = tot = 0
            for s in ps:
                ln = geo[s]
                k = max(1, int(ln.length // 5))
                for j in range(k):
                    q = ln.interpolate((j + 0.5) * ln.length / k)
                    tot += 1
                    up += side(t, c, q) > 0
            rec[f"w{int(W)}"] = {"hit_host_dm": hit_dm, "spans": spans, "k": len(hits), "hit": best, "to_end": to_end,
                                 "to_bnd": to_bnd, "up_share": round(up / tot, 3) if tot else None}
        # topology: host pieces this channel's pieces touch, below / above M
        touch_lo = touch_hi = False
        for s in ps:
            nb = {g.edges[i].to_node for i in g.down_adj.get(s, [])} | \
                 {g.edges[i].from_node for i in g.up_adj.get(s, [])}
            for y in nb & hpieces:
                n = g.nodes[y]
                if n.up_m <= M + 0.01:
                    touch_lo = True
                if n.down_m >= M - 0.01:
                    touch_hi = True
        rec["touch_lo"], rec["touch_hi"] = touch_lo, touch_hi
        rows.append(rec)
    # COMPONENTS of the candidate channels at this cut (same WSC, joined to each other), and what each
    # component touches on the host line: a component touching both sides is a topological straddler
    mine = [r for r in rows if r["split"] == sid and r["host"] == hblk and r["M"] == M]
    chs = set(by_wsc.get(hwsc, ())) - {hblk}            # the WHOLE network, not just what is near
    piece_ch = {s: b for b in chs for s in by_blk[b]}
    adj = defaultdict(set)
    tl = defaultdict(lambda: [False, False])
    for s, b in piece_ch.items():
        for i in g.down_adj.get(s, []) + g.up_adj.get(s, []):
            e = g.edges[i]
            o = e.to_node if e.from_node == s else e.from_node
            if o in piece_ch and piece_ch[o] != b:
                adj[b].add(piece_ch[o])
            elif o in hpieces:
                n = g.nodes[o]
                if n.up_m <= M + 0.01:
                    tl[b][0] = True
                if n.down_m >= M - 0.01:
                    tl[b][1] = True
    comp_of = {}
    for b in sorted({r["ch"] for r in mine}):
        if b in comp_of:
            continue
        stack, comp = [b], set()
        while stack:
            x = stack.pop()
            if x in comp:
                continue
            comp.add(x)
            stack.extend(adj[x] - comp)
        lo = any(tl[x][0] for x in comp); hi = any(tl[x][1] for x in comp)
        for x in comp:
            comp_of[x] = (min(comp), len(comp), lo, hi)
    for r in mine:
        r["comp"], r["comp_n"], r["comp_lo"], r["comp_hi"] = comp_of[r["ch"]]
print("rows", len(rows), round(time.time() - t0), flush=True)

# the channels today's sweep already cut, with their section ids (for the reach check)
cut_now = {}
for sid, blks in already.items():
    for b in blks:
        cut_now.setdefault(sid, {})[b] = by_blk.get(b, [])
json.dump({"rows": rows, "cut_now": cut_now}, open(OUT / "census_r13.json", "w"), default=float)
print("done", round(time.time() - t0), flush=True)
