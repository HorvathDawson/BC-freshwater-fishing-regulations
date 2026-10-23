"""R13 item R — end-to-end simulation of the three uniform policies on the R10 braid subgraph
(data/generated/scratch_r10/braids_sub.pkl, atlas/full 2026-10-10 vintage). Read-only.

FULL SPLIT: every channel with the host's WSC that the ±100 m averaged-tangent perpendicular crosses
(ends on opposite sides, `anchors._spans`) is cut at the crossing nearest the cut point, with the cut's
OWN split id, by the real sectionizer (`split_graph_at`). A crossing within SLIVER_M of the channel's
end is not cut: the channel's junction there is re-homed to the host piece on the channel's side.
Then the reach runs four ways:
  today      : original graph, E15 majority on (reproduces live)
  split+E15  : split graph, today's reach code unchanged
  split-bare : split graph, E15 off, nothing else (what breaks without a reach change)
  split-new  : split graph, E15 off, + the generic rule: a piece bounded by a window cut takes its
               side from that bound (far side -> outside) and adjacency across a window cut is dropped.
Truth for every braid sample: the side of the ±100 m tangent line (r12 showed it never disagrees with
the projection on the majority side). Writes sim_r13.json next to this script."""
import copy, json, math, pickle, sys
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, ".")
from shapely.geometry import LineString, Point
from pipeline.atlas.reach import extent as X
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.common.models import AnchorType, SplitPoint

HERE = Path(__file__).parent
D = pickle.load(open("data/generated/scratch_r10/braids_sub.pkl", "rb"))
G0, REG0, CASES, GEO0 = D["graph"], D["reg"], D["cases"], D["geoms"]
W, R, SLIVER_M = 100.0, 1500.0, 10.0
GUARD_M = 1e9
TOPO_GATE = (sys.argv[1] == 'topo') if len(sys.argv) > 1 else False
INF = float("inf")


def attach_geoms(g, geo):
    setattr(g, "_position_geoms", ({}, geo))


def by_blk_of(g, geo):
    out = defaultdict(list)
    for nid, n in g.nodes.items():
        if n.blk and geo.get(nid) is not None and geo[nid].length:
            out[n.blk].append(nid)
    for b in out:
        out[b].sort(key=lambda s: g.nodes[s].down_m)
    return out


def point_at(g, geo, ids, m):
    best = None
    for s in ids:
        n = g.nodes[s]
        if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
            ln = geo[s]
            f = (m - n.down_m) / (n.up_m - n.down_m) if n.up_m > n.down_m else 0.0
            return ln.interpolate(f * ln.length)
        for mm, e in ((n.down_m, 0.0), (n.up_m, 1.0)):
            if best is None or abs(m - mm) < best[0]:
                best = (abs(m - mm), s, e)
    return geo[best[1]].interpolate(best[2], normalized=True)


def tangent(g, geo, ids, m):
    a, b, c = point_at(g, geo, ids, m - W), point_at(g, geo, ids, m + W), point_at(g, geo, ids, m)
    dx, dy = b.x - a.x, b.y - a.y
    h = math.hypot(dx, dy)
    tx, ty = dx / h, dy / h
    return LineString([(c.x + R * ty, c.y - R * tx), (c.x - R * ty, c.y + R * tx)]), (tx, ty), c


def side(t, c, p):
    return (p.x - c.x) * t[0] + (p.y - c.y) * t[1]


def pts(x):
    if x.is_empty:
        return []
    if x.geom_type == "Point":
        return [x]
    if hasattr(x, "geoms"):
        return [q for y in x.geoms for q in pts(y)]
    return [Point(x.coords[0]), Point(x.coords[-1])]


# ------------------------------------------------------------------ the cuts the cases bind
def refs_of(reg, scope, sid):
    out = {f"split:{sid}"}
    for i in scope:
        it = reg.get(i)
        for b in (it.boundaries if it else ()):
            if b.id == sid or ({sid, f"split:{sid}"} & set(b.aliases or ())):
                if b.ref:
                    out.add(b.ref)
                out.update(b.aliases or ())
    return out


cuts = {}            # sid -> (host blk, M, refs)
for c in CASES:
    uni = set()
    for it in c["scope"]:
        if it in REG0:
            uni |= set(REG0[it].section_ids)
    for sid in c["ex"].get("splits") or []:
        if sid in cuts:
            continue
        refs = refs_of(REG0, c["scope"], sid)
        at = X._cut_at(G0, refs, uni)
        if at is not None:
            cuts[sid] = (at[0], at[1], refs)
print("cuts", len(cuts))


CUTLINE = {}
CONSIST = {"on": True, "moved": []}


def sweep(g, geo):
    """The sweep: SplitPoints on every crossed same-WSC channel, plus junction re-homes."""
    bb = by_blk_of(g, geo)
    by_wsc = defaultdict(set)
    for b, ids in bb.items():
        by_wsc[g.nodes[ids[0]].wsc].add(b)
    sps, rehome, log = [], [], []
    for sid, (hblk, M, refs) in sorted(cuts.items()):
        if hblk not in bb:
            continue
        cl, t, c = tangent(g, geo, bb[hblk], M)
        CUTLINE[sid] = (t, c, {hblk: (M, False)})
        hset = set(bb[hblk])
        # THE NETWORK DECIDES WHETHER, THE LINE DECIDES WHERE: only a channel network that touches the
        # host on BOTH sides of the cut is split; a network touching one side is wholly on that side.
        allch = by_wsc[g.nodes[bb[hblk][0]].wsc] - {hblk}
        pc = {x: b for b in allch for x in bb[b]}
        adj, tl = defaultdict(set), defaultdict(set)
        for x, b in pc.items():
            for i in g.down_adj.get(x, []) + g.up_adj.get(x, []):
                e = g.edges[i]
                o = e.to_node if e.from_node == x else e.from_node
                if o in pc and pc[o] != b:
                    adj[b].add(pc[o])
                elif o in hset:
                    tl[b].add("lo" if g.nodes[o].up_m <= M + .01 else "hi")
        straddling = set()
        seen = set()
        for b in sorted(allch):
            if b in seen:
                continue
            comp, st = set(), [b]
            while st:
                x = st.pop()
                if x in comp:
                    continue
                comp.add(x); st.extend(adj[x] - comp)
            seen |= comp
            if {"lo", "hi"} <= set().union(*(tl[x] for x in comp)):
                straddling |= comp
        for ob in sorted(straddling if TOPO_GATE else allch):
            ids = bb[ob]
            if min(geo[s].distance(c) for s in ids) > R:
                continue
            e0 = geo[ids[0]].interpolate(0.0)
            e1 = geo[ids[-1]].interpolate(1.0, normalized=True)
            s0, s1 = side(t, c, e0), side(t, c, e1)
            hits = []
            for s in ids:
                n, ln = g.nodes[s], geo[s]
                for q in pts(cl.intersection(ln)):
                    hits.append((n.down_m + ln.project(q) / ln.length * (n.up_m - n.down_m), q, s))
            spans = (s0 > 0 > s1) or (s0 < 0 < s1)
            lo_end, hi_end = g.nodes[ids[0]].down_m, g.nodes[ids[-1]].up_m
            def _hdm(q):
                h = min(bb[hblk], key=lambda x: geo[x].distance(q)); hn = g.nodes[h]; hl = geo[h]
                return abs(hn.down_m + hl.project(q) / hl.length * (hn.up_m - hn.down_m) - M)
            hits = [h for h in hits if _hdm(h[1]) <= GUARD_M]
            if spans and hits:
                m, q, s = min(hits, key=lambda h: h[1].distance(c))
                if min(m - lo_end, hi_end - m) > SLIVER_M:
                    # orientation: does the channel run upstream (same way as the host) at the hit?
                    ln, n = geo[s], g.nodes[s]
                    a = ln.project(q)
                    p0, p1 = ln.interpolate(max(a - 5, 0)), ln.interpolate(min(a + 5, ln.length))
                    flip = s0 > 0          # its measure-0 end lies on the cut's upstream side
                    sps.append(SplitPoint(split_id=sid, blk=ob, route_measure=float(m), fid="", label=sid,
                                          anchor_type=AnchorType.point, source="curated",
                                          concern="braid cut by the perpendicular line at the split"))
                    log.append({"split": sid, "ch": ob, "m": round(m, 1), "flip": flip, "k": len(hits)})
                    CUTLINE[sid][2][ob] = (float(m), flip)
                    continue
            # not cut: does it touch the host on both sides? then re-home its junction to its side
            def _hm(q):
                h = min(bb[hblk], key=lambda x: geo[x].distance(q)); hn = g.nodes[h]; hl = geo[h]
                return hn.down_m + hl.project(q) / hl.length * (hn.up_m - hn.down_m)
            ends_m = [_hm(e0), _hm(e1)]
            body_up = 1.0 if min(ends_m) >= M else 0.0 if max(ends_m) <= M else 0.5
            touch = defaultdict(list)
            for s in ids:
                for i in g.down_adj.get(s, []) + g.up_adj.get(s, []):
                    e = g.edges[i]
                    o = e.to_node if e.from_node == s else e.from_node
                    if o in hset:
                        touch["lo" if g.nodes[o].up_m <= M + .01 else "hi"].append(i)
            if touch["lo"] and touch["hi"] and body_up != 0.5:
                want = "hi" if body_up > 0.5 else "lo"
                rehome.append((sid, ob, want, touch["hi" if want == "lo" else "lo"]))
                log.append({"split": sid, "ch": ob, "rehome_to": want, "edges": len(touch["hi" if want == "lo" else "lo"])})
    return sps, rehome, log


def apply_split(g0, geo0, reg0):
    g = copy.deepcopy(g0)
    geo = dict(geo0)
    sps, rehome, log = sweep(g, geo)
    bb = by_blk_of(g, geo)
    # junction re-homes first (on the unsplit graph): move the edge end on the host to the piece
    # adjacent to the cut on the channel's side
    for sid, ob, want, edges in rehome:
        hblk, M, _ = cuts[sid]
        tgt = next(s for s in bb[hblk] if (abs(g.nodes[s].down_m - M) < .01 if want == "hi"
                                             else abs(g.nodes[s].up_m - M) < .01))
        for i in edges:
            e = g.edges[i]
            if g.nodes[e.to_node].blk == hblk:
                g.edges[i] = replace(e, to_node=tgt)
            else:
                g.edges[i] = replace(e, from_node=tgt)
    before = set(g.nodes)
    applied = []
    split_graph_at(g, geo, sps, None, proximity_pickup=True, applied=applied)
    new = set(g.nodes) - before
    if CONSIST["on"]:
        # A JUNCTION IS ON ONE SIDE OF THE CUT. For every channel the sweep cut, an edge joining one of
        # its two halves to the host line (or to another cut channel) must land on the same side there;
        # where FWA's duplicate braid edges say otherwise, the host-line end follows the channel's half.
        for sid, (t, c, lines) in CUTLINE.items():
            half = {}                                   # piece -> (line, "up"/"dn", (up_p, dn_p))
            for blk, (mm, flip) in lines.items():
                lo_p = next((x for x in g.nodes if g.nodes[x].blk == blk and abs(g.nodes[x].up_m - mm) < .01), None)
                hi_p = next((x for x in g.nodes if g.nodes[x].blk == blk and abs(g.nodes[x].down_m - mm) < .01), None)
                if not (lo_p and hi_p):
                    continue
                up_p, dn_p = (lo_p, hi_p) if flip else (hi_p, lo_p)
                half[up_p] = (blk, "up", (up_p, dn_p))
                half[dn_p] = (blk, "dn", (up_p, dn_p))
            hblk = cuts[sid][0]
            M = cuts[sid][1]
            hs = [x for x in g.nodes if g.nodes[x].blk == hblk]
            def host_side(x):
                n = g.nodes[x]
                return "dn" if n.up_m <= M + .01 else "up"
            host_pair = (next((x for x in hs if abs(g.nodes[x].down_m - M) < .01), None),
                         next((x for x in hs if abs(g.nodes[x].up_m - M) < .01), None))
            def side_of(x):
                if x in half:
                    return half[x][1]
                n = g.nodes.get(x)
                if n is not None and n.blk == hblk:
                    return host_side(x)
                return None
            for i, e in enumerate(g.edges):
                if e.kind == "continuation":
                    continue
                if not (e.from_node in half or e.to_node in half):
                    continue                            # only edges touching a cut channel's halves
                fs, ts = side_of(e.from_node), side_of(e.to_node)
                if fs is None or ts is None or fs == ts:
                    continue
                # the TO end is placed by at_measure (exact); the FROM end only by a coordinate
                f = e.from_node
                if f in half:
                    up_p, dn_p = half[f][2]
                    new_f = up_p if ts == "up" else dn_p
                else:
                    new_f = host_pair[0] if ts == "up" else host_pair[1]
                if new_f:
                    g.edges[i] = replace(e, from_node=new_f)
                    CONSIST["moved"].append((sid, i, f, new_f, e.to_node, round(e.at_measure)))
    # registry: a new upper piece belongs to every item its lower piece belongs to
    parent = {}
    for nid in new:
        blk = g.nodes[nid].blk
        cands = [s for s in g.nodes if g.nodes[s].blk == blk and s not in new and
                 abs(g.nodes[s].up_m - g.nodes[nid].down_m) < .01]
        # the lower half keeps the original id; the parent is the original piece on that line
        parent[nid] = min((s for s in g.nodes if g.nodes[s].blk == blk and s in before), key=lambda s: abs(g.nodes[s].up_m - g.nodes[nid].down_m))
    reg = {}
    for k, it in reg0.items():
        ids = list(it.section_ids)
        add = [n for n, p in parent.items() if p in it.section_ids]
        reg[k] = replace(it, section_ids=tuple(ids + add)) if add else it
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g, geo, reg, log, applied, parent


# ------------------------------------------------------------------ the generic reach rule
_orig_by_measure = X._by_measure
NEW_RULE = {"on": False, "g": None}
import inspect, textwrap
_src = inspect.getsource(_orig_by_measure)
_src = _src.replace("def _by_measure(", "def _by_measure_seeded(", 1)
_src = _src.replace("                seed_outside: frozenset = frozenset(),",
                    "                seed_outside: frozenset = frozenset(), seed_inside: frozenset = frozenset(),", 1)
_src = _src.replace("    inside = set(on_blk)\n", "    inside = set(on_blk) | (set(seed_inside) & universe)\n", 1)
_src = _src.replace("    pending = set(others) - outside\n", "    pending = set(others) - outside - inside\n", 1)
assert "seed_inside) & universe" in _src and "- outside - inside" in _src
exec(compile(_src, "by_measure_seeded", "exec"), X.__dict__)
_seeded = X.__dict__["_by_measure_seeded"]


def _window_ids(g, universe, blk, lo, hi):
    ids = {"lo": set(), "hi": set()}
    for s in universe:
        n = g.nodes.get(s)
        if n is None or n.blk != blk:
            continue
        for b, m in ((n.lower_bound, n.down_m), (n.upper_bound, n.up_m)):
            if b is None:
                continue
            for k, c in (("lo", lo), ("hi", hi)):
                if math.isfinite(c) and c > 0 and abs(m - c) < .01:
                    ids[k] |= {b.boundary_id, *(b.aliases or ())}
    return ids


def _by_measure_new(g, universe, blk, lo, hi, seed_outside=frozenset(), why=None, marks=None):
    if not NEW_RULE["on"]:
        return _orig_by_measure(g, universe, blk, lo, hi, seed_outside=seed_outside, why=why, marks=marks)
    ids = _window_ids(g, universe, blk, lo, hi)
    allids = ids["lo"] | ids["hi"]
    far, near = set(), set()
    has_lo, has_hi = math.isfinite(lo) and lo > 0, math.isfinite(hi)
    for s in universe:
        n = g.nodes.get(s)
        if n is None or not n.blk or n.blk == blk:
            continue
        ub = n.upper_bound.boundary_id if n.upper_bound else None
        lb = n.lower_bound.boundary_id if n.lower_bound else None
        if ub in ids["lo"] or lb in ids["hi"]:          # below the lower cut / above the upper cut
            far.add(s)
        # definitively inside: on the inside of every cut the window has, and bounded by one of them
        elif (lb in ids["lo"] or ub in ids["hi"]) and (not has_lo or lb in ids["lo"]) and (not has_hi or ub in ids["hi"]):
            near.add(s)
    # walls: drop adjacency across a window cut on any line other than the host
    gv = copy.copy(g)
    drop = set()
    for i, e in enumerate(g.edges):
        if e.kind != "continuation":
            continue
        lo_node = g.nodes.get(e.to_node)
        if lo_node is not None and lo_node.blk != blk and lo_node.upper_bound is not None \
                and lo_node.upper_bound.boundary_id in allids:
            drop.add(i)
    gv.down_adj = {k: [i for i in v if i not in drop] for k, v in g.down_adj.items()}
    gv.up_adj = {k: [i for i in v if i not in drop] for k, v in g.up_adj.items()}
    return _seeded(gv, universe, blk, lo, hi, seed_outside=frozenset(seed_outside) | far,
                   seed_inside=frozenset(near), why=why, marks=marks)


X._by_measure = _by_measure_new


def run(g, reg, geo, e15, new_rule):
    X.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE = e15
    NEW_RULE["on"] = new_rule
    attach_geoms(g, geo)
    out = {}
    for c in CASES:
        ex = dict(c["ex"])
        got = X.resolve_extent(reg, g, c["covered"], ex)
        key = (c["entry"], c["rule"] or "", c["seq"])
        out[key] = got
    return out


# ------------------------------------------------------------------ truth per braid piece × case
def truth_len(g, geo, piece_ids, sid_lohi, case_window):
    pass


attach_geoms(G0, GEO0)
today = run(G0, REG0, GEO0, True, False)
G1, GEO1, REG1, LOG, APPLIED, PARENT = apply_split(G0, GEO0, REG0)
attach_geoms(G1, GEO1)
v_e15 = run(G1, REG1, GEO1, True, False)
v_bare = run(G1, REG1, GEO1, False, False)
v_new = run(G1, REG1, GEO1, False, True)

# the braid pieces of interest: every case's piece and its split children
children = defaultdict(set)
for nid, p in PARENT.items():
    children[p].add(nid)
rows = []
for c in CASES:
    key = (c["entry"], c["rule"] or "", c["seq"])
    pc = c["piece"]
    fam = {pc} | children.get(pc, set())
    rec = {"entry": c["entry"], "rule": c["rule"], "seq": c["seq"], "piece": pc, "why": c["why"], "family": sorted(fam)}
    for name, V, geo in (("today", today, GEO0), ("split_e15", v_e15, GEO1), ("split_bare", v_bare, GEO1), ("split_new", v_new, GEO1)):
        got = V.get(key) or {}
        sec = set(got.get("sections") or [])
        unc = set(got.get("unclassified") or [])
        sp = got.get("straddle_placed") or {}
        rec[name] = {p: ("in" if p in sec else "UNPLACED" if p in unc else "out",
                         round(geo[p].length) if geo.get(p) is not None else None,
                         (sp.get(p) or {}).get("how")) for p in sorted(fam) if p in (G0.nodes if name == "today" else G1.nodes)}
        rec[name + "_n_unclassified"] = len(unc)
        rec[name + "_n_sections"] = len(sec)
    rows.append(rec)

# whole-water deltas between today and split_new, by original piece metres
summ = {"consistency_moves": len(CONSIST["moved"]), "consistency": CONSIST["moved"], "cuts": len(cuts), "split_points": len([l for l in LOG if "m" in l]),
        "rehomed": [l for l in LOG if "rehome_to" in l], "log": LOG,
        "applied": [(a.split_id, a.blk, round(a.route_measure, 1), a.picked_up) for a in APPLIED],
        "new_sections": sorted(PARENT)}
for name, V in (("today", today), ("split_e15", v_e15), ("split_bare", v_bare), ("split_new", v_new)):
    summ[name + "_unclassified_total"] = sum(len((v or {}).get("unclassified") or []) for v in V.values())
    summ[name + "_marks_total"] = sum(len((v or {}).get("straddle_placed") or {}) for v in V.values())
dbg = {}
X.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE = False
NEW_RULE["on"] = True
for c in CASES:
    key = (c["entry"], c["rule"] or "", c["seq"])
    got = v_new.get(key) or {}
    sec = set(got.get("sections") or [])
    for p, why in (got.get("unclassified_why") or {}).items():
        nb = []
        for i in G1.down_adj.get(p, []) + G1.up_adj.get(p, []):
            e = G1.edges[i]
            o = e.to_node if e.from_node == p else e.from_node
            nb.append((o, e.kind, round(e.at_measure), "in" if o in sec else ("unc" if o in got.get("unclassified", []) else "out")))
        dbg.setdefault("|".join(map(str, key)), {})[p] = {"why": why, "nb": sorted(set(nb))}
json.dump({"summary": summ, "rows": rows, "unplaced_new": dbg}, open(HERE / ("sim_r13_topo.json" if TOPO_GATE else "sim_r13_geom.json"), "w"), indent=1, default=str)
print(json.dumps({k: v for k, v in summ.items() if k not in ("log", "applied", "new_sections")}, indent=1, default=str))
