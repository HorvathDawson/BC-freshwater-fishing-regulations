"""R14 item 7 — the full braid census at every cut, on the sub-atlas cut by extract.py (atlas/full,
2026-10-10 vintage, same as the live bundle: handle digest d768d6e135e62b99). Read-only.

WHAT IS A CANDIDATE (wider than r12/r13, which tested only channels carrying the host's WSC):
every connected network of non-host stream pieces (any WSC, any name) lying within D of the cut point,
found by walking the graph from the host's neighbours. Lakes ON the host line are part of the host.
A network STRADDLES when it touches host pieces on both sides of the cut (the owner's "a piece attaching
to both sides of a split"). For each network the ±100 m averaged-tangent perpendicular (the median
tangent) is intersected with every member.

Classes:  cut        straddles, the line crosses a member >= SLIVER_M from that member's ends
          junction   straddles, the line misses (or only grazes a member end): junction at the cut
          gated      one-sided network the line crosses anyway (a bend or a bulge): NOT cut
Flags:    multi (a member crossed >= 2 times), net (>= 2 members crossed), far (crossing > FAR_M from
          the cut point), recross (the line re-crosses the host before reaching the braid), sliver,
          lake (the network passes through a lake), big (> 8 members), two_cuts (another cut on the
          same host crosses the same network).
Changes: each member is split by every relevant cut line; each part takes the rules of the host piece
on its side (the side vector over every relevant cut, nearest consistent host piece). Today = the
bundle's rules on the member's current section. Only rules that name one of the cuts are compared.
Writes <work>/cases.json and prints the summary."""
import json, math, pickle, sqlite3, sys, time
import networkx as nx
from collections import defaultdict, Counter
from pathlib import Path

from shapely.geometry import LineString, Point, box, MultiLineString
from shapely.ops import split as shp_split

W_DIR = Path(sys.argv[1])
ROOT = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
D_RADIUS, TAN_W, SLIVER_M, FAR_M = 3000.0, 100.0, 10.0, 800.0
t0 = time.time()
S = pickle.load(open(W_DIR / "sub.pkl", "rb"))
N, GEO, E = S["nodes"], S["geoms"], S["edges"]
print("loaded", round(time.time() - t0), flush=True)


def as_line(ln):
    if ln is None or ln.is_empty:
        return None
    if ln.geom_type == "LineString":
        return ln if len(ln.coords) > 1 else None
    if ln.geom_type == "MultiLineString":
        cs = [c for p in ln.geoms for c in p.coords]
        return LineString(cs) if len(cs) > 1 else None
    return None


GEO = {k: as_line(v) for k, v in GEO.items()}
GEO = {k: v for k, v in GEO.items() if v is not None}
adj = defaultdict(list)
for i, e in enumerate(E):
    adj[e["f"]].append(i); adj[e["t"]].append(i)
by_blk = defaultdict(list)
for nid, n in N.items():
    if n["blk"] and nid in GEO:
        by_blk[n["blk"]].append(nid)
for b in by_blk:
    by_blk[b].sort(key=lambda s: N[s]["down"])

# ---------------------------------------------------------------- rules (bundle + rule_extent)
B = ROOT / "data/generated/bundle/bundle.sqlite"
db = sqlite3.connect(str(B))
handles = (ROOT / "data/generated/atlas/full/section_handles.txt").read_text().split("\n")
hid = {s: i + 1 for i, s in enumerate(handles) if s}
rule_meta = {}
for eid, rid, typ, label, cg, verb in db.execute("select entry_id, rule_id, type, label, closure_grade, verbatim from rule"):
    rule_meta[(eid, rid)] = {"type": typ, "label": label, "closure": cg or "", "verbatim": (verb or "")[:240]}
rules_by_split = defaultdict(set)
for l in open(ROOT / "data/generated/reaches/full/rule_extent.jsonl"):
    r = json.loads(l)
    for s in r.get("splits") or []:
        rules_by_split[s].add((r["entry_id"], r["rule_id"]))
straddle_rows = defaultdict(list)
for sid, eid, rid, how, side, share in db.execute("select * from section_straddle"):
    straddle_rows[sid].append((eid, rid, side, share))
_rs_cache = {}


def rules_on(section):
    if section in _rs_cache:
        return _rs_cache[section]
    h = hid.get(section)
    out = set()
    if h:
        row = db.execute("select set_id from section_ruleset where sid=?", (h,)).fetchone()
        if row:
            out = {(a, b) for a, b in db.execute("select entry_id, rule_id from ruleset where set_id=?", (row[0],))}
    _rs_cache[section] = out
    return out


item_of = defaultdict(set)
for it in json.load(open(ROOT / "data/generated/atlas/full/registry.json"))["items"]:
    if it["kind"] in ("stream", "lake", "wetland"):
        for x in it["section_ids"]:
            item_of[x].add(it["id"])
host_items = item_of
region_home = json.load(open(ROOT / "data/generated/atlas/full/region_home.json"))

# ---------------------------------------------------------------- geometry helpers


def point_at(blk, m):
    best = None
    for s in by_blk[blk]:
        n = N[s]; ln = GEO[s]
        if n["down"] - 1e-6 <= m <= n["up"] + 1e-6:
            f = (m - n["down"]) / (n["up"] - n["down"]) if n["up"] > n["down"] else 0.0
            return ln.interpolate(f * ln.length)
        for mm, e in ((n["down"], 0.0), (n["up"], 1.0)):
            if best is None or abs(m - mm) < best[0]:
                best = (abs(m - mm), s, e)
    return GEO[best[1]].interpolate(best[2], normalized=True) if best else None


def measure_on(blk, p):
    """host measure of a point (nearest host piece)."""
    best = None
    for s in by_blk[blk]:
        d = GEO[s].distance(p)
        if best is None or d < best[0]:
            best = (d, s)
    s = best[1]; n = N[s]; ln = GEO[s]
    return n["down"] + (ln.project(p) / ln.length) * (n["up"] - n["down"]) if ln.length else n["down"], best[0]


def pts(x):
    if x.is_empty:
        return []
    if x.geom_type == "Point":
        return [x]
    if hasattr(x, "geoms"):
        return [q for y in x.geoms for q in pts(y)]
    if x.geom_type == "LineString":
        return [Point(x.coords[0]), Point(x.coords[-1])]
    return []


def cut_frame(blk, M):
    a, b, c = point_at(blk, M - TAN_W), point_at(blk, M + TAN_W), point_at(blk, M)
    if a is None or b is None or c is None:
        return None
    dx, dy = b.x - a.x, b.y - a.y
    h = math.hypot(dx, dy)
    if h == 0:
        return None
    t = (dx / h, dy / h)
    R = D_RADIUS
    line = LineString([(c.x + R * t[1], c.y - R * t[0]), (c.x - R * t[1], c.y + R * t[0])])
    return {"line": line, "t": t, "c": c, "a": a, "b": b}


def side(fr, p):
    return (p.x - fr["c"].x) * fr["t"][0] + (p.y - fr["c"].y) * fr["t"][1]


# ---------------------------------------------------------------- the cuts
cuts = []
sweep_cut = defaultdict(set)
for r in S["cuts"]:
    if "braid cut" in (r.get("concern") or ""):
        sweep_cut[r["split_id"]].add(r["blk"]); continue
    if r.get("anchor_type") in ("area_boundary", "mu_boundary"):
        continue
    if r.get("source") not in ("curated", "gauge", "length"):
        continue
    if r["blk"] not in by_blk:
        continue
    cuts.append(r)
print("cuts", Counter(c["source"] for c in cuts), flush=True)
cuts_on_host = defaultdict(list)
for c in cuts:
    cuts_on_host[c["blk"]].append(c)


def host_side_of(blk, nid, M):
    n = N[nid]
    if n["up"] <= M + 0.01:
        return "lo"
    if n["down"] >= M - 0.01:
        return "hi"
    return "x"


cases = []
frames = {}
for ci, c in enumerate(cuts):
    hblk, M, sid = c["blk"], c["route_measure"], c["split_id"]
    hp = set(by_blk[hblk])
    fr = cut_frame(hblk, M)
    if fr is None:
        continue
    frames[(sid, hblk, round(M, 2))] = fr
    P = fr["c"]
    disk = P.buffer(D_RADIUS)
    has_bnd = any(abs(N[s]["down"] - M) < 1.0 or abs(N[s]["up"] - M) < 1.0 for s in hp)
    # lakes on the host line count as host
    hostlake = set()

    def neighbours(x):
        for i in adj.get(x, []):
            e = E[i]
            yield (e["t"] if e["f"] == x else e["f"]), i

    for s in hp:
        for o, i in neighbours(s):
            if o in N and not N[o]["blk"]:
                sides_in = {N[E[j]["f"]]["blk"] for j in adj.get(o, []) if E[j]["t"] == o and E[j]["f"] in N}
                sides_out = {N[E[j]["t"]]["blk"] for j in adj.get(o, []) if E[j]["f"] == o and E[j]["t"] in N}
                if hblk in sides_in and hblk in sides_out:
                    hostlake.add(o)

    def candidate(x):
        if x in hp or x in hostlake or x not in N:
            return False
        if N[x]["blk"]:
            return x in GEO and GEO[x].intersects(disk)
        # lake: its edges' coordinates within the disk
        return any(Point(E[i]["x"], E[i]["y"]).within(disk) for i in adj.get(x, []))

    seeds = set()
    for s in hp:
        if not GEO[s].intersects(disk):
            continue
        for o, i in neighbours(s):
            if candidate(o):
                seeds.add(o)
    seen = set()
    for sd in sorted(seeds):
        if sd in seen:
            continue
        comp, stack = set(), [sd]
        while stack and len(comp) < 400:
            x = stack.pop()
            if x in comp:
                continue
            comp.add(x)
            for o, i in neighbours(x):
                if o not in comp and candidate(o):
                    stack.append(o)
        seen |= comp
        # touches on the host
        touches = []
        for x in comp:
            for o, i in neighbours(x):
                if o in hp:
                    e = E[i]
                    if e["t"] == o:
                        m = e["m"]
                    else:
                        m, _ = measure_on(hblk, Point(e["x"], e["y"]))
                    hs = host_side_of(hblk, o, M)
                    touches.append({"node": x, "host": o, "side": hs, "m": round(m, 1),
                                    "dm": round(m - M, 1), "xy": (round(e["x"]), round(e["y"]))})
            for o, i in neighbours(x):          # through a host lake
                if o in hostlake:
                    e = E[i]
                    m, _ = measure_on(hblk, Point(e["x"], e["y"]))
                    touches.append({"node": x, "host": o, "side": "lo" if m < M else "hi", "m": round(m, 1),
                                    "dm": round(m - M, 1), "xy": (round(e["x"]), round(e["y"])), "lake": 1})
        lo = any(t["side"] == "lo" for t in touches); hi = any(t["side"] == "hi" for t in touches)
        straddle = lo and hi
        streams_all = sorted(x for x in comp if N[x]["blk"])
        # THROUGH pieces: on some simple path between the host's two sides (biconnected block of the
        # virtual edge LO-HI). Dangling pieces (tributaries of a braid) are not through.
        through = set()
        if straddle:
            Gc = nx.Graph()
            for x in comp:
                Gc.add_node(x)
                for o, i in neighbours(x):
                    if o in comp:
                        Gc.add_edge(x, o)
            for t_ in touches:
                if t_["side"] in ("lo", "hi"):
                    Gc.add_edge(t_["node"], "LO" if t_["side"] == "lo" else "HI")
            Gc.add_edge("LO", "HI")
            for bc in nx.biconnected_components(Gc):
                if "LO" in bc and "HI" in bc:
                    through = bc - {"LO", "HI"}
                    break
        hitem = host_items.get(by_blk[hblk][0], set())
        through_item = sorted(x for x in through if N[x]["blk"] and (item_of.get(x, set()) & hitem))
        through_other = sorted(x for x in through if N[x]["blk"] and not (item_of.get(x, set()) & hitem))
        hwsc = N[by_blk[hblk][0]]["wsc"]
        side_ch = sorted(x for x in streams_all if N[x]["wsc"] == hwsc or (item_of.get(x, set()) & hitem))
        # pieces the rule could cut: through pieces of the host's water; for one-sided networks, the
        # host's own side channels (to show the gate)
        streams = through_item if straddle else side_ch
        crossings = []
        for x in streams:
            ln = GEO[x]
            for q in pts(fr["line"].intersection(ln)):
                n = N[x]
                mm = n["down"] + (ln.project(q) / ln.length) * (n["up"] - n["down"]) if ln.length else n["down"]
                to_end = min(mm - n["down"], n["up"] - mm)
                crossings.append({"node": x, "m": round(mm, 1), "dist": round(q.distance(P), 1),
                                  "to_end": round(to_end, 1), "xy": (q.x, q.y)})
        real = [k for k in crossings if k["to_end"] >= SLIVER_M]
        if not straddle and not real:
            continue
        if straddle and not through_item:
            cls = "other_water"
        elif straddle:
            cls = "cut" if real else "junction"
        else:
            cls = "gated"
        per = Counter(k["node"] for k in real)
        flags = []
        if any(v >= 2 for v in per.values()):
            flags.append("multi")
        if len(per) >= 2:
            flags.append("net")
        if real and max(k["dist"] for k in real) > FAR_M:
            flags.append("far")
        if any(k["to_end"] < SLIVER_M for k in crossings):
            flags.append("sliver")
        if any(not N[x]["blk"] for x in (through if straddle else comp)):
            flags.append("lake")
        if len(streams) > 8:
            flags.append("big")
        if straddle and cls == "junction":
            near = min((abs(t["dm"]) for t in touches if t["node"] in through), default=1e9)
            flags.append("junction_at_cut" if near <= 15 else "edge_anomaly")
        # host re-crossed before the farthest crossing
        if real:
            far = max(k["dist"] for k in real)
            hx = []
            for s in hp:
                if GEO[s].distance(P) > far + 50:
                    continue
                for q in pts(fr["line"].intersection(GEO[s])):
                    d = q.distance(P)
                    if 5 < d < far:
                        hx.append(round(d))
            if hx:
                flags.append("recross")
        else:
            hx = []
        cases.append({"i": len(cases), "split": sid, "src": c["source"], "anchor": c.get("anchor_type"),
                      "label": c.get("label", ""), "host": hblk, "M": round(M, 2), "has_bnd": has_bnd,
                      "cls": cls, "flags": flags, "comp": sorted(comp), "streams": streams,
                      "touches": touches, "crossings": crossings, "through_other": through_other,
                      "dangling": sorted(set(streams_all) - through), "side_ch": side_ch, "recross": sorted(hx),
                      "ch_len": round(sum(N[x]["len"] or 0 for x in streams))})
    if ci % 500 == 0:
        print(ci, len(cases), round(time.time() - t0), flush=True)

print("cases", Counter((k["src"], k["cls"]) for k in cases), flush=True)

# ---------------------------------------------------------------- changes (rules) for straddlers
def parts_of(x, frs):
    """split member x's geometry by every frame's line; [(LineString, side vector, length)]"""
    geoms = [GEO[x]]
    for fr in frs:
        nxt = []
        for g in geoms:
            if g.intersects(fr["line"]):
                try:
                    nxt.extend([p for p in shp_split(g, fr["line"]).geoms])
                except Exception:
                    nxt.append(g)
            else:
                nxt.append(g)
        geoms = nxt
    out = []
    for g in geoms:
        if g.length < 0.5:
            continue
        rp = g.interpolate(0.5, normalized=True)
        out.append((g, tuple(1 if side(fr, rp) > 0 else -1 for fr in frs), g.length))
    return out


for k in cases:
    k["rules"] = []
    k["proposed"] = []
    if k["cls"] in ("gated", "other_water"):
        continue
    hblk = k["host"]
    comp_streams = set(k["streams"])
    # every cut on this host whose line crosses this network (or the case's own cut)
    rel = []
    for c in cuts_on_host[hblk]:
        key = (c["split_id"], hblk, round(c["route_measure"], 2))
        fr = frames.get(key)
        if fr is None:
            continue
        if c["split_id"] == k["split"] and abs(c["route_measure"] - k["M"]) < 0.01:
            rel.append((c, fr)); continue
        if any(fr["line"].intersects(GEO[x]) for x in comp_streams):
            rel.append((c, fr))
    rel.sort(key=lambda z: z[0]["route_measure"])
    if len(rel) > 1:
        k["flags"].append("two_cuts")
    k["rel_cuts"] = [[c["split_id"], round(c["route_measure"], 1), c["source"]] for c, _ in rel]
    frs = [fr for _, fr in rel]
    Ms = [c["route_measure"] for c, _ in rel]
    Rset = set()
    for c, _ in rel:
        Rset |= rules_by_split.get(c["split_id"], set())
    hp = by_blk[hblk]
    tally = defaultdict(lambda: [0.0, 0.0])
    for x in sorted(comp_streams):
        today = rules_on(x) & Rset
        for g, sv, L in parts_of(x, frs):
            ok = [s for s in hp if all(((N[s]["down"] + N[s]["up"]) / 2 > Mi) == (v > 0) for Mi, v in zip(Ms, sv))]
            if not ok:
                continue
            rp = g.interpolate(0.5, normalized=True)
            tgt = min(ok, key=lambda s: GEO[s].distance(rp))
            prop = rules_on(tgt) & Rset
            k["proposed"].append({"node": x, "len": round(L), "sv": sv, "host_piece": tgt})
            for r in Rset:
                if r in today:
                    tally[r][0] += L
                if r in prop:
                    tally[r][1] += L
    for r in sorted(Rset):
        a, b = tally[r]
        m = rule_meta.get(r, {})
        k["rules"].append({"entry": r[0], "rule": r[1], "label": m.get("label", ""), "closure": m.get("closure", ""),
                           "type": m.get("type", ""), "today_m": round(a), "prop_m": round(b), "delta": round(b - a)})

# risk
for k in cases:
    clos = [abs(r["delta"]) for r in k["rules"] if r["closure"]]
    anyd = [abs(r["delta"]) for r in k["rules"]]
    bound = bool(rules_by_split.get(k["split"]))
    if k["cls"] in ("gated", "other_water"):
        risk = "none"
    elif k["src"] == "gauge":
        risk = "gauge"
    elif k["src"] == "length":
        risk = "auto"
    elif not bound:
        risk = "unbound"
    elif clos and max(clos) >= 300:
        risk = "high"
    elif (clos and max(clos) > 0) or (anyd and max(anyd) >= 100):
        risk = "medium"
    else:
        risk = "low"
    k["risk"] = risk
    k["bound"] = bound
    hs = by_blk[k["host"]]
    near = min(hs, key=lambda s: abs((N[s]["down"] + N[s]["up"]) / 2 - k["M"]))
    k["region"] = region_home.get(near, "")
    k["water"] = N[near]["name"] or f"blk {k['host']}"
    k["sweep_cut_today"] = sorted(sweep_cut.get(k["split"], ()))

json.dump({"cases": cases}, open(W_DIR / "cases.json", "w"), default=lambda o: list(o) if isinstance(o, (set, tuple)) else str(o))
print("risk", Counter((k["src"], k["cls"], k["risk"]) for k in cases))
print("done", round(time.time() - t0))
