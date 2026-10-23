"""R14 item 7 — page data for the braid review page (scratchpad/braid-review/index.html).
Reads <work>/sub.pkl, cases.json, canals.json, the bundle and the FWA gpkg (bbox reads only).
Writes <work>/page_data.json. Coordinates are metres, local to each map's origin (EPSG:3005)."""
import json, math, pickle, sqlite3, sys
from collections import defaultdict, Counter
from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, Point, box, mapping
from shapely.ops import split as shp_split

W = Path(sys.argv[1])
ROOT = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
GPKG = str(ROOT / "data/source/bc_fisheries_data.gpkg")
S = pickle.load(open(W / "sub.pkl", "rb"))
N, E = S["nodes"], S["edges"]
CASES = json.load(open(W / "cases.json"))["cases"]
CAN = json.load(open(W / "canals.json"))


def as_line(ln):
    if ln is None or ln.is_empty:
        return None
    if ln.geom_type == "LineString":
        return ln if len(ln.coords) > 1 else None
    if ln.geom_type == "MultiLineString":
        cs = [c for p in ln.geoms for c in p.coords]
        return LineString(cs) if len(cs) > 1 else None
    return None


GEO = {k: as_line(v) for k, v in S["geoms"].items()}
GEO = {k: v for k, v in GEO.items() if v is not None}
by_blk = defaultdict(list)
for nid, n in N.items():
    if n["blk"] and nid in GEO:
        by_blk[n["blk"]].append(nid)
for b in by_blk:
    by_blk[b].sort(key=lambda s: N[s]["down"])
from shapely.strtree import STRtree
ids = sorted(GEO)
tree = STRtree([GEO[i] for i in ids])

db = sqlite3.connect(str(ROOT / "data/generated/bundle/bundle.sqlite"))
handles = (ROOT / "data/generated/atlas/full/section_handles.txt").read_text().split("\n")
hid = {s: i + 1 for i, s in enumerate(handles) if s}
strad = defaultdict(list)
for sid, eid, rid, how, side, share in db.execute("select * from section_straddle"):
    strad[sid].append({"rule": rid, "side": side, "share": share})


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
    return GEO[best[1]].interpolate(best[2], normalized=True)


def frame(blk, M, R=3000.0):
    a, b, c = point_at(blk, M - 100), point_at(blk, M + 100), point_at(blk, M)
    dx, dy = b.x - a.x, b.y - a.y
    h = math.hypot(dx, dy) or 1.0
    t = (dx / h, dy / h)
    line = LineString([(c.x + R * t[1], c.y - R * t[0]), (c.x - R * t[1], c.y + R * t[0])])
    return line, t, c, a, b


def enc(g, ox, oy, tol):
    """geometry -> list of flat int coordinate arrays (local metres)."""
    if g is None or g.is_empty:
        return []
    g = g.simplify(tol, preserve_topology=False) if tol else g
    out = []
    geoms = getattr(g, "geoms", [g])
    for p in geoms:
        if p.is_empty:
            continue
        if p.geom_type == "LineString":
            cs = p.coords
        elif p.geom_type == "Polygon":
            cs = p.exterior.coords
        else:
            continue
        flat = []
        for x, y, *_ in cs:
            flat += [round(x - ox), round(y - oy)]
        if len(flat) >= 4:
            out.append(flat)
    return out


def polys_in(bb, layers=("lakes", "manmade")):
    out = []
    for L in layers:
        try:
            p = gpd.read_file(GPKG, layer=L, bbox=bb, columns=["WATERBODY_KEY", "GNIS_NAME_1"])
        except Exception:
            continue
        for _, r in p.iterrows():
            out.append((L, str(int(r.WATERBODY_KEY)) if r.WATERBODY_KEY else "", (r.GNIS_NAME_1 if isinstance(r.GNIS_NAME_1, str) else ""), r.geometry))
    return out


SHOW = []
for k in CASES:
    if k["cls"] in ("cut", "junction"):
        SHOW.append(k)
    elif k["src"] == "curated" and k["cls"] in ("gated", "other_water"):
        SHOW.append(k)
print("show", Counter((k["src"], k["cls"]) for k in SHOW), flush=True)

mu_of = {}
for it in json.load(open(ROOT / "data/generated/atlas/full/registry.json"))["items"]:
    if it["kind"] == "stream" and it.get("mus"):
        for x in it["section_ids"]:
            mu_of.setdefault(x, it["mus"][0])
out_cases = []
for k in SHOW:
    if not k["region"]:
        rr = [r_["entry"].split(":")[0][1:] for r_ in k.get("rules", []) if r_["entry"].startswith("r")]
        mu = next((mu_of[x] for x in by_blk[k["host"]] if x in mu_of), "")
        k["region"] = rr[0] if rr else (mu.split("-")[0] if mu else "")
    hblk, M = k["host"], k["M"]
    line, t, c, a, b = frame(hblk, M)
    ox, oy = c.x, c.y
    pts = [Point(x["xy"]) for x in k["crossings"]] + [Point(x["xy"]) for x in k["touches"]]
    members = k["streams"] if k["cls"] in ("cut", "junction") else (k["side_ch"] or [])
    if k["cls"] == "other_water":
        members = []
    r = max([p.distance(c) for p in pts] + [350.0])
    r = min(r * 1.2 + 120, 3200.0)
    bx = box(ox - r, oy - r, ox + r, oy + r)
    tol = r / 400
    host = []
    for s in by_blk[hblk]:
        g = GEO[s]
        if not g.intersects(bx):
            continue
        sd = "lo" if N[s]["up"] <= M + 0.01 else "hi"
        host.append({"id": s, "side": sd, "g": enc(g.intersection(bx), ox, oy, tol)})
    mem = []
    for x in members:
        g = GEO[x]
        try:
            parts = list(shp_split(g, line).geoms) if g.intersects(line) else [g]
        except Exception:
            parts = [g]
        for p in parts:
            if p.length < 0.5:
                continue
            rp = p.interpolate(0.5, normalized=True)
            sd = "hi" if (rp.x - ox) * t[0] + (rp.y - oy) * t[1] > 0 else "lo"
            mem.append({"id": x, "side": sd, "len": round(p.length), "g": enc(p.intersection(bx), ox, oy, tol)})
    other_ids = set(k["dangling"]) | set(k.get("through_other", []))
    dang = [{"id": x, "g": enc(GEO[x].intersection(bx), ox, oy, tol)} for x in sorted(other_ids) if x in GEO]
    ctx = []
    skip = set(by_blk[hblk]) | set(members) | other_ids
    near = [ids[int(i)] for i in tree.query(bx)]
    near = [x for x in near if x not in skip]
    near.sort(key=lambda x: GEO[x].distance(c))
    for x in near[:90]:
        ctx.append(enc(GEO[x].intersection(bx), ox, oy, tol * 1.5))
    lakes = []
    for L, wbk, nm, g in polys_in((ox - r, oy - r, ox + r, oy + r)):
        gi = g.intersection(bx)
        if gi.area < 50:
            continue
        lakes.append({"layer": L, "wbk": wbk, "name": nm, "g": enc(gi, ox, oy, tol)})
    # other cut lines on this host in the box
    others = []
    for sid2, m2, src2 in k.get("rel_cuts", []):
        if sid2 == k["split"] and abs(m2 - M) < 0.5:
            continue
        l2, *_ = frame(hblk, m2)
        others.append({"split": sid2, "g": enc(l2.intersection(bx), ox, oy, 0)})
    strad_rows = []
    for x in members:
        h = hid.get(x)
        if h and h in strad:
            for row in strad[h]:
                strad_rows.append({"node": x, **row})
    out_cases.append({
        "i": k["i"], "split": k["split"], "label": k["label"], "src": k["src"], "anchor": k["anchor"],
        "water": k["water"], "region": k["region"], "cls": k["cls"], "risk": k["risk"], "flags": k["flags"],
        "bound": k["bound"], "M": k["M"], "host": hblk, "r": round(r),
        "t": [round(t[0], 4), round(t[1], 4)], "A": [round(a.x - ox), round(a.y - oy)], "B": [round(b.x - ox), round(b.y - oy)],
        "line": enc(line.intersection(bx), ox, oy, 0),
        "host_g": host, "mem": mem, "dang": dang, "ctx": ctx, "lakes": lakes, "others": others,
        "cross": [{"node": x["node"], "dist": x["dist"], "to_end": x["to_end"], "m": x["m"],
                   "p": [round(x["xy"][0] - ox), round(x["xy"][1] - oy)]} for x in k["crossings"]],
        "touch": [{"node": x["node"], "side": x["side"], "dm": x["dm"], "p": [x["xy"][0] - round(ox), x["xy"][1] - round(oy)]}
                  for x in k["touches"]],
        "through": members, "n_dang": len(k["dangling"]), "through_other": k.get("through_other", []),
        "ch_len": k["ch_len"], "rules": k.get("rules", []), "rel_cuts": k.get("rel_cuts", []),
        "recross": k.get("recross", []), "sweep": k.get("sweep_cut_today", []), "straddle_today": strad_rows,
        "edge_types": {x: N[x]["et"] for x in members},
    })
print("cases out", len(out_cases), flush=True)

# ---------------------------------------------------------------- Serpentine dossier
SERP_W = "900-005473-000000-000000"
st = gpd.read_file(GPKG, layer="streams", where=f"FWA_WATERSHED_CODE like '900-005473-%'")
bb = st[st.FWA_WATERSHED_CODE.str.startswith(SERP_W)].total_bounds
bb = (bb[0] - 600, bb[1] - 600, bb[2] + 600, bb[3] + 600)
allst = gpd.read_file(GPKG, layer="streams", bbox=bb, columns=["BLUE_LINE_KEY", "EDGE_TYPE", "GNIS_NAME", "FWA_WATERSHED_CODE", "WATERBODY_KEY"])
ox, oy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
lines = []
for _, r in allst.iterrows():
    lines.append({"blk": str(int(r.BLUE_LINE_KEY)), "et": int(r.EDGE_TYPE), "name": (r.GNIS_NAME if isinstance(r.GNIS_NAME, str) else ""),
                  "serp": (r.FWA_WATERSHED_CODE or "").startswith(SERP_W), "g": enc(r.geometry, ox, oy, 2)})
spolys = []
for L, wbk, nm, g in polys_in(bb, ("lakes", "manmade", "wetlands")):
    spolys.append({"layer": L, "wbk": wbk, "name": nm, "g": enc(g, ox, oy, 2)})
scuts = []
for r0 in S["cuts"]:
    if r0["blk"] == "356351273" or r0["split_id"].startswith("serpentine"):
        if r0["blk"] not in by_blk:
            continue
        l, t, c, a, b = frame(r0["blk"], r0["route_measure"], R=1500)
        scuts.append({"split": r0["split_id"], "label": r0.get("label"), "M": r0["route_measure"], "src": r0.get("source"),
                      "p": [round(c.x - ox), round(c.y - oy)], "g": enc(l, ox, oy, 0)})
serp_nodes = {nid: {"blk": n["blk"], "down": round(n["down"]), "up": round(n["up"]), "len": round(n["len"] or 0), "et": n["et"]}
              for nid, n in N.items() if (n["wsc"] or "").startswith("900-005473") and n["blk"] and len(n["wsc"]) <= 10}
serp_edges = [e for e in E if e["f"] in serp_nodes or e["t"] in serp_nodes]
serp = {"origin": [ox, oy], "bb": [round(bb[0] - ox), round(bb[1] - oy), round(bb[2] - ox), round(bb[3] - oy)],
        "lines": lines, "polys": spolys, "cuts": scuts, "nodes": serp_nodes,
        "edges": [{"f": e["f"], "t": e["t"], "k": e["k"], "p": [round(e["x"] - ox), round(e["y"] - oy)]} for e in serp_edges]}
print("serp lines", len(lines), "polys", len(spolys), flush=True)

# ---------------------------------------------------------------- canals
def cls(r):
    return "loop" if r["kind"] == "loop" else ("main" if r["on_main"] else "side")


reg_blks = defaultdict(set)
for it in json.load(open(ROOT / "data/generated/atlas/full/registry.json"))["items"]:
    if it["kind"] == "stream":
        for ref in it["ref_ids"]:
            if ref.startswith("blk:"):
                reg_blks[it["id"]].add(ref[4:])
can_rows = []
for r0 in CAN:
    r0.setdefault("narrow", False)
    r0["cls"] = cls(r0)
    L = (r0["perim_m"] or 0) / 2
    r0["len_m"] = round(L)
    r0["ratio"] = round(L / r0["width_m"], 1) if r0["width_m"] else None
    can_rows.append(r0)
mapped = [r0 for r0 in can_rows if r0["layer"] == "manmade"]
narrow = [r0 for r0 in can_rows if r0["layer"] == "lakes" and not r0["poly_name"] and not r0["owners"]
          and r0["width_m"] and r0["width_m"] <= 30 and (r0["ratio"] or 0) >= 20]
narrow_counts = Counter(r0["cls"] for r0 in narrow)
narrow_counts40 = Counter(r0["cls"] for r0 in narrow if r0["ratio"] >= 40)
for r0 in narrow:
    r0["narrow"] = True
narrow.sort(key=lambda r0: -r0["ratio"])
loops_named = [r0 for r0 in can_rows if r0["cls"] != "main" and r0["layer"] == "lakes"]
loops_named.sort(key=lambda r0: (-(r0["ratio"] or 0)))
mapped += [r0 for r0 in loops_named if not r0["poly_name"] and r0["cls"] == "loop"][:12]
mapped += [r0 for r0 in loops_named if not r0["poly_name"] and r0["cls"] == "side"][:8]
seenw = {r0["wbk"] for r0 in mapped}
for r0 in narrow:
    if len(mapped) >= 130:
        break
    if r0["wbk"] not in seenw:
        mapped.append(r0); seenw.add(r0["wbk"])
canal_maps = []
for r0 in mapped:
    pl = []
    for L in ("manmade", "lakes"):
        p = gpd.read_file(GPKG, layer=L, where=f"WATERBODY_KEY = {r0['wbk']}") if r0["wbk"].lstrip("-").isdigit() else None
        if p is not None and len(p):
            pl = list(p.geometry); break
    if not pl:
        continue
    from shapely.ops import unary_union
    pg = unary_union(pl)
    x0, y0, x1, y1 = pg.bounds
    rr = max(x1 - x0, y1 - y0) / 2 + 250
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    bb2 = (cx - rr, cy - rr, cx + rr, cy + rr)
    s2 = gpd.read_file(GPKG, layer="streams", bbox=bb2, columns=["BLUE_LINE_KEY", "EDGE_TYPE", "GNIS_NAME"])
    bxx = box(*bb2)
    rb = reg_blks.get(r0["river"], set())
    ls = [{"riv": str(int(q.BLUE_LINE_KEY)) in rb, "et": int(q.EDGE_TYPE), "g": enc(q.geometry.intersection(bxx), cx, cy, rr / 300)}
          for _, q in s2.iterrows()]
    others = [{"g": enc(g.intersection(bxx), cx, cy, rr / 300), "layer": L, "wbk": wbk}
              for L, wbk, nm, g in polys_in(bb2) if wbk != r0["wbk"]]
    canal_maps.append({"wbk": r0["wbk"], "r": round(rr), "poly": enc(pg, cx, cy, rr / 400), "lines": ls, "others": others})
summary = Counter((r0["cls"], r0["layer"], "named" if r0["poly_name"] else "unnamed",
                   "owned" if r0["owners"] else "no item") for r0 in can_rows)
canals = {"rows": [{k2: r0[k2] for k2 in ("wbk", "river", "river_name", "cls", "layer", "type", "poly_name", "area_m2",
                                          "width_m", "len_m", "ratio", "owners", "river_stops_here", "mus", "edge_types", "narrow")}
                   for r0 in can_rows if r0["layer"] == "manmade" or (r0["cls"] != "main" and (r0["poly_name"] or r0["owners"]))
                   or r0 in mapped],
          "narrow": {"w30_r20": dict(narrow_counts), "w30_r40": dict(narrow_counts40)},
          "summary": [[list(k2), v] for k2, v in sorted(summary.items())],
          "maps": {m["wbk"]: m for m in canal_maps}}
print("canal rows", len(canals["rows"]), "maps", len(canal_maps), flush=True)

json.dump({"cases": out_cases, "serp": serp, "canals": canals}, open(W / "page_data.json", "w"), separators=(",", ":"), allow_nan=False)
print("bytes", (W / "page_data.json").stat().st_size)
