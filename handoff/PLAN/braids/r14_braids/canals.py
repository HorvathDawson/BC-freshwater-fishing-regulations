"""R14 item 7 (2) — census of polygons that carry a river's own flow but are not the river's.

Every lake node (FWA lakes / manmade / wetland polygon the network is routed through) whose inflow AND
outflow both lead — directly or through at most 2 unnamed line pieces — to the SAME stream item.
  on_line : the item's own blue line enters and leaves it (the river passes THROUGH it)
  loop    : the item is reached on both sides but not by one continuous line (a polygon on a side
            channel / connector that joins the river to itself — the Serpentine canal)
Width = 2 * area / perimeter (FWA AREA_HA, GEOMETRY_Length). Ownership from registry.json: does the
polygon belong to a water item, and does the river carry a lake boundary onto it (its rules stop there).
Writes <work>/canals.json."""
import json, pickle, sqlite3, sys
from collections import defaultdict, Counter, deque
from pathlib import Path

W = Path(sys.argv[1])
ROOT = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
T = pickle.load(open(W / "topo.pkl", "rb"))
NODES, EDGES = T["nodes"], T["edges"]
up, dn = defaultdict(list), defaultdict(list)
for i, (f, t, m, k, x, y) in enumerate(EDGES):
    dn[f].append(i); up[t].append(i)

reg = json.load(open(ROOT / "data/generated/atlas/full/registry.json"))["items"]
stream_of, water_of, items = {}, defaultdict(set), {}
for it in reg:
    if it["kind"] == "stream":
        for s in it["section_ids"]:
            stream_of.setdefault(s, it["id"])
    if it["kind"] in ("stream", "lake", "wetland"):
        items[it["id"]] = it
        for s in it["section_ids"]:
            water_of[s].add(it["id"])
lake_bnd = defaultdict(set)          # wbk -> stream items with a lake boundary onto it
for it in reg:
    if it["kind"] == "stream":
        for b in it.get("boundaries") or []:
            if b.get("kind") == "lake" and b.get("wbk"):
                lake_bnd[b["wbk"]].add(it["id"])

G = sqlite3.connect(str(ROOT / "data/source/bc_fisheries_data.gpkg"))
poly = {}
for layer in ("lakes", "manmade", "wetlands"):
    for wbk, typ, area, perim, n1, n2 in G.execute(
            f"select WATERBODY_KEY, WATERBODY_TYPE, AREA_HA, GEOMETRY_Length, GNIS_NAME_1, GNIS_NAME_2 from {layer}"):
        if wbk is None:
            continue
        k = str(int(wbk))
        a, p = float(area or 0) * 1e4, float(perim or 0)
        cur = poly.get(k)
        if cur:
            cur["area"] += a; cur["perim"] += p
        else:
            poly[k] = {"layer": layer, "type": typ, "area": a, "perim": p, "name": n1 or n2 or ""}
print("polys", len(poly), flush=True)


def owner_walk(start, avoid):
    """stream items reachable from `start` through <= 2 unnamed (no stream item) stream pieces."""
    out = {}
    q = deque([(start, 0, (start,))])
    seen = {start, avoid}
    while q:
        s, d, path = q.popleft()
        if s in stream_of:
            out.setdefault(stream_of[s], path); continue
        if d >= 2 or s not in NODES or not NODES[s][0]:
            continue
        for i in up[s] + dn[s]:
            f, t = EDGES[i][0], EDGES[i][1]
            o = f if t == s else t
            if o not in seen and o in NODES and NODES[o][0]:
                seen.add(o); q.append((o, d + 1, path + (o,)))
    return out


rows = []
for lid, nd in NODES.items():
    if nd[0]:          # stream piece
        continue
    ins = [EDGES[i][0] for i in up[lid] if EDGES[i][0] in NODES and NODES[EDGES[i][0]][0]]
    outs = [EDGES[i][1] for i in dn[lid] if EDGES[i][1] in NODES and NODES[EDGES[i][1]][0]]
    if not ins or not outs:
        continue
    A_in, A_out = {}, {}
    for s in ins:
        for k, v in owner_walk(s, lid).items():
            A_in.setdefault(k, (s, v))
    for s in outs:
        for k, v in owner_walk(s, lid).items():
            A_out.setdefault(k, (s, v))
    both = sorted(set(A_in) & set(A_out))
    if not both:
        continue
    wbk = nd[1]
    pg = poly.get(wbk, {})
    for S in both:
        sin, sout = A_in[S][0], A_out[S][0]
        direct_in = [s for s in ins if stream_of.get(s) == S]
        direct_out = [s for s in outs if stream_of.get(s) == S]
        threaded = any(NODES[a][0] == NODES[b][0] for a in direct_in for b in direct_out)
        # is the threading blk the item's main line? (longest blk of the item)
        it = items[S]
        blen = Counter()
        for s in it["section_ids"]:
            if s in NODES and NODES[s][0]:
                blen[NODES[s][0]] += NODES[s][7] or 0
        main_blk = blen.most_common(1)[0][0] if blen else ""
        on_main = any(NODES[a][0] == main_blk for a in direct_in) and any(NODES[b][0] == main_blk for b in direct_out)
        et = sorted({e for s in ins + outs for e in NODES[s][8]})
        kind = "on_line" if threaded else "loop"
        area, perim = pg.get("area", 0.0), pg.get("perim", 0.0)
        rows.append({"lake": lid, "wbk": wbk, "river": S, "river_name": it["name"], "kind": kind,
                     "on_main": on_main, "layer": pg.get("layer", "?"), "type": pg.get("type", ""),
                     "poly_name": pg.get("name", ""), "area_m2": round(area), "perim_m": round(perim),
                     "width_m": round(2 * area / perim, 1) if perim else None,
                     "owners": sorted(water_of.get(lid, ())), "river_stops_here": S in lake_bnd.get(wbk, set()),
                     "ins": ins, "outs": outs, "via_in": list(A_in[S][1]), "via_out": list(A_out[S][1]),
                     "edge_types": et, "mus": it.get("mus", [])})
print("rows", len(rows), flush=True)
c = Counter((r["kind"], r["layer"], bool(r["poly_name"]), bool(r["owners"])) for r in rows)
for k, v in sorted(c.items()):
    print(k, v)
json.dump(rows, open(W / "canals.json", "w"))
