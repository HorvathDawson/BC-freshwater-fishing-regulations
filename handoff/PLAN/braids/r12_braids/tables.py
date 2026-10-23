import json, pickle, sys, statistics as st
sys.path.insert(0, "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
S = "/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad"
d = json.load(open(f"{S}/out/braids_r12.json"))
D = pickle.load(open("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/scratch_r10/braids_sub.pkl", "rb"))
g = D["graph"]
B = {(b["key"][0], b["key"][1], tuple(b["key"][2])): b for b in d["braids"]}
bp = [p for p in d["placements"] if p["set"] == "bundle"]


def cut_label(blk, c):
    for n in g.nodes.values():
        if n.blk == blk and abs(n.up_m - c) < 0.5 and n.upper_bound is not None:
            return n.upper_bound.label or n.upper_bound.boundary_id
        if n.blk == blk and abs(n.down_m - c) < 0.5 and n.lower_bound is not None:
            return n.lower_bound.label or n.lower_bound.boundary_id
    return "?"


labels = {}
per_braid = {}
for p in bp:
    k = (p["piece"], p["key"][1], tuple(p["key"][2]))
    b = B[k]
    per_braid.setdefault(p["piece"], {"b": b, "cuts": {}, "pl": []})
    for pc in b["per_cut"]:
        per_braid[p["piece"]]["cuts"][round(pc["c"])] = pc
    per_braid[p["piece"]]["pl"].append(p)


def tan_in(b, p, W="100.0"):
    lo, hi = p["window"]
    lo = float(lo); hi = float(hi)
    up = {round(pc["c"]): pc["tangent"][W]["up_share"] for pc in b["per_cut"]}
    a = up.get(round(lo)) if lo == lo and abs(lo) != float("inf") else None
    z = up.get(round(hi)) if abs(hi) != float("inf") else None
    above_lo = a if a is not None else (1.0 if lo == float("-inf") or lo < b["m_range"][0] else 0.0)
    above_hi = z if z is not None else (0.0 if hi == float("inf") or hi > b["m_range"][1] else 1.0)
    return max(0.0, above_lo - above_hi)


print("## per braid")
print("| # | braid (section) | river | length m | max / mean offset m | cut (label) | proj. above | snap: piece / network m | junction-to-cut m | tangent ±50/±100/±200: hits; split at m from lower end | spread of split m |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
i = 0
rowsB = []
for piece, v in sorted(per_braid.items(), key=lambda x: x[1]["b"]["name"]):
    b = v["b"]
    for c, pc in sorted(v["cuts"].items()):
        i += 1
        lab = cut_label(b["blk"], pc["c"])
        t = pc["tangent"]
        hits = "/".join(str(t[W]["k"]) for W in ("50.0", "100.0", "200.0"))
        ss = [t[W]["s_at"][0] for W in ("50.0", "100.0", "200.0") if t[W]["k"] == 1]
        spl = "; ".join(",".join(str(round(x)) for x in t[W]["s_at"]) or "–" for W in ("50.0", "100.0", "200.0"))
        spread = round(max(ss) - min(ss)) if len(ss) == 3 else "–"
        print(f"| {i} | {piece} | {b['name']} | {b['len']:.0f} | {b['max_off']:.0f} / {b['mean_off']:.0f} | {lab[:48]} | {pc['share_above_proj']:.2f} | {pc['snap_piece']:.0f} / {pc['snap_comp']:.0f} | {pc['junction_nearest'] if pc['junction_nearest'] is None else round(pc['junction_nearest'])} | {hits}; {spl} | {spread} |")
        rowsB.append((piece, b, pc, lab, ss))

print("\n## per placement")
print("| sid section | rule | type | side | share (E15) | minority m (proj) | tangent ±100 in-share | tangent minority m | flips? |")
print("|---|---|---|---|---|---|---|---|---|")
out = []
for p in sorted(bp, key=lambda p: (p["piece"], p["entry"], p["rule"])):
    b = B[(p["piece"], p["key"][1], tuple(p["key"][2]))]
    L = b["len"]
    s_in = p["share_in_recomputed"]
    cur_in = p["side"] == "in"
    mino = (1 - p["share_rec"]) * L
    ti = tan_in(b, p)
    tmin = (1 - ti) * L if cur_in else ti * L
    flip = (ti > 0.5) != cur_in
    rid = p["rule"] or "(entry: " + p["entry"].split(":")[1][:40] + ")"
    print(f"| {p['piece']} | {rid} | {(p['label'] or '')[:60]} | {p['side']} | {p['share_rec']:.2f} | {mino:.0f} | {ti:.2f} | {tmin:.0f} | {'YES' if flip else ''} |")
    out.append({"piece": p["piece"], "rule": rid, "mino": mino, "tmin": tmin, "flip": flip, "L": L, "label": p["label"], "share": p["share_rec"]})

# option counts over the 18 braids (a braid is resolved when EVERY cut it straddles is)
print("\n## options")
br = {}
for piece, b, pc, lab, ss in rowsB:
    br.setdefault(piece, []).append((b, pc))
for X in (2, 50, 100, 250, 500):
    n_piece = sum(all(pc["snap_piece"] <= X for _, pc in v) for v in br.values())
    n_comp = sum(all(pc["snap_comp"] <= X for _, pc in v) for v in br.values())
    print(f"snap <= {X} m: piece {n_piece}/18, network {n_comp}/18")
for N in (50, 100, 200, 400):
    print(f"max offset <= {N} m: {sum(v[0][0]['max_off'] <= N for v in br.values())}/18")
for Lm in (250, 500, 1000):
    print(f"length <= {Lm} m: {sum(v[0][0]['len'] <= Lm for v in br.values())}/18")
sn = [pc["snap_comp"] for v in br.values() for _, pc in v]
sp = [pc["snap_piece"] for v in br.values() for _, pc in v]
print("snap network median/max", st.median(sn), max(sn), "piece median/max", st.median(sp), max(sp))
clean = {W: sum(pc["tangent"][W]["k"] == 1 for v in br.values() for _, pc in v) for W in ("50.0", "100.0", "200.0")}
print("tangent clean per cut", clean, "of", len(sn))
print("proj crossings", [pc["proj_crossings"] for v in br.values() for _, pc in v])
json.dump(out, open(f"{S}/out/placements_table.json", "w"), indent=1)
