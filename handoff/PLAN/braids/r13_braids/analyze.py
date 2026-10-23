import json, collections, sys
D = json.load(open("census_r13.json")); rows = D["rows"]
C = collections.Counter
def cls(r, W):
    w = r.get(f"w{W}")
    topo = r["touch_lo"] and r["touch_hi"]
    if w is None: return "noline"
    if w["spans"] and w["k"] >= 1:
        if r["already"]: return "already_cut"
        return "cut"
    if topo: return "topo_straddle_nocross"
    return "one_side"
for W in (50, 100):
    c = C((r["src"], cls(r, W)) for r in rows); print(W, sorted(c.items()))
# detail for W=100 cut rows
cut = [r for r in rows if cls(r,100) == "cut"]
print("cut rows", len(cut), "bound", sum(r["bound"] for r in cut), "curated", sum(r["src"]=="curated" for r in cut))
print("k dist", C(r["w100"]["k"] for r in cut))
print("multi-k", [(r["split"][-40:], r["ch"], r["w100"]["k"]) for r in cut if r["w100"]["k"]>1])
print("to_end<25", sorted([(round(r["w100"]["to_end"]), r["split"][-40:], r["ch"]) for r in cut if r["w100"]["to_end"] < 25]))
print("to_bnd<5", sorted([(round(r["w100"]["to_bnd"],1), r["split"][-40:], r["ch"]) for r in cut if r["w100"]["to_bnd"] < 5]))
# braids cut by >=2 splits
byc = collections.defaultdict(set)
for r in cut: byc[r["ch"]].add(r["split"])
print("channels cut by >=2 splits", sum(len(v)>1 for v in byc.values()), [(k, sorted(v)) for k,v in byc.items() if len(v)>1][:12])
# topo straddles that the line does not cross
ts = [r for r in rows if cls(r,100) == "topo_straddle_nocross"]
print("topo_nocross", len(ts))
for r in ts: print("  ", r["src"], r["bound"], r["split"][-45:], r["ch"], round(r["ch_len"]), "spans", r["w100"]["spans"], "k", r["w100"]["k"], "up", r["w100"]["up_share"], "enddist", round(r["end_dist_to_P"]))
# already cut but W100 disagrees?
al = [r for r in rows if r["already"]]
print("already rows", len(al), C(cls(r,100) for r in al), C(cls(r,50) for r in al))
# W50 vs W100 disagreements
dis = [(r["split"][-40:], r["ch"], cls(r,50), cls(r,100)) for r in rows if cls(r,50)!=cls(r,100)]
print("w50 vs w100 differ", len(dis)); [print("  ", d) for d in dis]
# curated + bound cut list
print("CURATED cut rows (W100):")
for r in sorted(cut, key=lambda r: r["split"]):
    if r["src"]=="curated":
        w=r["w100"]; print("  ", r["bound"], r["split"][-55:], r["ch"], round(r["ch_len"]), "k", w["k"], "to_end", round(w["to_end"]), "to_bnd", round(w["to_bnd"]), "up", w["up_share"], "topo", r["touch_lo"] and r["touch_hi"])
