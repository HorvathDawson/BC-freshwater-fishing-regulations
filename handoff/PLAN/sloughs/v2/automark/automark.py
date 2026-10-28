"""T55/T149 slough automark: learn the owner's slough/canal verdicts from their marks, score the rest.

Inputs (read-only):
  ../candidates_v2.json.gz          2,424 candidates with FWA/shape/line/regs fields
  ../page/complexes.json            complex membership
  imagery.json                      ESRI World Imagery pixel stats per outline (imagery.py)
  $MARKS_DIR/verdicts/*.json        owner marks read from the artifact DB (ArtifactData list, out_dir)
  $MARKS_DIR/spotcheck/*.json       intermittent spot-check marks (reported, not trained on)

Outputs:
  handoff/PLAN/curation-review/slough-automark.json   one row per candidate: owner / auto / owner_list / default
  report.json                       numbers the markdown quotes
Nothing is written to the artifact, its DB, or the registry.
"""
import collections, glob, gzip, json, math, os, random, sys

import numpy as np
from sklearn.model_selection import GroupKFold
from sklearn.tree import DecisionTreeClassifier, export_text

HERE = os.path.dirname(os.path.abspath(__file__))
MARKS_DIR = os.environ["MARKS_DIR"]
cands = json.load(gzip.open(os.path.join(HERE, "..", "candidates_v2.json.gz")))["candidates"]
cx = json.load(open(os.path.join(HERE, "..", "page", "complexes.json")))["complexes"]
img = json.load(open(os.path.join(HERE, "imagery.json")))
spot = json.load(open(os.path.join(HERE, "..", "page", "spot.json")))


def load(coll):
    out = {}
    for f in glob.glob(os.path.join(MARKS_DIR, coll, "*.json")):
        d = json.load(open(f))
        out[os.path.basename(f)[:-5]] = d
    return out


marks, smarks = load("verdicts"), load("spotcheck")
piece_cx = {p: c["id"] for c in cx for p in c["p"]}
cx_size = {c["id"]: len(c["p"]) for c in cx}


def codes(x):
    return {k.split("/")[-1] for k in (x["inside_line_codes"] or {})}


def rule_bound(x):
    return bool(((x["binds"] or {}).get("water_entries")) or x["catalogue_entries"])


def feats(x):
    cs = codes(x)
    im = img.get(x["id"]) or {}
    nm = " ".join(x["name"] or []).lower()  # suggested_name is NOT used: it comes from owner notes (leak)
    return {
        "area_ha": (x["area_m2"] or 0) / 1e4,
        "width_m": x["mean_width_m"] or 0,
        "elong_rect": x["elongation"] or 0,
        "elong_perim": x["elongation_perimeter"] or 0,
        "through_ratio": x["through_ratio"] or 0,
        "river_through": int(bool(x["river_through"])),
        "main_flow_1200": int("1200" in cs),
        "secondary_1300": int("1300" in cs),
        "lake_lines_1400_1450": int(bool(cs & {"1400", "1450"})),
        "only_lake_lines": int(x["lineclass"] == "only_1400_1450"),
        "main_flow_unnamed": int(x["lineclass"] == "main_flow_unnamed"),
        "blueline_1000s": int(x["lineclass"] == "blueline_1000s"),
        "lake_definite": int(x["fwa_feature_label"] == "Lake-Definite"),
        "lake_soft": int(x["fwa_feature_label"] in ("Lake-Indefinite", "Lake-Intermittent")),
        "wetland": int(x["poly_layer"] == "wetlands"),
        "manmade": int(x["poly_layer"] == "manmade"),
        "reservoir_or_dam": int(bool((x["dam_or_reservoir"] or {}).get("value")) or "Reservoir" in (x["fwa_feature_label"] or "")),
        "touches_named_lake": int(bool(x["part_of_named_lake"])),
        "named": int(bool(nm)),
        "slough_word": int(any(w in nm for w in ("slough", "canal", "channel", "ditch", "oxbow", "backwater"))),
        "on_stream_item": int(x["stream_item_kind"] == "stream"),
        "river_regulated": int(bool(x["river_regulated"])),
        "rule_bound": int(rule_bound(x)),
        "in_complex": int(x["id"] in piece_cx),
        "complex_size": cx_size.get(piece_cx.get(x["id"]), 1),
        "found_v2": int(any(f.startswith("v2:") for f in x["found_by"])),
        "img_water": im.get("water", np.nan),
        "img_veg": im.get("veg", np.nan),
        "img_bare": im.get("bare", np.nan),
        "img_bright": im.get("bright", np.nan),
        "img_texture": im.get("bright_std", np.nan),
        "img_green": im.get("exg", np.nan),
    }


rows = {x["id"]: x for x in cands}
F = {i: feats(x) for i, x in rows.items()}
NAMES = list(next(iter(F.values())).keys())


def X(ids, names):
    a = np.array([[F[i][n] for n in names] for i in ids], dtype=float)
    return np.nan_to_num(a, nan=-1)


# ---- labels: "would the owner call it stream?"  unsure (3; two notes say 'probably a lake') counts as not-stream
lab = {}
for i, m in marks.items():
    v = m.get("verdict")
    if v == "stream":
        lab[i] = 1
    elif v in ("not_stream", "unsure"):
        lab[i] = 0
ids = sorted(lab)
y = np.array([lab[i] for i in ids])
groups = np.array([piece_cx.get(i, i) for i in ids])  # a complex = one click = one group
ug = sorted(set(groups))
neg_groups = len({g for g, t in zip(groups, y) if t == 0})
print("labels", collections.Counter(y.tolist()), "groups", len(ug), "negative groups", neg_groups)

SETS = {
    "all_features": NAMES,
    "no_imagery": [n for n in NAMES if not n.startswith("img_")],
    "width+imagery": ["width_m", "img_veg", "img_water"],
    "width_only": ["width_m"],
}


def folds(reps=30):
    for r in range(reps):
        perm = ug[:]
        random.Random(r).shuffle(perm)
        gi = {g: k % 5 for k, g in enumerate(perm)}
        fold = np.array([gi[g] for g in groups])
        for k in range(5):
            yield fold != k, fold == k


def fit(names, depth, leaf, Xa, msk):
    return DecisionTreeClassifier(max_depth=depth, min_samples_leaf=leaf, class_weight="balanced",
                                  random_state=0).fit(Xa[msk], y[msk])


def leaf_prec(m, Xa, msk):
    """Laplace-smoothed share of owner 'stream' marks in each leaf (unweighted counts)."""
    lv = m.apply(Xa[msk])
    out = {}
    for l in set(lv):
        t = y[msk][lv == l]
        out[l] = ((t == 1).sum() + 1) / (len(t) + 2)
    return out


AUTO_P = 0.9


def cv(names, depth, leaf):
    """Two measures under grouped 5-fold CV x30: the tree's own confusion, and the auto policy:
    held-out pieces landing in a leaf whose TRAINING stream share >= 0.9 are auto-marked stream."""
    Xa = X(ids, names)
    conf = np.zeros((2, 2))
    auto = np.zeros(2)  # [negatives auto-marked stream, positives auto-marked]
    n = np.zeros(2)
    reps = 30
    for tr, te in folds(reps):
        m = fit(names, depth, leaf, Xa, tr)
        for t, q in zip(y[te], m.predict(Xa[te])):
            conf[t, q] += 1
        lp = leaf_prec(m, Xa, tr)
        for t, l in zip(y[te], m.apply(Xa[te])):
            n[t] += 1
            if lp.get(l, 0) >= AUTO_P:
                auto[t] += 1
    conf /= reps
    r0, r1 = conf[0, 0] / conf[0].sum(), conf[1, 1] / conf[1].sum()
    return {"acc": round(float(np.trace(conf) / conf.sum()), 3), "recall_not_stream": round(float(r0), 3),
            "recall_stream": round(float(r1), 3), "balanced": round(float((r0 + r1) / 2), 3),
            "confusion_true_ns_s_by_pred_ns_s": conf.round(1).tolist(),
            "auto_precision": round(float(auto[1] / max(auto.sum(), 1)), 3),
            "auto_coverage_of_stream_marks": round(float(auto[1] / n[1]), 3),
            "not_stream_marks_auto_marked_stream": round(float(auto[0] / n[0]), 3)}


grid = {}
for sname, names in SETS.items():
    for depth in (1, 2, 3):
        for leaf in (3, 5):
            grid[f"{sname}|d{depth}|l{leaf}"] = cv(names, depth, leaf)
for k in sorted(grid, key=lambda k: -grid[k]["balanced"]):
    print(k, grid[k])

# baseline: the existing v2 rule_class (STREAM_BY_BOOK / LIKELY_STREAM = stream)
def rule_says(i):
    return 1 if rows[i]["rule_class"] in ("STREAM_BY_BOOK", "LIKELY_STREAM") else 0
bc = np.zeros((2, 2), int)
for i, t in zip(ids, y):
    bc[t, rule_says(i)] += 1
baseline = {"confusion_true_ns_s_by_pred_ns_s": bc.tolist(), "acc": round(float(np.trace(bc) / bc.sum()), 3),
            "recall_not_stream": round(float(bc[0, 0] / bc[0].sum()), 3),
            "recall_stream": round(float(bc[1, 1] / bc[1].sum()), 3)}

# ---- the chosen readable model: width only, ONE split (fit on all marks: 44.4 m).
# width_only|d3 scored a little higher held-out, but only through a 20.5-22 m "lake" band (4 lake, 4 stream marks:
# Lichen Creek, Coquitlam, Elk) that has no physical meaning; the one-split rule is what the owner can check.
# Imagery separates the medians (open water 0.30 vs 0.07) but every imagery tree scored WORSE held-out: it is kept
# as a display field (img_veg / img_water on each row), not a decision input.
CHOSEN = "width_only|d1|l5"
names = SETS["width_only"]
Xl = X(ids, names)
final = DecisionTreeClassifier(max_depth=1, min_samples_leaf=5, class_weight="balanced", random_state=0).fit(Xl, y)
tree_txt = export_text(final, feature_names=names, show_weights=True)
print(tree_txt)
lp_all = leaf_prec(final, Xl, np.ones(len(ids), bool))
leaf_counts = {}
for l in set(final.apply(Xl)):
    t = y[final.apply(Xl) == l]
    leaf_counts[int(l)] = {"stream": int((t == 1).sum()), "not_stream": int((t == 0).sum()), "p": round(float(lp_all[l]), 3)}
print(leaf_counts)

allids = sorted(rows)
Xall = X(allids, names)
LEAF = dict(zip(allids, final.apply(Xall).tolist()))
P = {i: round(float(lp_all.get(LEAF[i], 0.5)), 3) for i in allids}

# ---- policy
IN_DIST = {"STREAM_BY_BOOK", "LIKELY_STREAM", "UNSURE"}  # 147 of the 150 verdicts sit in these classes


def impact(x):
    if rule_bound(x) or x["name"]:
        return "A_rule_bound_or_named"
    if (x["area_m2"] or 0) >= 20000:
        return "B_big_2ha_plus"
    return "C_small_rule_free"


def as_drawn(x):  # the v2 rule verdict: what happens with no further decision
    return "stream" if x["rule_class"] in ("STREAM_BY_BOOK", "LIKELY_STREAM") else "not_stream"


out = []
for i in allids:
    x = rows[i]
    p = P[i]
    rec = {"id": i, "name": " / ".join(x["name"] or []) or ((x["suggested_name"] or {}).get("name")),
           "stream": x["stream_name"], "region": x["region"], "rule_class": x["rule_class"],
           "fwa": x["fwa_feature_label"], "area_ha": round((x["area_m2"] or 0) / 1e4, 2), "width_m": x["mean_width_m"],
           "img_veg": F[i]["img_veg"], "img_water": F[i]["img_water"], "p_stream": p, "impact": impact(x),
           "complex": piece_cx.get(i), "lat": x["lat"], "lon": x["lon"]}
    m = marks.get(i)
    if m and m.get("verdict"):
        rec.update(decision="owner", verdict=m["verdict"], by="owner")
    elif x["rule_class"] == "NOT_FISHING_WATER":
        rec.update(decision="default", verdict="not_stream", by="machine", why="sewage basin / tailing pond (R0)")
    elif x["rule_class"] not in IN_DIST:
        rec.update(decision="default", verdict="not_stream", by="machine",
                   why="rules say LAKE; owner marked 2 of 702 LAKE-class pieces (both lake), so no basis to override")
    elif p >= AUTO_P:
        w = x["mean_width_m"] or 0
        rec.update(decision="auto", verdict="stream", by="machine", confidence="very_high" if w <= 20 else "high",
                   why=f"{w:.0f} m wide: owner marked " + ("57 of 57 under 20 m stream" if w <= 20 else "117 of 122 under 44 m stream"))
    elif impact(x) == "C_small_rule_free":
        rec.update(decision="default", verdict=as_drawn(x), by="machine",
                   why=f"uncertain ({p:.2f}) but small, unnamed, rule-free: keeps the v2 rule verdict")
    else:
        wet = x["poly_layer"] == "wetlands"
        rec.update(decision="owner_list", verdict=None, by=None, wide_wetland=wet,
                   why=f"over 44 m wide: owner marked 18 of 28 such pieces stream" + (
                       " (wide marsh/swamp: 3 of 3 complexes marked stream; ranked after lake-drawn pieces)" if wet else ""))
    out.append(rec)

imp_rank = {"A_rule_bound_or_named": 0, "B_big_2ha_plus": 1, "C_small_rule_free": 2}
owner_list = sorted([r for r in out if r["decision"] == "owner_list"],
                    key=lambda r: (bool(r.get("wide_wetland")), imp_rank[r["impact"]], -r["area_ha"]))
for k, r in enumerate(owner_list, 1):
    r["owner_rank"] = k
autos = [r for r in out if r["decision"] == "auto"]
spot_sample = sorted(r["id"] for r in random.Random(28).sample(autos, min(25, len(autos))))
for r in out:
    if r["id"] in spot_sample:
        r["spot_check"] = True

# optional probe: LAKE-class pieces the width rule would call stream (no marks there to learn from)
lake_probe_pool = [r for r in out if r["rule_class"] == "LAKE" and r["decision"] == "default" and r["p_stream"] >= AUTO_P
                   and r["impact"] == "A_rule_bound_or_named"]
lake_probe = sorted(r["id"] for r in random.Random(29).sample(lake_probe_pool, min(10, len(lake_probe_pool))))
for r in out:
    if r["id"] in lake_probe:
        r["lake_probe"] = True

counts = collections.Counter(r["decision"] for r in out)
counts_v = collections.Counter(f'{r["decision"]}|{r.get("verdict")}' for r in out)
by_class = collections.defaultdict(collections.Counter)
for r in out:
    by_class[r["rule_class"]][r["decision"]] += 1
ol_by = collections.Counter(f'{r["impact"]}|p{r["p_stream"]}' for r in owner_list)
ol_cx = len({r["complex"] for r in owner_list if r["complex"]})
ol_clicks = len({r["complex"] or r["id"] for r in owner_list})
ol_split = collections.Counter(("wetland" if r.get("wide_wetland") else "lake-drawn") + "|" + r["impact"] for r in owner_list)

sp_grp = {p["i"]: it["grp"] for it in spot["items"] for p in it["p"]}
sp = collections.Counter(f'{sp_grp.get(i, "?")}|{m.get("verdict")}' for i, m in smarks.items())


def med_by(fn):
    d = collections.defaultdict(list)
    for i, t in zip(ids, y):
        v = F[i][fn]
        if v == v:
            d["stream" if t else "not_stream"].append(v)
    return {k: round(float(np.median(v)), 3) for k, v in d.items()}


report = {
    "labels": {"stream": int(y.sum()), "not_stream_or_unsure": int((1 - y).sum()), "groups": len(ug),
               "negative_groups": neg_groups, "marks_total": len(marks)},
    "chosen": CHOSEN, "cv_chosen": grid[CHOSEN], "cv_grid": grid, "baseline_v2_rules_on_marks": baseline,
    "tree": tree_txt, "leaf_counts": leaf_counts,
    "counts": dict(counts), "counts_verdict": dict(counts_v), "by_rule_class": {k: dict(v) for k, v in by_class.items()},
    "owner_list_by_impact_leaf": dict(ol_by), "owner_list_complexes": ol_cx, "owner_list_clicks": ol_clicks, "owner_list_split": dict(ol_split),
    "spot_sample": spot_sample, "lake_probe": lake_probe, "lake_probe_pool": len(lake_probe_pool),
    "spotcheck_marks": dict(sp), "imagery_coverage": sum(1 for i in allids if img.get(i)),
    "median_by_label": {n: med_by(n) for n in ("width_m", "area_ha", "elong_perim", "through_ratio", "img_water",
                                               "img_veg", "img_green", "img_bright")},
}
json.dump({"generated": "2026-10-28", "source": "handoff/PLAN/sloughs/v2/automark/automark.py",
           "artifact": "https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8 (read-only; nothing written)",
           "policy": "owner marks never overwritten; decision in {owner, auto, owner_list, default}; by=machine on every "
                     "non-owner verdict; spot_check=true on the 25 random auto-marks; lake_probe=true on 10 optional LAKE-class probes",
           "rows": out}, open(os.path.join(HERE, "..", "..", "..", "curation-review", "slough-automark.json"), "w"), indent=1)
json.dump(report, open(os.path.join(HERE, "report.json"), "w"), indent=1)
print(json.dumps({k: report[k] for k in ("labels", "cv_chosen", "baseline_v2_rules_on_marks", "counts", "counts_verdict",
                                         "by_rule_class", "owner_list_by_impact_leaf", "owner_list_complexes", "owner_list_clicks", "owner_list_split",
                                         "lake_probe_pool", "spotcheck_marks", "median_by_label")}, indent=1))
