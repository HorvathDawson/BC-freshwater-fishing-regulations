"""T67 — the owner's review data: every proposed braid cut, and every candidate the gate refused near a curated
cut, from the side atlas (heavy: run through scripts/heavy.sh; loads geometries.pkl).

    python handoff/PLAN/braids/v2/build_cuts.py NEW_ATLAS NEW_RUN OLD_ATLAS OLD_RUN LENGTH_DIFF_DIR OUT_DIR

Writes OUT_DIR/cuts.json and OUT_DIR/ditches.json. Coordinates are WGS84 [lat, lon], 6 dp.
Read-only on every input.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from pyproj import Transformer  # noqa: E402
from shapely.geometry import LineString, Point  # noqa: E402
from shapely.ops import substring  # noqa: E402

from pipeline.common.io.serialize import read_artifact  # noqa: E402

NEW_ATLAS, NEW_RUN, OLD_ATLAS, OLD_RUN, LD, OUT = (Path(a) for a in sys.argv[1:7])
OUT.mkdir(parents=True, exist_ok=True)
HOST_M = 3000.0
SIMPLIFY_M = 2.0
T = Transformer.from_crs(3005, 4326, always_xy=True)


def ll(x, y):
    lon, lat = T.transform(x, y)
    return [round(lat, 6), round(lon, 6)]


def line_ll(g):
    if g is None or g.is_empty:
        return []
    g = g.simplify(SIMPLIFY_M)
    parts = list(g.geoms) if hasattr(g, "geoms") else [g]
    return [[ll(x, y) for x, y in p.coords] for p in parts if p.geom_type == "LineString"]


print("graph", flush=True)
g = read_artifact(str(NEW_ATLAS / "graph.pkl"))
print("geoms", flush=True)
geo = read_artifact(str(NEW_ATLAS / "geometries.pkl"))
by_blk = defaultdict(list)
for nid, n in g.nodes.items():
    if n.blk:
        by_blk[n.blk].append(nid)
for b in by_blk:
    by_blk[b].sort(key=lambda s: g.nodes[s].down_m)

sweep = json.loads((NEW_ATLAS / "braid_sweep.json").read_text())
lsweep = json.loads((NEW_ATLAS / "braid_sweep_length.json").read_text())
ldiff = json.loads((LD / "length_diff.json").read_text())["rules"]

# rules bound to each split, in the side run
rules_of = defaultdict(set)
with (NEW_RUN / "rule_extent.jsonl").open() as fh:
    for line in fh:
        r = json.loads(line)
        for s in r.get("splits") or []:
            rules_of[s].add((r["entry_id"], r["rule_id"]))
old_rules_of = defaultdict(set)
with (OLD_RUN / "rule_extent.jsonl").open() as fh:
    for line in fh:
        r = json.loads(line)
        for s in r.get("splits") or []:
            old_rules_of[s].add((r["entry_id"], r["rule_id"]))
rule_label = {}
try:
    import sqlite3
    _db = sqlite3.connect("file:data/generated/bundle/bundle.sqlite?mode=ro", uri=True)
    for e, r, lab in _db.execute("select entry_id, rule_id, label from rule"):
        rule_label[(e, r)] = lab or ""
except Exception as exc:                       # labels are a review aid only
    print("no rule labels:", exc, flush=True)


def point_at(blk, m):
    for s in by_blk.get(blk, ()):
        n = g.nodes[s]
        ln = geo.get(s)
        if ln is None or ln.is_empty or ln.geom_type != "LineString":
            continue
        if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
            return ln.interpolate(min(max(m - n.down_m, 0.0), ln.length))
    return None


def host_window(blk, M):
    out = []
    for s in by_blk.get(blk, ()):
        n = g.nodes[s]
        ln = geo.get(s)
        if ln is None or ln.is_empty or ln.geom_type != "LineString":
            continue
        lo, hi = max(n.down_m, M - HOST_M), min(n.up_m, M + HOST_M)
        if hi <= lo:
            continue
        out.extend(line_ll(substring(ln, lo - n.down_m, hi - n.down_m)))
    return out


records = []
for case in sweep["cases"]:
    gate = case["gate"]
    if case["source"] == "gauge" and gate != "cut":
        continue                                   # refused candidates: near CURATED cuts only
    if gate == "junction at the cut (already on its side)":
        continue
    blk, M = case["host_blk"], case["M"]
    c = point_at(blk, M)
    up = point_at(blk, M + 50.0)
    if c is None:
        continue
    fl = case["frame_line"]
    tx, ty = (up.x - c.x, up.y - c.y) if up is not None else (0.0, 0.0)

    def side(p):
        v = (p.x - c.x) * tx + (p.y - c.y) * ty
        return "above" if v > 0 else "below" if v < 0 else "on"

    member_blks = {x.split(":")[0] for x in case["members"]}
    cut_ids = {f"{p['blk']}:{int(p['m'])}" for p in case["cut_points"]}
    pieces, lens = [], defaultdict(float)
    for b in sorted(member_blks):
        for s in by_blk.get(b, ()):
            ln = geo.get(s)
            if ln is None or ln.is_empty or ln.distance(c) > HOST_M:
                continue
            n = g.nodes[s]
            mid = ln.interpolate(0.5, normalized=True)
            sd = side(mid)
            through = s in case["through_own"] or s in cut_ids or any(
                s.split(":")[0] == x.split(":")[0] and s not in case["members"] and s in cut_ids
                for x in case["through_own"])
            is_member = s in case["members"] or s in cut_ids
            pieces.append({"section": s, "name": n.display_name, "length_m": round(n.length_m or 0.0, 1),
                           "side": sd, "member": is_member,
                           "through_own": s in case["through_own"] or s in cut_ids,
                           "line": line_ll(ln)})
            if is_member:
                lens[sd] += n.length_m or 0.0
    hp = min(by_blk.get(blk, [None]), key=lambda s: abs(g.nodes[s].down_m - M) if s else 0) if by_blk.get(blk) else None
    water = g.nodes[hp].display_name if hp else ""
    rs = []
    for e, r in sorted(rules_of.get(case["split_id"], set()) | old_rules_of.get(case["split_id"], set())):
        d = ldiff.get(f"{e}|{r}")
        here = {blk} | member_blks
        rs.append({"entry_id": e, "rule_id": r, "label": rule_label.get((e, r), ""),
                   "bound_before_m": d["before_m"] if d else None, "bound_after_m": d["after_m"] if d else None,
                   "delta_m": d["delta_m"] if d else 0.0,
                   "delta_by_cause_m": {w: v["net_m"] for w, v in (d["by"] if d else {}).items()},
                   "delta_on_these_lines_m": round(sum(x["delta_m"] for x in (d["blk_changes"] if d else [])
                                                       if x["blk"] in here), 1),
                   "changed": bool(d) and not d.get("id_only")})
    records.append({
        "split_id": case["split_id"], "label": case["label"], "source": case["source"],
        "anchor_type": case["anchor_type"], "water": water, "host_blk": blk, "M": round(M, 1),
        "gate": gate, "cut_point": ll(c.x, c.y),
        "cut_line": [ll(x, y) for x, y in fl],
        "host": host_window(blk, M),
        "pieces": pieces,
        "length_m": {k: round(v, 1) for k, v in sorted(lens.items())},
        "braid_cuts": [{"blk": p["blk"], "m": p["m"], "point": ll(*next(
            ((k["x"], k["y"]) for k in case["crossings"] if k["used"] and k["node"].split(":")[0] == p["blk"]),
            (c.x, c.y)))} for p in case["cut_points"]],
        "crossings": [{"section": k["node"], "dist_m": k["dist"], "to_end_m": k["to_end"], "used": k["used"],
                       "point": ll(k["x"], k["y"])} for k in case["crossings"]],
        "touches": [{"section": t["node"], "host_section": t["host"], "m": t["m"], "side": t["side"]}
                    for t in case["touches"]],
        "residual_straddle": any(r["split_id"] == case["split_id"] and abs(r["M"] - M) < 0.01
                                 and set(r["pieces"]) & (set(case["members"]) | cut_ids)
                                 for r in sweep["residual_straddles"]),
        "rules": rs,
    })

counts = defaultdict(int)
for r in records:
    counts[f"{r['source']}: {r['gate']}"] += 1

# length cuts: old vs new (by blk, measure)
old_len = {(r["blk"], round(r["route_measure"], 1)) for r in json.loads((OLD_ATLAS / "splits.resolved.json").read_text())
           if r.get("source") == "length"}
new_res = json.loads((NEW_ATLAS / "splits.resolved.json").read_text())
new_len = {(r["blk"], round(r["route_measure"], 1)) for r in new_res if r.get("source") == "length"}
o_by, n_by = defaultdict(set), defaultdict(set)
for b, m in old_len:
    o_by[b].add(m)
for b, m in new_len:
    n_by[b].add(m)
moved = dropped = added = 0
for b in set(o_by) | set(n_by):
    o, n = o_by[b] - n_by[b], n_by[b] - o_by[b]
    k = min(len(o), len(n))
    moved += k
    dropped += len(o) - k
    added += len(n) - k
bound_len = sorted(s for s in set(rules_of) | set(old_rules_of) if s.startswith("length:"))
length_block = {
    "old": len(old_len), "new": len(new_len), "unchanged": len(old_len & new_len),
    "moved": moved, "dropped": dropped, "added": added,
    "braid_spanned_junctions_skipped": len(lsweep.get("skipped_junctions") or []),
    "sweep_on_length_cuts": {"braid_cuts": lsweep["braid_cuts"], "cases": lsweep["counts"]},
    "rules_bound_to_a_length_cut": bound_len,
    "note": "length cuts change section ids and gauge colouring, never regulations; the build fails if the "
            "sweep would cut a braid at one (braid_sweep.length_cut_failures)",
}

ditches = json.loads((LD / "ditch_sections.json").read_text())
# rules a ditch carried (old run), by section
want = {d["section"] for d in ditches}
carried = defaultdict(set)
with (OLD_RUN / "rule_section.jsonl").open() as fh:
    for line in fh:
        sid = line.rsplit('"section_id":"', 1)[-1].split('"', 1)[0]
        if sid not in want:
            continue
        r = json.loads(line)
        carried[r["section_id"]].add(f"{r['entry_id']}|{r['rule_id']}")
for d in ditches:
    d["rules_before"] = sorted(carried.get(d["section"], ()))
    gg = geo.get(d["section"])
    d["line"] = line_ll(gg) if gg is not None else []

# gauge cuts the old anchors sweep had put on another water, which the gate no longer cuts
old_res = json.loads((OLD_ATLAS / "splits.resolved.json").read_text())
new_keys = {(r["split_id"], r["blk"]) for r in new_res}
gauge_gone = []
for r in old_res:
    if r.get("source") == "gauge" and "braid cut" in (r.get("concern") or "") and (r["split_id"], r["blk"]) not in new_keys:
        host = [x for x in new_res if x["split_id"] == r["split_id"]]
        case = next((c for c in sweep["cases"] if c["split_id"] == r["split_id"]), None)
        p = point_at(r["blk"], r["route_measure"])
        gauge_gone.append({"split_id": r["split_id"], "blk": r["blk"], "m": round(r["route_measure"], 1),
                           "point": ll(p.x, p.y) if p is not None else None,
                           "host_now": [(x["blk"], round(x["route_measure"], 1)) for x in host],
                           "gate_now": case["gate"] if case else "not a network of the host"})
reach_gate = []
rep = NEW_RUN / "report.json"
for line in (NEW_RUN / "rule_diagnostic.jsonl").open():
    dd = json.loads(line)
    if dd.get("kind") in ("unclassified", "scope_unclassified"):
        pl = dd.get("payload") or {}
        pl = json.loads(pl) if isinstance(pl, str) else pl
        for pc, why_ in (pl.get("reasons") or {}).items():
            if why_ in ("straddles_the_cut", "joins_a_straddling_braid"):
                reach_gate.append({"entry_id": dd["entry_id"], "rule_id": dd["rule_id"], "section": pc, "reason": why_})

doc = {"$comment": "T67 braid cuts for owner review (2026-10-24). One record per proposed braid cut (curated and "
                   "gauge) and per candidate the gate refused near a curated cut. side 'below'/'above' is the cut "
                   "line's side (the host's downstream / upstream). Coordinates WGS84 [lat, lon].",
       "counts": dict(sorted(counts.items())), "length_cuts": length_block,
       "residual_straddles": sweep["residual_straddles"], "reach_gate_failures": reach_gate,
       "gauge_cuts_no_longer_made": gauge_gone, "cuts": records}
(OUT / "cuts.json").write_text(json.dumps(doc, separators=(",", ":")))
(OUT / "ditches.json").write_text(json.dumps({"$comment": "2300-only ditch sections (no gnis of their own) that "
                                              "leave their river's item (owner Q1); rules_before = rules the old run "
                                              "bound to them", "count": len(ditches), "ditches": ditches},
                                             separators=(",", ":")))
print("records", len(records), dict(counts), "ditches", len(ditches), flush=True)
