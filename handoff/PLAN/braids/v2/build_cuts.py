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


def frame_side_fn(case, c):
    """Side of a point against the case's cut line: the sweep's own frame (`frame_c`, `frame_t`, `frame_u`)."""
    fc, ft, fu = case.get("frame_c"), case.get("frame_t"), case.get("frame_u")
    if fc is None or ft is None:
        return None
    cx, cy = fc

    def side(pt):
        if fu is None:
            v = (pt.x - cx) * ft[0] + (pt.y - cy) * ft[1]
        else:
            v = fu[2] * (fu[0] * (pt.y - cy) - fu[1] * (pt.x - cx))
        return "above" if v > 0 else "below" if v < 0 else None
    return side


def name_of(nid):
    n = g.nodes.get(nid)
    if n is None:
        return nid
    if n.display_name:
        return n.display_name
    return "a pond" if not n.blk else "an unnamed channel"


def sides_by_topology(case, side_fn, pieces):
    """{section: "below" | "above" | "left out"} by TOPOLOGY, the reach's own reading: a host piece by the
    cut's measure, a piece of a line the sweep cut by its side of that crossing; every other piece takes the
    side of the nearest such piece it is CONNECTED to (either flow direction), never stepping across a bound that carries this cut — so a braid half meets the host on its
    own side, and a creek on the braid takes the side where it JOINS the braid. A piece that reaches no host
    piece that way (the far half of a creek the cut line crossed) is "left out"."""
    sid = case["split_id"]
    ref = f"split:{sid}"
    host = {}
    for s_ in by_blk.get(case["host_blk"], ()):
        n = g.nodes[s_]
        if n.up_m <= case["M"] + 1e-3:
            host[s_] = "below"
        elif n.down_m >= case["M"] - 1e-3:
            host[s_] = "above"
    # a line the sweep cut: each piece by which side of its crossing it lies on (the cut line's own frame)
    for cp in case["cut_points"]:
        p = point_at(cp["blk"], cp["m"] - 5.0)
        below = side_fn(p) if (side_fn and p is not None) else None
        if below is None:
            continue
        above = "above" if below == "below" else "below"
        for s_ in by_blk.get(cp["blk"], ()):
            host.setdefault(s_, below if g.nodes[s_].up_m <= cp["m"] + 0.05 else above)   # m is rounded to 2 dp

    def severed(a, b_):
        na, nb = g.nodes.get(a), g.nodes.get(b_)
        if na is None or nb is None or na.blk != nb.blk or not na.blk:
            return False
        bnd = na.lower_bound if abs(na.down_m - nb.up_m) < 1e-3 else (
            na.upper_bound if abs(na.up_m - nb.down_m) < 1e-3 else None)
        return bnd is not None and (bnd.boundary_id == ref or ref in (bnd.aliases or ()))

    def nbrs(x):
        for i in g.down_adj.get(x, ()):
            yield g.edges[i].to_node
        for i in g.up_adj.get(x, ()):
            yield g.edges[i].from_node

    out = {}
    for s_ in pieces:
        if s_ in host:
            out[s_] = host[s_]
            continue
        seen, frontier, got = {s_}, [s_], None
        while frontier and got is None and len(seen) < 800:
            nxt = []
            for x in frontier:
                for y in sorted(set(nbrs(x))):
                    if y in seen or severed(x, y):
                        continue
                    if y in host:
                        got = host[y]
                        break
                    seen.add(y)
                    nxt.append(y)
                if got is not None:
                    break
            frontier = nxt
        out[s_] = got or "left out"
    return out


def why_text(case, water, side_by):
    gate = case["gate"]
    own = case["through_own"]
    other = case["through_other"]
    far = {t["side"] for t in case["touches"] if abs(t["m"] - case["M"]) >= 10}
    trib = [k for k in case["crossings"] if k.get("tributary")]
    if gate == "cut":
        d = min((k["dist"] for k in case["crossings"] if k["used"] and not k.get("tributary")), default=0)
        t = (f"Cut: this side channel leaves the {water} below the cut and rejoins it above, so the cut line "
             f"crosses it too ({round(d)} m from the river).")
        if trib:
            t += (f" {len(trib)} creek(s) on it cross the cut line and are cut there too; each other creek "
                  "goes with the part of the braid it flows into.")
        return t
    if gate == "refused: one side":
        where = {"lo": "below", "hi": "above"}.get(next(iter(far)), "on one side of") if len(far) == 1 else "on one side of"
        return (f"Left alone: this channel touches the {water} only {where} the cut, so the cut does not "
                "separate it (a bend or bulge the cut line happens to cross).")
    if gate == "refused: another water":
        names = sorted({name_of(x) for x in other}) or ["another water"]
        return (f"Left alone: the only path across the cut runs through {', '.join(names)} — that is its own "
                f"water, not the {water}'s, so the {water}'s cut does not take it.")
    if gate == "refused: line misses":
        fl = LineString(case["frame_line"])
        gap = min((geo[x].distance(fl) for x in own if x in geo), default=None)
        gap_t = f"{round(gap)} m" if gap is not None else "short"
        if gap is not None and gap < 10:
            return (f"Left alone: it joins the {water} on both sides of the cut, but the cut line meets it only "
                    "within 10 m of one of its ends (a junction, not a crossing), so there is nothing to cut.")
        return (f"Left alone: it joins the {water} on both sides of the cut, but the cut line ends {gap_t} "
                "short of it.")
    if gate.startswith("rehomed"):
        return ("Not cut: it joins the river within 10 m of the cut; that junction is moved to the side the "
                "rest of the channel is on.")
    if gate == "refused: junction <10 m":
        return "Left alone: the cut line meets it only within 10 m of one of its ends (a junction, not a crossing)."
    return gate


records = []
for case in sweep["cases"]:
    gate = case["gate"]
    if case["source"] == "gauge" and gate != "cut":
        continue                                   # refused candidates: near CURATED cuts only
    if gate == "junction at the cut (already on its side)":
        continue
    blk, M = case["host_blk"], case["M"]
    c = point_at(blk, M)
    if c is None:
        continue
    fl = case["frame_line"]
    side_fn = frame_side_fn(case, c)
    member_blks = {x.split(":")[0] for x in case["members"] if not x.startswith("lake")}
    cut_ids = {f"{p['blk']}:{int(p['m'])}" for p in case["cut_points"]}
    refused_set = set(case["through_own"]) | set(case["through_other"]) if gate.startswith("refused") else set()
    if gate == "refused: one side":
        refused_set = set(case["side_channels"])
    shown = []
    for b in sorted(member_blks):
        for s_ in by_blk.get(b, ()):
            ln = geo.get(s_)
            if ln is None or ln.is_empty or ln.distance(c) > HOST_M:
                continue
            shown.append(s_)
    topo = sides_by_topology(case, side_fn, shown) if gate == "cut" or gate.startswith("rehomed") else {}
    pieces, lens = [], defaultdict(float)
    for s_ in shown:
        n = g.nodes[s_]
        is_member = s_ in case["members"] or s_ in cut_ids or s_.split(":")[0] in {p["blk"] for p in case["cut_points"]}
        if gate.startswith("refused"):
            sd = "refused" if s_ in refused_set else "untouched"
        else:
            sd = topo.get(s_, "left out")
        pieces.append({"section": s_, "name": n.display_name, "length_m": round(n.length_m or 0.0, 1),
                       "side": sd, "member": is_member,
                       "through_own": s_ in case["through_own"] or s_.split(":")[0] in
                       {p["blk"] for p in case["cut_points"] if not any(
                           k.get("tributary") and k["node"].split(":")[0] == p["blk"] for k in case["crossings"])},
                       "tributary_cut": any(k.get("tributary") and k["node"].split(":")[0] == n.blk
                                            for k in case["crossings"]),
                       "line": line_ll(geo[s_])})
        if is_member and sd in ("above", "below"):
            lens[sd] += n.length_m or 0.0
    # GUARD: each piece carries its OWN geometry — two different sections never share one drawn line
    # (round 3 shipped every piece with the last shown piece's line)
    _lines = [json.dumps(p["line"]) for p in pieces if p["line"]]
    assert len(set(_lines)) == len(_lines), (
        f"{case['split_id']}: {len(_lines) - len(set(_lines))} piece(s) share another section's line")
    hp = min(by_blk.get(blk, [None]), key=lambda s_: abs(g.nodes[s_].down_m - M) if s_ else 0) if by_blk.get(blk) else None
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
        "gate": gate, "why": why_text(case, water or "river", None), "cut_point": ll(c.x, c.y),
        "cut_line": [ll(x, y) for x, y in fl],
        "host": host_window(blk, M),
        "pieces": pieces,
        "length_m": {k: round(v, 1) for k, v in sorted(lens.items())},
        "braid_cuts": [{"blk": p["blk"], "m": p["m"], "point": ll(*next(
            ((k["x"], k["y"]) for k in case["crossings"] if k["used"] and k["node"].split(":")[0] == p["blk"]),
            (c.x, c.y))), "tributary": any(k.get("tributary") and k["node"].split(":")[0] == p["blk"]
                                           for k in case["crossings"])} for p in case["cut_points"]],
        "crossings": [{"section": k["node"], "dist_m": k["dist"], "to_end_m": k["to_end"], "used": k["used"],
                       "tributary": bool(k.get("tributary")), "point": ll(k["x"], k["y"])} for k in case["crossings"]],
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
