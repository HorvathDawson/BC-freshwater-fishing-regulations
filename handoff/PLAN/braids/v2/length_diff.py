"""T67 phase 2 — per-rule bound LENGTH, before vs after, attributed (heavy: run through scripts/heavy.sh).

    python handoff/PLAN/braids/v2/length_diff.py OLD_RUN NEW_RUN OLD_ATLAS NEW_ATLAS OUT_DIR

Streams both runs' `rule_section.jsonl` (sorted by entry, rule, section — `io.write_run`) one rule at a time,
so a 20 GB run fits; section lengths come from each atlas's own graph. Section ids differ between the atlases
where a cut was added or removed, so a rule is compared by LENGTH, and every gained/lost section is
attributed:

  braid    — on a blue line the side atlas's braid sweep cut, or a member of a network it cut/re-homed
  ditch    — a 2300-only ditch the registry no longer puts in its river's item (owner Q1)
  e15      — a piece the old run placed by E15's majority side (straddle.jsonl), E15 being gone
  rehome   — a blue line one of whose edges the sweep re-homed by geometry
  gauge    — on a blue line the old anchors perpendicular sweep cut under a gauge id (removed)
  other    — anything else (unexplained)
A rule whose bound length changes on no blue line changed section ids only (length cuts moved).

Writes OUT_DIR/length_diff.json ({rules: {entry|rule: {...}}, totals}) and OUT_DIR/ditch_sections.json.
Read-only on both runs and both atlases.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from pipeline.common.io.serialize import read_artifact  # noqa: E402

OLD_RUN, NEW_RUN, OLD_ATLAS, NEW_ATLAS, OUT = (Path(a) for a in sys.argv[1:6])
OUT.mkdir(parents=True, exist_ok=True)


def lengths(atlas: Path):
    g = read_artifact(str(atlas / "graph.pkl"))
    L = {nid: (n.blk, float(n.length_m or 0.0), tuple(n.edge_types), bool(n.gnis_id),
               getattr(n.kind, "value", str(n.kind))) for nid, n in g.nodes.items()}
    del g
    return L


print("old graph", flush=True)
OL = lengths(OLD_ATLAS)
print("new graph", flush=True)
NL = lengths(NEW_ATLAS)

# attribution sets
sweep = json.loads((NEW_ATLAS / "braid_sweep.json").read_text())
braid_blks, braid_nodes = set(), set()
for c in sweep["cases"]:
    if c["gate"] == "cut" or c["gate"].startswith("rehomed"):
        braid_nodes.update(c["members"])
        for p in c["cut_points"]:
            braid_blks.add(p["blk"])
        for x in c["members"]:
            braid_blks.add(x.split(":")[0]) if c["gate"] == "cut" and x in c["through_own"] else None
for c in sweep["cases"]:
    if c["gate"] == "cut" or c["gate"].startswith("rehomed"):
        braid_blks.update(x.split(":")[0] for x in c["members"] if not x.startswith("lake"))
# E15's placements in the old run (the majority side): those pieces are now cut, or unplaced
e15 = set()
if (OLD_RUN / "straddle.jsonl").exists():
    for line in (OLD_RUN / "straddle.jsonl").open():
        e15.add(json.loads(line)["section_id"].split(":")[0])
# edges the sweep re-homed by geometry (curated and length stages): the walk may follow them differently
rehome_blks = set()
for f in ("braid_sweep.json", "braid_sweep_length.json"):
    doc = json.loads((NEW_ATLAS / f).read_text())
    for m in doc.get("edges_rehomed_by_geometry") or []:
        for k in ("from", "to", "to_node"):
            if m.get(k):
                rehome_blks.add(str(m[k]).split(":")[0])
old_res = json.loads((OLD_ATLAS / "splits.resolved.json").read_text())
gauge_blks = {r["blk"] for r in old_res if "braid cut" in (r.get("concern") or "")}
old_braid_cut_blks = set(gauge_blks)

# ditches: 2300-only, no gnis, in a stream item of the OLD registry
reg = json.loads((OLD_ATLAS / "registry.json").read_text())
in_stream_item = {}
for it in reg["items"]:
    if it["kind"] == "stream":
        for s in it["section_ids"]:
            in_stream_item[s] = (it["id"], it["name"])
del reg
ditch = {}
for nid, (blk, ln, et, has_gnis, kind) in OL.items():
    if kind == "stream" and not has_gnis and et and set(et) <= {"2300"} and nid in in_stream_item:
        ditch[nid] = {"section": nid, "blk": blk, "length_m": round(ln, 1), "item": in_stream_item[nid][0],
                      "name": in_stream_item[nid][1]}
ditch_blks = {d["blk"] for d in ditch.values()}
print("ditch sections", len(ditch), "blks", len(ditch_blks), flush=True)


def why(sid: str) -> str:
    blk = sid.split(":")[0]
    if sid in ditch or blk in ditch_blks:
        return "ditch"
    if blk in braid_blks or sid in braid_nodes:
        return "braid"
    if blk in e15:
        return "e15"
    if blk in rehome_blks:
        return "rehome"
    if blk in old_braid_cut_blks:
        return "gauge"
    return "other"


def groups(path: Path):
    cur, secs = None, []
    with path.open() as fh:
        for line in fh:
            if not line.strip():
                continue
            r = json.loads(line)
            k = (r["entry_id"], r["rule_id"])
            if k != cur:
                if cur is not None:
                    yield cur, secs
                cur, secs = k, []
            secs.append(r["section_id"])
    if cur is not None:
        yield cur, secs


def nxt(it):
    try:
        return next(it)
    except StopIteration:
        return None


rules = {}
totals = defaultdict(float)
a_it, b_it = groups(OLD_RUN / "rule_section.jsonl"), groups(NEW_RUN / "rule_section.jsonl")
a, b = nxt(a_it), nxt(b_it)
n = 0
while a is not None or b is not None:
    if b is None or (a is not None and a[0] < b[0]):
        k, old, new = a[0], set(a[1]), set()
        a = nxt(a_it)
    elif a is None or b[0] < a[0]:
        k, old, new = b[0], set(), set(b[1])
        b = nxt(b_it)
    else:
        k, old, new = a[0], set(a[1]), set(b[1])
        a, b = nxt(a_it), nxt(b_it)
    n += 1
    if n % 500 == 0:
        print(n, flush=True)
    if old == new:
        continue
    lost, gained = old - new, new - old
    # PER BLUE LINE: a section can keep its id and change its length (a cut added or removed above it), so a
    # rule is compared by its bound length on each blk, and each blk's change attributed by the blk
    ob, nb = defaultdict(float), defaultdict(float)
    for s_ in old:
        x = OL.get(s_)
        if x:
            ob[x[0]] += x[1]
    for s_ in new:
        x = NL.get(s_)
        if x:
            nb[x[0]] += x[1]
    lo_len, ne_len = sum(ob.values()), sum(nb.values())
    by = defaultdict(lambda: {"net_m": 0.0, "blks": 0})
    blk_changes = []
    for blk in sorted(set(ob) | set(nb)):
        dlt = nb.get(blk, 0.0) - ob.get(blk, 0.0)
        if abs(dlt) < 0.5:
            continue
        w = why(f"{blk}:0") if blk not in ditch_blks else "ditch"
        by[w]["net_m"] += dlt
        by[w]["blks"] += 1
        blk_changes.append({"blk": blk, "before_m": round(ob.get(blk, 0.0), 1), "after_m": round(nb.get(blk, 0.0), 1),
                            "delta_m": round(dlt, 1), "why": w})
    rules[f"{k[0]}|{k[1]}"] = {
        "entry_id": k[0], "rule_id": k[1], "before_m": round(lo_len, 1), "after_m": round(ne_len, 1),
        "delta_m": round(ne_len - lo_len, 1), "sections_lost": len(lost), "sections_gained": len(gained),
        "id_only": not blk_changes,
        "by": {w: {"net_m": round(v["net_m"], 1), "blks": v["blks"]} for w, v in sorted(by.items())},
        "blk_changes": sorted(blk_changes, key=lambda r: -abs(r["delta_m"]))[:25],
        "sample_lost": sorted(lost)[:5], "sample_gained": sorted(gained)[:5],
    }
    for w, v in by.items():
        totals[f"{w}_net_m"] += v["net_m"]

attr = defaultdict(int)
for r in rules.values():
    attr["+".join(sorted(r["by"])) or "id-only (no length change)"] += 1
doc = {"rules_compared": n, "rules_changed": len(rules),
       "rules_by_attribution": dict(sorted(attr.items())),
       "totals_m": {k: round(v, 1) for k, v in sorted(totals.items())}, "rules": rules}
(OUT / "length_diff.json").write_text(json.dumps(doc, indent=1, sort_keys=True))
(OUT / "ditch_sections.json").write_text(json.dumps(sorted(ditch.values(), key=lambda d: d["section"]), indent=1))
print("rules compared", n, "changed", len(rules), dict(attr), flush=True)
