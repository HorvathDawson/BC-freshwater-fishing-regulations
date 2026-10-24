"""T67 — light post-pass on cuts.json (no atlas load): the length-cut binding effect measured by the length
diff, and what became of every R14 census case on a curated cut (so the owner sees what is now left alone).

    python handoff/PLAN/braids/v2/annotate_cuts.py CUTS_JSON LENGTH_DIFF_JSON R14_CASES_JSON
"""
import json
import sys
from collections import defaultdict

cuts_p, ld_p, r14_p = sys.argv[1:4]
doc = json.load(open(cuts_p))
ld = json.load(open(ld_p))
r14 = json.load(open(r14_p))["cases"]

# rules whose bound length moved on a blue line where a LENGTH cut moved (not a braid, not a ditch)
braid_or_ditch = set()
for r in doc["cuts"]:
    braid_or_ditch.add(r["host_blk"])
    braid_or_ditch.update(p["section"].split(":")[0] for p in r["pieces"])
moved_len = defaultdict(list)
for k, r in ld["rules"].items():
    for b in r.get("blk_changes") or []:
        if b["why"] not in ("braid", "ditch", "e15"):
            moved_len[b["blk"]].append({"entry_id": r["entry_id"], "rule_id": r["rule_id"], "delta_m": b["delta_m"]})
lc = doc["length_cuts"]
lc["note"] = ("A length cut binds no rule, but MOVING one can move a binding: a section straddling a region line "
              "takes its zone rules from its home region (the region holding most of its length), so a length "
              "cut moved off a braid-spanned junction can move a straddling piece's home. Measured: "
              f"{sum(len(v) for v in moved_len.values())} rule binding(s) on {len(moved_len)} blue line(s), listed "
              "in region_home_moves. Otherwise length cuts change section ids and gauge colouring only. The build "
              "fails if the sweep would cut a braid at a length cut (braid_sweep.length_cut_failures).")
lc["region_home_moves"] = {blk: sorted(v, key=lambda x: (x["entry_id"], x["rule_id"])) for blk, v in sorted(moved_len.items())}

# R14 census (atlas full, 2026-10-10) vs the gate now, curated cuts only
now = defaultdict(list)
for r in doc["cuts"]:
    now[(r["split_id"], r["host_blk"])].append(r["gate"])
comp = []
for k in r14:
    if k["src"] != "curated":
        continue
    comp.append({"split_id": k["split"], "host_blk": k["host"], "r14_class": k["cls"], "r14_risk": k.get("risk"),
                 "r14_flags": k.get("flags", []), "r14_streams": k.get("streams", [])[:6],
                 "now": now.get((k["split"], k["host"])) or ["no case now: one-sided and the cut line does not "
                                                             "cross it, or the gate no longer sees a straddle"]})
doc["r14_census_vs_now"] = comp
json.dump(doc, open(cuts_p, "w"), separators=(",", ":"))
print("length rehome blks", len(moved_len), "r14 curated cases", len(comp))
