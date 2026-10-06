"""Stage 1 of the consumer page (handoff/consumer-pipeline.md §1): the format-2 export -> the page's
embedded `data` and `cases` blocks.

REFERENCE ONLY. This is the consumer's build step, written from their description, so the page's own
logic (`page_v35.js`) can be run over the CURRENT export by `harness.js`. It is never shipped and
nothing in the pipeline reads its output.

  1.1 decode:   `pipeline.tools.export_codec.expand` (the reference decoder; it checks the digests)
  1.2 pick:     the display waters (default: the page's 28, in page order; `--waters` adds more)
  1.3 reshape:  rules / licensing records / parts / waters as the page reads them
  1.4 remap:    the page's own 8 cases onto this export's rule set ids

Usage:
  python -m pipeline.deliver.answers.reference.convert --out DIR [--waters FILE] [--export-dir DIR]
writes DIR/data.json and DIR/cases.json.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from pipeline.tools.export_codec import expand

HERE = Path(__file__).resolve().parent
INPUTS = HERE / "page_v35_inputs.json"
#: the live export (read-only); the main tree's copy, since worktrees carry no generated data
MAIN_TREE = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")

RULE_KEEP = ("entry_id", "rule_id", "type", "family", "dimension", "label", "parts", "verbatim",
             "fields", "binds", "not_yet_mapped")
RULE_PROV_KEEP = ("rank", "who", "authority", "binds_to", "uncertain", "why", "entry_name")
LIC_KEEP = ("entry_id", "record_id", "kind", "label", "verbatim", "fields", "placement", "period",
            "stamp_waiver", "records", "not_yet_mapped", "parts")
LIC_PROV_KEEP = ("entry_name", "uncertain", "why")
ENTRY_KEEP = ("kind", "name", "full_name", "pages", "scope_note", "mus")
#: vias a part's `rules` carry (the Checks run reach + trib only; `trib_pending` / `contested`
#: never decide, so the page never sees them as members)
RULE_VIAS = ("reach", "trib")
GLOBAL_PLACEMENTS = ("province", "not_placed", "on_designation")
RUN_END = re.compile(r"^(lake_inlet|lake_outlet|confluence):(.+)$")


def _slim(d: dict, keep) -> dict:
    return {k: d[k] for k in keep if k in d}


def rule_out(r: dict) -> dict:
    y = _slim(r, RULE_KEEP)
    y["prov"] = _slim(r["provenance"], RULE_PROV_KEEP)
    return y


def lic_out(l: dict) -> dict:
    y = _slim(l, LIC_KEEP)
    y["prov"] = _slim(l["provenance"], LIC_PROV_KEEP)
    return y


def part_out(X: dict, p: dict) -> dict:
    rs = X["rulesets"][p["ruleset"]]
    rules = [[k, via] for via in RULE_VIAS for k in rs.get(via, [])
             if X["rules"][k]["family"] != "information"]
    lic = []
    if p["licensing_set"] is not None:
        ls = X["licensing_sets"][p["licensing_set"]]
        lic = [[k, via] for via, ks in ls.items() if via != "sections" for k in ks]
    return {"set": p["ruleset"], "lset": p["licensing_set"], "sections": p["sections"],
            "rules": rules, "lic": lic,
            "anadromous_rainbow": p.get("anadromous_rainbow", False),
            "province_except": p.get("province_except") or [],
            "steelhead": p.get("steelhead"), "runs": p["runs"],
            "steelhead_rules": p.get("steelhead_rules"), "home_region": p.get("home_region")}


def water_out(X: dict, wid: str) -> dict:
    w = X["waters"][wid]
    parts = [part_out(X, p) for p in w["parts"] if p["ruleset"] is not None]
    # the water's own order: biggest part first (stable)
    parts.sort(key=lambda p: -p["sections"])
    return {"id": wid, "name": w["name"], "kind": w["kind"], "sections": w["sections"],
            "outside_bc": w.get("outside_bc", 0), "part_of": w.get("part_of"),
            "province_except": w.get("province_except"), "entries": w.get("entries", []),
            "parts": parts, "uncertain": w.get("uncertain", []),
            "steelhead": w.get("steelhead"), "steelhead_source": w.get("steelhead_source"),
            "tidal": w.get("tidal"), "steelhead_rules": w.get("steelhead_rules")}


def remap_ours(X: dict, ours: list, old_sets: dict) -> list:
    out = []
    for c in ours:
        w = X["waters"].get(c["water"]["item_id"])
        want = set(c["expect"])
        old = set(old_sets.get(c["ruleset"], {}).get("reach", []))
        best = None
        for p in (w["parts"] if w else []):
            if p["ruleset"] is None:
                continue
            rs = X["rulesets"][p["ruleset"]]
            have = {k for via, ks in rs.items() if via != "sections" for k in ks}
            if not want <= have:
                continue
            d = len(set(rs.get("reach", [])) ^ old)
            if best is None or d < best[0]:
                best = (d, p["ruleset"])
        out.append({**c, "ruleset": best[1] if best else c["ruleset"], "_remap": {
            "from": c["ruleset"], "found": best is not None, "reach_diff": best[0] if best else None}})
    return out


def build(X: dict, waters: list[str], inputs: dict) -> tuple[dict, dict]:
    W = [water_out(X, wid) for wid in waters]
    rule_ids, lic_ids = [], []
    seen_r, seen_l = set(), set()
    for w in W:
        for p in w["parts"]:
            for k, _ in p["rules"]:
                if k not in seen_r:
                    seen_r.add(k); rule_ids.append(k)
            for k, _ in p["lic"]:
                if k not in seen_l:
                    seen_l.add(k); lic_ids.append(k)
        for k in w["uncertain"]:
            if k not in seen_r and k in X["rules"]:
                seen_r.add(k); rule_ids.append(k)
    own = {e for w in W for e in w["entries"]}
    for k, l in X["licensing"].items():
        if k not in seen_l and (l.get("placement") in GLOBAL_PLACEMENTS or l["entry_id"] in own):
            seen_l.add(k); lic_ids.append(k)
    rules = {k: rule_out(X["rules"][k]) for k in rule_ids}
    licensing = {k: lic_out(X["licensing"][k]) for k in lic_ids}
    ents = {r["entry_id"] for r in rules.values()} | {l["entry_id"] for l in licensing.values()}
    entries = {e: _slim(X["entries"][e], ENTRY_KEEP) for e in sorted(ents) if e in X["entries"]}
    split_ids, wn = set(), {}
    for w in W:
        for p in w["parts"]:
            for run in p["runs"]:
                for end in (run.get("from"), run.get("to")):
                    if not end:
                        continue
                    if end in X["splits"]:
                        split_ids.add(end)
                    m = RUN_END.match(end)
                    if m and m.group(2) in X["waters"]:
                        wn[m.group(2)] = X["waters"][m.group(2)]["name"]
    # §1.2 also names "a kept rule's extents[].splits", but the page's own block holds exactly the
    # run ends (94 of 94 on v35) and `endName`'s duplicate test reads every split it holds: follow
    # the block.
    splits = {k: X["splits"][k] for k in sorted(split_ids)}
    gear = X["guide"]["gear"]
    data = {"rules": rules, "licensing": licensing, "licences": X["licences"],
            "species": X["species"], "waters": W, "entries": entries,
            "conduct": {k: v["means"] if isinstance(v, dict) else v
                        for k, v in gear["conduct"]["acts"].items()},
            "province_methods": gear["methods"]["allowed_by_the_province"],
            "splits": splits, "wnames": wn}

    gc = X["guide"]["cases"]["cases"]
    ours = remap_ours(X, inputs["ours"], inputs["old_sets"])
    set_ids = [c["ruleset"] for c in gc if c.get("ruleset")] + [c["ruleset"] for c in ours]
    rulesets, crules = {}, {}
    for sid in set_ids:
        rs = {via: ks for via, ks in X["rulesets"][sid].items() if via != "sections"}
        rulesets[sid] = rs
        for ks in rs.values():
            for k in ks:
                r = X["rules"][k]
                crules[k] = {**_slim(r, ("entry_id", "rule_id", "type", "family", "dimension",
                                         "label", "fields", "binds")),
                             "provenance": {"rank": r["provenance"]["rank"]}}
    cases = {"rules": crules, "rulesets": rulesets, "cases": gc,
             "ours": [{k: v for k, v in c.items() if k != "_remap"} for c in ours],
             "_remap": {c["id"]: c["_remap"] for c in ours},
             "_about": {"bundle": X["about"]["bundle"]}}
    return data, cases


def load(export_dir: Path) -> dict:
    E = json.loads((export_dir / "ui-rules-export.json").read_text())
    G = json.loads((export_dir / "ui-rules-guide.json").read_text())
    return expand(E, G)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--export-dir", type=Path, default=MAIN_TREE / "data/generated/regs")
    ap.add_argument("--waters", type=Path,
                    help="JSON list of item ids to show (default: the page's 28)")
    a = ap.parse_args(argv)
    inputs = json.loads(INPUTS.read_text())
    X = load(a.export_dir)
    waters = json.loads(a.waters.read_text()) if a.waters else inputs["display_waters"]
    data, cases = build(X, waters, inputs)
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "data.json").write_text(json.dumps(data, ensure_ascii=False))
    (a.out / "cases.json").write_text(json.dumps(cases, ensure_ascii=False))
    print(f"{len(data['waters'])} waters, {len(data['rules'])} rules, "
          f"{len(data['licensing'])} licensing, {len(cases['cases'])} + {len(cases['ours'])} cases "
          f"-> {a.out}")
    for k, v in cases["_remap"].items():
        print(f"  ours {k}: set {v['from']} -> found={v['found']} diff={v['reach_diff']}")


if __name__ == "__main__":
    main()
