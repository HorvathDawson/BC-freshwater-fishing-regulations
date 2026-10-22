"""Pick the waters the golden outputs cover: the page's 28 display waters, then a deterministic sample
of the current export across every region x kind x (own row | zone only), plus the special shapes
(several parts, lake parts, tidal, outside B.C., national park, steelhead water, steelhead rules off,
home region, tributary-walk rules, classified waters).

  python -m pipeline.deliver.answers.reference.sample --out FILE [--per-bucket 6] [--per-special 6]

The order inside a bucket is the SHA-1 of the item id, so the sample is stable for one export and
moves only where the export's waters do.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from pipeline.deliver.answers.reference.convert import INPUTS, MAIN_TREE, load


def _h(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def region_of(X: dict, wid: str) -> str:
    w = X["waters"][wid]
    regs = {e.split(":", 1)[0][1:] for e in w.get("entries", []) if e.startswith("r")}
    if not regs:
        for p in w["parts"]:
            if p["ruleset"] is None:
                continue
            rs = X["rulesets"][p["ruleset"]]
            regs |= {k.split(":", 1)[0][1:] for k in rs.get("reach", [])
                     if k.startswith("z") and not k.startswith("zp:")}
    return "+".join(sorted(regs)) or "none"


def specials(X: dict, wid: str) -> set[str]:
    w, out = X["waters"][wid], set()
    parts = [p for p in w["parts"] if p["ruleset"] is not None]
    if len(parts) > 1: out.add("several_parts")
    if len(parts) > 4: out.add("many_parts")
    if w.get("part_of"): out.add("lake_part")
    if w.get("tidal"): out.add("tidal")
    if w.get("outside_bc"): out.add("outside_bc")
    for p in parts:
        if p.get("province_except"): out.add("province_except")
        if p.get("anadromous_rainbow"): out.add("steelhead_water")
        if p.get("steelhead_rules") is False: out.add("steelhead_rules_off")
        if p.get("home_region"): out.add("home_region")
        rs = X["rulesets"][p["ruleset"]]
        if rs.get("trib"): out.add("trib")
        if rs.get("trib_pending") or rs.get("contested"): out.add("trib_pending_or_contested")
        if p["licensing_set"] is not None:
            ls = X["licensing_sets"][p["licensing_set"]]
            if any(X["licensing"][k]["kind"] == "designation"
                   for v, ks in ls.items() if v != "sections" for k in ks):
                out.add("classified")
        if any(X["rules"][k].get("binds") == "sections_in_part" or X["rules"][k].get("not_yet_mapped")
               for v, ks in rs.items() if v != "sections" for k in ks):
            out.add("undrawn_part_rule")
    return out


def pick(X: dict, page28: list[str], per_bucket: int, per_special: int) -> tuple[list[str], dict]:
    cands = [wid for wid, w in X["waters"].items() if any(p["ruleset"] is not None for p in w["parts"])]
    cands.sort(key=_h)
    chosen, why = list(page28), {wid: ["page"] for wid in page28}
    buckets, spec = defaultdict(list), defaultdict(list)
    for wid in cands:
        w = X["waters"][wid]
        buckets[(region_of(X, wid), w["kind"], "own_row" if w.get("entries") else "zone_only")].append(wid)
        for s in specials(X, wid):
            spec[s].append(wid)
    def take(lst, n, tag):
        got = 0
        for wid in lst:
            if got >= n:
                break
            if wid not in why:
                chosen.append(wid); why[wid] = []
            if tag not in why[wid]:
                why[wid].append(tag); got += 1
    for b in sorted(buckets):
        take(buckets[b], per_bucket, "bucket:" + "/".join(b))
    for s in sorted(spec):
        take(spec[s], per_special, "special:" + s)
    return chosen, why


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--export-dir", type=Path, default=MAIN_TREE / "data/generated/regs")
    ap.add_argument("--per-bucket", type=int, default=6)
    ap.add_argument("--per-special", type=int, default=6)
    a = ap.parse_args(argv)
    X = load(a.export_dir)
    page28 = json.loads(INPUTS.read_text())["display_waters"]
    chosen, why = pick(X, page28, a.per_bucket, a.per_special)
    a.out.write_text(json.dumps(chosen, indent=0))
    a.out.with_suffix(".why.json").write_text(json.dumps(why, indent=1, ensure_ascii=False))
    kinds = defaultdict(int)
    for wid in chosen:
        kinds[X["waters"][wid]["kind"]] += 1
    regions = sorted({region_of(X, w) for w in chosen})
    print(f"{len(chosen)} waters ({dict(kinds)}); regions {regions} -> {a.out}")


if __name__ == "__main__":
    main()
