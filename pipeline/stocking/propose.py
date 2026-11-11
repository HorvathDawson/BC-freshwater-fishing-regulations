"""Stocked waterbody (FIDQ) -> lake PROPOSALS under the owner's stocking rules (T634 third pass, PROTOTYPE).

LICENCE: catalogue "Access Only"; owner says OGL (2026-11-11, link owed). Every row and file carries LICENCE_FLAG.
Confirm before any production publish.

THE RULES (owner 2026-11-11): the lake CODE (FIDQ `waterbody_identifier` = FWA `WATERBODY_KEY_GROUP_CODE_50K`, the
v1 chain's "group code") picks the lake; the NAME confirms it (FIDQ gazetted name or an alias against the lake's
gazetted names: core / word order / a curated variant whose source is not the old matchers). NEVER a name alone, NO
"same name within 2,000 m" fallback, NO name-inside-a-name. A lake not yet in the frozen file is `first_time` and goes
to the owner. Stores the lake's durable id (`wbk:`). Buckets, in order:
  no_code, no_lake, two_named, unnamed, name_disagrees, variant_circular, not_in_app (held), clean.
`python -m pipeline.bathymetry.accept --set stocking` is the only writer of `data/curated/stocking/matches.json`.

    python -m pipeline.stocking.propose          # reads ~/data-offgit/bc-stocking-fidq/groups/*.json
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Mapping, Sequence

LICENCE_FLAG = "owner says OGL, catalogue says Access Only, unconfirmed"


def waterbodies(records: Sequence[Mapping]) -> List[dict]:
    """FIDQ release rows -> one row per FIDQ waterbody (wbody_id), with its code, names and a release summary."""
    by: Dict[str, List[Mapping]] = defaultdict(list)
    for r in records:
        by[str(r.get("wbody_id") or r.get("waterbody_identifier") or "")].append(r)
    out = []
    for wid in sorted(by):
        rs = by[wid]
        codes = sorted({(r.get("waterbody_identifier") or "").strip() for r in rs} - {""})
        names = sorted({(r.get("gazetted_name") or "").strip() for r in rs} - {""})
        aliases = sorted({a.strip() for r in rs for a in (r.get("aliases") or "").split(",")} - {""})
        dates = sorted(str(r.get("release_date") or "") for r in rs)
        out.append({"fidq_id": wid, "codes": codes, "names": names, "aliases": aliases, "releases": len(rs),
                    "first_release": dates[0] if dates else None, "last_release": dates[-1] if dates else None,
                    "species": sorted({str(r.get("species_name")) for r in rs if r.get("species_name")}),
                    "region": sorted({str(r.get("primary_region_name")) for r in rs if r.get("primary_region_name")})})
    return out


def propose(wbs: Sequence[dict], lakes_by_code: Mapping[str, List[dict]], var_wbk, var_gnis,
            app: Mapping[str, Mapping], frozen_ids: set) -> dict:
    from pipeline.bathymetry.contours import name_agrees
    rows = []
    for w in wbs:
        row = dict(w, licence=LICENCE_FLAG, wbks=[], evidence={}, chart_id=w["fidq_id"], titles=w["names"])
        ev = row["evidence"]
        if len(w["codes"]) != 1:
            ev["codes"] = w["codes"]
            rows.append(_done(row, "no_code", frozen_ids))
            continue
        lakes = lakes_by_code.get(w["codes"][0], [])
        if not lakes:
            rows.append(_done(row, "no_lake", frozen_ids))
            continue
        by_wbk: Dict[int, dict] = {}
        for l in lakes:
            cur = by_wbk.setdefault(l["wbk"], {"wbk": l["wbk"], "names": [], "gnis": set()})
            cur["names"] += [n for n in l["names"] if n not in cur["names"]]
            if l["gnis_id"]:
                cur["gnis"].add(l["gnis_id"])
        named = [l for l in by_wbk.values() if l["names"]]
        if len(named) >= 2:
            ev["named_lakes"] = [{"wbk": f"wbk:{l['wbk']}", "names": l["names"]} for l in sorted(named, key=lambda l: l["wbk"])]
            rows.append(_done(row, "two_named", frozen_ids))
            continue
        if not named:
            ev["unnamed_lakes"] = [f"wbk:{k}" for k in sorted(by_wbk)]
            rows.append(_done(row, "unnamed", frozen_ids))
            continue
        lake = named[0]
        row["wbks"] = [f"wbk:{lake['wbk']}"]
        ev["lake_names"] = lake["names"]
        if len(by_wbk) > 1:
            ev["unnamed_left_out"] = [f"wbk:{k}" for k in sorted(by_wbk) if k != lake["wbk"]]
        variants = list(var_wbk.get(lake["wbk"], [])) + [v for g in sorted(lake["gnis"]) for v in var_gnis.get(g, [])]
        best = None
        for t in w["names"] + w["aliases"]:
            e = name_agrees(t, lake["names"], variants)
            if e and (best is None or _RANK[e] < _RANK[best[0]]):
                best = (e, t)
        ev["name_evidence"], ev["name_agreed_with"] = (best or (None, None))
        if best is None:
            rule = "name_disagrees"
        elif best[0] == "variant_circular":
            rule = "variant_circular"
        elif row["wbks"][0] not in app:
            rule = "not_in_app"
        else:
            rule = "clean"
        rows.append(_done(row, rule, frozen_ids))
    counts = Counter(r["rule"] for r in rows)
    return {"_about": "FIDQ stocked waterbody -> lake PROPOSALS (pipeline.stocking.propose, T634 prototype). Nothing "
                      "accepted; `python -m pipeline.bathymetry.accept --set stocking` writes the frozen file.",
            "licence": LICENCE_FLAG, "counts": dict(sorted(counts.items())),
            "first_time": sum(1 for r in rows if r["first_time"]),
            "clean_by_name_evidence": dict(sorted(Counter(r["evidence"].get("name_evidence") for r in rows
                                                          if r["rule"] == "clean").items())),
            "proposals": rows}


_RANK = {"equal": 0, "word_order": 1, "variant": 2, "variant_circular": 3}


def _done(row: dict, rule: str, frozen_ids: set) -> dict:
    row["rule"] = rule
    row["verdict"] = {"clean": "clean", "not_in_app": "held"}.get(rule, "review")
    row["first_time"] = row["fidq_id"] not in frozen_ids
    return row


def main(argv=None) -> int:
    from pipeline.bathymetry import accept as acc
    from pipeline.bathymetry.contours import _app, fwa_lakes
    from pipeline.bathymetry.propose import read_app, variants_by_lake
    from pipeline.common.curated import CURATED
    ap = argparse.ArgumentParser(description="FIDQ stocking -> lake proposals (T634 prototype)")
    ap.add_argument("--dir", type=Path, default=Path.home() / "data-offgit" / "bc-stocking-fidq")
    ap.add_argument("--release", type=Path)
    a = ap.parse_args(argv)
    gdir = a.dir.expanduser() / "groups"
    files = sorted(gdir.glob("*.json"))
    if not files:
        raise SystemExit(f"{gdir}: no fetched groups; run python -m pipeline.stocking.fetch_fidq first")
    records = [r for f in files for r in json.loads(f.read_text())["records"]]
    wbs = waterbodies(records)
    codes = sorted({c for w in wbs for c in w["codes"]})
    lakes: Dict[str, List[dict]] = defaultdict(list)
    for k in range(0, len(codes), 500):
        part = codes[k:k + 500]
        quoted = ",".join("'" + c.replace("'", "''") + "'" for c in part)
        for l in fwa_lakes(f"WATERBODY_KEY_GROUP_CODE_50K IN ({quoted})"):
            lakes[l["code"]].append(l)
    var_wbk, var_gnis = variants_by_lake(json.loads(Path(CURATED.waters.name_variants).read_text()))
    fz = acc.frozen_path("stocking")
    frozen_ids = {m["chart_id"] for m in json.loads(fz.read_text())["matches"]} if fz.exists() else set()
    out = propose(wbs, lakes, var_wbk, var_gnis, _app(a.release, read_app), frozen_ids)
    out["inputs"] = {"groups": len(files), "records": len(records)}
    dest = acc.proposals_path("stocking")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False))
    os.replace(tmp, dest)
    print(json.dumps({k: out[k] for k in ("counts", "first_time", "clean_by_name_evidence", "inputs")}))
    print(f"wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
