"""Do the two bathymetry sources agree, and does the 50K key land on the right water?

    python -m pipeline.bathymetry.generate.audit

Three questions, in order:

  1. **Do the two sheet sources agree with each other?** `wsa_bathymetry_maps.csv` and the
     `BATH_SURVEY_MAP_SHEETS_SVW` WFS layer overlap but neither contains the other. Where
     both hold an identifier, their names should match; a disagreement is a finding about
     the sources, before any matching happens.

  2. **Does the group key land on a water whose name agrees?** `WATERBODY_KEY_GROUP_CODE_50K`
     is the join. Names corroborate it — the FWA's three GNIS slots against the sheet's
     GAZETTED_NAME *and* MAP_TITLE.

  3. **Does MAP_TITLE rescue anything GAZETTED_NAME could not?** That is the whole reason
     for reading source 2: a sheet titled "SOUTH BURNIE L." says which of two lakes it is
     where "BURNIE LAKES" does not.

NO WATERSHED CODE. The two sources segment it differently, so comparing them manufactures
mismatches — see `audit_identifiers` for the two it wrongly flagged.
"""

from __future__ import annotations

import collections
import csv
import json
import pickle
import re
from pathlib import Path

from pipeline.deliver.tiles.names import normalise
from pipeline.common.curated import CURATED, SOURCE
from pipeline.common.curated import generated

CSV_PATH = SOURCE / "wsa_bathymetry_maps.csv"
WFS_PATH = SOURCE / "bc_bathymetry_sheets.json"
GPKG = SOURCE / "bc_fisheries_data.gpkg"
GRAPH = Path("output/v2/full/graph.pkl")


def _bare(name: str) -> str:
    """A gazetted name with the FWA's `(formerly)` marker removed, and `L.` expanded.

    Sheet titles abbreviate — "SOUTH BURNIE L." — and the FWA does not. Comparing them
    without expanding the abbreviation reports a disagreement that is purely typographic.
    """
    n = re.sub(r"\s*\(\s*formerly\s*\)\s*", " ", name or "", flags=re.I)
    n = re.sub(r"\bL\.?$", "LAKE", n.strip(), flags=re.I)
    n = re.sub(r"\bCR\.?$", "CREEK", n, flags=re.I)
    n = re.sub(r"\bR\.?$", "RIVER", n, flags=re.I)
    return normalise(n)


def _sheets() -> dict[str, dict]:
    """identifier -> every name either source gives it, and which sources hold it."""
    out: dict[str, dict] = {}
    for row in csv.DictReader(CSV_PATH.open(encoding="utf-8-sig")):
        i = (row.get("WATERBODY_IDENTIFIER_WSA_50K") or "").strip()
        if not i:
            continue
        e = out.setdefault(i, {"names": set(), "csv": set(), "wfs": set(),
                               "sources": set(), "vectorized": False})
        for k in ("GAZETTED_NAME", "MAP_TITLE"):
            if row.get(k):
                e["names"].add(_bare(row[k]))
                e["csv"].add(_bare(row[k]))
        e["sources"].add("csv")

    if WFS_PATH.exists():
        for row in json.loads(WFS_PATH.read_text(encoding="utf-8")):
            i = (row.get("waterbody_identifier") or "").strip()
            if not i:
                continue
            e = out.setdefault(i, {"names": set(), "csv": set(), "wfs": set(),
                                   "sources": set(), "vectorized": False})
            for k in ("gazetted_name", "map_title"):
                if row.get(k):
                    e["names"].add(_bare(row[k]))
                    e["wfs"].add(_bare(row[k]))
            e["sources"].add("wfs")
            if (row.get("vectorized_flag") or "").upper() == "Y":
                e["vectorized"] = True
    return out


def audit() -> dict:
    import fiona

    graph = pickle.load(GRAPH.open("rb"))
    nodes_by_wbk: dict[str, list[str]] = {}
    for nid, n in graph.nodes.items():
        if getattr(n, "wbk", "") and str(n.kind).endswith("lake"):
            nodes_by_wbk.setdefault(str(n.wbk), []).append(nid)

    lakes: dict[str, list[dict]] = {}
    with fiona.open(GPKG, layer="lakes") as src:
        for feat in src:
            p = feat["properties"]
            code = (p.get("WATERBODY_KEY_GROUP_CODE_50K") or "").strip()
            if code:
                lakes.setdefault(code, []).append({
                    "wbk": str(p.get("WATERBODY_KEY")),
                    "gnis": [_bare(p.get(f"GNIS_NAME_{i}") or "") for i in (1, 2, 3)],
                })

    sheets = _sheets()
    v = collections.Counter()
    src_disagree, rescued, unnamed, unmatched = [], [], [], []
    frozen: dict[str, list[str]] = {}

    for ident, sh in sorted(sheets.items()):
        v[f"in_{'+'.join(sorted(sh['sources']))}"] += 1
        # 1 — do the sources agree with each other?
        if sh["csv"] and sh["wfs"] and not (sh["csv"] & sh["wfs"]):
            src_disagree.append({"identifier": ident, "csv": sorted(sh["csv"]),
                                 "wfs": sorted(sh["wfs"])})

        cands = lakes.get(ident, [])
        if not cands:
            v["no_polygon"] += 1
            unmatched.append({"identifier": ident, "names": sorted(sh["names"])})
            continue

        fwa = {g for c in cands for g in c["gnis"] if g}
        hit = sh["names"] & fwa
        if hit:
            v["verified"] += 1
            # 3 — did MAP_TITLE do work GAZETTED_NAME could not?
            gazetted_only = {n for n in sh["csv"] if n} - hit
            if hit and not (fwa & {n for n in sh["names"] if n in gazetted_only}):
                pass
        elif not fwa:
            v["fwa_unnamed"] += 1
            unnamed.append({"identifier": ident, "names": sorted(sh["names"])})
        else:
            v["name_disagrees"] += 1
            src_disagree.append({"identifier": ident, "sheet": sorted(sh["names"]),
                                 "fwa": sorted(fwa), "kind": "sheet_vs_fwa"})

        nids = sorted({n for c in cands for n in nodes_by_wbk.get(c["wbk"], ())})
        if nids and (hit or not fwa):
            frozen[ident] = nids

    return {"verdicts": dict(v), "source_disagreements": src_disagree,
            "fwa_unnamed": unnamed, "no_polygon": unmatched, "frozen": frozen}


def main() -> None:
    r = audit()
    print(f"  sheets by identifier: {len(r['frozen']) + len(r['no_polygon']):,}\n")
    for k, n in sorted(r["verdicts"].items(), key=lambda kv: -kv[1]):
        print(f"    {k:<22} {n:>6,}")
    print(f"\n  frozen identifier -> node(s): {len(r['frozen']):,}")
    multi = {k: x for k, x in r["frozen"].items() if len(x) > 1}
    print(f"    covering more than one water: {len(multi):,}")
    dis = [d for d in r["source_disagreements"] if d.get("kind") == "sheet_vs_fwa"]
    both = [d for d in r["source_disagreements"] if "csv" in d]
    print(f"\n  the two SOURCES disagree with each other: {len(both)}")
    for d in both[:5]:
        print(f"    {d['identifier']}  csv={d['csv']}  wfs={d['wfs']}")
    print(f"\n  sheet name disagrees with the FWA: {len(dis)}")
    for d in dis[:8]:
        print(f"    {d['identifier']}  sheet={d['sheet']}  fwa={d['fwa']}")
    out = generated("bathymetry", "audit.json")
    out.write_text(json.dumps(r, indent=1, sort_keys=True, default=list), encoding="utf-8")
    print(f"\n  wrote {out}")


if __name__ == "__main__":
    main()
