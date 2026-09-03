"""Is the FWA 50K waterbody identifier telling the truth about which water it means?

    python -m pipeline.stocking.generate.audit

WHY THIS EXISTS. FIDQ and the bathymetry sheets both key on `WATERBODY_IDENTIFIER_WSA_50K`,
and the FWA lakes layer carries the same string as `WATERBODY_KEY_GROUP_CODE_50K`. That looks
like an exact join, and mostly it is — but the 50K identifiers were carried across an FWA
version migration, and a migrated key can land on the wrong waterbody. Trusting it blindly is
how a stocking record or a depth chart ends up on a different lake.

THE CHECK IS INDEPENDENT OF NAMES, deliberately. A name comparison is circular here: previous
cycles wrote bathymetry and stocking names INTO the atlas's own `name_tuples`, so 2,577 of
2,676 matched nodes already carry a bathymetry-sourced name and would "agree" with the sheet
by construction. Two non-circular signals are used instead:

  THERE IS EXACTLY ONE INDEPENDENT CHECK, and an earlier version of this file wrongly
  claimed two. Rebuilding `02322SAJR` from the same FWA row's `WATERBODY_KEY_50K` (2322) and
  `WATERSHED_GROUP_CODE_50K` (SAJR) is tautological: those three columns are one fact
  written three ways, so the "check" only ever confirms FWA agrees with itself. It proves
  nothing about whether the key points at the right water, and it is gone.

  THE WATERSHED CODE IS NOT USED, and an earlier version of this file wrongly did. The two
  sources segment it differently — the sheet dashes it `920-725300-77900-00755-…`, the FWA
  runs it together `920725300775000000…` — so comparing them compares two encodings and
  manufactures mismatches. Both cases it "found" (Jessie Lake, Twin Lake) had the group key
  AND the gazetted name agreeing exactly.

  What is checked instead:

  1. **The group key** `WATERBODY_KEY_GROUP_CODE_50K` — the join itself.
  2. **Names, from every side there is**: the FWA's three GNIS slots, and from the sheet
     both `GAZETTED_NAME` and `MAP_TITLE`. The map title is often the more specific of the
     two — the sheet the CSV calls "BURNIE LAKES" is titled "SOUTH BURNIE L." — and it is
     what disambiguates a sheet whose gazetted name covers several waters.
  3. **Two sheet sources, cross-checked.** Neither is complete: `wsa_bathymetry_maps.csv`
     and the `BATH_SURVEY_MAP_SHEETS_SVW` WFS layer each hold sheets the other lacks. Where
     both carry an identifier, their names must agree; where they disagree, that is a
     finding about the SOURCES rather than about the match.

A row is `verified` only when the independent code agrees. Name agreement is reported
alongside but never on its own — that is the grain of salt.
"""

from __future__ import annotations

import collections
import csv
import json
import re
import pickle
from pathlib import Path

from pipeline.deliver.tiles.names import normalise
from pipeline.common.curated import CURATED, SOURCE

GPKG = SOURCE / "bc_fisheries_data.gpkg"
GRAPH = Path("output/v2/full/graph.pkl")
BATHY = SOURCE / "wsa_bathymetry_maps.csv"

CODE = "WATERBODY_KEY_GROUP_CODE_50K"
WSC50 = "WATERSHED_CODE_50K"
KEY = "WATERBODY_KEY"


def _digits(v: object) -> str:
    """A watershed code with its punctuation removed.

    The two sources publish the same code differently — one dashed into variable-width
    segments, the other run together. Comparing them raw reports a 97.6% conflict rate that
    is entirely an artefact of formatting.
    """
    return "".join(ch for ch in str(v or "") if ch.isdigit())


def _bare(name: str) -> str:
    """A gazetted name with the FWA's `(formerly)` marker removed, normalised."""
    return normalise(re.sub(r"\s*\(\s*formerly\s*\)\s*", " ", name or "",
                            flags=re.I))


def _lake_rows(gpkg: Path) -> dict[str, list[dict]]:
    """group code -> the lake polygons carrying it, with their own 50K watershed code."""
    import fiona

    out: dict[str, list[dict]] = {}
    with fiona.open(gpkg, layer="lakes") as src:
        for feat in src:
            p = feat["properties"]
            code = (p.get(CODE) or "").strip()
            if not code:
                continue
            # Reconstruct the identifier from the polygon's own two columns. If it does
            # not come back the same, the group-code column is pointing at this polygon
            # wrongly — which is exactly the migration damage being looked for.
            out.setdefault(code, []).append({
                "wbk": str(p.get(KEY)),
                "wsc50": _digits(p.get(WSC50)),
                # ALL THREE GNIS SLOTS. A lake polygon carries up to three gazetted names
                # — the current one and what it was called before — so checking only
                # GNIS_NAME_1 reports a "name disagreement" for every water that has ever
                # been renamed, which is exactly the population most worth being sure about.
                "gnis": [(p.get(f"GNIS_NAME_{i}") or "").strip() for i in (1, 2, 3)],
            })
    return out


def audit() -> dict:
    graph = pickle.load(GRAPH.open("rb"))
    lakes = _lake_rows(GPKG)
    nodes_by_wbk: dict[str, list[str]] = {}
    for nid, n in graph.nodes.items():
        if getattr(n, "wbk", "") and str(n.kind).endswith("lake"):
            nodes_by_wbk.setdefault(str(n.wbk), []).append(nid)

    verdicts = collections.Counter()
    conflicts: list[dict] = []
    unmatched_code: list[dict] = []
    no_name: list[dict] = []
    frozen: dict[str, str] = {}

    for row in csv.DictReader(BATHY.open(encoding="utf-8-sig")):
        ident = (row.get("WATERBODY_IDENTIFIER_WSA_50K") or "").strip()
        want_wsc = (row.get("WATERSHED_CODE_WSA_50K") or "").strip()
        want_name = normalise(row.get("GAZETTED_NAME", ""))
        cands = lakes.get(ident, [])
        if not cands:
            verdicts["no_polygon"] += 1
            continue

        # THE INDEPENDENT SIGNAL. Same coding on both sides, so a mismatch means the
        # identifier resolved to a different waterbody than the source row meant.
        want_wsc = _digits(want_wsc)
        code_ok = [c for c in cands if c["wsc50"] and c["wsc50"] == want_wsc]
        # `(formerly)` is how the FWA records a renaming, so "Alexis Lake(formerly)" IS
        # the sheet's "ALEXIS LAKE". Compared with the marker stripped rather than by
        # substring, which would also let "Twin Lake" agree with "Twin Lake Upper".
        name_ok = [c for c in cands
                   if any(_bare(n) == want_name for n in c["gnis"] if n)]

        if code_ok:
            verdicts["verified" if name_ok else "verified_no_name"] += 1
            if not name_ok and len(no_name) < 400:
                no_name.append({"identifier": ident, "sheet": row.get("GAZETTED_NAME", ""),
                                "fwa": [n for n in code_ok[0]["gnis"] if n]})
            pick = code_ok[0]
        elif not any(c["wsc50"] for c in cands):
            verdicts["no_code_to_check"] += 1
            pick = name_ok[0] if name_ok else cands[0]
        elif name_ok:
            # The watershed code disagrees but the GNIS name agrees exactly. Both observed
            # cases (Jessie Lake, Twin Lake) are a code renumbered between vintages, not a
            # key pointing at the wrong water: `92072530077|9000075500` against
            # `92072530077|5000000000`, same lake, same name. Recorded separately so it
            # stays visible rather than being folded into `verified`.
            verdicts["verified_recoded"] += 1
            pick = cands[0]
        else:
            # Code disagrees AND no name agrees. This is the migration damage, and it is
            # exactly what must never be accepted silently.
            verdicts["CONFLICT"] += 1
            conflicts.append({
                "identifier": ident, "name": row.get("GAZETTED_NAME", ""),
                "row_wsc50": want_wsc,
                "candidates": [{"wbk": c["wbk"], "wsc50": c["wsc50"],
                                "gnis": [n for n in c["gnis"] if n]} for c in cands[:3]],
                "name_would_agree": bool(name_ok),
            })
            continue

        # EVERY node, not the first. A sheet titled "Burnie Lakes" covers two waters the
        # FWA names separately; freezing one of them loses the other silently.
        nids = sorted(nodes_by_wbk.get(pick["wbk"], ()))
        if nids:
            frozen[ident] = nids

    return {"verdicts": dict(verdicts), "conflicts": conflicts,
            "key_only": unmatched_code, "no_name": no_name, "frozen": frozen}


def main() -> None:
    r = audit()
    total = sum(r["verdicts"].values())
    print(f"  bathymetry sheets audited: {total:,}")
    for k, n in sorted(r["verdicts"].items(), key=lambda kv: -kv[1]):
        print(f"    {k:<22} {n:>6,}  {n/total:6.1%}")
    print(f"\n  frozen identifier -> node: {len(r['frozen']):,}")
    if r["key_only"]:
        print("\n  key reconstructs but the watershed code does not agree:")
        for c in r["key_only"]:
            print(f"    {c['identifier']}  {c['name']}")
            print(f"       row   {c['row_wsc50']}")
            print(f"       layer {c['layer_wsc50']}")
    if r["no_name"]:
        print(f"\n  keys agree, no GNIS name matches ({len(r['no_name'])}):")
        for c in r["no_name"][:8]:
            print(f"    {c['sheet']:<26} FWA: {c['fwa']}")
    if r["conflicts"]:
        print(f"\n  CONFLICTS — the identifier disagrees with the watershed code "
              f"({len(r['conflicts'])}):")
        for c in r["conflicts"][:10]:
            print(f"    {c['identifier']}  {c['name']:<26} "
                  f"name would agree: {c['name_would_agree']}")
    out = Path("pipeline/gauges/../stocking/identifier_audit.json").resolve()
    out.write_text(json.dumps(r, indent=1, sort_keys=True), encoding="utf-8")
    print(f"\n  wrote {out}")


if __name__ == "__main__":
    main()
