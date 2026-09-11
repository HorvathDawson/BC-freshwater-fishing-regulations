"""Cut the data the lake splitter needs into `app/design/lake-splitter.html`.

THE BOOK TREATS ONE LAKE AS SEVERAL WATERS and FWA has one polygon. Kootenay Lake is written
as a Main Body and two West Arms with different kokanee rules; Williston Lake is written as
Zone A, Zone B, Nation Arm and Davis Bay. Every one of those rows matches the same registry
item, so all of them bind the whole lake and the strictest rule wins everywhere — which is
safe, and wrong, and the reason `sub_lake_areas.json` exists.

What is missing is the polygons. This exports what a curator needs to draw them:

  the lake        its FWA outline, by WATERBODY_KEY
  the parts       the worklist from sub_lake_areas.json, with the rows that name each one
  the MUs         the management-unit polygons the lake touches

THE MUs ARE THE POINT. A part named by an MU does not need drawing at all. "WILLISTON LAKE
(in Zone A)" carries MUs 7-30, 7-37 and 7-38 in its own entry id, and "(in Zone B)" carries
7-31 and 7-36; the MU polygons are in the same FWA file the lake came from, so Zone A is the
lake intersected with the union of its three, and Zone B with the union of its two. No hand
work, and no chance of a hand-drawn line disagreeing with the boundary the regulation names.

Nation Arm, Davis Bay, the Kootenay arms and the Shannon netted-off portion are not MU
boundaries and do have to be drawn.

    python pipeline/tools/build_lake_splitter.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PAGE = REPO_ROOT / "app" / "design" / "lake-splitter.html"
WORKLIST = REPO_ROOT / "data" / "curated" / "waters" / "sub_lake_areas.json"
ADDED = REPO_ROOT / "data" / "curated" / "waters" / "added_lakes.geojson"


def _to_lonlat():
    from pyproj import Transformer
    return Transformer.from_crs(3005, 4326, always_xy=True).transform


def _ring(geom, tf, ndigits=5):
    """(Multi)Polygon -> [[[lon,lat], ...], ...] — one ring per part, exterior only.

    Interior rings are dropped on purpose: a curator is drawing a boundary across open water,
    and an island's hole in the outline is noise they cannot act on.
    """
    from shapely.ops import transform
    g = transform(tf, geom)
    parts = list(g.geoms) if g.geom_type.startswith("Multi") else [g]
    out = []
    for p in parts:
        if p.is_empty:
            continue
        out.append([[round(x, ndigits), round(y, ndigits)] for x, y in p.exterior.coords])
    return out


def main() -> int:
    from pipeline.atlas.fwa import FWADataAccessor
    from pipeline.atlas.build import get_mu_polys, get_waterbody_polys

    work = json.loads(WORKLIST.read_text(encoding="utf-8"))
    lakes = work["lakes"]
    next_id = int(work.get("_next_id") or 1)

    fwa = FWADataAccessor(str(REPO_ROOT / "data" / "source" / "bc_fisheries_data.gpkg"))
    tf = _to_lonlat()

    wbks = {L["item_id"].split(":", 1)[1] for L in lakes if L["item_id"].startswith("wbk:")}
    polys = get_waterbody_polys(fwa, wbks)
    mu_polys = get_mu_polys(fwa)

    out = {"next_id": next_id, "lakes": []}
    for L in lakes:
        wbk = L["item_id"].split(":", 1)[1]
        g = polys.get(wbk)
        if g is None:
            print(f"  !! {L['lake']}: no polygon for wbk {wbk}")
            continue
        # The MUs this lake's rows actually name, taken from the entry ids so the page and
        # the corpus cannot drift apart. `r7:...@7-30+7-37+7-38` -> {7-30, 7-37, 7-38}.
        rows = []
        for r in L.get("rows", []):
            rid = r.split("  ")[0].strip()
            mus = re.findall(r"\d+-\d+", rid.split("@")[-1]) if "@" in rid else []
            rows.append({"entry_id": rid, "mus": mus})
        touched = sorted({m for r in rows for m in r["mus"]})
        out["lakes"].append({
            "name": L["lake"], "item_id": L["item_id"], "wbk": wbk,
            "outline": _ring(g, tf),
            "rows": rows,
            "parts": [{"name": p.get("name"), "id": p.get("id")} for p in L.get("parts", [])],
            "mus": {m: _ring(mu_polys[m], tf) for m in touched if m in mu_polys},
        })
        print(f"  {L['lake']}: {len(out['lakes'][-1]['outline'])} ring(s), "
              f"{len(rows)} row(s), {len(out['lakes'][-1]['mus'])} MU polygon(s), "
              f"{len(L.get('parts', []))} part(s) wanted")

    # ids already taken, so a redraw cannot collide with a polygon already curated
    if ADDED.exists():
        used = [f["properties"].get("id") for f in
                json.loads(ADDED.read_text(encoding="utf-8")).get("features", [])]
        out["used_ids"] = sorted(i for i in used if isinstance(i, int))

    blob = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
    html = PAGE.read_text(encoding="utf-8")
    html = re.sub(r'(<script id="d" type="application/json">).*?(</script>)',
                  lambda m: m.group(1) + blob + m.group(2), html, count=1, flags=re.S)
    PAGE.write_text(html, encoding="utf-8")
    print(f"\nwrote {len(blob):,} bytes into {PAGE.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
