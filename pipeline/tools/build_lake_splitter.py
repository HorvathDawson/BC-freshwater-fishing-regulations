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



#: CUT LINES SEEDED FROM THE BOOK'S OWN BOUNDARY TEXT.
#:
#: Kootenay Lake prints its boundaries in prose, in `z4:kootenay_lake_boundaries`:
#:
#:   The Main Body is the area EAST of a line between boundary signs on opposite shores near
#:   Balfour Point and Procter Lighthouse.
#:   The Upper West Arm is the area WEST of that line to McDonalds Landing (Six Mile).
#:   The Lower West Arm is the area between McDonalds Landing (Six Mile) and Corra Linn Dam.
#:
#: Three of the four landmarks are places the bundle already knows, so those endpoints are
#: data rather than a guess. McDonalds Landing is not: the only "McDonalds Landing" in the
#: place table is in Region 6, 700 km away, and neither it nor "Six Mile" appears anywhere in
#: the West Arm corridor. Its seed is a reading of the text — six miles along the arm from
#: Nelson, between Nelson and Willow Point — and it is marked `approx` so the page says so and
#: the curator drags it onto the narrows.
#:
#: Every endpoint is draggable regardless. A seed is a starting point, not an answer.
CUT_SEEDS = {
    "wbk:328974235": [
        {"id": "balfour_procter",
         "label": "Balfour Point → Procter Lighthouse",
         "note": "Main Body is east of this line; Upper West Arm is west of it.",
         "from": [-116.961725, 49.624937], "to": [-116.961382, 49.617047],
         "approx": False,
         "from_name": "Balfour", "to_name": "Procter"},
        {"id": "mcdonalds_landing",
         "label": "McDonalds Landing (Six Mile)",
         "note": "Upper West Arm is east of this line; Lower West Arm is west of it, "
                 "down to Corra Linn Dam.",
         # McDonald's Landing Regional Park (RDCK), 49.57957 / -117.21792 — the landing is on
         # the NORTH shore, and the arm at that longitude runs 49.5650 to 49.5776, so the line
         # drops south from the park across the water. Not from the place table: the only
         # "McDonalds Landing" on file is in Region 6, 700 km north.
         "from": [-117.21792, 49.56500], "to": [-117.21792, 49.57957],
         "approx": False,
         "source": "McDonald's Landing Regional Park (RDCK)",
         "from_name": "south shore", "to_name": "McDonalds Landing"},
    ],
    # Shannon Lake is 520 m across. The book gives no landmarks — "the netted off portion on
    # the south end of the lake" — so this is a line across the south end at the latitude where
    # the water runs -119.61602 to -119.60982, for the curator to drag onto the actual fence.
    # Its worklist wants ONE part, so the piece north of this line keeps the lake's identity.
    "wbk:329459193": [
        {"id": "shannon_netting",
         "label": "Netting across the south end",
         "note": "The netted-off portion is south of this line; the rest stays Shannon Lake.",
         "from": [-119.61650, 49.85588], "to": [-119.60930, 49.85588],
         "approx": True,
         "from_name": "west shore", "to_name": "east shore"},
    ],
}


def _mu_regions(mu_polys: dict) -> dict:
    """`{mu: region}` — from the book's own rows, with adjacency filling the gaps.

    A ZONE IS A REGION, NOT A LIST OF UNITS. Williston Lake's rows name the units they fall in:
    Zone A carries 7-30, 7-37, 7-38 and Zone B carries 7-31, 7-36. Cutting by the union of
    those names looks right and covers 1,302 of the lake's 1,727 km² — because four more units
    hold water the rows never mention:

        7-29  350 km²      7-24    6 km²
        7-38  ...          7-35   40 km²
        7-40   29 km²

    A quarter of the lake, in no zone at all. But the zones are not lists of units; Zone A is
    the part of the lake in Region 7A and Zone B the part in 7B, and between them they are the
    whole lake. So the cut is by REGION, and a region is every unit that belongs to it.

    The mapping comes from the synopsis rows, which state a region and its units on every line.
    One unit on Williston — 7-29 — appears on no row in the book at all, so it is placed by the
    region of the unit it shares its longest border with (7-28, 171 km, Omineca). Geometry, not
    a guess about numbering.
    """
    import collections
    from pipeline.regs.parsing.rows import load_synopsis_rows

    votes: dict = collections.defaultdict(collections.Counter)
    for row in load_synopsis_rows():
        reg = str(row.get("region") or "")
        mu = row.get("mu")
        for m in (mu if isinstance(mu, list) else ([mu] if mu else [])):
            if reg:
                votes[str(m)][reg] += 1
    known = {m: c.most_common(1)[0][0] for m, c in votes.items() if c}

    missing = [m for m in mu_polys if m not in known]
    for m in missing:
        g = mu_polys[m]
        best, best_len = None, 0.0
        for o, og in mu_polys.items():
            if o == m or o not in known:
                continue
            try:
                shared = g.boundary.intersection(og.boundary).length
            except Exception:
                continue
            if shared > best_len:
                best, best_len = o, shared
        if best:
            known[m] = known[best]
    return known


def _short_region(name: str) -> str:
    """'REGION 7A - Omineca' -> '7A'."""
    import re as _re
    m = _re.search(r"REGION\s+([0-9]+[A-Z]?)", str(name or ""), _re.I)
    return m.group(1) if m else str(name or "")


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
    mu_region = _mu_regions(mu_polys)
    from shapely.ops import unary_union

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
        # every unit whose water is actually in this lake, not only the ones a row names
        here = {m: g2 for m, g2 in mu_polys.items()
                if g2.intersects(g) and g2.intersection(g).area > 5e5}
        by_region: dict = {}
        for m, g2 in here.items():
            rname = _short_region(mu_region.get(m, ""))
            if not rname:
                continue
            by_region.setdefault(rname, []).append(g2)
        region_rings = {}
        for rname, gs in by_region.items():
            merged = gs[0] if len(gs) == 1 else unary_union(gs)
            clipped = merged.intersection(g)
            if not clipped.is_empty:
                region_rings[rname] = _ring(clipped, tf)
        # which region does each row speak for?
        for r in rows:
            regs = sorted({_short_region(mu_region.get(m, "")) for m in r["mus"]} - {""})
            r["region"] = regs[0] if len(regs) == 1 else ""
            r["mu_region_split"] = len(regs) > 1
        out["lakes"].append({
            "name": L["lake"], "item_id": L["item_id"], "wbk": wbk,
            "outline": _ring(g, tf),
            "rows": rows,
            "parts": [{"name": p.get("name"), "id": p.get("id")} for p in L.get("parts", [])],
            "mus": {m: _ring(mu_polys[m], tf) for m in touched if m in mu_polys},
            "regions": region_rings,
            "cuts": CUT_SEEDS.get(L["item_id"], []),
        })
        cov = sum(mu_polys[m].intersection(g).area for m in here) / g.area if here else 0
        print(f"  {L['lake']}: {len(rows)} row(s), regions "
              f"{sorted(region_rings) or '-'}, {len(here)} unit(s) on the water "
              f"({cov*100:.0f}% covered), {len(L.get('parts', []))} part(s) wanted")

    # THE WHOLE EXISTING FILE, so the page can hand back a complete added_lakes.geojson rather
    # than a fragment to merge by hand. Merging is where a curated polygon gets lost.
    if ADDED.exists():
        existing = json.loads(ADDED.read_text(encoding="utf-8")).get("features", [])
        out["existing"] = existing
        out["used_ids"] = sorted(f["properties"].get("id") for f in existing
                                 if isinstance(f.get("properties", {}).get("id"), int))
        print(f"  carrying {len(existing)} existing added-lake feature(s); "
              f"ids in use: {out['used_ids']}")

    blob = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
    html = PAGE.read_text(encoding="utf-8")
    html = re.sub(r'(<script id="d" type="application/json">).*?(</script>)',
                  lambda m: m.group(1) + blob + m.group(2), html, count=1, flags=re.S)
    PAGE.write_text(html, encoding="utf-8")
    print(f"\nwrote {len(blob):,} bytes into {PAGE.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
