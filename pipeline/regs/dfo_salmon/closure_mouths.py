"""WHERE A NAMED CREEK MEETS ITS LAKE'S SHORE (T573, owner 2026-11-10): the centre of a "within 400 m of <creek>" circle.

    "you can find the mouths (make sure it is where stream meets lake (not including the connecting stream))"

THE CENTRE is the point where the creek's OWN channel meets the lake polygon's outline — never the far end of the
construction line the network draws on through the lake to join the lake's flow line (those segments carry the lake's
waterbody key; they are dropped before anything is measured). Read from the source network (`bc_fisheries_data.gpkg`:
`streams` with their names, `lakes`), so a creek the registry does not carry is still found.

    shore_mouth(lake, segs, name)   pure: the proposal for one creek — {centre (EPSG:3005 point), confidence, why} or
                                    {looked: why nothing is proposed}
        high     a stream of that name whose own channel touches the lake's outline: that point
        medium   its channel ends on ANOTHER lake within `REACH_M` of this one (Tsezakwa Creek enters Nilkitkwa Lake,
                 on the Babine's outlet): the point on that lake's outline, said so
        low      no stream of the name, but a hand-recorded hint (`HINTS`: a gazetteer feature of the same name) and
                 one stream meeting the shore near it: the largest such stream, said so
    main()                          rewrites the CIRCLE proposals of `<generated>/review/dfo_closed_area_geometry.json`
                                    (every stored wording's items, so an older-only item is placed too); stretches and
                                    everything else in the file are kept as they are

Proposals only: each carries its confidence and its evidence, and the owner confirms or moves it in the review app.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, Mapping, Optional

#: how far from the lake a named creek may end on another lake and still be proposed (metres)
REACH_M = 5000.0
#: a channel "touches" an outline within this (the network's own snapping)
TOUCH_M = 2.0
#: a hint's search radius along the shore (metres)
HINT_M = 1500.0
#: WHAT WAS LOOKED UP BY HAND, per water and creek (2026-11-10; BC Geographic Names web service, apps.gov.bc.ca/pub/
#: bcgnws, and the FWA network within 6 km of the lake): `near` = a gazetteer feature of the same name [lon, lat];
#: `looked` = what was tried, said on a creek that stays unplaced.
HINTS: dict[str, dict[str, dict]] = {
    "6:babine-lake": {
        "Pendleton Creek": {"near": [-125.708611, 54.519444],
                            "why": "BC Geographic Names has Pendleton Bay on Babine Lake (name id 6951) and a Pendleton "
                                   "Creek with no point at the lake; the network has no stream of that name"},
        "Six Mile Creek": {"looked": "no stream named Six Mile / Sixmile Creek in the FWA network within 6 km of the "
                                     "lake, and no BC Geographic Names creek of that name at the lake. By local "
                                     "accounts it is near Four Mile Creek at the lake's east end: three unnamed streams "
                                     "meet the shore within 2 km of Four Mile's mouth, none provably it"},
        "Hazelwood Creek": {"looked": "no stream named Hazelwood (or Heazlewood) Creek in the FWA network within 6 km "
                                      "of the lake; BC Geographic Names' only Heazlewood Creek is not at Babine Lake"},
        "Sockeye Creek": {"looked": "no stream named Sockeye Creek in the FWA network within 6 km of the lake; BC "
                                    "Geographic Names' Sockeye Creeks are not at Babine Lake"},
    },
}


def _norm(name: Optional[str]) -> str:
    return re.sub(r"[^a-z]", "", (name or "").lower())


def shore_mouth(lake, lake_key: int, segs: Iterable[tuple[Mapping, object]], name: str,
                other_lakes: Iterable[tuple[str, object]] = (), hint: Optional[Mapping] = None,
                to_albers=None) -> dict:
    """the proposal for one creek (see the module's words). `segs`: (properties, line) of the network near the lake —
    properties GNIS_NAME, WATERBODY_KEY, STREAM_ORDER, BLUE_LINE_KEY; `other_lakes`: (name, polygon) of lakes near it."""
    from shapely.geometry import Point
    from shapely.ops import nearest_points
    shore = lake.boundary
    own = [(q, g) for q, g in segs if q.get("WATERBODY_KEY") != lake_key]      # never the in-lake connector
    mine = [(q, g) for q, g in own if _norm(q.get("GNIS_NAME")) == _norm(name)]
    if mine:
        q, g = min(mine, key=lambda t: t[1].distance(shore))
        d = g.distance(shore)
        if d <= TOUCH_M:
            return {"centre": nearest_points(g, shore)[1], "confidence": "high", "stream": q.get("BLUE_LINE_KEY"),
                    "why": f"where {q.get('GNIS_NAME')}'s own channel (FWA blue line {q.get('BLUE_LINE_KEY')}) meets the "
                           "lake's outline; the in-lake connector line is not counted"}
        for lname, poly in other_lakes:
            best = min(mine, key=lambda t: t[1].distance(poly.boundary))
            if best[1].distance(poly.boundary) <= TOUCH_M and poly.distance(lake) <= REACH_M:
                return {"centre": nearest_points(best[1], poly.boundary)[1], "confidence": "medium",
                        "stream": best[0].get("BLUE_LINE_KEY"),
                        "why": f"{best[0].get('GNIS_NAME')} does not reach this lake: its channel enters {lname} "
                               f"({poly.distance(lake):.0f} m from this lake's outline). The centre is where it meets "
                               f"{lname}'s outline"}
        return {"looked": f"a stream named {name} is in the network but its channel ends {d:.0f} m from the lake's "
                          "outline, on no lake near it"}
    if hint and hint.get("near") and to_albers is not None:
        at = Point(*to_albers(*hint["near"]))
        # an UNNAMED stream only: one the network names is another creek (Cross Creek is not Pendleton Creek)
        cands = [(q, g) for q, g in own if not q.get("GNIS_NAME") and g.distance(shore) <= TOUCH_M
                 and g.distance(at) <= HINT_M]
        if cands:
            q, g = max(cands, key=lambda t: ((t[0].get("STREAM_ORDER") or 0), -t[1].distance(at)))
            pt = nearest_points(g, shore)[1]
            return {"centre": pt, "confidence": "low", "stream": q.get("BLUE_LINE_KEY"),
                    "why": f"{hint.get('why')}. Proposed: the largest UNNAMED stream meeting the shore within {HINT_M:g} m "
                           f"of that point (FWA blue line "
                           f"{q.get('BLUE_LINE_KEY')}, order {q.get('STREAM_ORDER')}, {pt.distance(at):.0f} m from it; "
                           f"{len(cands)} segments meet the shore there)"}
    return {"looked": (hint or {}).get("looked") or f"no stream named {name} in the network near the lake"}


def _read(source: Path, lake_key: int, pad: float = 6000.0):
    """the lake polygon, the network within `pad` m of its bounds, and the other lakes there (EPSG:3005)"""
    import fiona
    from shapely.geometry import shape
    from shapely.ops import unary_union
    with fiona.open(source, layer="lakes") as f:
        lake = unary_union([shape(r["geometry"]) for r in f.filter(where=f"WATERBODY_KEY = {int(lake_key)}")])
        b = lake.bounds
        bb = (b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad)
        others = [(r["properties"]["GNIS_NAME_1"], shape(r["geometry"])) for r in f.filter(bbox=bb)
                  if r["properties"]["WATERBODY_KEY"] != lake_key and r["properties"]["GNIS_NAME_1"]]
    with fiona.open(source, layer="streams") as f:
        segs = [(dict(r["properties"]), shape(r["geometry"])) for r in f.filter(bbox=bb)]
    return lake, segs, others


def main(argv: Optional[list[str]] = None) -> int:
    import argparse
    import pyproj
    from pipeline.common.curated import GENERATED
    from pipeline.regs.dfo_salmon import closures as K
    from pipeline.regs.review import dfo as D
    ap = argparse.ArgumentParser(description="re-derive the circle proposals of entry B: each creek's mouth on the shore")
    ap.add_argument("--source", type=Path, default=Path("data/source/bc_fisheries_data.gpkg"))
    ap.add_argument("--file", type=Path, default=Path(GENERATED.review) / "dfo_closed_area_geometry.json")
    a = ap.parse_args(argv)
    to_ll = pyproj.Transformer.from_crs(3005, 4326, always_xy=True).transform
    to_albers = pyproj.Transformer.from_crs(4326, 3005, always_xy=True).transform
    locs = list(D.locations(D.default_dir()).values())
    lake_of = {l.get("water_id"): next((i for i in (w or {}).get("item_ids") or [] if str(i).startswith("wbk:")), None)
               for _s, l, w in locs}
    entries = K.derive(l for _s, l, _w in locs)
    doc = json.loads(a.file.read_text(encoding="utf-8"))
    was = {(p["water_id"], p["key"]): p for p in doc["pieces"] if p["kind"] == "circle"}
    pieces = [p for p in doc["pieces"] if p["kind"] != "circle"]
    looked: dict[str, str] = {}
    report = []
    for water_id in sorted({e["water_id"] for e in entries.values()}):
        want = [x for x in K.water_areas(entries, water_id) if x["kind"] == "circle"]
        if not want:
            continue
        item = lake_of.get(water_id)
        if item is None:
            raise SystemExit(f"{water_id}: its circles need a lake, and the place's water names none")
        key = int(item.split(":", 1)[1])
        lake, segs, others = _read(a.source, key)
        for x in want:
            got = shore_mouth(lake, key, segs, x["name"], others, (HINTS.get(water_id) or {}).get(x["name"]), to_albers)
            if "centre" not in got:
                looked[f"{water_id}#{x['key']}"] = got["looked"]
                report.append({"name": x["name"], "placed": False, "looked": got["looked"]})
                continue
            lon, lat = to_ll(got["centre"].x, got["centre"].y)
            old = was.get((water_id, x["key"]))
            moved = None
            if old is not None:
                from shapely.geometry import Point
                moved = round(Point(*to_albers(*old["centre"])).distance(got["centre"]))
            pieces.append(K.PieceGeometry.model_validate({
                "key": x["key"], "water_id": water_id, "name": x["name"], "item": x["item"], "kind": "circle",
                "on_item": item, "centre": [round(lon, 6), round(lat, 6)], "radius_m": x["radius_m"],
                "centre_from": got["why"], "confidence": got["confidence"]}).model_dump(mode="json", exclude_none=True))
            report.append({"name": x["name"], "placed": True, "confidence": got["confidence"],
                           "centre": [round(lat, 6), round(lon, 6)], "moved_m": moved})
    doc["pieces"], doc["looked"] = pieces, looked
    a.file.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
