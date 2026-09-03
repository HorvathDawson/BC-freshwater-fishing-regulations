"""Merge `pipeline/added_lakes.geojson` into a build's fids / lake_kind / lake_names / wbk_polys.

The whole mechanism is one re-stamp. `graph/graph.py::_assign_owners` gives a fid whose `wbk` is a
known lake key to that lake node, and BREAKS the stream run there — so re-stamping the fids inside a
polygon is what cuts the stream, mints the `lake_in`/`lake_out` edges, and produces the `lake:{wbk}`
boundary a regulation binds to. Nothing in the graph code changes. See README.md.

Used by `pipeline/atlas/build.py`; runs immediately after `load_stream_fids`, before `build_blk_chains`,
because the chain pass is already the thing that reads `wbk`.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_lakes.ingest --check
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from pipeline.common.curated import CURATED, GENERATED, SOURCE

GEOJSON = CURATED.waters.added_lakes


def load(path: str | Path | None = None) -> list[dict]:
    """The curated features, validated. [] when the file is absent (added lakes are optional)."""
    p = Path(path) if path else GEOJSON
    if not p.exists():
        return []
    fc = json.loads(p.read_text(encoding="utf-8"))
    out: list[dict] = []
    seen: set[str] = set()
    for f in fc.get("features", []):
        props = f.get("properties") or {}
        wbk = str(props.get("wbk", "")).strip()
        if not wbk:
            raise ValueError("added lake with no wbk")
        if not wbk.startswith("-"):
            # Not fussiness: a positive key could collide with a real FWA waterbody and silently
            # re-home its fids. The negative band is what makes a synthetic id unmistakable.
            raise ValueError(f"added lake wbk {wbk!r} must be NEGATIVE (see README.md)")
        if wbk in seen:
            raise ValueError(f"duplicate added-lake wbk {wbk!r}")
        seen.add(wbk)
        if (f.get("geometry") or {}).get("type") not in ("Polygon", "MultiPolygon"):
            raise ValueError(f"added lake {wbk!r}: geometry must be a (Multi)Polygon")
        gnis = str(props.get("gnis_id", "")).strip()
        if gnis and not gnis.startswith("-"):
            raise ValueError(f"added lake {wbk!r}: gnis_id {gnis!r} must be NEGATIVE (see README.md)")
        out.append({"wbk": wbk, "gnis_id": gnis, "name": props.get("name", ""),
                    "kind": props.get("kind", "lake"), "props": props,
                    "geometry": f["geometry"]})
    return out


def _shapes(lakes: list[dict]):
    """(wbk, kind, name, shapely polygon in BC Albers) per lake — the CRS the fids are in."""
    import geopandas as gpd
    from shapely.geometry import shape

    if not lakes:
        return []
    polys = gpd.GeoSeries([shape(l["geometry"]) for l in lakes], crs=4326).to_crs(3005)
    return [(l["wbk"], l["kind"], l["name"], g) for l, g in zip(lakes, polys)]


def apply_to_fids(fids: list, lakes: list[dict]) -> dict:
    """Re-stamp every fid whose geometry falls inside an added lake. Mutates `fids` in place.

    A fid is claimed on MAJORITY OVERLAP, not on touching: a polygon inevitably clips the ends of the
    fids just outside it, and claiming those would move the cut out into the river. `FidRow` is
    `slots=True` but not frozen, so the stamp is a plain assignment.

    Returns {wbk: [claimed fid ids]} for reporting.
    """
    claimed: dict[str, list[str]] = {}
    shapes = _shapes(lakes)
    if not shapes:
        return claimed
    import geopandas as gpd

    idx = gpd.GeoSeries([g for _w, _k, _n, g in shapes], crs=3005).sindex
    for row in fids:
        geom = getattr(row, "geometry", None)
        if geom is None or geom.is_empty:
            continue
        for j in idx.query(geom, predicate="intersects"):
            wbk, _kind, _name, poly = shapes[j]
            inside = geom.intersection(poly).length
            if inside > geom.length / 2:                   # majority, not a clipped end
                row.wbk = wbk
                claimed.setdefault(wbk, []).append(row.fid)
                break
    return claimed


def merge(fids: list, lake_kind: dict, lake_names: dict, wbk_polys: dict,
          path: str | Path | None = None) -> dict:
    """Ingest the curated lakes into a build's four inputs. Returns a report dict."""
    lakes = load(path)
    if not lakes:
        return {"lakes": 0, "claimed": {}}
    by_wbk = {l["wbk"]: l for l in lakes}
    for wbk, kind, name, poly in _shapes(lakes):
        lake_kind[wbk] = kind
        if name:
            # (name, gnis_id) PAIRS, the shape `get_lake_names` produces — the paired gnis is what
            # lets the lake node carry one (`lake_gnis` in build_stream_graph) and therefore what
            # lets a gnis-keyed name_variants entry or override resolve onto it. A bare string here
            # would work for the name and silently leave the lake reachable by wbk only.
            lake_names[wbk] = ((name, by_wbk[wbk].get("gnis_id", "")),)
        wbk_polys[wbk] = poly                              # display geometry + MU assignment
    claimed = apply_to_fids(fids, lakes)
    return {"lakes": len(lakes), "claimed": claimed,
            "names": {l["wbk"]: l["name"] for l in lakes},
            "gnis": {l["wbk"]: l.get("gnis_id", "") for l in lakes}}


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Inspect the curated added-lake polygons.")
    ap.add_argument("--geojson", help=f"default: {GEOJSON}")
    ap.add_argument("--check", action="store_true", help="report what each polygon would claim")
    ap.add_argument("--graph", default=str(GENERATED.build() / "graph.gpkg"),
                    help="a built graph.gpkg, used only to show which pieces WOULD be split")
    args = ap.parse_args()

    lakes = load(args.geojson)
    print(f"{len(lakes)} curated added lake(s) in {args.geojson or GEOJSON}")
    for wbk, kind, name, poly in _shapes(lakes):
        print(f"\n  wbk:{wbk}  {name!r}  kind={kind}  area={poly.area / 1e4:.2f} ha")
        if not args.check or not Path(args.graph).exists():
            continue
        from pyogrio import read_dataframe

        b = poly.bounds
        st = read_dataframe(args.graph, layer="streams",
                            bbox=(b[0] - 500, b[1] - 500, b[2] + 500, b[3] + 500),
                            columns=["node_id", "blk", "wsc", "display_name", "length_m"])
        st["inside"] = st.geometry.intersection(poly).length
        hit = st[st["inside"] > 0].sort_values("inside", ascending=False)
        print(f"      stream pieces the polygon overlaps: {len(hit)}")
        for r in hit.itertuples():
            frac = r.inside / r.length_m if r.length_m else 0
            print(f"        {r.node_id:<22} blk={r.blk} wsc={r.wsc} {r.display_name!r}")
            print(f"            {r.inside:.0f} m of {r.length_m:.0f} m inside ({frac:.0%}) "
                  f"-> {'SPLIT here' if frac < 0.99 else 'wholly inside'}")


if __name__ == "__main__":
    main()
