"""Merge `pipeline/added_lakes.geojson` into a build's fids / lake_kind / lake_names / wbk_polys.

A feature authors ONE positive `id`; its `wbk` (`-id`) and `gnis_id` (`-(9_000_000 + id)`) are
derived here, so the two synthetic keys can never drift apart. See README.md.

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

GNIS_BASE = 9_000_000
"""Real gnis ids run 1,642..8,000,027, so ``-(GNIS_BASE + id)`` is out of range in MAGNITUDE as well
as in sign — it cannot be mistaken for a real id even if a sign is dropped downstream."""


def wbk_for(lake_id: int) -> str:
    """``1 -> '-1'``. FWA waterbody keys are positive 9-digit integers, so the negative band is free."""
    return f"-{lake_id}"


def gnis_for(lake_id: int) -> str:
    """``1 -> '-9000001'``. See GNIS_BASE."""
    return f"-{GNIS_BASE + lake_id}"


def parse_id(props: dict) -> int:
    """The one authored number behind a lake, validated.

    A feature carries ONE id and both synthetic keys are derived from it, so they can never drift
    apart — authoring them separately is how you get a lake whose polygon and whose name resolve to
    different things. A file still carrying the old `wbk`/`gnis_id` pair is refused outright rather
    than half-read, because a stale pair that disagrees with `id` is exactly the silent mismatch this
    shape exists to prevent."""
    stale = [k for k in ("wbk", "gnis_id") if k in props]
    if stale:
        raise ValueError(
            f"added lake {props.get('id', '?')!r}: {', '.join(stale)} is DERIVED from `id` and must "
            f"not be authored (see README.md) — drop it and keep `id` alone")
    if "id" not in props:
        raise ValueError("added lake with no `id` (see README.md)")
    raw = props["id"]
    if isinstance(raw, bool) or not isinstance(raw, int):
        # A string "1" would still derive the right keys, but it makes ordering and
        # next-id-is-max-plus-one wrong the moment there are ten of them.
        raise ValueError(f"added lake id {raw!r} must be a JSON integer, not {type(raw).__name__}")
    if raw < 1:
        # The id is authored POSITIVE and negated on derivation. Taking a negative here would
        # produce wbk '--1' / gnis '-8999999' — the second of which is a plausible real id.
        raise ValueError(f"added lake id {raw!r} must be a POSITIVE integer (see README.md)")
    return raw


def load(path: str | Path | None = None) -> list[dict]:
    """The curated features, validated. RAISES when the file is absent (AGENTS 37).

    Absent curated data is a bug, never an empty set: an empty list here would build an atlas
    with no lake parts and a bundle whose `item.part_of` is silently empty. A build that wants
    no added lakes says so (`--no-added-lakes`); it does not get there by a missing file.

    Each feature authors a single positive `id`; `wbk` and `gnis_id` are derived from it here, so
    every consumer downstream still sees the same two negative keys it always did."""
    p = Path(path) if path else GEOJSON
    if not p.exists():
        raise FileNotFoundError(f"added lakes: curated file {p} is missing — absent curated data "
                                "is a bug, not an empty set (pass --no-added-lakes to build without)")
    fc = json.loads(p.read_text(encoding="utf-8"))
    out: list[dict] = []
    seen: set[int] = set()
    for f in fc.get("features", []):
        props = f.get("properties") or {}
        lake_id = parse_id(props)
        if lake_id in seen:
            raise ValueError(f"duplicate added-lake id {lake_id!r}")
        seen.add(lake_id)
        if (f.get("geometry") or {}).get("type") not in ("Polygon", "MultiPolygon"):
            raise ValueError(f"added lake {lake_id!r}: geometry must be a (Multi)Polygon")
        out.append({"id": lake_id, "wbk": wbk_for(lake_id), "gnis_id": gnis_for(lake_id),
                    "name": props.get("name", ""),
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
    parent_of = {l["wbk"]: str((l.get("props", {}).get("part_of") or {}).get("wbk") or "")
                 for l in lakes}
    if any(parent_of.values()):
        _parts_follow_the_route(fids, parent_of, claimed)
    return claimed


def _parts_follow_the_route(fids: list, parent_of: dict[str, str],
                            claimed: dict[str, list[str]]) -> int:
    """Make each PART of a split lake one run along every blue line routed under it.

    Majority overlap decides fid by fid, and where a route runs ALONG the line between two parts it
    can go back and forth. The Peace's under-lake route (blk 359572348) near Finlay Forks crosses the
    Zone A / Zone B line three times in 1.6 km, so its fids went -18, -19, -18 (fid 166061648, 144 m),
    -19 — and that one fid made Zone B an INFLOW of Zone A: `lake:-19 -> lake:-18` beside the real
    `lake:-18 -> lake:-19`. A walk from Zone A then climbed every Zone B tributary as "a tributary
    lake" (7,707 sections), and Zone B's walk took the whole Zone A half (98,634).

    The parts must follow the river's route in order. Along each blue line, a run of measure-
    contiguous fids claimed by parts of ONE parent is cut into same-part runs; while a run has the
    SAME part on both sides, the shortest such run is handed to that part. The Peace's 144 m of -18
    goes to -19; an unnamed route under Zone A that crosses 10 km of the Nation Arm polygon
    (blk 359001899: -18, -16, -18) stays Zone A's, so Nation Arm is not both above and below Zone
    A. Handing a run to a NEIGHBOUR that is not on both sides would move a whole reach: 70 km of
    that same route went to Nation Arm under that rule. Only parts of the same parent trade fids:
    a separate lake is never absorbed.
    Returns the number of fids re-stamped.
    """
    by_blk: dict[str, list] = {}
    for r in fids:
        if parent_of.get(getattr(r, "wbk", "") or ""):
            by_blk.setdefault(str(r.blk), []).append(r)
    moved = 0
    for blk in sorted(by_blk):
        rows = sorted(by_blk[blk], key=lambda r: (r.down_m, r.up_m, str(r.fid)))
        chains: list[list] = []
        for r in rows:
            prev = chains[-1][-1] if chains else None
            if (prev is not None and abs(prev.up_m - r.down_m) < 1.0
                    and parent_of[prev.wbk] == parent_of[r.wbk]):
                chains[-1].append(r)
            else:
                chains.append([r])
        for chain in chains:
            while True:
                runs: list[list] = []
                for r in chain:
                    if runs and runs[-1][-1].wbk == r.wbk:
                        runs[-1].append(r)
                    else:
                        runs.append([r])
                length = [sum(x.up_m - x.down_m for x in run) for run in runs]
                # SANDWICHED: a run with the same part on both sides is a detour of that part's
                # route; the shortest goes first, so a 144 m wiggle yields before the run it
                # interrupts.
                cands = [(length[i], i) for i in range(1, len(runs) - 1)
                         if runs[i - 1][0].wbk == runs[i + 1][0].wbk]
                if not cands:
                    break
                _ln, i = min(cands)
                to = i - 1
                new = runs[to][0].wbk
                for r in runs[i]:
                    old = r.wbk
                    claimed[old].remove(r.fid)
                    if not claimed[old]:
                        del claimed[old]
                    r.wbk = new
                    claimed.setdefault(new, []).append(r.fid)
                    moved += 1
                    print(f"    part run: fid {r.fid} (blk {blk}, {r.down_m:.0f}-{r.up_m:.0f} m) "
                          f"lake {old} -> {new}, so the parts follow the route in order")
    return moved


def lake_parts(lakes: list[dict]) -> dict[str, str]:
    """{part wbk: parent wbk} for every curated lake that is a PART of another (`part_of`)."""
    out: dict[str, str] = {}
    for l in lakes:
        parent = str((l.get("props", {}).get("part_of") or {}).get("wbk") or "")
        if parent:
            out[str(l["wbk"])] = parent
    return out


def merge(fids: list, lake_kind: dict, lake_names: dict, wbk_polys: dict,
          path: str | Path | None = None, lake_wsc: dict | None = None) -> dict:
    """Ingest the curated lakes into a build's inputs. Returns a report dict.

    `lake_wsc` ({wbk: polygon code}) gains each PART's code: its parent's (`part_of`). A part is a
    piece of an FWA lake, so it sits where FWA puts that lake; a curated lake that is no part has no
    polygon code and falls back as any lake does."""
    lakes = load(path)
    if not lakes:
        return {"lakes": 0, "claimed": {}}
    by_wbk = {l["wbk"]: l for l in lakes}
    if lake_wsc is not None:
        for l in lakes:
            parent = str((l["props"].get("part_of") or {}).get("wbk") or "")
            if parent and lake_wsc.get(parent):
                lake_wsc[l["wbk"]] = lake_wsc[parent]
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
            "gnis": {l["wbk"]: l.get("gnis_id", "") for l in lakes},
            # each PART's lake, for the registry (`registry.add_lake_parts`): the one record of the
            # relation the bundle's `item.part_of` is written from
            "part_of": lake_parts(lakes)}


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
    for lake, (wbk, kind, name, poly) in zip(lakes, _shapes(lakes)):
        print(f"\n  id {lake['id']} -> wbk:{wbk} gnis:{lake['gnis_id']}  {name!r}  "
              f"kind={kind}  area={poly.area / 1e4:.2f} ha")
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
