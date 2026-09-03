"""Curated non-FWA lake polygons (pipeline/atlas/waters/added_lakes).

The mechanism is one field. `graph.graph._assign_owners` hands a fid whose `wbk` is a known lake key
to that lake node and BREAKS the stream run there — so re-stamping the fids inside a polygon is what
cuts the stream, and nothing in the graph code has to change to add a lake.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.atlas.graph.blk_chains import FidRow
from pipeline.atlas.graph.graph import _assign_owners, _lake_name_pairs
from pipeline.atlas.waters.added_lakes import ingest
from pipeline.common.curated import CURATED

ROOT = Path(__file__).resolve().parents[1]


def _fid(fid, down, up, geom, wbk="riverpoly"):
    return FidRow(fid=fid, blk="B", wsc="w", edge_type="", wbk=wbk, gnis_id="", gnis_name="",
                  stream_order=1, stream_magnitude=1, down_m=down, up_m=up, geometry=geom,
                  down_node="", up_node="")


def test_the_curated_file_is_loadable_and_every_id_is_negative():
    """Both synthetic ids must be negative: a positive one could collide with a real FWA key and
    silently re-home its fids, which is the one failure this whole package could cause."""
    lakes = ingest.load(CURATED.waters.added_lakes)
    assert lakes, "expected at least one curated lake"
    for lk in lakes:
        assert lk["wbk"].startswith("-"), lk["wbk"]
        assert lk["gnis_id"].startswith("-"), lk["gnis_id"]
        # ...and the gnis is out of range in MAGNITUDE too (real ids run 1,642..8,000,027), so it
        # cannot be mistaken for a real one even if a sign is lost downstream.
        assert abs(int(lk["gnis_id"])) > 8_000_027, lk["gnis_id"]
    assert len({lk["wbk"] for lk in lakes}) == len(lakes), "duplicate wbk"


@pytest.mark.parametrize("bad, msg", [
    ({"wbk": "329241963"}, "NEGATIVE"),
    ({"wbk": "-1", "gnis_id": "12345"}, "NEGATIVE"),
])
def test_a_positive_id_is_refused(tmp_path, bad, msg):
    p = tmp_path / "added_lakes.geojson"
    p.write_text(json.dumps({"features": [{"type": "Feature", "properties": bad,
                                           "geometry": {"type": "Polygon", "coordinates": [
                                               [[0, 0], [0, 1], [1, 1], [0, 0]]]}}]}))
    with pytest.raises(ValueError, match=msg):
        ingest.load(p)


def test_the_polygon_cuts_the_stream_it_covers():
    """The point of the package: fids inside the polygon change owner, and the stream becomes TWO
    pieces. A fid merely clipped by the polygon's edge must NOT be claimed, or the cut would drift
    out into the river."""
    import geopandas as gpd
    from shapely.geometry import LineString, shape

    lakes = ingest.load(CURATED.waters.added_lakes)
    poly = gpd.GeoSeries([shape(lakes[0]["geometry"])], crs=4326).to_crs(3005)[0]
    c = poly.centroid

    def line(x0, x1):
        return LineString([(c.x + x0, c.y), (c.x + x1, c.y)])

    below, through, above = _fid("f1", 0, 100, line(-900, -800)), \
        _fid("f2", 100, 200, line(-20, 20)), _fid("f3", 200, 300, line(800, 900))
    fids = [below, through, above]
    lake_kind, lake_names, polys = {}, {}, {}
    rep = ingest.merge(fids, lake_kind, lake_names, polys, CURATED.waters.added_lakes)

    assert rep["claimed"] == {lakes[0]["wbk"]: ["f2"]}, "only the fid INSIDE is claimed"
    assert through.wbk == lakes[0]["wbk"] and below.wbk == "riverpoly" and above.wbk == "riverpoly"
    assert lake_kind[lakes[0]["wbk"]] == "lake"
    assert polys[lakes[0]["wbk"]] is not None, "the polygon must reach wbk_polys (display + MUs)"

    # lake_names must be (name, gnis) PAIRS — a bare string names the lake but leaves it with no
    # gnis, so a gnis-keyed name_variants entry or override could never resolve onto it.
    assert _lake_name_pairs(lake_names[lakes[0]["wbk"]]) == ((lakes[0]["name"], lakes[0]["gnis_id"]),)

    _owner, pieces, lake_fids = _assign_owners(fids, lake_kind)
    assert len(pieces) == 2, f"the lake must cut the stream in two, got {sorted(pieces)}"
    assert lake_fids == {f"lake:{lakes[0]['wbk']}": [through]}


def test_a_double_space_name_tuple_does_not_claim_the_names_inside_it():
    """Treston Lake carries the FWA tuple 'TRESTON LAKE  REDSAND LAKE', which LOOKS like it has
    swallowed Redsand's name. It has not: a name indexes under its whole normalised string, so
    Treston answers to 'treston lake redsand lake' and never to 'redsand lake'.

    Written down because the opposite was assumed and nearly "fixed" — the belief came from a
    SUBSTRING grep, which is not how the matcher works. 110 registry names contain a double space and
    they are not one convention: several lakes ('ELINOR L.  NARAMATA L.'), a qualifier ('LOON LAKE
    NEAR AINSWORTH'), a typo ('Waller  Creek'). Redsand needed a polygon, not a name rescue.
    """
    from pipeline.regs.matching.matcher import _norm, build_name_index
    from pipeline.atlas.registry import load_registry

    reg = Path("output/v2/full/registry.json")
    if not reg.exists():
        pytest.skip("no built registry")
    TRESTON = "wbk:329241963"
    idx = build_name_index(load_registry(reg))
    assert TRESTON in (idx.get(_norm("TRESTON LAKE  REDSAND LAKE")) or []), \
        "expected Treston to carry the combined FWA tuple"
    assert TRESTON not in (idx.get(_norm("Redsand Lake")) or []), \
        "Treston must never answer to the bare name 'Redsand Lake'"

    nv = json.loads((CURATED.waters.name_variants).read_text(encoding="utf-8"))
    by_wbk = {w: e for e in nv for w in (e.get("target") or {}).get("wbks", [])}
    redsand = by_wbk["-1"]["names"][0]
    assert redsand["name"] == "Redsand Lake" and redsand["display"] is True, \
        "nothing named this water at all, so the curated name is the label"
    # Treston's own entry is PRE-EXISTING and legitimate: bathymetry-sourced names, one of which is
    # the combined sheet label. It must be left exactly as it is — nothing here should "fix" it.
    treston = by_wbk["329241963"]["names"]
    assert {n["source"] for n in treston} == {"bathymetry"}
    assert not any(n.get("display") for n in treston), "no display pin should have been added"


def test_a_wsc_target_can_be_confined_to_streams():
    """A LAKE carries the wsc of the river threading it, so a wsc-targeted name lands on both.

    That renamed Long Lake to "Docee River" in a real build: the Docee's wsc target caught the lake
    the Docee drains. `blks` is stream-only by construction; `wscs` was not. The `kinds` filter makes
    the intent expressible, and every DFO wsc entry now carries `kinds: ["stream"]`.
    """
    from pipeline.atlas.graph.names import _node_matches
    from pipeline.common.models import NodeKind, StreamNode

    stream = StreamNode(node_id="s", kind=NodeKind.stream, blk="B", wsc="910-020688",
                        down_m=0, up_m=1, length_m=1)
    lake = StreamNode(node_id="lake:9", kind=NodeKind.lake, wbk="9", wsc="910-020688",
                      down_m=0, up_m=0, length_m=0)

    loose = {"wscs": ["910-020688"]}
    assert _node_matches(stream, loose, None) and _node_matches(lake, loose, None), \
        "unfiltered, a wsc target names the threaded lake too — the bug"

    confined = {"wscs": ["910-020688"], "kinds": ["stream"]}
    assert _node_matches(stream, confined, None)
    assert not _node_matches(lake, confined, None), "kinds must keep the name off the lake"


def test_every_dfo_wsc_name_variant_is_confined_to_streams():
    """Guarding the data, not just the mechanism: one un-confined entry silently renames a lake."""
    nv = json.loads((CURATED.waters.name_variants).read_text(encoding="utf-8"))
    bad = [e["target"]["wscs"] for e in nv
           if (e.get("target") or {}).get("wscs")
           and any((n.get("source") or "") == "dfo" for n in e["names"])
           and "stream" not in {str(k).lower() for k in (e["target"].get("kinds") or [])}]
    assert not bad, f"DFO wsc variants missing kinds=['stream']: {bad}"
