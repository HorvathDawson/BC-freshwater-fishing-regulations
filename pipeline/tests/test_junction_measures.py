"""The mis-measured junctions (2026-11-13), cause B: FWA's UPSTREAM_ROUTE_MEASURE is inflated on 1,317 stream features
(371 blue lines, mostly the Comox / Courtenay codes) — it reads as DRM counted twice — while DRM and LENGTH_METRE agree
with the geometry. A junction's measure is the URM of the feature below it, so a tributary joined its receiver
kilometres up the line (Black Creek, 354154247), and the section ids cut there carried the inflated number.

Pinned on a small GeoPackage written here (the Black Creek pattern): the loader takes DRM + LENGTH_METRE where FWA's
URM disagrees, and the graph's junction measure follows. MUTATION: read URM as given (`upstream_measure` returning
`up_m`) -> the tributary joins at 4,587.5, past the end of the line it joins.
"""
from __future__ import annotations

import pytest
from shapely.geometry import LineString

from pipeline.atlas.graph import blk_chains as BC
from pipeline.atlas.graph.graph import build_stream_graph


def test_upstream_measure_keeps_a_true_urm_and_mends_an_inflated_one():
    assert BC.upstream_measure(0.0, 1686.6, 1686.6) == 1686.6               # agrees: FWA's own number
    assert BC.upstream_measure(10.0, 110.4, 100.0) == 110.4                  # rounding (under 1 m) is left alone
    assert BC.upstream_measure(1686.6, 4587.5, 1214.3) == pytest.approx(2900.9)   # Black Creek's second feature


@pytest.fixture
def gpkg(tmp_path):
    import geopandas as gpd
    # receiver R: three features along x, measures DRM 0 / 1000 / 2500, URM inflated on the 2nd and 3rd (DRM twice)
    rows = [
        dict(LINEAR_FEATURE_ID=1, BLUE_LINE_KEY=100, DOWNSTREAM_ROUTE_MEASURE=0.0, UPSTREAM_ROUTE_MEASURE=1000.0,
             LENGTH_METRE=1000.0, geometry=LineString([(0, 0), (1000, 0)])),
        dict(LINEAR_FEATURE_ID=2, BLUE_LINE_KEY=100, DOWNSTREAM_ROUTE_MEASURE=1000.0, UPSTREAM_ROUTE_MEASURE=3500.0,
             LENGTH_METRE=1500.0, geometry=LineString([(1000, 0), (2500, 0)])),
        dict(LINEAR_FEATURE_ID=3, BLUE_LINE_KEY=100, DOWNSTREAM_ROUTE_MEASURE=2500.0, UPSTREAM_ROUTE_MEASURE=5500.0,
             LENGTH_METRE=500.0, geometry=LineString([(2500, 0), (3000, 0)])),
        # tributary T joins where R's 2nd feature ends (x = 2500)
        dict(LINEAR_FEATURE_ID=4, BLUE_LINE_KEY=200, DOWNSTREAM_ROUTE_MEASURE=0.0, UPSTREAM_ROUTE_MEASURE=800.0,
             LENGTH_METRE=800.0, geometry=LineString([(2500, 0), (2500, 800)])),
    ]
    common = dict(FWA_WATERSHED_CODE="", GNIS_ID=None, GNIS_NAME=None, WATERBODY_KEY=None, STREAM_ORDER=1,
                  STREAM_MAGNITUDE=1, EDGE_TYPE=1000, WATERSHED_KEY=None)
    for r in rows:
        r.update(common)
        r["FWA_WATERSHED_CODE"] = "905-000001" if r["BLUE_LINE_KEY"] == 100 else "905-000001-000002"
        r["WATERSHED_KEY"] = r["BLUE_LINE_KEY"]
    p = tmp_path / "streams.gpkg"
    gpd.GeoDataFrame(rows, crs="EPSG:3005").to_file(p, layer="streams", driver="GPKG")
    return p


def test_the_junction_measure_is_on_the_drm_scale(gpkg):
    rows = BC.load_stream_fids(str(gpkg))
    up = {int(r.fid): r.up_m for r in rows}
    assert up == {1: 1000.0, 2: 2500.0, 3: 3000.0, 4: 800.0}
    g = build_stream_graph(BC.build_blk_chains(rows, {}), rows)
    (e,) = [e for e in g.edges if g.nodes[e.from_node].blk in ("200", 200)]
    assert e.at_measure == 2500.0 and (e.x, e.y) == (2500.0, 0.0)
    recv = g.nodes[e.to_node]
    assert recv.down_m <= e.at_measure <= recv.up_m <= 3000.0
