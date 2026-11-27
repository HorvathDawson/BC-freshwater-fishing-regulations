"""(T1246, owner 2026-11-27 "Fix it properly now") A city creek with its own tidal mouth takes the DFO Area its mouth lies
in; a mouth on the line between two Areas takes the lower Area number. On a GeoPackage the test builds: two Areas side
by side (28 west, 29 east) and one FWA river."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import LineString, box

from pipeline.atlas.splits import pfma_drainage as P
from pipeline.common.models import NodeKind, StreamGraph, StreamNode


@pytest.fixture
def gpkg(tmp_path):
    path = str(tmp_path / "mini.gpkg")
    areas = gpd.GeoDataFrame({"MANAGEMENT_AREA": [28, 29, 0]},
                             geometry=[box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000), box(0, 1000, 2000, 3000)],
                             crs=3005)
    areas.to_file(path, layer=P.LAYER, driver="GPKG")
    # an FWA river whose mouth (its first point, measure 0) lies in Area 29
    streams = gpd.GeoDataFrame({"FWA_WATERSHED_CODE": ["900-100000-000000"], "DOWNSTREAM_ROUTE_MEASURE": [0.0]},
                               geometry=[LineString([(1500, 900), (1500, 1500)])], crs=3005)
    streams.to_file(path, layer="streams", driver="GPKG")
    yield path
    P._CACHE.pop(f"{path}|all", None)


def _creek(blk, wsc, mouth, receiver="tidal"):
    return {"blk": blk, "wsc": wsc, "name": f"Creek {blk}", "receiver_kind": receiver,
            "segments": [{"coords3005": [list(mouth), [mouth[0], mouth[1] + 300]]}]}


def test_a_city_creek_takes_the_area_its_own_mouth_lies_in(gpkg):
    mouths = P.city_tidal_mouths([_creek("-1", "900-200000", (500, 950))])
    assert mouths == [("900-200000-", 500.0, 950.0)]
    got = P.basin_areas((28, 29), gpkg, city_mouths=mouths)
    assert got == {"900-100000-": 29, "900-200000-": 28}
    # its tributaries' codes nest under its own: they take its Area through the prefix
    assert P.area_of("900-200000-436900", got) == 28


def test_a_mouth_on_the_line_between_two_areas_takes_the_lower_number(gpkg):
    got = P.basin_areas((28, 29), gpkg, city_mouths=P.city_tidal_mouths([_creek("-1", "900-300000", (1000, 950))]))
    assert got["900-300000-"] == 28


def test_only_a_line_that_drains_to_the_sea_has_a_mouth_of_its_own(gpkg):
    trib = _creek("-2", "900-200000-436900", (520, 1100), receiver="added")
    assert P.city_tidal_mouths([_creek("-1", "900-200000", (500, 950)), trib]) == [("900-200000-", 500.0, 950.0)]
    with pytest.raises(ValueError, match="no code or no line"):
        P.city_tidal_mouths([{"blk": "-3", "wsc": "900-400000", "receiver_kind": "tidal", "segments": []}])


def test_policy_pinned(gpkg, monkeypatch):
    """Mutation pin: the two rules are named policy. Off, the city creek drains to no Area (the state before T1246)."""
    assert P.OWN_MOUTH_TAKES_ITS_AREA is True and P.TIE_TAKES_LOWER_AREA is True
    mouths = P.city_tidal_mouths([_creek("-1", "900-200000", (500, 950))])
    monkeypatch.setattr(P, "OWN_MOUTH_TAKES_ITS_AREA", False)
    assert "900-200000-" not in P.basin_areas((28, 29), gpkg, city_mouths=mouths)
    monkeypatch.setattr(P, "OWN_MOUTH_TAKES_ITS_AREA", True)
    monkeypatch.setattr(P, "TIE_TAKES_LOWER_AREA", False)
    ties = [P._pick([(0.0, 29), (0.0, 28)]) for _ in range(3)]
    assert ties == [(0.0, 29)] * 3                                    # off: the first row wins, whatever it is


def test_the_registry_refuses_city_lines_without_their_mouths(monkeypatch):
    from pipeline.atlas.registry import build_registry
    monkeypatch.setattr(P, "basin_areas", lambda *a, **kw: {})
    g = StreamGraph()
    g.nodes["-1:0"] = StreamNode(node_id="-1:0", kind=NodeKind.stream, blk="-1", wsc="900-200000")
    with pytest.raises(ValueError, match="no `city_mouths`"):
        build_registry(g, pinned={})
    build_registry(g, pinned={}, city_mouths=[("900-200000-", 0.0, 0.0)])
