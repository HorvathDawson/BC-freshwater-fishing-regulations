"""(T1246, owner 2026-11-27 "Fix it properly now") A city creek with its own tidal mouth takes the DFO Area its mouth lies
in; a city mouth on the line between two Areas is refused by name; an FWA basin keeps its own answer. On a GeoPackage
the test builds: two Areas side by side (28 west, 29 east) and one FWA river."""

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
    assert mouths == [("900-200000-", 500.0, 950.0, "Creek -1 (blk -1)")]
    got = P.basin_areas((28, 29), gpkg, city_mouths=mouths)
    assert got == {"900-100000-": 29, "900-200000-": 28}
    # its tributaries' codes nest under its own: they take its Area through the prefix
    assert P.area_of("900-200000-436900", got) == 28


def test_a_city_mouth_on_the_line_between_two_areas_is_refused_by_name(gpkg):
    with pytest.raises(ValueError, match=r"Creek -1 \(blk -1\).*900-300000-\).*Areas \[28, 29\]"):
        P.basin_areas((28, 29), gpkg, city_mouths=P.city_tidal_mouths([_creek("-1", "900-300000", (1000, 950))]))


def test_a_city_mouth_beyond_the_offset_limit_is_refused_by_name(gpkg):
    # 12 km inland of every Area: an FWA basin that far is dropped (an inland major); a city creek to the sea never is
    far = P.city_tidal_mouths([_creek("-4", "900-500000", (500, -12_000))])
    with pytest.raises(ValueError, match=r"Creek -4 \(blk -4\).*12,000 m from the nearest Area"):
        P.basin_areas((28, 29), gpkg, city_mouths=far)


def test_two_mouths_of_one_creek_equally_near_two_areas_are_refused(gpkg):
    # one creek in two pieces on one code, each mouth 50 m from a different Area (land below the polygons)
    two = P.city_tidal_mouths([_creek("-5", "900-600000", (500, -50)), _creek("-6", "900-600000", (1500, -50))])
    with pytest.raises(ValueError, match=r"two mouths of one city creek \(code 900-600000-.*Areas \[28, 29\]"):
        P.basin_areas((28, 29), gpkg, city_mouths=two)


def test_an_fwa_basin_keeps_its_own_mouths_area(gpkg):
    # a city line still on the FWA river's code (T1150's seven) does not re-decide that basin's Area
    got = P.basin_areas((28, 29), gpkg, city_mouths=P.city_tidal_mouths([_creek("-1", "900-100000", (500, 950))]))
    assert got == {"900-100000-": 29}


def test_only_a_line_that_drains_to_the_sea_has_a_mouth_of_its_own(gpkg):
    trib = _creek("-2", "900-200000-436900", (520, 1100), receiver="added")
    assert P.city_tidal_mouths([_creek("-1", "900-200000", (500, 950)), trib]) == [("900-200000-", 500.0, 950.0,
                                                                                    "Creek -1 (blk -1)")]
    with pytest.raises(ValueError, match="no code or no line"):
        P.city_tidal_mouths([{"blk": "-3", "wsc": "900-400000", "receiver_kind": "tidal", "segments": []}])


def test_policy_pinned(gpkg, monkeypatch):
    """Mutation pin: the rules are named policy. Off, the city creek drains to no Area (the state before T1246)."""
    assert P.OWN_MOUTH_TAKES_ITS_AREA is True and P.CITY_MOUTH_ON_AN_AREA_LINE == "refuse"
    mouths = P.city_tidal_mouths([_creek("-1", "900-200000", (500, 950))])
    monkeypatch.setattr(P, "OWN_MOUTH_TAKES_ITS_AREA", False)
    assert "900-200000-" not in P.basin_areas((28, 29), gpkg, city_mouths=mouths)


def test_the_registry_refuses_city_lines_without_their_mouths(monkeypatch):
    from pipeline.atlas.registry import build_registry
    monkeypatch.setattr(P, "basin_areas", lambda *a, **kw: {})
    g = StreamGraph()
    g.nodes["-1:0"] = StreamNode(node_id="-1:0", kind=NodeKind.stream, blk="-1", wsc="900-200000")
    with pytest.raises(ValueError, match="no `city_mouths`"):
        build_registry(g, pinned={})
    build_registry(g, pinned={}, city_mouths=[("900-200000-", 0.0, 0.0, "Creek -1 (blk -1)")])


@pytest.mark.needs_source
def test_the_city_mouths_change_no_fwa_basins_area(request):
    """On the province's gpkg: every FWA basin's Area is the same with the city mouths as without them (one read, the
    second call is the cache). Pinned against the city-free answer, not the live atlas: that answer IS today's pick
    (the code path is unchanged), and comparing with the live registry would need its 120 MB read for nothing more."""
    import json
    from pipeline.tests.conftest import GPKG_HINT, need
    from pipeline.tests import under_test
    need(request, "source", P._GPKG, GPKG_HINT)
    streams = json.loads(under_test.curated().waters.added_streams.read_text("utf-8"))["streams"]
    without = P.basin_areas(P.MINTED_AREAS, P._GPKG, city_mouths=())
    withc = P.basin_areas(P.MINTED_AREAS, P._GPKG, city_mouths=P.city_tidal_mouths(streams))
    assert {b: withc[b] for b in without} == without
    added = {b: a for b, a in withc.items() if b not in without}
    assert added and all(b.startswith("900-") for b in added)       # the city creeks' own basins, nothing else
