"""`data/fetch_data.py` (the hand-run source refresh), the parts that need no network: T561.

Item 1 — each fetched layer records the day its SOURCE is from (the FWA publication, the OpenStreetMap base, the WFS
fetch day) beside the GeoPackage, so the release never dates the FWA by the day the layer was written locally.

Item 3 — a partial or empty answer fails the layer: an Overpass `remark`, no `elements`, an empty result, a hand-listed
area OSM no longer has, a failed live roster. Nothing is written from one.
"""
from __future__ import annotations

import json

import pytest

from data import fetch_data as F
from pipeline.common import source_dates


@pytest.fixture(autouse=True)
def _no_gpkg(monkeypatch):
    """The layers here are never written: their row count (read from the gpkg's metadata) is stubbed."""
    monkeypatch.setattr(F, "_layer_rows", lambda gpkg, layer: 7)


class _Resp:
    def __init__(self, headers):
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _head(monkeypatch, headers):
    seen = []

    def urlopen(req, timeout=None):
        seen.append((req.get_method(), req.full_url))
        return _Resp(headers)
    monkeypatch.setattr(F.urllib.request, "urlopen", urlopen)
    return seen


def test_an_fwa_layer_is_dated_by_its_publication_not_the_reupload(tmp_path, monkeypatch):
    # the object store's Last-Modified is the 2026 re-upload; the s3b header is the 2024 FWA publication
    seen = _head(monkeypatch, {"Last-Modified": "Wed, 18 Mar 2026 20:52:58 GMT",
                               "x-amz-meta-s3b-last-modified": "20240326T172119Z"})
    g = tmp_path / "bc_fisheries_data.gpkg"
    F.record_layer_day({"type": "FWA_STREAMS", "ftp": "https://x/FWA_STREAM_NETWORKS_SP.zip"}, "streams", g)
    assert seen == [("HEAD", "https://x/FWA_STREAM_NETWORKS_SP.zip")]
    assert source_dates.day(g, "streams") == "2024-03-26"


def test_a_source_that_states_no_day_fails_the_layer(monkeypatch, tmp_path):
    _head(monkeypatch, {"Last-Modified": "Wed, 18 Mar 2026 20:52:58 GMT"})
    with pytest.raises(RuntimeError, match="x-amz-meta-s3b-last-modified"):
        F.record_layer_day({"type": "FWA_GDB", "ftp": "https://x/FWA_BC.zip"}, "lakes", tmp_path / "a.gpkg")
    assert source_dates.read(tmp_path / "a.gpkg") is None


def test_an_overpass_layer_is_dated_by_its_osm_base(tmp_path):
    day = F._osm_base_day({"osm3s": {"timestamp_osm_base": "2026-07-15T23:00:00Z"}, "elements": []})
    F.record_layer_day({"type": "OVERPASS_LAND_ACCESS"}, "land_access", tmp_path / "a.gpkg", day)
    assert source_dates.day(tmp_path / "a.gpkg", "land_access") == "2026-07-15"
    with pytest.raises(RuntimeError, match="timestamp_osm_base"):
        F._osm_base_day({"elements": []})
    with pytest.raises(ValueError):  # an Overpass layer with no day is a failure, never undated
        F.record_layer_day({"type": "OVERPASS_WATERFALLS"}, "waterfalls", tmp_path / "a.gpkg", None)


def test_a_wfs_file_layer_dates_its_own_gpkg(tmp_path):
    g = tmp_path / "bc_fisheries_data.gpkg"
    (tmp_path / "fish_obstacles").mkdir()
    F.record_layer_day({"type": "WFS_FILE", "source": "WHSE_FISH.FISS_OBSTACLES_PNT_SP",
                        "dest": "fish_obstacles/fish_obstacles.gpkg", "layer": "fiss"}, "fish_obstacles_fiss", g)
    assert source_dates.read(g) is None
    assert source_dates.day(tmp_path / "fish_obstacles" / "fish_obstacles.gpkg", "fiss") is not None


# ---- item 3: partial and empty answers fail ------------------------------------------------------------------------

class _Body(_Resp):
    def __init__(self, body: dict):
        super().__init__({})
        self._b = json.dumps(body).encode()

    def read(self):
        return self._b


def test_an_overpass_remark_is_a_partial_answer_retried_then_failed(monkeypatch):
    calls = []

    def urlopen(req, timeout=None):
        calls.append(1)
        return _Body({"osm3s": {"timestamp_osm_base": "2026-11-10T00:00:00Z"}, "elements": [{"type": "node", "id": 1}],
                      "remark": "runtime error: Query timed out in \"query\" at line 3 after 181 seconds."})
    monkeypatch.setattr(F.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(F.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="partial: remark"):
        F._overpass_query("q", "land access", retries=3)
    assert len(calls) == 3                      # retried like a 503, never returned


def test_an_overpass_answer_without_elements_fails():
    with pytest.raises(F.OverpassPartial, match="no `elements`"):
        F._overpass_answer({"osm3s": {}}, "waterfalls")
    ok = {"elements": [], "osm3s": {"timestamp_osm_base": "2026-11-10T00:00:00Z"}}
    assert F._overpass_answer(ok, "waterfalls") is ok


def test_an_empty_result_is_a_failed_fetch():
    with pytest.raises(RuntimeError, match="returned nothing"):
        F._require_rows(0, "waterfalls")
    F._require_rows(1, "waterfalls")


def test_the_named_area_is_looked_up_by_its_osm_element(monkeypatch):
    import geopandas as gpd
    from shapely.geometry import Polygon

    asked = []
    sq = Polygon([(-122.6, 49.2), (-122.5, 49.2), (-122.5, 49.3), (-122.6, 49.3)])

    def by_id(q, by_osmid=False):
        asked.append((q, by_osmid))
        return gpd.GeoDataFrame([{"name": "UBC Malcolm Knapp Research Forest", "geometry": sq}], crs="EPSG:4326")
    monkeypatch.setattr(F.ox.geocoder, "geocode_to_gdf", by_id)
    name, osm_type, osm_id = F._NAMED_RESTRICTED_AREAS[0]
    rec = F._named_area_record(name, osm_type, osm_id, lambda i, tags, geom, t: (i, tags, t))
    assert asked == [("W1166294466", True)]
    assert rec == (1166294466, {"name": "UBC Malcolm Knapp Research Forest", "access": "permit"}, "way")

    monkeypatch.setattr(F.ox.geocoder, "geocode_to_gdf",
                        lambda q, by_osmid=False: gpd.GeoDataFrame({"geometry": []}, crs="EPSG:4326"))
    with pytest.raises(RuntimeError, match="not found in OSM"):
        F._named_area_record(name, osm_type, osm_id, lambda *a: a)


def test_a_failed_live_roster_writes_no_station_file(tmp_path, monkeypatch):
    def urlopen(req, timeout=None):
        raise F.urllib.error.URLError("down")
    monkeypatch.setattr(F.urllib.request, "urlopen", urlopen)
    dest = tmp_path / "bc_hydrometric_stations.json"
    with pytest.raises(RuntimeError, match="real-time roster is unavailable"):
        F.fetch_hydrometric_stations("hydrometric_stations", dest)
    assert not dest.exists()


def test_a_layers_row_count_is_recorded_with_its_day(tmp_path, monkeypatch):
    monkeypatch.setattr(F, "_layer_rows", lambda gpkg, layer: 136)
    F.record_layer_day({"type": "OVERPASS_LAND_ACCESS"}, "land_access", tmp_path / "a.gpkg", "2026-11-10")
    assert source_dates.read(tmp_path / "a.gpkg")["land_access"]["rows"] == 136
