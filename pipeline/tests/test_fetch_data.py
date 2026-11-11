"""`data/fetch_data.py` (the hand-run source refresh), the parts that need no network: T561.

Item 1 — each fetched layer records the day its SOURCE is from (the FWA publication, the OpenStreetMap base, the WFS
fetch day) beside the GeoPackage, so the release never dates the FWA by the day the layer was written locally.
"""
from __future__ import annotations

import pytest

from data import fetch_data as F
from pipeline.common import source_dates


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
