"""T85 / T202 — the basemap's zoom-cap audit (owner 2026-10-24: "basemap to z13 if no feature is lost"; owner
2026-10-27: keep trails, FSRs and their names, drop what only matters in cities).

`basemap.audit_zoom_cap` builds a small extent twice through the build's own path (`_trim`): once to z14, once
capped, the cap including the z13 roads rebuild. It compares feature kinds, named features and road length. These
tests run it on a tiny fixture archive made with tippecanoe, never on the 4 GB source. They check that:
  - a road name kept for z14 only survives the cap, because the rebuild carries it;
  - a non-road kind or name that exists only at z14 is reported lost;
  - an accepted urban kind is reported as accepted, not lost;
  - a feature the filter drops does not count;
  - a clean cap passes.
The live measurement is in `basemap.py` beside `AUDIT_EXTENTS` and in T202-basemap-audit.md.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import orjson
import pytest

from pipeline.deliver.tiles import basemap as B

BBOX = (-123.20, 49.66, -123.08, 49.76)


def _line(layer: str, minzoom: int, maxzoom: int = 14, x: float = -123.15, **props) -> dict:
    return {"type": "Feature", "properties": props,
            "geometry": {"type": "LineString", "coordinates": [[x, 49.69], [x + 0.03, 49.73]]},
            "tippecanoe": {"layer": layer, "minzoom": minzoom, "maxzoom": maxzoom}}


def _archive(tmp_path: Path, features: list[dict]) -> Path:
    src = tmp_path / "features.geojsonl"
    src.write_bytes(b"".join(orjson.dumps(f) + b"\n" for f in features))
    out = tmp_path / "fixture.pmtiles"
    subprocess.run(["tippecanoe", "-q", "-f", "-o", str(out), "-Z10", "-z14", "-r1", "--no-tile-size-limit",
                    "--no-feature-limit", "--no-line-simplification", "-P", str(src)], check=True)
    return out


CLEAN = [
    _line("roads", 10, kind="major_road", kind_detail="primary", name="Highway 99"),
    _line("landuse", 10, x=-123.12, kind="wood"),
    # an OSM stream line: the build's filter drops it at every zoom, so it can never be "lost" by the cap
    _line("water", 14, x=-123.10, kind="stream", name="Some Creek"),
]


def test_a_clean_cap_is_lossless(tmp_path):
    rep = B.audit_zoom_cap(_archive(tmp_path, CLEAN), cap=13, extents={"fixture": BBOX})
    assert rep["fixture"]["kinds"] == {} and rep["fixture"]["names"] == []
    assert B.lossless(rep)
    # the filter ran: the stream line is in neither build
    assert not any(k[1] == "stream" for k in rep["fixture"]["fragments"])


def test_the_roads_rebuild_carries_a_z14_only_name_and_drops_the_urban_kinds(tmp_path):
    lossy = CLEAN + [
        _line("roads", 14, x=-123.13, kind="path", kind_detail="sidewalk"),
        # Protomaps' minor road: its line from z12, its NAME only from z14 (a forest service road, say)
        _line("roads", 12, maxzoom=13, x=-123.11, kind="minor_road", kind_detail="unclassified"),
        _line("roads", 14, x=-123.11, kind="minor_road", kind_detail="unclassified", name="Ashlu River FSR"),
    ]
    rep = B.audit_zoom_cap(_archive(tmp_path, lossy), cap=13, extents={"fixture": BBOX})["fixture"]
    assert rep["kinds"] == {} and rep["names"] == [] and rep["short"] == {}
    assert set(rep["accepted"]["kinds"]) == {("roads", "path", "sidewalk")}
    assert B.lossless({"fixture": rep})


def test_a_kind_and_a_name_kept_for_z14_outside_roads_are_reported_lost(tmp_path):
    lossy = CLEAN + [
        _line("landuse", 14, x=-123.13, kind="school"),
        _line("landuse", 14, x=-123.11, kind="wood", name="Some Grove"),
    ]
    rep = B.audit_zoom_cap(_archive(tmp_path, lossy), cap=13, extents={"fixture": BBOX})["fixture"]
    assert set(rep["kinds"]) == {("landuse", "school", None)}
    assert rep["names"] == [("landuse", "wood", "Some Grove", None)]
    assert not B.lossless({"fixture": rep})


def test_a_road_kind_that_comes_up_short_is_a_loss_unless_accepted():
    k, urban = ("roads", "path", "track"), ("roads", "path", "sidewalk")
    deep = ({k: 3, urban: 2}, set(), {k: 10.0, urban: 5.0})
    capped = ({k: 1, urban: 1}, set(), {k: 8.0, urban: 1.0})
    rep = B.zoom_cap_loss(deep, capped)
    assert rep["short"] == {k: (10.0, 8.0)} and rep["accepted"]["short"] == {urban: (5.0, 1.0)}
    assert B.zoom_cap_loss(deep, ({k: 1, urban: 1}, set(), {k: 9.5, urban: 1.0}))["short"] == {}


def test_the_same_archive_is_lossless_at_its_own_depth(tmp_path):
    lossy = CLEAN + [_line("roads", 14, x=-123.13, kind="path", kind_detail="sidewalk")]
    assert B.lossless(B.audit_zoom_cap(_archive(tmp_path, lossy), cap=14, extents={"fixture": BBOX}))


def test_the_build_caps_at_z13_and_refuses_a_lossy_cap(tmp_path, monkeypatch):
    # T202 (owner 2026-10-27): z13, with the roads rebuilt from the source's z14 ...
    assert (B.MAX_ZOOM, B.SOURCE_ZOOM) == (13, 14)
    built = []
    monkeypatch.setattr(B, "build", lambda *a, **k: built.append(a))
    lossy = {"x": {"kinds": {("landuse", "school", None): 1}, "names": [], "short": {}, "fragments": {}}}
    monkeypatch.setattr(B, "audit_zoom_cap", lambda src, cap, **k: lossy)
    # ... and the audit runs before every capped build: a loss refuses it before anything is built
    with pytest.raises(SystemExit, match="z13 cap loses features"):
        B.main(["--no-release", "--out", str(tmp_path / "b.pmtiles")])
    assert built == []
    assert B.main(["--audit-zoom", "13"]) == 1
    clean = {"x": {"kinds": {}, "names": [], "short": {}, "fragments": {}}}
    monkeypatch.setattr(B, "audit_zoom_cap", lambda src, cap, **k: clean)
    assert B.main(["--no-release", "--out", str(tmp_path / "b.pmtiles")]) == 0 and len(built) == 1
