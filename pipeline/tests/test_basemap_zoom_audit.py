"""T85 — the basemap's zoom-cap audit (owner 2026-10-24: "basemap to z13 if no feature is lost").

`basemap.audit_zoom_cap` builds a small extent twice through the build's own path (`_trim`: extract + feature
filter) — once to z14, once capped — and compares feature kinds and named features. These tests run it on a tiny
fixture archive made with tippecanoe, never on the 4 GB source: a feature that exists only from z14 must be
reported lost, a feature the filter drops must not count, and a clean cap must pass. The live measurement (not
lossless at z13) is recorded in `basemap.py` beside `AUDIT_EXTENTS` and in the T85 ticket.
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


def test_a_kind_and_a_name_kept_for_z14_are_reported_lost(tmp_path):
    lossy = CLEAN + [
        _line("roads", 14, x=-123.13, kind="path", kind_detail="sidewalk"),
        # Protomaps' minor road: its line from z12, its NAME only from z14
        _line("roads", 12, maxzoom=13, x=-123.11, kind="minor_road", kind_detail="residential"),
        _line("roads", 14, x=-123.11, kind="minor_road", kind_detail="residential", name="Cedar Drive"),
    ]
    rep = B.audit_zoom_cap(_archive(tmp_path, lossy), cap=13, extents={"fixture": BBOX})["fixture"]
    assert set(rep["kinds"]) == {("roads", "path", "sidewalk")}
    assert rep["names"] == [("roads", "minor_road", "Cedar Drive")]
    assert not B.lossless({"fixture": rep})


def test_the_same_archive_is_lossless_at_its_own_depth(tmp_path):
    lossy = CLEAN + [_line("roads", 14, x=-123.13, kind="path", kind_detail="sidewalk")]
    assert B.lossless(B.audit_zoom_cap(_archive(tmp_path, lossy), cap=14, extents={"fixture": BBOX}))


def test_the_build_stays_at_z14_and_refuses_a_lossy_cap(tmp_path, monkeypatch):
    # z13 was measured lossy (basemap.py AUDIT_EXTENTS): the default cap stays z14 ...
    assert B.MAX_ZOOM == 14
    built = []
    monkeypatch.setattr(B, "build", lambda *a, **k: built.append(a))
    monkeypatch.setattr(B, "audit_zoom_cap", lambda src, cap, **k: {"x": {"kinds": {("roads", "path", None): 1},
                                                                         "names": [], "fragments": {}}})
    # ... and a shallower one is refused before anything is built
    with pytest.raises(SystemExit, match="z13 cap loses features"):
        B.main(["--max-zoom", "13", "--no-release", "--out", str(tmp_path / "b.pmtiles")])
    assert built == []
    assert B.main(["--audit-zoom", "13"]) == 1
