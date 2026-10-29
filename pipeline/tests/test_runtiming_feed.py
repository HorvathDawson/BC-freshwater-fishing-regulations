"""THE RUN-TIMING FEED (T199): bind -> the app's files, on a synthetic base (no atlas, no network). Whole values: the
relations each section takes, the paint index's profiles and month bits, the section-free reaches, and the checks
that refuse a mismatched set.
"""
from __future__ import annotations

import gzip
import json

import pytest

from pipeline.runtiming.feed import base as BASE
from pipeline.runtiming.feed import build as B
from pipeline.runtiming.timing import PENTADS, write_timing

# a river (sids 1, 2: mouth then upper, one blue line), its tributary (3) and a first-order creek (4) inside the CU, a
# lake (6) draining into the tributary, and a second river (5) whose mouth lies beside the first's — never walked into
ROWS = {  # sid: (k, order, wsc, blk, m0, m1, names, down, item, x, y)
    1: (0, 5, "100-000001", "B1", 0, 1000, ["big river"], [], "gnis:1", -122.0, 49.0),
    2: (0, 5, "100-000001", "B1", 1000, 2000, ["big river"], [1], "gnis:1", -122.0, 49.1),
    3: (0, 3, "100-000001-000100", "B2", 0, 500, ["test creek"], [2], "gnis:3", -122.0, 49.5),
    4: (0, 1, "100-000001-000100-000200", "B4", 0, 100, ["tiny creek"], [3], "gnis:4", -122.01, 49.51),
    5: (0, 5, "200-000001", "B5", 0, 1000, ["other river"], [], "gnis:5", -122.1, 49.0),
    6: (1, 0, "100-000001-000100", "", 0, 0, ["test lake"], [3], "gnis:6", -122.02, 49.52),
}


def make_base(tmp_path):
    sids = sorted(ROWS)
    st = {"wsc": [], "wbk": [], "nm": [], "it": []}
    ix = lambda t, v: st[t].index(v) if v in st[t] else (st[t].append(v) or len(st[t]) - 1)   # noqa: E731
    cols = {c: [] for c in ("sid", "k", "o", "len", "wsc", "blk", "m0", "m1", "wbk", "nm", "dn", "it", "x", "y")}
    prev = 0
    for s in sids:
        k, o, wsc, blk, m0, m1, nm, dn, it, x, y = ROWS[s]
        for c, v in (("sid", s - prev), ("k", k), ("o", o), ("len", m1 - m0), ("wsc", ix("wsc", wsc)), ("blk", blk),
                     ("m0", m0), ("m1", m1), ("wbk", ix("wbk", "W6" if k else "")), ("nm", [ix("nm", n) for n in nm]),
                     ("dn", dn), ("it", ix("it", it)), ("x", int(x * 1e5)), ("y", int(y * 1e5))):
            cols[c].append(v)
        prev = s
    b = {"format": BASE.FORMAT, "section_handles": "h" * 16, "tile_version": "h" * 16 + ".t", "count": len(sids),
         "strings": st, "cols": cols}
    p = tmp_path / "base.json.gz"
    p.write_bytes(gzip.compress(json.dumps(b).encode()))
    return p


def curve(months):
    """A curve in the water (100) through the pentads wholly inside `months`, else 0 (a pentad across a month's end
    puts the run in both months: that is the reading, not a rounding)."""
    others = lambda m: {p for n, ps in enumerate(B.MONTH_PENTADS) if n not in months for p in ps}   # noqa: E731
    on = {p for m in months for p in B.MONTH_PENTADS[m] if p not in others(m)}
    return [100 if i in on else 0 for i in range(PENTADS)]


def make_inputs(tmp_path):
    psf = [{"key": "psf:7", "source": "psf", "species": "Steelhead", "slot": "Steelhead", "name": "Big Summer",
            "label": "summer", "curve": curve([6, 7]), "peak": "Jul 20", "window": ["Jul 1", "Aug 30"], "cuid": 7}]
    dfo = [{"key": "dfo:9:Coho:-", "source": "dfo", "species": "Coho", "slot": "Coho", "name": "Test Creek",
            "label": None, "curve": curve([9]), "peak": "Oct 10", "window": ["Oct 1", "Oct 30"], "years": 10,
            "first_year": 2010, "last_year": 2024, "wsc": "100-000001-000100", "lon": -122.0, "lat": 49.5,
            "site_kind": "Stream"}]
    sources = {"psf": {"title": "t", "dataset": "d", "updated": "2024-09-10", "fetched": "2026-10-28"},
               "dfo": {"title": "t", "dataset": "d", "updated": "20260601", "fetched": "2026-10-28",
                       "licence": "Open Government Licence – Canada"}}
    timing = tmp_path / "timing.json"
    write_timing(timing, psf, dfo, sources)
    poly = tmp_path / "cu.geojson"
    square = [[-122.2, 49.4], [-121.9, 49.4], [-121.9, 49.6], [-122.2, 49.6], [-122.2, 49.4]]
    poly.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "id": 7, "properties": {}, "geometry": {"type": "Polygon", "coordinates": [square]}}]}))
    return timing, poly


@pytest.fixture
def built(tmp_path):
    timing, poly = make_inputs(tmp_path)
    files, index, report = B.build(timing, poly, make_base(tmp_path))
    return {k: json.loads(v) for k, v in files.items()}, index, report, files


def test_each_section_takes_its_relations_and_the_paint_index_says_when(built):
    docs, index, report, _ = built
    runs, paint = docs["runs"], docs["paint"]
    assert [r["id"] for r in runs["runs"]] == ["psf:7", "dfo:9:Coho:-"]
    assert runs["slots"][runs["default"]] == "Steelhead"
    assert runs["runs"][0]["months"] == (1 << 6) | (1 << 7)
    sids, s = [], 0
    for d in paint["sid"]:
        s += d
        sids.append(s)
    per = {sid: paint["profiles"][p]["r"] for sid, p in zip(sids, paint["p"])}
    # 3 spawns (order 3, in the CU) AND is the coho's surveyed stream; 6 spawns (a lake that drains); 4 (order 1) takes
    # nothing; 2 and 1 are the way to the sea; 5 (another river's mouth) is never walked into
    assert per == {1: [0 * 3 + 1], 2: [0 * 3 + 1], 3: [0 * 3 + 0, 1 * 3 + 2], 6: [0 * 3 + 0]}
    p3 = paint["profiles"][paint["p"][sids.index(3)]]
    steel, coho = runs["slots"].index("Steelhead"), runs["slots"].index("Coho")
    assert p3["s"] == (1 << steel) | (1 << coho)
    assert p3["m"] == [[coho, 1 << 9, 0], [steel, (1 << 6) | (1 << 7), 0]]
    assert paint["section_handles"] == index["section_handles"] == "h" * 16
    assert paint["runs_sha256"] == index["files"]["runs"]["sha256"]
    # the sheet's km: tens of km + 1, only where a PSF run is (3 km of river: rounds to 0 tens)
    assert dict(zip(sids, paint["km"])) == {1: 1, 2: 1, 3: 1, 6: 1}
    assert report["psf_runs_bound"] == 1 and report["dfo_records_bound"] == 1


def test_the_waters_name_no_section(built):
    docs, *_ = built
    w = docs["waters"]
    assert w["run_ids"] == ["psf:7", "dfo:9:Coho:-"]
    reach = [(w["items"][i] if i >= 0 else None, w["blks"][b], m0, m1, w["profiles"][p])
             for i, b, m0, m1, p in zip(*(w["streams"][k] for k in ("it", "blk", "m0", "m1", "p")))]
    # the river's two passing pieces merge into one range on its blue line
    assert sorted(reach) == [("gnis:1", "B1", 0, 2000, [1]), ("gnis:3", "B2", 0, 500, [0, 5])]
    assert (w["lakes"]["wbk"], [w["items"][i] for i in w["lakes"]["it"]]) == (["W6"], ["gnis:6"])
    assert "sid" not in w and "p" not in w


def test_a_mismatched_set_is_refused(built):
    _, index, _, files = built
    B.check(files, index)
    bad = dict(files, paint=files["paint"].replace(b'"runs_sha256":"', b'"runs_sha256":"x'))
    with pytest.raises(SystemExit):
        B.check(bad, index)


def test_months_and_pentads_read_the_curve():
    from pipeline.runtiming.timing import TimedRun
    r = TimedRun(key="k", source="psf", species="Steelhead", slot="Steelhead", name="n", label=None,
                 curve=tuple(curve([0, 11])), peak=None, window=None)
    assert B.months_of(r) == 1 | (1 << 11)
    bits = int(B.pentads_of(r), 16)
    assert bits & 1 and bits >> 72 & 1 and not bits >> 36 & 1
