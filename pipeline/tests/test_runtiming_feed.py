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


def make_base(tmp_path, wg=None):
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
    if wg is not None:                       # {sid: basin id}: the optional watershed-group column (T261)
        st["wg"] = sorted(set(wg.values()))
        cols["wg"] = [st["wg"].index(wg[s]) if s in wg else -1 for s in sids]
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
            "site_kind": "Stream", "counts": [[2023, 40], [2024, 0]], "adults": [[2023, 30], [2024, 0]]}]
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


def test_dfo_runs_carry_their_counts_and_psf_runs_none(built):
    docs, *_ = built
    runs = {r["id"]: r for r in docs["runs"]["runs"]}
    assert runs["dfo:9:Coho:-"]["n"] == [[2023, 40], [2024, 0]]
    assert runs["dfo:9:Coho:-"]["na"] == [[2023, 30], [2024, 0]]          # the adult series, beside the total
    assert "n" not in runs["psf:7"] and "na" not in runs["psf:7"]
    assert docs["runs"]["counts"] == {"dfo": {"field": "NATURAL_SPAWNERS_TOTAL",
                                              "words": "total spawners (DFO estimate)", "years": 12,
                                              "adult": {"field": "NATURAL_ADULT_SPAWNERS",
                                                        "words": "adult spawners (DFO estimate)"}}}


def test_a_base_without_wg_paints_no_basins(built):
    docs, _, report, _ = built
    assert "basins" not in docs["paint"] and report["basins_painted"] is None


def test_basins_or_the_runs_of_their_painted_sections(tmp_path):
    timing, poly = make_inputs(tmp_path)
    # A holds the river (passage only) and the lake (steelhead spawning); B holds the tributary (steelhead spawning +
    # coho observed) and the trickle (nothing painted); C holds only the other river (nothing painted: left out)
    wg = {1: "AAAA", 2: "AAAA", 6: "AAAA", 3: "BBBB", 4: "BBBB", 5: "CCCC"}
    files, index, report = B.build(timing, poly, make_base(tmp_path, wg))
    paint = json.loads(files["paint"])
    runs = json.loads(files["runs"])["runs"]
    slots = json.loads(files["runs"])["slots"]
    steel, coho = slots.index("Steelhead"), slots.index("Coho")
    pent = {r["id"]: r["pentads"] for r in runs}
    zero = "0" * 19
    assert paint["basins"]["ids"] == ["AAAA", "BBBB"]
    assert paint["basins"]["m"] == [[[steel, pent["psf:7"], pent["psf:7"]]],
                                    [[coho, pent["dfo:9:Coho:-"], zero], [steel, pent["psf:7"], zero]]]
    assert report["basins_painted"] == 2 and report["dfo_runs_with_counts"] == 1 and report["dfo_runs_with_both"] == 1
    B.check(files, index)
    bad = json.loads(files["paint"])
    bad["basins"]["m"][0][0][1] = "f" * 19                  # bits past pentad 72
    with pytest.raises(SystemExit):
        B.check(dict(files, paint=B._bytes(bad)), index)


def test_add_basins_keeps_the_handles_and_the_name(tmp_path, monkeypatch):
    p = make_base(tmp_path)
    # stand-in for the FWA groups: one box around the first river's mouth, nothing elsewhere
    import geopandas as gpd
    from shapely.geometry import box
    from pyproj import Transformer
    t = Transformer.from_crs(4326, 3005, always_xy=True)
    x0, y0 = t.transform(-122.05, 48.95)
    x1, y1 = t.transform(-121.95, 49.05)
    gdf = gpd.GeoDataFrame({"basin_id": ["LFRA"]}, geometry=[box(x0, y0, x1, y1)], crs=3005)
    monkeypatch.setattr("pipeline.deliver.tiles.basins.groups", lambda gpkg: gdf)
    out = tmp_path / "out"
    assert BASE.main(["--add-basins", str(p), "--out", str(out), "--gpkg", "unused.gpkg"]) == 0
    q = out / "base-" "hhhhhhhhhhhhhhhh.json.gz"
    b = BASE.read(q)
    orig = BASE.read(p)
    assert b["section_handles"] == orig["section_handles"] and b["cols"]["x"] == orig["cols"]["x"]
    assert b["strings"]["wg"] == ["LFRA"] and b["cols"]["wg"] == [0, -1, -1, -1, -1, -1]
    assert B.wg_of(b) == {1: "LFRA"}


def test_t373_a_release_without_its_runtiming_inputs_is_refused_not_given_an_old_feed(tmp_path):
    """(T373) `runtiming.feed.release.bind` binds for the release's own map; missing timing / polygons stop it, with
    the commands that make them — never a feed of another day or atlas in their place."""
    import json
    import pytest
    from pipeline.runtiming.feed import release as R
    tiles = tmp_path / "tiles"
    tiles.mkdir()
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": "h1", "tile_version": "h1.t1"}))
    with pytest.raises(SystemExit, match="fetch and parse first"):
        R.bind(tmp_path / "r-x", tiles, tmp_path / "out", timing=tmp_path / "none.json", polygons=tmp_path / "none.geojson")
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": "h1"}))
    with pytest.raises(SystemExit, match="no section_handles / tile_version"):
        R.bind(tmp_path / "r-x", tiles, tmp_path / "out")


def test_t412_the_release_stages_the_base_of_its_own_map(tmp_path, monkeypatch):
    """(T412) a release ships the BASE of its tiles' handles (+ base.json naming it), never a feed: the workflow is the
    only publisher of run timing."""
    import json
    from pipeline.runtiming.feed import release as R
    tiles = tmp_path / "tiles"
    tiles.mkdir()
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": "h" * 16, "tile_version": "h" * 16 + ".t2"}))
    base = make_base(tmp_path)
    named = tmp_path / ("base-" + "h" * 16 + ".json.gz")
    base.rename(named)
    monkeypatch.setattr(R, "base_for", lambda handles, t, rel, b=None: named)
    doc = R.stage_base(tmp_path / "r-x", tiles, tmp_path / "out")
    assert doc["section_handles"] == "h" * 16 and doc["tile_version"] == "h" * 16 + ".t2"
    assert (tmp_path / "out" / named.name).read_bytes() == named.read_bytes()
    assert json.loads((tmp_path / "out" / "base.json").read_text()) == doc
    assert not (tmp_path / "out" / "runtiming").exists()


def test_t412_built_twice_the_same_bytes(tmp_path):
    """(T412) the same inputs give the same files and index (only `built_at` moves): a refresh with nothing new is
    not a change, and the workflow publishes nothing."""
    timing, poly = make_inputs(tmp_path)
    base = make_base(tmp_path)
    f1, i1, _ = B.build(timing, poly, base, tile_version="T")
    f2, i2, _ = B.build(timing, poly, base, tile_version="T")
    assert f1 == f2
    assert {k: v for k, v in i1.items() if k != "built_at"} == {k: v for k, v in i2.items() if k != "built_at"}
    d1, d2 = B.write(tmp_path / "a", f1, i1, {}), B.write(tmp_path / "b", f2, i2, {})
    assert {p.name: p.read_bytes() for p in d1.glob("*.gz")} == {p.name: p.read_bytes() for p in d2.glob("*.gz")}


def test_t412_the_diff_says_a_real_change_only(built):
    from pipeline.runtiming.feed.diff import diff
    docs, index, _report, _files = built
    runs = {r["id"]: r for r in docs["runs"]["runs"]}
    assert diff(index, runs, None, None)["changed"] is True
    same = diff(index, runs, {**index, "built_at": "another day"}, runs)
    assert same["changed"] is False and same["runs"] == {"added": 0, "dropped": 0, "changed": 0}
    k = next(iter(runs))
    moved = {**runs, k: {**runs[k], "peak": "Jan 1"}}
    live = {**index, "files": {**index["files"], "runs": {**index["files"]["runs"], "sha256": "old"}}}
    d = diff(index, runs, live, moved)
    assert d["changed"] is True and d["why"] == "runs" and d["runs"]["changed"] == 1
    assert diff(index, runs, {**index, "tile_version": "older"}, runs)["why"] == "tiles"
