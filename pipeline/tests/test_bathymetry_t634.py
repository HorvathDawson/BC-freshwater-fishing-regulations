"""T634: the vector depth reader, the placing fit, the chart -> lake proposals and the owner-only accept.

The failure that matters is the WRONG lake (a name picking a lake, a rerun rewriting an accepted row) and a half-read
chart passing as whole. Synthetic inputs only: no Province file is in the repo.
"""
from __future__ import annotations

import math

import pytest

from pipeline.bathymetry import accept as acc
from pipeline.bathymetry import e00, place
from pipeline.bathymetry.propose import (Lake, LakePoly, SheetRow, name_evidence, propose, word_order)


# ---- e00 ------------------------------------------------------------------------------------------------------------

def _f(v: float) -> str:
    return f"{v:14.7E}"


def _info(name: str, items, rows) -> list[str]:
    """An IFO table in the export's own layout: header, item lines, records wrapped at 80."""
    out = [f"{name:<32}XX{len(items):4d}{len(items):4d}{0:4d}{len(rows):10d}"]
    for k, (iname, size, typ) in enumerate(items, 1):
        out.append(f"{iname:<16}{size:3d}-1{4 * k:5d}-1{5:4d}-1{typ:3d}-1  -1  -1-1{k:20d}-")
    widths = [e00._text_width(s, t) for _, s, t in items]
    for r in rows:
        buf = "".join(v.rjust(w) if not isinstance(v, str) or t != 20 else v.ljust(w)
                      for v, w, (_, _, t) in zip(r, widths, items))
        out += [buf[i:i + 80].rstrip() for i in range(0, len(buf), 80)]
    return out


def _lines_e00() -> str:
    arcs = [(1, [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)]),       # shore: a 100 m square
            (2, [(20, 20), (80, 20), (80, 80), (20, 80), (20, 20)])]      # the 2 m contour
    L = ["EXP  0 /X/TEST.E00", "ARC  2"]
    for aid, pts in arcs:
        L.append(f"{aid:10d}{aid:10d}{1:10d}{1:10d}{1:10d}{2:10d}{len(pts):10d}")
        for i in range(0, len(pts), 2):
            L.append("".join(_f(c) for p in pts[i:i + 2] for c in p))
    L.append(f"{-1:10d}{0:10d}{0:10d}{0:10d}{0:10d}{0:10d}{0:10d}")
    L.append("TX7  2")                      # annotation the reader skips, holding a word that looks like a header
    L.append("SINE")
    L.append("LOG  2")
    L.append("200109271009 hmiller build TEST")
    L.append("EOL")
    L.append("IFO  2")
    L += _info("TEST.AAT", [("FNODE#", 4, 50), ("LENGTH", 4, 60), ("FCODE", 10, 20), ("CONTOUR", 10, 40)],
               [(str(1), _f(400.0), "GA27110000", _f(0.0)), (str(2), _f(240.0), "HA06200000", _f(-2.0))])
    L += _info("TEST.META", [("KEY", 16, 20), ("VALUE", 100, 20)],
               [("TITLE", "Test Lake Bathymetry"), ("RESOLUTION", "Scale 1:5000; diagrammatic - no projection"),
                ("DESCRIPTION", "Contains lake contours and depths in metres")])
    L += ["EOI", "EOS"]
    return "\n".join(L)


def test_e00_reads_arcs_contours_and_meta():
    cov = e00.read_coverage(_lines_e00(), "test.e00")
    assert [a.id for a in cov.arcs] == [1, 2]
    assert cov.arcs[0].points[2] == (100.0, 100.0)
    sheet = e00.Sheet(cov, None)
    got = [(c, f) for c, f, _ in sheet.contours()]
    assert got == [(0.0, "GA27110000"), (-2.0, "HA06200000")]
    assert cov.meta()["RESOLUTION"].endswith("no projection")
    assert e00.units_stated([sheet.text()]) == "metres"


def test_e00_refuses_what_it_cannot_read():
    with pytest.raises(e00.E00Error, match="not an uncompressed"):
        e00.read_coverage("EXP  1 /X/TEST.E00\nEOS", "c.e00")
    text = _lines_e00().replace("EOS", "")
    with pytest.raises(e00.E00Error):
        e00.read_coverage(text.replace("EOI", ""), "t.e00")


def test_e00_an_aat_that_does_not_cover_every_arc_is_refused():
    cov = e00.read_coverage(_lines_e00(), "test.e00")
    cov.arcs.append(e00.Arc(3, 3, [(0, 0), (1, 1)]))
    with pytest.raises(e00.E00Error, match="AAT rows"):
        e00.Sheet(cov, None).contours()


def test_units_are_only_what_the_words_say():
    assert e00.units_stated(["DEPTHS IN FEET"]) == "feet"
    assert e00.units_stated(["DEPTHS IN METRES", "Foot trail", "113     ft"]) == "metres"
    assert e00.units_stated(["DEPTHS IN METRES", "Depth in feet"]) == "both"
    assert e00.units_stated(["Contains lake contours and depths", "Access by Foot"]) is None


# ---- placing --------------------------------------------------------------------------------------------------------

def _blob():
    from shapely.geometry import Polygon
    pts = [(300 * math.cos(t) * (1 + 0.3 * math.sin(3 * t)), 180 * math.sin(t)) for t in
           [2 * math.pi * k / 120 for k in range(120)]]
    return Polygon(pts)


def test_shore_polygonises_the_shoreline_arcs_only():
    cov = e00.read_coverage(_lines_e00(), "test.e00")
    s = place.shore(e00.Sheet(cov, None).contours())
    assert s is not None and abs(s.area - 100 * 100) < 1e-6


def test_shore_closes_an_inlet_gap_and_keeps_islands_out():
    shore_open = [(0.0, 0.0), (100.0, 0.0), (100.0, 100.0), (0.0, 100.0), (0.0, 10.0)]     # 10 m gap at a stream mouth
    island = [(40.0, 40.0), (60.0, 40.0), (60.0, 60.0), (40.0, 60.0), (40.0, 40.0)]
    stream = [(0.0, 5.0), (-50.0, 5.0)]
    contours = [(0, "GA27110000", shore_open), (0, "GA27110000", island), (0, "GA24850000", stream),
                (-2.0, "HA06200000", [(10.0, 10.0), (30.0, 10.0)])]
    s = place.shore(contours)
    assert s is not None and abs(s.area - (100 * 100 - 20 * 20)) < 1e-6
    assert place.shore(contours, max_gap=5.0) is None


def test_fit_recovers_a_turn_and_a_shift():
    from shapely import affinity
    sheet = _blob()
    lake = affinity.translate(affinity.rotate(sheet, 37, origin="centroid"), 1_200_000, 600_000)
    got = place.fit(sheet, lake)
    assert got["iou"] > 0.98
    assert abs(((got["angle_deg"] - 37) + 180) % 360 - 180) < 1.0
    assert got["median_m"] < 3
    assert abs(got["area_ratio"] - 1) < 1e-6


def test_fit_reports_a_wrong_size_instead_of_stretching():
    from shapely import affinity
    sheet = _blob()
    lake = affinity.scale(sheet, 1.6, 1.6)
    got = place.fit(sheet, lake)
    assert got["area_ratio"] < 0.5 and got["iou"] < 0.5


# ---- proposals ------------------------------------------------------------------------------------------------------

def test_word_order_and_variants():
    assert word_order("Lake Maxwell") == word_order("MAXWELL LAKE") == "maxwell"
    assert name_evidence("LAKE MAXWELL", ["Maxwell Lake"], [])[0] == "word_order"
    assert name_evidence("BOBB'S LAKE", ["Bobbs Lake"], [])[0] == "equal"
    assert name_evidence("TRUDA LAKE", ["Tranquille Lake"], [("Truda Lake", "regulation")])[0] == "variant"
    # a variant the old matchers wrote from this very chart cannot confirm it
    assert name_evidence("TRUDA LAKE", ["Tranquille Lake"], [("TRUDA LAKE", "bathymetry")])[0] == "variant_circular"
    assert name_evidence("WILSON L. LITTLE WILSON L.", ["Wilson Lake"], [])[0] == "contains"
    assert name_evidence("FISH LAKE", ["Little Fish Lake"], []) is None


def _poly(code, wbk, name, x, area=1e6, layer="lakes", gnis=None):
    side = area ** 0.5
    return LakePoly(layer, code, wbk, ((gnis or wbk, name), (None, None), (None, None)) if name else
                    ((None, None), (None, None), (None, None)), area, (x, 0, x + side, side))


def _sheet(code, title, x, area=1e6, gaz=None, outline=True):
    from shapely.geometry import box
    side = area ** 0.5
    return SheetRow(code, f"{code}.TIF", title, gaz or title, 1, True, True,
                    outline=box(x, 0, x + side, side) if outline else None)


def _run(sheets, polys, variants=(), app=None):
    app = app if app is not None else {f"wbk:{p.wbk}": {"has_rows": 0} for p in polys}
    return {r["chart_id"]: r for r in propose(sheets, polys, list(variants), app)["proposals"]}


def test_rules_each_end_in_their_bucket():
    polys = [_poly("A", 1, "Bobbs Lake", 0), _poly("B", 2, "Twin Lake", 0), _poly("B", 3, "Twin Lake", 5000),
             _poly("C", 4, None, 0), _poly("D", 5, "Dester Lake", 0), _poly("D", 6, None, 900, area=1000),
             _poly("E", 7, "Nendatoo Lake", 0), _poly("F", 8, "Far Lake", 0), _poly("G", 9, "Goose Lake", 0,
                                                                                   layer="manmade")]
    sheets = [_sheet("A", "BOBB'S LAKE", 0), _sheet("B", "TWIN LAKE", 0), _sheet("C", "ROSE LAKE", 0),
              _sheet("D", "DESTER LAKE", 0), _sheet("E", "CRIPPLE LAKE", 0), _sheet("F", "FAR LAKE", 9000),
              _sheet("G", "GOOSE LAKE", 0), _sheet(None, "DINOSAUR LAKE", 0)]
    got = _run(sheets, polys)
    assert {k: v["rule"] for k, v in got.items()} == {
        "A": "clean", "B": "two_named", "C": "unnamed", "D": "clean", "E": "name_disagrees",
        "F": "position_or_size", "G": "no_lake"}
    assert got["D"]["wbks"] == ["wbk:5"] and got["D"]["evidence"]["unnamed_left_out"][0]["wbk"] == "wbk:6"
    assert got["G"]["evidence"]["offered"][0]["layer"] == "manmade"
    assert got["B"]["wbks"] == [] and got["E"]["wbks"] == ["wbk:7"] and got["E"]["verdict"] == "review"


def test_a_name_never_picks_a_lake():
    # the chart's code is on Rose Lake; a same-named lake elsewhere under ANOTHER code is never offered
    polys = [_poly("A", 1, "Rose Lake", 0), _poly("Z", 2, "Trout Lake", 50_000)]
    got = _run([_sheet("A", "TROUT LAKE", 0)], polys)
    assert got["A"]["rule"] == "name_disagrees" and got["A"]["wbks"] == ["wbk:1"]


def test_gazetted_only_and_not_in_app_and_no_outline():
    polys = [_poly("A", 1, "Tranquille Lake", 0), _poly("B", 2, "Bobbs Lake", 0), _poly("C", 3, "Cee Lake", 0)]
    sheets = [_sheet("A", "TRUDA LAKE", 0, gaz="TRANQUILLE LAKE"), _sheet("B", "BOBBS LAKE", 0),
              _sheet("C", "CEE LAKE", 0, outline=False)]
    got = _run(sheets, polys, app={"wbk:1": {}, "wbk:3": {}})
    assert got["A"]["rule"] == "gazetted_only"
    assert got["B"]["rule"] == "not_in_app" and got["B"]["verdict"] == "held"
    assert got["C"]["rule"] == "no_outline"


# ---- accept ---------------------------------------------------------------------------------------------------------

def test_accept_never_rewrites_and_never_guesses():
    props = [{"chart_id": "A", "wbks": ["wbk:1"], "rule": "clean", "titles": ["A LAKE"], "evidence": {}, "gnis_id": 9},
             {"chart_id": "B", "wbks": [], "rule": "two_named", "titles": ["B"], "evidence": {}}]
    frozen = acc.load_frozen(acc.Path("/nonexistent/matches.json"))
    acc.accept(frozen, props, ["A"], by="owner", today="2026-11-11")
    assert frozen["matches"][0]["wbks"] == ["wbk:1"] and frozen["matches"][0]["accepted_by"] == "owner"
    with pytest.raises(SystemExit, match="already accepted"):
        acc.accept(frozen, props, ["A"], by="owner", today="2026-11-12")
    with pytest.raises(SystemExit, match="proposes no lake"):
        acc.accept(frozen, props, ["B"], by="owner", today="2026-11-11")
    acc.accept(frozen, props, ["B"], by="owner", today="2026-11-11", wbk="wbk:2")
    acc.accept(frozen, props, ["A"], by="owner", today="2026-11-12", refuse="not this lake", replace=True)
    assert [(m["chart_id"], m["wbks"], m.get("refused")) for m in frozen["matches"]] == [
        ("A", [], "not this lake"), ("B", ["wbk:2"], None)]


def test_the_watershed_code_is_compared_without_hyphens_and_never_gates():
    p = LakePoly("lakes", "A", 1, ((5, "Bobbs Lake"), (None, None), (None, None)), 1e6, (0, 0, 1000, 1000),
                 "129360400000")
    s = _sheet("A", "BOBBS LAKE", 0)
    s.wsc = "129-360400-000"
    assert _run([s], [p])["A"]["evidence"]["wsc_agrees"] is True
    s.wsc = "999-000000-000"
    got = _run([s], [p])["A"]
    assert got["evidence"]["wsc_agrees"] is False and got["rule"] == "clean"
