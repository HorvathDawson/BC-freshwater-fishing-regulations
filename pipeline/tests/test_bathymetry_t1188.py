"""T1188: the wider fitter (`pipeline.bathymetry.refit`) on small built shapes, no Province file.

What must hold: each step lifts a placing only when it is a real, unambiguous fit; a two-lake sheet splits its contours
per lake; a wrong-lake code is found by shape and refused by a different name; a stretch of 5 % or less is adopted and
flagged, a bigger one never adopted but listed with what it would change; the same input gives the same output.
"""
from __future__ import annotations

import json
import math

import pytest
from shapely import affinity
from shapely.geometry import LineString, Point, Polygon, box
from shapely.ops import unary_union

from pipeline.bathymetry import place, refit
from pipeline.bathymetry import refit_all

EAST, NORTH = 1_200_000.0, 600_000.0      # BC Albers-like: far from the sheet's own coordinates


def _chiral(size: float, x0: float = 0.0, y0: float = 0.0, n: int = 160) -> Polygon:
    """an asymmetric lake: no mirror or turn of it matches itself"""
    pts = []
    for k in range(n):
        t = 2 * math.pi * k / n
        r = size * (1 + 0.25 * math.sin(2 * t + 0.4) + 0.15 * math.cos(3 * t) + 0.1 * math.sin(5 * t + 1))
        pts.append((x0 + r * math.cos(t), y0 + 0.6 * r * math.sin(t)))
    return Polygon(pts)


def _mirror_symmetric(size: float, n: int = 160) -> Polygon:
    """a lake that is its own mirror image (x -> -x) but not its own half turn"""
    pts = []
    for k in range(n):
        t = 2 * math.pi * k / n
        r = size * (1 + 0.3 * math.sin(t) + 0.15 * math.cos(2 * t) + 0.1 * math.sin(3 * t))
        pts.append((r * math.cos(t), 0.6 * r * math.sin(t)))
    return Polygon(pts)


def _earth(g, angle=37.0, mirror=False):
    if mirror:
        g = affinity.scale(g, xfact=-1, yfact=1, origin=(0, 0))
    return affinity.translate(affinity.rotate(g, angle, origin=(0, 0)), EAST, NORTH)


def _base(sheet, lake, wbk=1):
    f = place.fit(sheet, lake)
    f["wbk"] = f"wbk:{wbk}"
    return f


def _near(lakes):
    """a near_lakes stand-in: every lake whose box meets the padded box"""
    def near(bounds, pad):
        q = box(bounds[0] - pad, bounds[1] - pad, bounds[2] + pad, bounds[3] + pad)
        return {w: dict(e) for w, e in sorted(lakes.items()) if e["geom"].intersects(q)}
    return near


# ---- 1. rigid, wider --------------------------------------------------------------------------------------------------

def test_search_finds_the_mirror_image_and_says_it_is_one():
    sheet = _chiral(800)
    lake = _earth(sheet, 123.0, mirror=True)
    fits = refit.search(sheet, lake, (sheet.centroid.x, sheet.centroid.y))
    best = fits[0]
    assert best["mirror"] is True and best["iou"] > 0.97 and best["scale"] == 1.0
    assert max(f["iou"] for f in fits if not f["mirror"]) < best["iou"] - refit.MARGIN


def test_search_keeps_several_distinct_starts_per_mirror():
    sheet = _chiral(800)
    fits = refit.search(sheet, _earth(sheet, 250.0), (sheet.centroid.x, sheet.centroid.y))
    for m in (False, True):
        starts = [f["start_deg"] for f in fits if f["mirror"] is m]       # polishing may bring two back together
        assert len(starts) == refit.STARTS
        assert all(refit.angdiff(a, b) > refit.SEP_DEG for i, a in enumerate(starts) for b in starts[i + 1:])
    assert abs(((fits[0]["angle_deg"] - 250.0) + 180) % 360 - 180) < 1.0


def test_a_symmetric_sheet_is_ambiguous_and_not_adopted():
    sheet = box(0, 0, 3000, 900)                    # a rectangle fits turned half round, and mirrored, just as well
    lake = _earth(box(0, 0, 3000, 900), 30.0)
    b = refit.best_over({"wbk:1": refit.search(sheet, lake, (1500.0, 450.0))})
    assert b["ambiguous"] is True and b["gap"] < refit.MARGIN


def test_verdict_and_rivals():
    best = {"iou": 0.92, "mirror": False, "angle_deg": 10.0}
    fits = [best, {"iou": 0.91, "mirror": False, "angle_deg": 15.0},     # the same placing, nudged: no rival
            {"iou": 0.80, "mirror": False, "angle_deg": 190.0}, {"iou": 0.90, "mirror": True, "angle_deg": 12.0}]
    rv = refit.rivals_of(best, fits)
    assert sorted(r[0] for r in rv) == ["mirror", "orientation 190"]
    v = refit.verdict(best, rv)
    assert v["ambiguous"] is True and v["rival"] == "mirror" and v["gap"] == 0.02
    assert refit.verdict(best, [("orientation 190", 0.8)])["ambiguous"] is False


# ---- 2. two lakes -----------------------------------------------------------------------------------------------------

def _two_lake_case():
    a = _chiral(900)
    b = affinity.translate(affinity.rotate(_chiral(450), 70, origin=(0, 0)), 2600, 300)
    sheet = unary_union([a, b])
    lines = [(-2.0, LineString([(-300, -50), (300, 60)])), (-4.0, LineString([(-100, 0), (150, 20)])),
             (-1.0, LineString([(2500, 300), (2750, 320)])), (0, LineString([(0, 0), (10, 10)])),
             (-3.0, LineString([(9000, 9000), (9100, 9100)]))]       # outside both lakes: unassigned, never guessed
    lake_a, lake_b = _earth(a), _earth(b)
    return sheet, lines, lake_a, lake_b


def test_a_two_lake_sheet_fits_the_union_and_splits_its_contours():
    sheet, lines, lake_a, lake_b = _two_lake_case()
    base = _base(sheet, lake_a, 1)
    assert place_grade(base) == "poor"
    near = _near({2: {"geom": lake_b, "name": "Beaver Lake", "layer": "lakes"},
                  3: {"geom": _earth(affinity.translate(_chiral(500), 30000, 0)), "name": "Far Lake", "layer": "lakes"}})
    got = refit.refit_sheet(sheet, lines, {1: {"geom": lake_a, "name": "Elk Lake", "layer": "lakes"}}, near,
                            base_fit=base, units="metres")
    st = got["steps"]["two_lake"]
    assert st["adopted"] is True and st["target"] == "wbk:1+wbk:2" and st["fit"]["grade"] == "strict"
    assert got["after"]["step"] == "two_lake" and "two_lake" in got["flags"]
    split = st["split"]
    assert split["per_lake"]["1"]["contours"] == 2 and split["per_lake"]["2"]["contours"] == 1
    assert split["unassigned_contours"] == 1
    assert all(v["lake_covered"] > 0.95 for v in split["per_lake"].values())
    assert got["classes"] == {"baseline": "poor", "rigid": "poor", "two_lake": "strict", "other_lake": "strict",
                              "small_stretch": "strict", "depth_line": "strict"}


def test_split_reports_a_lake_the_sheet_does_not_cover():
    sheet = _chiral(900)
    placed = _earth(sheet)
    other = _earth(affinity.translate(_chiral(300), 5000, 0))
    s = refit.split_per_lake(placed, [], {1: placed, 2: other})
    assert s["per_lake"]["2"]["lake_covered"] == 0.0 and s["rings"] == [{"lake": "wbk:1", "ring_ha": round(
        placed.area / 1e4, 2)}]


def test_a_union_member_the_sheet_does_not_draw_is_not_adopted(monkeypatch):
    """the union lifts the class, but a member is covered less than DRAWN_MIN: the placing is refused, kept as it was"""
    sheet, lines, lake_a, lake_b = _two_lake_case()
    monkeypatch.setattr(refit, "DRAWN_MIN", 0.999)          # both lakes are covered about 0.97-0.99 here
    got = refit.refit_sheet(sheet, lines, {1: {"geom": lake_a, "name": "Elk Lake", "layer": "lakes"}},
                            _near({2: {"geom": lake_b, "name": "Beaver Lake", "layer": "lakes"}}),
                            base_fit=_base(sheet, lake_a, 1), units="metres")
    st = got["steps"]["two_lake"]
    assert st["lifts"] is True and st["every_lake_drawn"] is False and st["adopted"] is False
    assert "two_lake_not_drawn" in got["flags"] and "two_lake" not in got["flags"]
    assert got["after"]["target"] == "wbk:1" and got["classes"]["two_lake"] == "poor"


def place_grade(f):
    from pipeline.bathymetry.place_more import grade
    return grade(f)


# ---- 3. wrong-lake codes ----------------------------------------------------------------------------------------------

def _wrong_code_case(found_name, taken=None):
    sheet = _chiral(700)
    coded = _earth(box(-900, -100, 900, 100))                    # the code's lake: a different shape
    true = affinity.translate(_earth(sheet, 81.0), 40_000, 0)     # 40 km away (Stake)
    decoy = affinity.translate(_earth(box(-700, -300, 700, 300)), 20_000, 0)
    near = _near({7: {"geom": true, "name": found_name, "layer": "lakes"},
                  8: {"geom": decoy, "name": "Square Lake", "layer": "lakes"}})
    base = _base(sheet, coded, 5)
    kw = {"taken": taken} if taken is not None else {}
    return refit.refit_sheet(sheet, [], {5: {"geom": coded, "name": "Long Lake", "layer": "lakes"}}, near,
                             base_fit=base, units="metres", titles=["STAKE LAKE"], **kw)


def test_a_wrong_lake_code_is_found_by_shape_and_confirmed_by_the_title():
    got = _wrong_code_case("Stake Lake")
    st = got["steps"]["other_lake"]
    assert st["adopted"] is True and st["target"] == "wbk:7" and st["name_check"] == "title"
    assert st["dist_m"] > 35_000 and got["after"]["grade"] == "strict" and "other_lake" in got["flags"]


def test_a_shape_find_with_a_different_name_is_refused():
    got = _wrong_code_case("Davies Lake")
    st = got["steps"]["other_lake"]
    assert st["lifts"] is True and st["adopted"] is False and st["name_check"] == "differs"
    assert "other_lake_name_differs" in got["flags"] and got["after"]["target"] == "wbk:5"


def test_an_unnamed_find_far_from_the_coded_lake_is_refused():
    got = _wrong_code_case(None)                                  # unnamed, 40 km away: beyond NEAR_M
    st = got["steps"]["other_lake"]
    assert st["lifts"] is True and st["adopted"] is False and st["name_check"] == "unnamed_far"
    assert "other_lake_name_unnamed_far" in got["flags"] and got["after"]["target"] == "wbk:5"


def test_a_find_on_a_lake_another_sheet_ends_on_is_refused():
    got = _wrong_code_case("Stake Lake", taken={7: ["00000002.zip/other.e00"]})
    st = got["steps"]["other_lake"]
    assert st["lifts"] is True and st["adopted"] is False and st["taken_by"] == ["00000002.zip/other.e00"]
    assert "other_lake_taken" in got["flags"] and got["after"]["target"] == "wbk:5"


def test_name_check_cases():
    assert refit.name_check("Stake Lake", ["STAKE LAKE"], [], 40_000) == "title"
    assert refit.name_check("South Gataga Lakes", ["SOUTH GATAGA LAKE NO. #3 *"], ["South Gataga Lakes"], 157) == \
        "coded_name"
    assert refit.name_check(None, ["FOWL CREEK LAKE NO. 1*"], [], 362) == "unnamed_near"
    assert refit.name_check(None, ["X LAKE"], [], 40_000) == "unnamed_far"
    assert refit.name_check("Davies Lake", ["DUNLEVY LAKE"], ["Dunlevy Lake"], 39_000) == "differs"


def test_shape_rank_keeps_the_area_band_and_the_closest_shapes_first():
    sheet = _chiral(700)
    lakes = {1: affinity.scale(sheet, 1.5, 1.5), 2: box(0, 0, 2600, 400), 3: affinity.rotate(sheet, 90), 4: box(0, 0, 1, 1)}
    lakes[5] = box(0, 0, 1000, sheet.area / 1000)              # the right area, a different shape
    got = refit.shape_rank(sheet, lakes)
    assert [w for w, _d in got][0] == 3 and {w for w, _d in got} <= {2, 3, 5} and 1 not in {w for w, _d in got}


# ---- 4. stretches -----------------------------------------------------------------------------------------------------

def test_a_small_stretch_is_adopted_and_flagged():
    sheet = _chiral(3000)
    lake = _earth(affinity.scale(sheet, 1.04, 1.04, origin=(0, 0)), 200.0)
    base = _base(sheet, lake)
    assert place_grade(base) != "strict"
    got = refit.refit_sheet(sheet, [], {1: {"geom": lake, "name": "A Lake", "layer": "lakes"}}, _near({}),
                            base_fit=base, units="metres")
    st = got["steps"]["small_stretch"]
    assert st["adopted"] is True and abs(st["fit"]["scale"] - 1.04) < 0.01 and st["fit"]["grade"] == "strict"
    assert "small_stretch" in got["flags"] and "small_stretch_at_limit" not in got["flags"] and got["would_help"] is None
    assert got["classes"]["other_lake"] == place_grade(base)


def test_a_bigger_stretch_is_never_adopted_but_listed_with_its_change():
    sheet = _chiral(3000)
    lake = _earth(affinity.scale(sheet, 1.12, 1.12, origin=(0, 0)), 300.0)
    base = _base(sheet, lake)
    got = refit.refit_sheet(sheet, [], {1: {"geom": lake, "name": "A Lake", "layer": "lakes"}}, _near({}),
                            base_fit=base, units="metres")
    assert got["after"]["grade"] != "strict" and abs(got["after"]["fit"]["scale"] - 1) <= refit.SMALL_STRETCH + 1e-9
    wh = got["would_help"]
    assert wh is not None and abs(wh["fit"]["scale"] - 1.12) < 0.01 and wh["fit"]["grade"] == "strict"
    assert "grows the chart 12" in wh["change"] and "-> strict" in wh["change"]
    assert "bigger_stretch_would_help" in got["flags"]


def test_stretch_modes_offer_feet_only_when_the_units_are_not_stated():
    assert [m for m, _s, _b in refit.stretch_modes("metres", 10.7)] == ["stretch"]
    assert [m for m, _s, _b in refit.stretch_modes("not stated", 10.7)] == ["stretch", "feet"]
    assert [m for m, _s, _b in refit.stretch_modes("not stated", 0.09)] == ["stretch", "feet_up"]
    before = {"iou": 0.4, "median_m": 300.0, "grade": "poor"}
    after = {"iou": 0.93, "median_m": 8.0, "grade": "strict", "scale": 0.3049}
    assert refit.stretch_change(before, after).startswith("feet drawn as metres?")


def test_an_ambiguous_rigid_lift_is_reported_never_adopted():
    """a rectangle the repo's fitter left poor: every step finds strict, but turned half round it fits as well"""
    sheet = box(0, 0, 3000, 900)
    lake = _earth(box(0, 0, 3000, 900), 30.0)
    origin = (1500.0, 450.0)
    base = dict(refit.measures(sheet, lake, refit.Pose(angle=75.0), origin), wbk="wbk:1")
    assert base["grade"] == "poor"
    got = refit.refit_sheet(sheet, [], {1: {"geom": lake, "name": "A Lake", "layer": "lakes"}}, _near({}),
                            base_fit=base, units="metres")
    st = got["steps"]["rigid"]
    assert st["lifts"] is True and st["ambiguous"] is True and st["adopted"] is False
    assert "rigid_ambiguous" in got["flags"] and "rigid_wider" not in got["flags"]
    assert got["steps"]["small_stretch"]["adopted"] is False and "small_stretch_ambiguous" in got["flags"]
    assert got["after"]["step"] == "baseline" and got["after"]["grade"] == "poor"


def test_a_mirror_symmetric_sheet_needing_a_small_stretch_is_not_adopted():
    """the stretch step searches the mirror too: a sheet that is its own mirror image is ambiguous there"""
    sheet = _mirror_symmetric(3000)
    lake = _earth(affinity.scale(sheet, 1.04, 1.04, origin=(0, 0)), 200.0)
    base = _base(sheet, lake)
    assert place_grade(base) != "strict"
    got = refit.refit_sheet(sheet, [], {1: {"geom": lake, "name": "A Lake", "layer": "lakes"}}, _near({}),
                            base_fit=base, units="metres")
    st = got["steps"]["small_stretch"]
    assert st["lifts"] is True and st["ambiguous"] is True and st["adopted"] is False
    assert st["rival"] == "mirror" and "small_stretch_ambiguous" in got["flags"]
    assert got["after"]["step"] == "baseline"


def test_a_stretch_is_not_adopted_when_another_lake_fits_unstretched_as_well():
    """Myles: the coded lake needs a stretch; a lake the wrong-lake search found (refused by its name) fits within the
    margin without one. The stretch is ambiguous."""
    sheet = _chiral(3000)
    coded = _earth(affinity.scale(sheet, 1.12, 1.12, origin=(0, 0)), 200.0)
    other = affinity.translate(_earth(affinity.scale(sheet, 1.07, 1.07, origin=(0, 0)), 81.0), 40_000, 0)
    base = _base(sheet, coded, 5)
    assert place_grade(base) == "poor"
    got = refit.refit_sheet(sheet, [], {5: {"geom": coded, "name": "Myles Lake", "layer": "lakes"}},
                            _near({7: {"geom": other, "name": "Davies Lake", "layer": "lakes"}}),
                            base_fit=base, units="metres", titles=["MYLES LAKE"])
    assert got["steps"]["other_lake"]["name_check"] == "differs" and got["steps"]["other_lake"]["adopted"] is False
    st = got["steps"]["small_stretch"]
    assert st["lifts"] is True and st["adopted"] is False and st["rival"] == "target wbk:7" and st["gap"] < refit.MARGIN
    assert got["after"]["target"] == "wbk:5" and got["after"]["grade"] == "poor"


def test_a_stretch_is_weighed_unstretched_against_another_lake():
    """the coded lake needs the full 5 %: before it the placing is behind another lake's unstretched fit, after it
    0.05 ahead. The rival is unstretched, so the placing is weighed before its stretch: ambiguous."""
    sheet = _chiral(3000)
    coded = _earth(affinity.scale(sheet, 1.12, 1.12, origin=(0, 0)), 200.0)
    other = affinity.translate(_earth(affinity.scale(sheet, 1.10, 1.10, origin=(0, 0)), 81.0), 40_000, 0)
    base = _base(sheet, coded, 5)
    got = refit.refit_sheet(sheet, [], {5: {"geom": coded, "name": "Myles Lake", "layer": "lakes"}},
                            _near({7: {"geom": other, "name": "Davies Lake", "layer": "lakes"}}),
                            base_fit=base, units="metres", titles=["MYLES LAKE"])
    st = got["steps"]["small_stretch"]
    assert st["fit"]["iou"] - got["steps"]["other_lake"]["fit"]["iou"] >= refit.MARGIN     # stretched: clear
    assert st["rival"] == "target wbk:7" and st["gap"] < refit.MARGIN and st["adopted"] is False
    assert st["before"]["iou"] == base["iou"]


def test_the_neighbour_gap_is_from_the_placing_before_its_stretch():
    sheet = _chiral(3000)
    lake = _earth(affinity.scale(sheet, 1.04, 1.04, origin=(0, 0)), 200.0)
    got = refit.refit_sheet(sheet, [], {1: {"geom": lake, "name": "A Lake", "layer": "lakes"}}, _near({}),
                            base_fit=_base(sheet, lake), units="metres")
    assert got["steps"]["small_stretch"]["adopted"] is True
    assert refit.unstretched_iou(got) == got["steps"]["small_stretch"]["before"]["iou"] < got["after"]["fit"]["iou"]


# ---- the trust checks: neighbours, complexity, confidence ----------------------------------------------------------

def test_a_near_identical_neighbour_is_a_rival():
    sheet = _chiral(800)
    lake = _earth(sheet)
    twin = affinity.translate(_earth(affinity.scale(sheet, 1.01, 1.01, origin=(0, 0)), 140.0), 6000, 0)
    other = affinity.translate(_earth(box(-1000, -330, 1000, 330)), -6000, 0)    # the right size, another shape
    o = (sheet.centroid.x, sheet.centroid.y)
    placed = refit.measures(sheet, lake, refit.Pose(angle=37.0, dx=EAST, dy=NORTH), o)
    near = _near({1: {"geom": lake, "name": "A Lake", "layer": "lakes"}, 2: {"geom": twin, "name": None, "layer": "lakes"},
                  3: {"geom": other, "name": "Box Lake", "layer": "lakes"}})
    rec, g = refit.neighbour_check(sheet, o, placed["iou"], lake, [1], near)
    assert rec["lake"] == "wbk:2" and rec["rival"] is True and rec["gap"] < refit.MARGIN and g is not None
    assert rec["similar_size"] == 2
    rec, _g = refit.neighbour_check(sheet, o, placed["iou"], lake, [1, 2], near)
    assert rec["rival"] is False and (rec["lake"] is None or rec["gap"] >= refit.MARGIN)


def _hook(w: float = 250.0) -> Polygon:
    """a hooked lake: no turn of it matches its mirror image"""
    return LineString([(0, 0), (3000, 0), (3000, 1600), (1400, 1600), (1400, 900)]).buffer(w, cap_style=2, join_style=2)


def test_a_neighbour_that_fits_only_mirrored_reaches_the_full_search():
    sheet = _hook()
    lake = _earth(sheet)
    twin = affinity.translate(_earth(sheet, 100.0, mirror=True), 9000, 0)
    o = (sheet.centroid.x, sheet.centroid.y)
    assert refit.screen(sheet, twin, o) < refit.SCREEN_MIN <= refit.screen(sheet, twin, o, mirrored=True)
    placed = refit.measures(sheet, lake, refit.Pose(angle=37.0, dx=EAST, dy=NORTH), o)
    near = _near({1: {"geom": lake, "name": "A Lake", "layer": "lakes"}, 2: {"geom": twin, "name": None, "layer": "lakes"}})
    rec, _g = refit.neighbour_check(sheet, o, placed["iou"], lake, [1], near)
    assert rec["searched"] == 1 and rec["lake"] == "wbk:2" and rec["fit"]["mirror"] is True and rec["rival"] is True


def test_complexity_is_low_for_an_oval_and_high_for_an_irregular_shore():
    oval = affinity.scale(Point(0, 0).buffer(1000, 64), 1.6, 1.0)
    o = refit.self_similarity(oval, (0.0, 0.0))
    c = _chiral(1000)
    k = refit.self_similarity(c, (c.centroid.x, c.centroid.y))
    assert o["complexity"] < refit_all.SIMPLE_BELOW < k["complexity"]


def _placed(grade="strict", complexity=0.3, similar=3, rival=False, ambiguous=False, refit_part=None):
    r = {"zip": "z.zip", "coverage": "z.e00", "state": "fitted", "grade": grade,
         "fit": {"wbk": "wbk:1", "iou": 0.95, "median_m": 10.0, "area_ratio": 1.0, "angle_deg": 5.0},
         "check": {"neighbour": {"rival": rival, "similar_size": similar}, "orientation": {"ambiguous": ambiguous},
                   "shape": {"complexity": complexity}}}
    if refit_part:
        r["refit"] = refit_part
    return r


def test_confidence_high_high_stretched_and_review():
    assert refit_all.confidence(_placed()) == ("high", [])
    assert refit_all.confidence(_placed(rival=True)) == ("review", ["neighbour_rival"])
    assert refit_all.confidence(_placed(ambiguous=True)) == ("review", ["ambiguous"])
    assert refit_all.confidence(_placed(complexity=0.02)) == ("review", ["simple_shape"])
    assert refit_all.confidence(_placed(complexity=0.02, similar=0)) == ("high", [])    # the only lake its size
    assert refit_all.confidence(_placed(grade="loose")) == ("review", ["loose"])
    fit = {"iou": 0.93, "median_m": 12.0, "area_ratio": 1.0, "angle_deg": 5.0, "mirror": False, "scale": 1.03}
    part = {"after": {"grade": "strict", "step": "small_stretch", "target": "wbk:1", "fit": fit},
            "steps": {"small_stretch": {"adopted": True}}, "flags": ["small_stretch"]}
    assert refit_all.confidence(_placed(grade="loose", refit_part=part)) == ("high_stretched", [])
    part = dict(part, flags=["small_stretch", "small_stretch_at_limit"])
    assert refit_all.confidence(_placed(grade="loose", refit_part=part)) == ("review", ["stretch_at_limit"])
    part = dict(part, after=dict(part["after"], step="other_lake"), flags=["other_lake"])
    assert refit_all.confidence(_placed(grade="poor", refit_part=part))[1] == ["other_lake"]
    # a find then a stretch: the stretch step owns after["step"]; the flags still say what found the lake (Moffat)
    for found in ("two_lake", "other_lake"):
        part = {"after": {"grade": "strict", "step": "small_stretch", "target": "wbk:1", "fit": fit},
                "steps": {"small_stretch": {"adopted": True}}, "flags": [found, "small_stretch"]}
        assert refit_all.confidence(_placed(grade="poor", refit_part=part)) == ("review", [found])
    # a placing the wider rigid search changed: the owner has never seen it
    rig = dict(fit, scale=1.0)
    part = {"after": {"grade": "strict", "step": "rigid", "target": "wbk:1", "fit": rig},
            "steps": {"small_stretch": {"skipped": "already strict"}}, "flags": ["rigid_wider"]}
    assert refit_all.confidence(_placed(grade="loose", refit_part=part)) == ("review", ["rigid_wider"])
    part = dict(part, flags=["rigid_better_same_class"], after=dict(part["after"], step="baseline"))
    assert refit_all.confidence(_placed(refit_part=part)) == ("high", [])
    assert refit_all.confidence({"zip": "n.zip", "state": "no_shore"}) == ("review", ["not_placed"])


# ---- a lake is free ---------------------------------------------------------------------------------------------------

def _claim_row(z, target, find=False):
    r = {"zip": z, "coverage": "c.e00", "state": "fitted", "grade": "poor", "fit": {"wbk": target}}
    r["refit"] = {"after": {"target": target, "step": "other_lake" if find else "baseline", "grade": "poor",
                            "fit": {"iou": 0.5, "mirror": False, "scale": 1.0}},
                  "steps": {"other_lake": {"adopted": find}, "small_stretch": {}}, "flags": ["other_lake"] if find else []}
    return r


def test_a_find_on_a_lake_another_sheet_ends_on_is_rerun_with_it_taken():
    results = {"a.zip": ([_claim_row("a.zip", "wbk:9", find=True)], {}), "b.zip": ([_claim_row("b.zip", "wbk:9")], {})}
    coded_by = {1: ["a.zip"], 9: ["b.zip"]}
    calls = []

    def rerun(zs, taken):
        calls.append((zs, taken))
        return [([_claim_row("a.zip", "wbk:1")], {})]          # refused: back on its own coded lake
    taken = refit_all.settle(results, coded_by, rerun)
    assert calls == [(["a.zip"], {"a.zip": {9: ["b.zip/c.e00"]}})]
    assert taken == {"a.zip": {9: ["b.zip/c.e00"]}} and results["a.zip"][0][0]["refit"]["after"]["target"] == "wbk:1"


def test_a_find_on_a_coded_lake_whose_sheet_moved_off_is_kept_and_flagged():
    a, b = _claim_row("a.zip", "wbk:9", find=True), _claim_row("b.zip", "wbk:8", find=True)
    results = {"a.zip": ([a], {}), "b.zip": ([b], {})}

    def rerun(zs, taken):
        raise AssertionError("nothing to rerun")
    assert refit_all.settle(results, {1: ["a.zip"], 9: ["b.zip"]}, rerun) == {}
    assert "takes_a_coded_lake" in a["refit"]["flags"] and a["refit"]["takes_a_coded_lake"] == {"wbk:9": ["b.zip"]}
    assert "takes_a_coded_lake" not in b["refit"]["flags"]
    assert refit_all.confidence(dict(a, grade="poor"))[0] == "review"


def test_a_union_neighbour_another_sheet_ends_on_is_refused():
    a = _claim_row("a.zip", "wbk:1+wbk:9")
    a["refit"]["steps"]["two_lake"] = {"adopted": True}
    b = dict(_claim_row("b.zip", "wbk:9"), grade="strict")
    refuse, takes = refit_all.claims([a, b], {1: ["a.zip"], 9: ["b.zip"]})
    assert refuse == {"a.zip/c.e00": {9: ["b.zip/c.e00"]}} and takes == {}
    b["refit"]["after"]["target"] = "wbk:8"                       # b moved off its coded lake: kept, flagged
    assert refit_all.claims([a, b], {1: ["a.zip"], 9: ["b.zip"]}) == ({}, {"a.zip/c.e00": {9: ["b.zip"]}})


def test_a_two_lake_union_on_a_taken_lake_is_not_adopted():
    sheet, lines, lake_a, lake_b = _two_lake_case()
    got = refit.refit_sheet(sheet, lines, {1: {"geom": lake_a, "name": "Elk Lake", "layer": "lakes"}},
                            _near({2: {"geom": lake_b, "name": "Beaver Lake", "layer": "lakes"}}),
                            base_fit=_base(sheet, lake_a, 1), units="metres", taken={2: ["b.zip/b.e00"]})
    st = got["steps"]["two_lake"]
    assert st["lifts"] is True and st["adopted"] is False and st["taken_by"] == {"wbk:2": ["b.zip/b.e00"]}
    assert "two_lake_taken" in got["flags"] and got["after"]["target"] == "wbk:1"


def test_two_sheets_coded_for_one_lake_may_both_end_on_it():
    rows = [dict(_claim_row("a.zip", "wbk:9"), grade="strict"), dict(_claim_row("b.zip", "wbk:9"), grade="strict")]
    assert refit_all.claims(rows, {9: ["a.zip", "b.zip"]}) == ({}, {})


# ---- the run's inputs -------------------------------------------------------------------------------------------------

def test_an_empty_zips_folder_raises_before_anything_is_read(tmp_path, monkeypatch):
    (tmp_path / "zips").mkdir()
    import pipeline.bathymetry.fetch_vectors as fv
    monkeypatch.setattr(fv, "sheets", lambda *_a: (_ for _ in ()).throw(AssertionError("read before the zips")))
    with pytest.raises(SystemExit) as e:
        refit_all.main(["--vec-dir", str(tmp_path)])
    assert str(tmp_path / "zips") in str(e.value) and "fetch_vectors --out" in str(e.value)
    with pytest.raises(SystemExit) as e:
        refit_all.zip_names(tmp_path / "nothing", tmp_path)
    assert "no vector zips at" in str(e.value)


# ---- determinism and the outputs --------------------------------------------------------------------------------------

def test_the_same_sheet_gives_the_same_answer():
    sheet, lines, lake_a, lake_b = _two_lake_case()
    near = _near({2: {"geom": lake_b, "name": "Beaver Lake", "layer": "lakes"}})
    runs = []
    for _ in range(2):
        got = refit.refit_sheet(sheet, lines, {1: {"geom": lake_a, "name": "Elk Lake", "layer": "lakes"}}, near,
                                base_fit=_base(sheet, lake_a), units="metres")
        got.pop("_geoms")
        got.pop("_outline")
        runs.append(json.dumps(got, sort_keys=True))
    assert runs[0] == runs[1]


def _row(zipname, grade, classes, adopted_small=False, after=None, flags=(), would_help=None):
    steps = {"two_lake": {}, "other_lake": {}, "small_stretch": {"adopted": adopted_small},
             "depth_line": {"skipped": "test"}}
    classes = dict(classes, depth_line=classes["small_stretch"])     # step 5 (T1188 v5) left it where step 4 did
    fit = {"wbk": "wbk:1", "iou": 0.85, "median_m": 30.0, "area_ratio": 1.0, "angle_deg": 1.0}
    after = after or {"grade": classes["small_stretch"], "step": "rigid", "target": "wbk:1",
                      "fit": dict(fit, mirror=False, scale=1.0)}
    return {"zip": zipname, "coverage": "a.e00", "state": "fitted", "grade": grade, "fit": fit, "codes": ["X"],
            "units": "metres", "refit": {"classes": classes, "steps": steps, "after": after, "flags": list(flags),
                                         "would_help": would_help, "lakes": {"wbk:1": {"name": "A Lake"}}}}


def test_summary_counts_each_step_and_gives_the_small_stretch_its_own_class():
    c1 = {"baseline": "poor", "rigid": "loose", "two_lake": "loose", "other_lake": "loose", "small_stretch": "strict"}
    c2 = {"baseline": "loose", "rigid": "loose", "two_lake": "loose", "other_lake": "loose", "small_stretch": "loose"}
    rows = [_row("a.zip", "poor", c1, adopted_small=True, flags=["rigid_wider", "small_stretch"]),
            _row("b.zip", "loose", c2, flags=["bigger_stretch_would_help"],
                 would_help={"change": "grows the chart 9.0%", "fit": {"scale": 1.09, "iou": 0.93, "grade": "strict"}}),
            {"zip": "c.zip", "coverage": "c.e00", "state": "fitted", "grade": "strict", "fit": {"wbk": "wbk:3"}},
            {"zip": "d.zip", "coverage": "d.e00", "state": "no_shore"}]
    rows.append(_row("e.zip", "poor", dict(c2, baseline="poor", rigid="poor", two_lake="poor", other_lake="poor",
                                            small_stretch="poor"), flags=["rigid_ambiguous"]))
    rows[-1]["refit"]["steps"]["rigid"] = {"lifts": True, "adopted": False, "fit": {"grade": "strict"}}
    rows[-1]["refit"]["after"]["grade"] = "poor"
    s = refit_all.summarise(rows)
    assert s["lists"]["lift_not_adopted"] == ["e.zip/a.e00"]
    rows.pop()
    s = refit_all.summarise(rows)
    assert s["counts"]["baseline"] == {"loose": 1, "no_shore": 1, "poor": 1, "strict": 1}
    assert s["counts"]["rigid"] == {"loose": 2, "no_shore": 1, "strict": 1}
    assert s["counts"]["small_stretch"] == {"loose": 1, "no_shore": 1, "strict": 1, "strict (stretch <= 5 %)": 1}
    assert s["list_sizes"] == {"bigger_stretch_would_help": 1, "changed_class": 1, "review_loose": 1,
                               "review_rigid_wider": 1, "small_stretch": 1}     # a.zip: moved by the wider search
    assert s["confidence"] == {"high": 1, "review": 3}
    text = refit_all.csv_rows(rows, {"a.zip": ["A LAKE"]})
    lines = text.splitlines()
    assert lines[0].split(",") == refit_all.CSV_COLS and len(lines) == 5
    assert "strict (stretch <= 5 %)" in lines[1] and "grows the chart 9.0%" in lines[2]


# ---- T1188 v5: the shore's joins, the depth lines as a witness, a depth line as the outline -------------------------

SOUNDING = [(400.0, 300.0, -5.0)]       # a sounding in the open water: it marks the water face


def _gap_sheet(north=None, depth_top=700.0):
    """an 800 x 600 m lake whose coded shore (GA2711) leaves the north side open (an 800 m gap), a stream running off
    the east shore, and one depth line reaching `depth_top` (above y = 600: into the north shallows)"""
    shore = [(0.0, 600.0), (0.0, 0.0), (800.0, 0.0), (800.0, 600.0)]
    arcs = [(0, "GA27110000", shore), (0, "GA24850000", [(800.0, 300.0), (950.0, 300.0)]),
            (-1.0, "HA06200000", [(100.0, 100.0), (700.0, 100.0), (700.0, depth_top), (100.0, depth_top),
                                  (100.0, 100.0)])]
    if north is not None:
        arcs.append((0, "GA24850000", north))
    return arcs


def test_a_long_gap_is_completed_with_the_sheets_stream_coded_shore():
    # Abruzzi: the north-west shore is coded as a stream; the straight join cut the shallows off
    bulge = [(800.0, 600.0), (600.0, 800.0), (200.0, 800.0), (0.0, 600.0)]
    water, info = place.shore_report(_gap_sheet(bulge), SOUNDING, max_gap=1000.0)
    assert abs(water.area - (800 * 600 + (800 + 400) / 2 * 200)) < 1e-6
    assert info["joins"] == [{"m": 800.0, "how": "arcs", "arcs_m": round(2 * math.hypot(200, 200) + 400, 1),
                              "codes": ["GA24850000"]}]
    assert info["flags"] == [] and info["depth_lines_outside_shore_pct"] == 0.0
    assert info["longest_straight_join_m"] == 0.0
    assert abs(place.shore(_gap_sheet(bulge), SOUNDING, max_gap=1000.0).area - water.area) < 1e-9


def test_a_long_gap_with_no_arcs_keeps_its_straight_join_and_is_flagged():
    # Boy Lake: nothing runs between the gap's ends; the chord stays, and says so
    water, info = place.shore_report(_gap_sheet(), SOUNDING, max_gap=1000.0)
    assert abs(water.area - 800 * 600) < 1e-6
    assert info["joins"] == [{"m": 800.0, "how": "straight"}] and info["longest_straight_join_m"] == 800.0
    assert info["flags"] == ["shore_joined_straight", "depth_lines_outside_shore"]
    assert info["depth_lines_outside_shore_pct"] > 1.0


def test_a_completion_that_cuts_the_depth_lines_off_is_refused():
    dip = [(800.0, 600.0), (600.0, 400.0), (200.0, 400.0), (0.0, 600.0)]       # runs INTO the water
    water, info = place.shore_report(_gap_sheet(dip, depth_top=550.0), SOUNDING, max_gap=1000.0)
    assert abs(water.area - 800 * 600) < 1e-6 and info["joins"][0]["how"] == "straight"
    assert info["flags"] == ["shore_joined_straight"]                         # the chord holds the depth lines


def test_no_completion_without_depth_lines_to_check_it():
    bulge = [(800.0, 600.0), (600.0, 800.0), (200.0, 800.0), (0.0, 600.0)]
    arcs = [a for a in _gap_sheet(bulge) if a[0] in (None, 0)]          # the depth line dropped
    water, info = place.shore_report(arcs, SOUNDING, max_gap=1000.0)
    assert info["joins"] == [{"m": 800.0, "how": "straight"}] and info["flags"] == ["shore_joined_straight"]
    assert abs(water.area - 800 * 600) < 1e-6


def test_a_completion_never_runs_through_another_shore_node_and_stays_short():
    a, b = (0.0, 0.0), (100.0, 0.0)
    arcs = [("GA24850000", LineString([a, (50.0, 50.0)])), ("GA24850000", LineString([(50.0, 50.0), b])),
            ("GA24850000", LineString([a, (50.0, -500.0), b]))]
    ks, m = place._completion(a, b, arcs, set(), 3 * 100.0)
    assert ks == [0, 1] and abs(m - 2 * math.hypot(50, 50)) < 1e-9
    assert place._completion(a, b, arcs, {(50.0, 50.0)}, 3 * 100.0) is None     # the short way crosses the shore
    assert place._completion(a, b, arcs, {(50.0, 50.0)}, 1e4)[0] == [2]          # the long way, when allowed


def test_a_short_join_is_never_replaced():
    bulge = [(800.0, 600.0), (600.0, 800.0), (200.0, 800.0), (0.0, 600.0)]
    arcs = [(cv, fc, [(x * 0.3, y * 0.3) for x, y in p]) for cv, fc, p in _gap_sheet(bulge)]   # a 240 m gap
    water, info = place.shore_report(arcs, [(120.0, 90.0, -5.0)], max_gap=300.0)
    assert info["joins"] == [{"m": 240.0, "how": "straight"}] and "shore_joined_straight" not in info["flags"]
    assert abs(water.area - 240 * 180) < 1e-6


def test_the_shore_flags_and_the_depth_lines_on_land_make_a_placing_review():
    boy = _placed()
    boy["shore"] = {"flags": ["shore_joined_straight", "depth_lines_outside_shore"]}
    assert refit_all.confidence(boy) == ("review", ["shore_joined_straight", "depth_lines_outside_shore"])
    assert refit_all.confidence(dict(_placed(), depth_on_land_pct=12.5)) == ("review", ["depth_lines_on_land"])
    assert refit_all.confidence(dict(_placed(), depth_on_land_pct=7.6)) == ("high", [])   # the worst high one: a warning
    part = {"after": {"grade": "loose", "step": "depth_line", "target": "wbk:1",
                      "fit": {"iou": 0.82, "median_m": 10.0, "area_ratio": 0.91, "angle_deg": 5.0, "mirror": False,
                              "scale": 1.0}},
            "flags": ["fitted_on_depth_line"]}
    part["steps"] = {"small_stretch": {}, "depth_line": {"adopted": True, "fit": part["after"]["fit"]}}
    assert refit_all.confidence(_placed(grade="poor", refit_part=part)) == ("review", ["fitted_on_depth_line", "loose"])


def test_depth_on_land_is_the_share_more_than_10_m_outside_our_lake():
    lake = box(0, 0, 100, 100)
    lines = [LineString([(10, 50), (90, 50)]), LineString([(50, 105), (50, 125)])]     # 80 m in, 20 m: 5 near, 15 out
    assert refit_all.depth_on_land_pct(lines, lambda g: g, lake) == 15.0
    assert refit_all.depth_on_land_pct([], lambda g: g, lake) is None


def _depth_line_case(ring_shape, rival_m=None):
    """a chart whose shoreline is 1.5 x our lake (it fits poorly) and whose first depth line is our lake's shape"""
    inner = ring_shape(400.0)
    sheet = affinity.scale(inner, 1.5, 1.5, origin=(0, 0))
    lines = [(0, LineString(sheet.exterior.coords)), (-1.0, LineString(inner.exterior.coords)),
             (-2.0, LineString(affinity.scale(inner, 0.5, 0.5, origin=(0, 0)).exterior.coords))]
    lake = _earth(inner)
    lakes = {1: {"geom": lake, "name": "A Lake", "layer": "lakes"}}
    if rival_m is not None:
        lakes[2] = {"geom": affinity.translate(lake, rival_m, 0), "name": "Other Lake", "layer": "lakes"}
    return sheet, lines, lake, lakes


def test_a_depth_line_is_adopted_as_the_outline_under_the_safe_rule():
    sheet, lines, lake, lakes = _depth_line_case(_chiral)
    got = refit.refit_sheet(sheet, lines, {1: lakes[1]}, _near(lakes), base_fit=_base(sheet, lake), units="metres")
    st = got["steps"]["depth_line"]
    assert st["adopted"] and st["line"] == "depth_1" and st["depth"] == 1.0
    assert got["after"]["step"] == "depth_line" and got["after"]["grade"] == "strict"
    assert got["fit_line"] == "depth_1" and "fitted_on_depth_line" in got["flags"]
    assert got["classes"]["small_stretch"] == "poor" and got["classes"]["depth_line"] == "strict"
    assert st["tried"][1]["refused"].startswith("does not lift")             # the second line is too small
    assert abs(got["_outline"].area - _chiral(400.0).area) < 1.0              # the outline fitted is the 1 m line


def test_a_depth_line_is_refused_when_its_mirror_or_another_lake_fits_as_well():
    sheet, lines, lake, lakes = _depth_line_case(_mirror_symmetric)
    got = refit.refit_sheet(sheet, lines, {1: lakes[1]}, _near(lakes), base_fit=_base(sheet, lake), units="metres")
    assert not got["steps"]["depth_line"]["adopted"] and got["fit_line"] == "shore"
    assert got["steps"]["depth_line"]["tried"][0]["refused"].startswith("ambiguous")
    sheet, lines, lake, lakes = _depth_line_case(_chiral, rival_m=6000.0)   # the same shape 6 km away
    got = refit.refit_sheet(sheet, lines, {1: lakes[1]}, _near(lakes), base_fit=_base(sheet, lake), units="metres")
    assert not got["steps"]["depth_line"]["adopted"]
    assert got["steps"]["depth_line"]["tried"][0]["refused"].startswith("another lake fits")


def test_a_depth_line_is_refused_when_our_lake_is_not_inside_the_charts_shoreline():
    sheet, lines, lake, lakes = _depth_line_case(_chiral)
    sheet = affinity.translate(sheet, 450.0, 0.0)               # the shoreline drawn off to one side of the line
    lines = [(0, LineString(sheet.exterior.coords))] + lines[1:]
    got = refit.refit_sheet(sheet, lines, {1: lakes[1]}, _near(lakes), base_fit=_base(sheet, lake), units="metres")
    assert not got["steps"]["depth_line"]["adopted"]
    assert got["steps"]["depth_line"]["tried"][0]["refused"].startswith("our lake not inside")
