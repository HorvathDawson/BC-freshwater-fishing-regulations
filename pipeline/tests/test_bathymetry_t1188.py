"""T1188: the wider fitter (`pipeline.bathymetry.refit`) on small built shapes, no Province file.

What must hold: each step lifts a placing only when it is a real, unambiguous fit; a two-lake sheet splits its contours
per lake; a wrong-lake code is found by shape and refused by a different name; a stretch of 5 % or less is adopted and
flagged, a bigger one never adopted but listed with what it would change; the same input gives the same output.
"""
from __future__ import annotations

import json
import math

from shapely import affinity
from shapely.geometry import LineString, Polygon, box
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
                              "small_stretch": "strict"}


def test_a_union_member_the_sheet_does_not_draw_is_not_adopted():
    sheet = _chiral(900)
    placed = _earth(sheet)
    other = _earth(affinity.translate(_chiral(300), 5000, 0))
    s = refit.split_per_lake(placed, [], {1: placed, 2: other})
    assert s["per_lake"]["2"]["lake_covered"] == 0.0 and s["rings"] == [{"lake": "wbk:1", "ring_ha": round(
        placed.area / 1e4, 2)}]


def place_grade(f):
    from pipeline.bathymetry.place_more import grade
    return grade(f)


# ---- 3. wrong-lake codes ----------------------------------------------------------------------------------------------

def _wrong_code_case(found_name):
    sheet = _chiral(700)
    coded = _earth(box(-900, -100, 900, 100))                    # the code's lake: a different shape
    true = affinity.translate(_earth(sheet, 81.0), 40_000, 0)     # 40 km away (Stake)
    decoy = affinity.translate(_earth(box(-700, -300, 700, 300)), 20_000, 0)
    near = _near({7: {"geom": true, "name": found_name, "layer": "lakes"},
                  8: {"geom": decoy, "name": "Square Lake", "layer": "lakes"}})
    base = _base(sheet, coded, 5)
    return refit.refit_sheet(sheet, [], {5: {"geom": coded, "name": "Long Lake", "layer": "lakes"}}, near,
                             base_fit=base, units="metres", titles=["STAKE LAKE"])


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


# ---- determinism and the outputs --------------------------------------------------------------------------------------

def test_the_same_sheet_gives_the_same_answer():
    sheet, lines, lake_a, lake_b = _two_lake_case()
    near = _near({2: {"geom": lake_b, "name": "Beaver Lake", "layer": "lakes"}})
    runs = []
    for _ in range(2):
        got = refit.refit_sheet(sheet, lines, {1: {"geom": lake_a, "name": "Elk Lake", "layer": "lakes"}}, near,
                                base_fit=_base(sheet, lake_a), units="metres")
        got.pop("_geoms")
        runs.append(json.dumps(got, sort_keys=True))
    assert runs[0] == runs[1]


def _row(zipname, grade, classes, adopted_small=False, after=None, flags=(), would_help=None):
    steps = {"two_lake": {}, "other_lake": {}, "small_stretch": {"adopted": adopted_small}}
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
    assert s["list_sizes"] == {"bigger_stretch_would_help": 1, "changed_class": 1, "small_stretch": 1}
    text = refit_all.csv_rows(rows, {"a.zip": ["A LAKE"]})
    lines = text.splitlines()
    assert lines[0].split(",") == refit_all.CSV_COLS and len(lines) == 5
    assert "strict (stretch <= 5 %)" in lines[1] and "grows the chart 9.0%" in lines[2]
