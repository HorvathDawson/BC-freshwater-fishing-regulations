"""THE WATER MAP EXPORT — `pipeline.tools.audit.water_map_export`, on a tiny synthetic water (no data).

  * every section (part or tributary) is drawn in the status the STATUS INDEX gives it on each
    date — read through `status_index.Index`, the app's `statusOn` twin — and a swapped profile
    changes the drawing (the check can fail);
  * every place two parts touch becomes a marker labelled with the split the export names there;
    a part that touches nothing gets none;
  * upstream / downstream order follows the water: items by the flow table, parts by km, side
    channels last;
  * the export, the display and the bundle must agree on the parts;
  * the legend's words and colours are the app's own (`WATER_STATUS`, the map's themes).
"""
from __future__ import annotations

import json
import re
from datetime import date

import pytest

from pipeline.deliver import status_index as SI
from pipeline.tools.audit import water_map_export as W

DATES = [date(2026, 10, 10), date(2026, 5, 15)]
OCT, MAY = SI.day_of(DATES[0]) - 1, SI.day_of(DATES[1]) - 1


def _line(*pts):
    return {"type": "LineString", "coordinates": [list(p) for p in pts]}


def _profile(oct_code, may_code):
    p = [SI.BASE] * 366
    p[OCT], p[MAY] = oct_code, may_code
    return tuple(p)


def _index(sections: dict) -> SI.Index:
    """sid -> (oct code, may code) as a real status index file, read back by `Index`."""
    profs = sorted({_profile(*v) for v in sections.values()})
    ix = {p: i for i, p in enumerate(profs)}
    return SI.Index(SI.encode({"handles": "00000000000000aa", "reach_digest": "00000000000000bb",
                               "profiles": profs, "items": {},
                               "sections": {s: ix[_profile(*v)] for s, v in sections.items()}}))


# A river (gnis:1, mouth at x=0) in three parts — 0-5 km, 5-10 km and a side channel off the first
# — and the creek above it (gnis:2, flows into gnis:1 at its top), plus two tributary lines.
GEOM = {
    11: _line((-122.00, 49.0), (-121.99, 49.0)), 12: _line((-121.99, 49.0), (-121.98, 49.0)),
    21: _line((-121.98, 49.0), (-121.97, 49.001), (-121.96, 49.0)),
    31: _line((-121.99, 49.0), (-121.99, 48.99)),                   # side channel, leaves part 0
    41: _line((-121.96, 49.0), (-121.94, 49.0)),                    # the creek
    90: _line((-121.97, 49.001), (-121.97, 49.01)),                 # a tributary
    91: _line((-121.97, 49.01), (-121.97, 49.02)),                  # another, too small to draw
}
ORDER = {11: 5, 12: 5, 21: 5, 31: 2, 41: 3, 90: 3, 91: 1}
STATUS = {11: (SI.CLOSED, SI.OWN), 12: (SI.CLOSED, SI.OWN), 21: (SI.OWN, SI.OWN),
          31: (SI.OWN, SI.CLOSED), 41: (SI.BASE, SI.CLOSED), 90: (SI.CLOSED, SI.BASE),
          91: (SI.OWN, SI.OWN)}
PART_SIDS = {("gnis:1", 0): [11, 12], ("gnis:1", 1): [21], ("gnis:1", 2): [31],
             ("gnis:2", 0): [41]}
EXPORT = {
    "gnis:1": {"name": "Test River", "kind": "stream", "parts": [
        {"sections": 2, "runs": [{"from": "test__bridge", "to": "mouth"}]},
        {"sections": 1, "runs": [{"from": "test__falls", "to": "test__bridge"}]},
        {"sections": 1, "runs": [{"from": "lake_outlet", "to": "mouth", "branch": True}]}]},
    "gnis:2": {"name": "Test Creek", "kind": "stream", "parts": [
        {"sections": 1, "runs": [{"from": "source", "to": "mouth"}]}]},
}
DISPLAY = {
    "gnis:1": {"parts": [{"label": "From the mouth up to the Bridge", "short": "Mouth → Bridge", "km": 5.0},
                         {"label": "From the Bridge up to the Falls", "short": "Bridge → Falls", "km": 10.0},
                         {"label": "Side channel", "short": "Side channel", "km": None}]},
    "gnis:2": {"parts": [{"label": "Whole water", "short": "Test Creek", "km": 3.0}]},
}
SPLITS = {"test__bridge": {"name": "Test Bridge"}, "test__falls": {"name": "Test Falls"}}


def _build(index=None, items=("gnis:2", "gnis:1"), **kw):
    index = index or _index(STATUS)
    args = dict(export_waters=EXPORT, display_waters=DISPLAY, splits=SPLITS, part_sids=PART_SIDS,
                drains={"gnis:2": {"gnis:1"}}, trib_sids=[90, 91, 11], geom=GEOM, order_of=ORDER,
                code_on=index.code, dates=DATES)
    args.update(kw)
    return W.build_group("t", list(items), **args)


def _drawn(g):
    """Every (status string, path) the map draws, parts and tributaries."""
    return [(s["s"], s["d"]) for p in g["parts"] for s in p["segs"]] + \
        [(t["s"], t["d"]) for t in g["tribs"]]


def test_every_section_is_coloured_by_the_status_index():
    idx = _index(STATUS)
    g = _build(idx)
    want = {sid: "".join(W.LETTER[idx.code(sid, d)] for d in DATES) for sid in STATUS}
    assert want[11] == "co" and want[41] == "bc"                   # the index, read back
    by_part = {(p["item"], p["v"]): {s["s"] for s in p["segs"]} for p in g["parts"]}
    for (item, v), sids in PART_SIDS.items():
        assert by_part[(item, v)] == {want[s] for s in sids}
    assert {t["s"] for t in g["tribs"]} == {want[90]}              # 91 is below the order cut
    assert [p["s"] for p in g["parts"]] == ["co", "oo", "oc", "bc"]   # a part's word: its roll-up
    # MUTATION: one section's profile changed -> the drawing changes
    assert _drawn(_build(_index({**STATUS, 21: (SI.CLOSED, SI.CLOSED)}))) != _drawn(g)


def test_the_part_rollup_is_the_index_item_rule():
    assert W.rollup(["cc", "cc"]) == "cc"         # closed only when every section is
    assert W.rollup(["co", "bo"]) == "bo"         # one closed, one base -> base
    assert W.rollup(["cb", "ob"]) == "ob"         # any own -> own
    assert W.rollup(["xx", "oo"]) == "oo"         # outside ignored beside B.C.
    assert W.rollup(["tt"]) == "tt"


def test_every_split_becomes_a_labelled_marker():
    g = _build()
    names = [(g["parts"][m["down"]]["short"], g["parts"][m["up"]]["short"], m["label"])
             for m in g["marks"]]
    assert ("Mouth → Bridge", "Bridge → Falls", "Test Bridge") in names
    assert ("Bridge → Falls", "Test Creek", "Test Falls") in names
    assert ("Mouth → Bridge", "Side channel", None) in names   # a branch: only its own ends name it
    assert len(names) == 3                                               # nothing that does not touch
    m = next(m for m in g["marks"] if m["label"] == "Test Falls")
    x, y = W.Projection(W.bbox(GEOM[s] for s in (11, 12, 21, 31, 41)))(-121.96, 49.0)
    assert (m["x"], m["y"]) == (round(x), round(y))                      # at the shared end


def test_an_unnamed_boundary_has_no_label():
    assert W.split_label({"runs": [{"from": "lake_outlet"}]}, {"runs": [{"to": "mouth"}]}, SPLITS) is None
    # a branch (no km) is named only by its own ends, never by the main stem's next split
    assert W.split_label({"km": 5, "runs": [{"from": "test__bridge"}]},
                         {"km": None, "runs": [{"from": "lake_outlet", "to": "mouth"}]}, SPLITS) is None


def test_upstream_and_downstream_follow_the_parts():
    g = _build(items=("gnis:2", "gnis:1"))                 # given creek first: the flow table wins
    assert [(p["item"], p["v"]) for p in g["parts"]] == \
        [("gnis:1", 0), ("gnis:1", 1), ("gnis:1", 2), ("gnis:2", 0)]
    # no flow link (a lake cut into parts): the given order stands
    assert W.order_items(["b", "a"], {}) == ["b", "a"]
    assert W.order_items(["a", "b", "c"], {"a": {"b"}, "b": {"c"}}) == ["c", "b", "a"]


def test_the_export_display_and_bundle_must_agree_on_the_parts():
    with pytest.raises(ValueError, match="sections"):
        _build(part_sids={**PART_SIDS, ("gnis:1", 0): [11]})
    with pytest.raises(ValueError, match="parts"):
        _build(display_waters={**DISPLAY, "gnis:2": {"parts": []}})


def test_paths_are_small_integer_svg():
    g = _build()
    for _, d in _drawn(g):
        assert re.fullmatch(r"(M-?\d+ -?\d+l[-\d ]+z?)+", d), d
    assert g["view"]["w"] == W.GRID and 0 < g["view"]["h"] < W.GRID
    assert len(json.dumps(g)) < 4000


def test_simplify_keeps_the_ends_and_drops_the_straight():
    pts = [(0, 0), (1, 0.01), (2, 0), (3, 5), (4, 0)]
    assert W.simplify(pts, 0.5) == [(0, 0), (2, 0), (3, 5), (4, 0)]


def test_the_legend_is_the_apps_words_and_colours():
    words = W.status_words(W.STATUS_TS.read_text())
    assert {k: v["token"] for k, v in words.items()} == {
        "closed": "color.status.closed", "own": "color.status.own", "base": "color.status.base"}
    themes = {n: json.loads((W.THEMES / f"{n}.json").read_text()) for n in ("light", "dark")}
    col = W.theme_colours(words, themes)
    for n, t in themes.items():
        for s in ("closed", "own", "base"):
            assert col[n][s] == t["values"][f"color.status.{s}"]
    with pytest.raises(ValueError):
        W.status_words("export const WATER_STATUS = {}")


# --------------------------------------------------------------------------------------------
# Braid review (ROUND11 owner 6): a braid the round-10 placement marked (bundle `section_straddle`,
# RULINGS E15) is drawn so a person can SEE it before trusting it — its water, the rule, the cut it
# crosses, the side it was counted on and the share of its length there. No regulation is changed.
# --------------------------------------------------------------------------------------------

def test_the_side_a_braid_is_counted_on_is_said_from_the_cut_and_the_rules_stretch():
    km = {"crag": 3.75, "canyon": 5.24}
    # the rule's stretch lies above its lower cut and below its upper one
    assert W.stretch_side({"op": "between", "splits": ["crag", "canyon"]}, km, "crag") == "above"
    assert W.stretch_side({"op": "between", "splits": ["crag", "canyon"]}, km, "canyon") == "below"
    assert W.stretch_side({"op": "upstream_of", "splits": ["crag"]}, km, "crag") == "above"
    assert W.stretch_side({"op": "downstream_of", "splits": ["crag"]}, km, "crag") == "below"
    assert W.stretch_side({"op": "whole"}, km, "crag") is None
    # counted IN the rule: the stretch's side; OUT: the other side; no stretch known: no words
    assert W.placed_words("in", "below") == "below" and W.placed_words("out", "below") == "above"
    assert W.placed_words("out", "above") == "below" and W.placed_words("in", None) is None


def test_a_cut_is_where_its_two_main_stem_sections_meet_and_a_braid_crosses_the_nearest():
    below, above = _line((0, 0), (1, 0)), _line((1, 0), (2, 0))
    assert W.cut_point(below, above) == (1, 0)
    braid = [_line((1.8, 0.0005), (2.2, 0.0005))]
    cuts = [("a", 1.0, 0.0), ("b", 2.0, 0.0), ("c", 5.0, 0.0)]
    assert W.nearest_cut(braid, cuts) == 1
    assert W.crossed_cuts(braid, cuts) == [1]                    # 2.0 lies inside the braid's frame
    assert W.crossed_cuts([_line((0.9, 0.0), (2.1, 0.0))], cuts) == [0, 1]   # over both ends: no one cut


def test_a_braid_map_carries_side_share_rule_and_cut_and_never_a_section_id():
    geom = {1: _line((0, 0), (0.01, 0)), 2: _line((0.01, 0), (0.02, 0)), 9: _line((0.008, 0.0005), (0.0115, 0.0005))}
    idx = _index({1: (SI.CLOSED, SI.BASE), 2: (SI.OWN, SI.OWN), 9: (SI.CLOSED, SI.CLOSED)})
    out = W.build_braid(
        water="Dean River", kind="stream", braid=[9], lines=[1, 2],
        cuts=[{"split": "canyon", "name": "signs ~0.5 km above the canyon", "at": (0.01, 0.0)}],
        rows=[{"entry_id": "r5:dean", "rule_id": "dean.r4", "label": "No fishing — canyon", "side": "out",
               "share": 0.69, "how": "projected", "cut": "canyon", "stretch": "below", "sids": [1]}],
        geom=geom, code_on=idx.code, dates=DATES)
    assert out["water"] == "Dean River" and out["braid"]["s"] == "cc" and out["braid"]["d"].startswith("M")
    assert out["cuts"] == [{"x": out["cuts"][0]["x"], "y": out["cuts"][0]["y"], "name": "signs ~0.5 km above the canyon"}]
    row = out["rows"][0]
    assert (row["rule"], row["side"], row["share"], row["cut"], row["words"]) == ("No fishing — canyon", "out", 0.69, 0, "above")
    assert row["stretch"].startswith("M")
    assert {ln["s"] for ln in out["lines"]} == {"cb", "oo"}
    text = json.dumps(out)
    assert not re.search(r'"sids?"|section', text)            # section ids never leave the bundle (AGENTS 5)
