"""P09: part topology + camera hints (`part_geo`, `part_mark`, `water_exit`), the map navigation's data with no geometry.

No data: the pure rules (km, rank, midpoint walk, mark merge, split labels). On a bundle (`needs_bundle`; point
`UI_EXPORT_BUNDLE` at a SIDE bundle built with `python -m pipeline.deliver bundle --out …/bundle.next.sqlite`, never the
live one before promotion): the Vedder / Chilliwack and the Cowichan against the prototype's water map
(`pipeline/tests/fixtures/watermap.js`, frozen: written by `water_map_export` for the deleted prototype, T87), and every part's midpoint.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import topology as T


# ---- the rules, no data ------------------------------------------------------------------------------------------

def test_part_km_is_the_highest_main_stem_km_else_the_side_channels():
    assert T.part_km([{"km_from": 3.0}, {"km_from": 7.5}, {"km_from": 9.0, "branch": True}]) == 7.5
    assert T.part_km([{"km_from": 2.0, "branch": True}, {"km_from": None}]) == 2.0
    assert T.part_km([]) is None


def test_parts_rank_mouth_first_and_parts_without_km_go_last_in_part_order():
    parts = [{"v": 0, "km": 30.0}, {"v": 1, "km": 5.0}, {"v": 2, "km": None}, {"v": 3, "km": 12.0}, {"v": 4}]
    assert [p["v"] for p in T.order_parts(parts)] == [1, 3, 0, 2, 4]


def test_the_midpoint_is_the_section_holding_half_the_stem_length():
    assert T.stem_midpoint([(10, 100.0), (11, 100.0), (12, 100.0), (13, 700.0)]) == 13
    assert T.stem_midpoint([(10, 400.0), (11, 100.0), (12, 500.0)]) == 11
    assert T.stem_midpoint([(10, None), (11, None)]) == 10


def test_marks_at_one_point_with_one_down_part_merge():
    raw = [{"lon": -122.0, "lat": 49.1, "down": 0, "up": 2, "label": None, "token": None},
           {"lon": -122.0, "lat": 49.1, "down": 0, "up": 1, "label": "Bridge", "token": "x__bridge"},
           {"lon": -122.1, "lat": 49.2, "down": 1, "up": 3, "label": None, "token": None}]
    assert T.merge_marks(raw) == [
        {"lon": -122.0, "lat": 49.1, "down": 0, "up": [1, 2], "label": "Bridge", "token": "x__bridge"},
        {"lon": -122.1, "lat": 49.2, "down": 1, "up": [3], "label": None, "token": None}]


def test_two_meeting_places_of_one_pair_of_parts_are_two_marks():
    """Kanaka Creek: since T517 the creek below the 112 Avenue bridge and above Cliff Park Falls are two parts (0 and
    2) with part 1 between; each meeting place is its own mark."""
    raw = [{"lon": -122.55, "lat": 49.20, "down": 0, "up": 1, "label": "112 Avenue bridge"},
           {"lon": -122.53, "lat": 49.21, "down": 1, "up": 2, "label": "Cliff Park Falls"}]
    assert [(m["down"], m["up"], m["label"]) for m in T.merge_marks(raw)] == [
        (0, [1], "112 Avenue bridge"), (1, [2], "Cliff Park Falls")]


def test_a_mark_says_the_same_words_as_the_two_parts_names_there():
    """T517: one function (`answers.display.end_words`) names a mark and the parts' ends it joins: the lower part's
    top end and the upper part's bottom end are one token, said once."""
    doc = {"splits": {"x__bridge": {"name": "Big Bridge", "kind": "point"}}, "waters": {}}
    down = {"runs": [{"from": "x__bridge", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]}
    up = {"runs": [{"from": "source", "to": "x__bridge", "km_from": 9.0, "km_to": 3.0}]}
    assert T.mark_words(down, up, doc, doc["splits"])[:2] == ("Big Bridge", None)


def test_a_gauge_cut_is_said_by_the_regulations_own_split_there_else_the_nearest_named_feature():
    """T517 (review finding 7): a gauge cut a part ends at is named by the regulation's own split at that place
    (`same_place_as`), else the nearest named feature on the ground (`near`, the bundle's `topology.gauge_near`) — at
    it within `SAME_PLACE_M`, else "near" it. Its code and station name are never the place's words."""
    from pipeline.deliver.answers.display.part import end_words, run_ends
    from pipeline.deliver.answers.common import AnswersError
    doc = {"splits": {"gauge__1": {"name": "08FC003 · Dean River Below Tanswanket Creek", "kind": "gauge",
                                   "water_id": "gnis:1", "km": 121.9, "near": "confluence:gnis:2", "near_m": 80.0},
                      "far": {"name": "Moat Lake", "kind": "lake_edge", "water_id": "gnis:1", "km": 23.0}},
           "waters": {"gnis:2": {"name": "Tanswanket Creek"}}}
    assert end_words(doc, "gauge__1", "from") == "Tanswanket Creek confluence"
    doc["splits"]["gauge__1"]["near_m"] = 1500.0
    assert end_words(doc, "gauge__1", "from") == "Near the Tanswanket Creek confluence"
    doc["splits"]["gauge__1"]["same_place_as"] = "far"
    assert end_words(doc, "gauge__1", "from") == "Moat Lake"
    # a regulation's cut named after its gauge says its place, the gauge reference left to the alternates
    doc["splits"]["sk"] = {"name": "above Babine River (WSC gauge 08EB005)", "kind": "point", "water_id": "gnis:1",
                           "km": 337.7}
    assert end_words(doc, "sk", "from") == "Above Babine River"
    # a gauge cut with no regulation split and no named feature within `topology.NEAR_M` (2 km; the Dean's 08FC003:
    # Tanswanket Creek's drawn sections are 13.2 km off) is said by its km — a deliberate rule, never the gauge's code
    del doc["splits"]["gauge__1"]["same_place_as"], doc["splits"]["gauge__1"]["near"]
    assert end_words(doc, "gauge__1", "from") == "km 121.9"
    assert run_ends(doc, {"runs": [{"from": "gauge__1", "to": "mouth", "km_from": 121.9, "km_to": 0.0}]}) == \
        ("Mouth", "km 121.9")
    # MUTATION: with no km either it stops the build
    doc["splits"]["gauge__1"]["km"] = None
    with pytest.raises(AnswersError, match="no one km"):
        end_words(doc, "gauge__1", "from")


def test_a_gauge_cut_keeps_every_name_it_is_known_by():
    """T517 (owner 2026-11-08: "any can be used"): the regulation's own name is said; the gauge's stays as an alternate
    (`end_variants`, the mark's `alt`), never discarded."""
    from pipeline.deliver.answers.display.part import end_variants
    doc = {"splits": {"gauge__1": {"name": "08HB074 · Cruickshank River Near The Mouth", "kind": "gauge",
                                   "water_id": "gnis:1", "km": 2.5, "same_place_as": "park"},
                      "park": {"name": "Strathcona Park boundary", "kind": "area", "water_id": "gnis:1", "km": 2.5}},
           "waters": {}}
    assert end_variants(doc, "gauge__1", "from") == ["Strathcona Park boundary",
                                                     "08HB074 · Cruickshank River Near The Mouth"]
    down = {"runs": [{"from": "gauge__1", "to": "mouth", "km_from": 2.5, "km_to": 0.0}]}
    up = {"runs": [{"from": "source", "to": "gauge__1", "km_from": 23.0, "km_to": 2.5}]}
    assert T.mark_words(down, up, doc, doc["splits"])[0] == "Strathcona Park boundary"
    assert T.mark_alt(down, up, doc) == ["08HB074 · Cruickshank River Near The Mouth"]


def _span(rows):
    """{sid: (lo_m, hi_m, lo, hi, off_stem, shape)} from (sid, lo_m, hi_m[, off_stem])."""
    return {r[0]: (r[1], r[2], "a", "b", r[3] if len(r) > 3 else 0, 0) for r in rows}


def test_a_part_is_split_into_stretches_only_where_another_part_lies_between():
    """T204: Kanaka's part 0 runs 0-7197 m and 8207-24768 m (with a lake between 24522 and 24725); part 1 is 7197-8207.
    Two stretches: the lake does not separate, the other part does."""
    span = _span([(1, 0, 7197), (3, 8207, 24522), (4, 24725, 24768), (9, 3000, 3000, 1)])
    runs = [{"sids": [4]}, {"sids": [3]}, {"sids": [1]}, {"sids": [9], "branch": True, "km_from": 3.0}]
    got = T.stretches(runs, span, other=[(7197, 8207)])
    assert [(s["lo"], s["hi"], s["sids"]) for s in got] == [(0, 7197, [1, 9]), (8207, 24768, [3, 4])]
    # no other part between: one stretch
    assert len(T.stretches(runs, span, other=[])) == 1
    # side channels only (no main stem): one stretch of every section
    assert T.stretches([{"sids": [9], "branch": True, "km_from": None}], span, []) == [
        {"lo": None, "hi": None, "sids": [9]}]


def test_a_boundary_is_named_by_the_downstream_runs_start_else_the_upstream_runs_end():
    splits = {"a": {"name": "Vedder Crossing Bridge"}, "b": {"name": "Tamihi"}}
    down, up = {"km": 1.0, "runs": [{"from": "a"}]}, {"km": 5.0, "runs": [{"to": "b"}]}
    assert T.split_label(down, up, splits) == "Vedder Crossing Bridge"
    assert T.split_label({"km": 1.0, "runs": [{"from": None}]}, up, splits) == "Tamihi"
    # a branch is named only by its own runs' ends
    assert T.split_label(down, {"km": None, "runs": [{"to": None, "from": "b"}]}, splits) == "Tamihi"
    assert T.split_label({"runs": []}, {"km": 2.0, "runs": []}, splits) is None


class _N:
    """A graph node as `lines.piece` reads it: `{blk}:{measure}` ids are stream pieces, `lake:` ones lakes."""
    def __init__(self, nid, length_m=100.0):
        self.blk = "" if nid.startswith("lake:") else nid.split(":")[0]
        self.kind = "lake" if nid.startswith("lake:") else "stream"
        self.length_m = length_m


class _E:
    def __init__(self, f, t, kind="continuation"):
        self.from_node, self.to_node, self.kind = f, t, kind


class _Fraser:
    """T59: the Fraser's shape. Water 1 (line F) reaches the sea at F:0 (no edge out); water 2 (Annacis Channel, line A)
    leaves F:200 and comes back into F:100 — item_down says each flows into the other. Water 3 (a creek, line C) flows
    into F:200 through an unnamed piece U:0. Water 4's main line (G) ends in its own side piece H:0 (water 4 too), which
    flows into the creek. Water 5 (line L) ends in a loop of unnamed pieces. Water 7 (the Atnarko's shape: line K) has
    braid pieces of its own (B:0, D:0: one line each, shorter than K's, so K is the main line) leaving and rejoining its mouth piece,
    which flows into the Fraser; the braid's lower piece also has an edge out to Annacis Channel, never reached: the
    mouth leaves the water first. Water 8's main line (M, two pieces) ends in an unnamed lake on its course, whose
    outlet is water 8's own lower line R, which flows into the Fraser (Park Creek's shape). Every piece is 100 m but
    G:0 and K:0 (300 m), so each water's main line is the one named (`lines.main_line`: the most length, T118)."""
    nodes = {n: _N(n, 300.0 if n in ("G:0", "K:0") else 100.0) for n in ("F:0", "F:100", "F:200", "A:0", "A:50", "C:0", "U:0", "G:0", "H:0", "L:0", "V:0", "V:1",
                               "K:0", "B:0", "D:0", "M:0", "M:90", "lake:9", "R:0")}
    edges = [_E("F:200", "F:100"), _E("F:100", "F:0"), _E("F:200", "A:50", "braid"), _E("A:50", "A:0"),
             _E("A:0", "F:100"), _E("C:0", "U:0"), _E("U:0", "F:200"), _E("G:0", "H:0"), _E("H:0", "C:0", "trib"),
             _E("L:0", "V:0"), _E("V:0", "V:1"), _E("V:1", "V:0"),
             _E("K:0", "B:0", "confluence"), _E("B:0", "K:0", "confluence"), _E("K:0", "F:200", "confluence"),
             _E("B:0", "D:0", "confluence"), _E("D:0", "A:50", "confluence"),
             _E("M:90", "M:0"), _E("M:0", "lake:9", "lake_in"), _E("lake:9", "R:0", "lake_out"),
             _E("R:0", "F:200", "confluence")]
    down_adj = {}
    for i, e in enumerate(edges):
        down_adj.setdefault(e.from_node, []).append(i)


def test_a_water_exits_where_its_mouth_flows_never_into_its_own_side_channel():
    ids = ["F:0", "F:100", "F:200", "A:0", "A:50", "C:0", "U:0", "G:0", "H:0", "L:0", "V:0", "V:1",
           "K:0", "B:0", "D:0", "M:0", "M:90", "lake:9", "R:0"]                                  # handles 1..19
    secs = {1: {1, 2, 3}, 2: {4, 5}, 3: {6}, 4: {8, 9}, 5: {10}, 6: set(), 7: {13, 14, 15}, 8: {16, 17, 19}}
    # the Fraser reaches the sea: no receiver; Annacis Channel and the creek flow into the Fraser; water 4 leaves its
    # main line's mouth through its own piece H:0 into the creek; water 5's way down never meets a named water; water 7
    # leaves by its mouth into the Fraser, not by its braid into Annacis Channel; water 8 goes on through the lake
    assert T.mouth_downs(_Fraser(), ids, secs) == {1: set(), 2: {1}, 3: {1}, 4: {3}, 5: set(), 7: {1}, 8: {1}}


def test_a_braid_pair_s_smaller_water_is_never_the_bigger_one_s_exit():
    """T519: since T497 named the delta's connectors, the Fraser's mouth walk meets Morey Channel. A smaller water on
    the river's own watershed code is its channel: never its exit. The Courtenay (its own code) below the Puntledge
    stays the Puntledge's exit, however short it is."""
    size = {9697: 300, 14905: 5}
    codes = {9697: "100-", 14905: "100-"}
    assert T.exit_receivers(9697, [14905], {9697: {14905}, 14905: set()}, {9697: [14905]}, size, codes) == []
    assert T.exit_receivers(9697, [14905], {9697: set(), 14905: set()}, {9697: [14905]}, size, codes) == []
    assert T.exit_receivers(14905, [9697], {9697: {14905}, 14905: {9697}}, {14905: [9697]}, size, codes) == [9697]
    puntledge = T.exit_receivers(27060, [12206], {27060: {12206}, 12206: set()}, {27060: [12206]},
                                 {27060: 13, 12206: 2}, {27060: "920-1", 12206: "920-"})
    assert puntledge == [12206]
    # a lake on the river carries its code and is where the river goes (Okanagan River into Osoyoos Lake)
    assert T.exit_receivers(1, [2], {1: {2}, 2: set()}, {1: [2]}, {1: 40, 2: 1}, {1: "300-", 2: "300-"},
                            streams={1}) == [2]
    # two channels of one river: the upper Jeperson Side Channel still exits into the lower (the Fraser is the main)
    assert T.exit_receivers(5, [6], {5: {6}, 6: set()}, {5: [6]}, {9697: 300, 5: 5, 6: 1},
                            {9697: "100-", 5: "100-", 6: "100-"}, streams={9697, 5, 6}) == [6]
    # without codes the old rule stands
    assert T.exit_receivers(9697, [14905], {9697: {14905}, 14905: set()}, {9697: [14905]}, size) == [14905]


def test_a_water_never_exits_into_a_water_that_flows_into_it():
    # the Fraser (9697): item_down says it flows into Annacis Channel (176), Seabird Island North Side Channel (1) and
    # Moose Lake (18837, a lake: no main line); each of them flows into the Fraser, so the Fraser exits into none
    downs = {9697: [1, 176, 18837], 176: [9697], 1: [9697], 18837: [9697], 30: [176, 9697]}
    mouths = {9697: set(), 176: {9697}, 1: {9697}, 30: {176}}
    assert T.exit_receivers(9697, downs[9697], mouths, downs) == []
    # Annacis Channel keeps the Fraser: the Fraser's mouth is the sea, not Annacis Channel
    assert T.exit_receivers(176, downs[176], mouths, downs) == [9697]
    # a creek touching both keeps both: neither flows into it (the nearest part still picks one)
    assert T.exit_receivers(30, downs[30], mouths, downs) == [176, 9697]
    # a side channel reaching the Fraser through another (the upper Jeperson Side Channel through the lower) is none
    # either; a loop elsewhere in the chain ends the search
    downs = {9697: [4, 519], 4: [519], 519: [9697]}
    assert T.exit_receivers(9697, downs[9697], {9697: set(), 4: {519}, 519: {9697}}, downs) == []
    assert T.exit_receivers(9697, [4], {9697: set(), 4: {519}, 519: {4}}, downs) == [4]
    # two waters whose mouths each reach the other (the two Jeperson Side Channels): each keeps the other — its own
    # mouth reaching it is evidence the other's cannot overrule
    downs = {4: [519], 519: [4, 9697]}
    mouths = {4: {519}, 519: {4, 9697}}
    assert (T.exit_receivers(4, downs[4], mouths, downs), T.exit_receivers(519, downs[519], mouths, downs)) == (
        [519], [4, 9697])
    # a lake the creek runs out of (item_down both ways) is no exit; the lake it runs into is
    downs = {5: [6, 7], 6: [5], 7: []}
    assert T.exit_receivers(5, downs[5], {5: {7}}, downs) == [7]
    # a lake (no main line) keeps every receiver, even a creek whose main line says it flows into the lake
    downs = {6: [5], 5: [6, 7]}
    assert T.exit_receivers(6, downs[6], {5: {6}}, downs) == [5]


def test_the_water_map_export_reads_the_same_rules():
    from pipeline.tools.audit import water_map_export as W
    assert W.order_parts is T.order_parts and W.split_label is T.split_label


# ---- on a bundle -------------------------------------------------------------------------------------------------

def _bundle(request) -> sqlite3.Connection:
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, BUNDLE_HINT
    p = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'part_geo'").fetchone():
        pytest.fail(f"{p} predates P09 (no part_geo): build a side bundle with `python -m pipeline.deliver bundle "
                    f"--out <dir>/bundle.next.sqlite` and point UI_EXPORT_BUNDLE at it")
    return db


def _watermap() -> dict:
    from pipeline.common.curated import REPO_ROOT
    t = (REPO_ROOT / "pipeline" / "tests" / "fixtures" / "watermap.js").read_text()
    return json.loads(t[t.index("{"):t.rindex("}") + 1])["waters"]


def _marks(db, item: str) -> list[tuple]:
    return [(d, tuple(json.loads(u)), lab) for d, u, lab in db.execute(
        "SELECT m.down_part, m.up_parts, m.label FROM part_mark m JOIN item i ON i.ord = m.ord "
        "WHERE i.item_id = ? ORDER BY m.mark_ix", (item,))]


@pytest.mark.needs_bundle
def test_the_chilliwack_and_the_cowichan_meet_as_the_prototype_draws_them(request):
    """THE OUTSIDE REFERENCE (restored, review finding 9): the prototype's water map (`fixtures/watermap.js`, the
    regs-v11 page's own data) draws each water as its rule-set stretches mouth up with the boundary between each. T517
    cuts one more place (the DFO salmon chain, inside a run: the prototype draws no salmon cut) and lists no tidal
    piece, so the bundle's parts JOINED by (grp, run), without tidal water, must be the prototype's stretches in the
    same order, and the marks between them say the prototype's boundary words (`_end_short`: sign-post words off)."""
    from pipeline.deliver.answers.display.part import _SIGN_WORDS
    from pipeline.deliver.answers.common import cap
    db = _bundle(request)
    wm = _watermap()
    for group, item in (("vedder", "gnis:8634"), ("cowichan", "gnis:12227")):
        proto = [p for p in wm[group]["parts"] if p["item"] == item]
        at = {i for i, p in enumerate(wm[group]["parts"]) if p["item"] == item}
        want = [cap(_SIGN_WORDS.sub("", m["label"])) for m in wm[group]["marks"] if m["down"] in at and m["up"] in at]
        (o,) = db.execute("SELECT ord FROM item WHERE item_id = ?", (item,)).fetchone()
        parts = db.execute("SELECT part_ix, grp, run, set_id IS NULL FROM part WHERE ord = ? ORDER BY part_ix",
                           (o,)).fetchall()
        runs = []
        for ix, g, r, tidal in parts:
            if not tidal and (not runs or runs[-1][0] != (g, r)):
                runs.append(((g, r), ix))
        assert len(runs) == len(proto), (group, runs)
        run_of = {ix: (g, r) for ix, g, r, tidal in parts if not tidal}
        got = [lab for lab, d, u in db.execute("SELECT label, down_part, up_parts FROM part_mark WHERE ord = ? "
                                               "ORDER BY mark_ix", (o,))
               if d in run_of and all(x in run_of and run_of[x] != run_of[d] for x in json.loads(u))]
        assert got == want, group


@pytest.mark.needs_bundle
def test_the_chilliwack_and_the_cowichan_are_runs_in_river_order_with_a_named_mark_between_each(request):
    """T517: one part per connected run, numbered from the mouth, each pair of neighbours met at ONE named mark in
    the words their names say there. The Chilliwack's lowest 200 m (to DFO's orange triangular signs) is a part of its
    own: its DFO salmon chain differs from the rest of its run (same rule-set group and run)."""
    db = _bundle(request)

    def rows(item):
        (o,) = db.execute("SELECT ord FROM item WHERE item_id = ?", (item,)).fetchone()
        parts = db.execute("SELECT part_ix, grp, run FROM part WHERE ord = ? ORDER BY part_ix", (o,)).fetchall()
        marks = db.execute("SELECT label, down_part, up_parts FROM part_mark WHERE ord = ? ORDER BY mark_ix",
                           (o,)).fetchall()
        return parts, marks
    parts, marks = rows("gnis:8634")
    assert [(g, r) for _, g, r in parts] == [(1, 0), (1, 0), (2, 0), (0, 0), (3, 0)]
    assert marks == [("Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)", 0, "[1]"),
                     ("Tamihi Rapids Bridge", 1, "[2]"),
                     ("100 m below the Slesse Creek confluence", 2, "[3]"),
                     ("Chilliwack River Ecological Reserve boundary", 3, "[4]")]
    parts, marks = rows("gnis:12227")
    assert len(parts) == 5 and [(m[1], m[2]) for m in marks] == [(k, f"[{k + 1}]") for k in range(4)]
    assert [m[0] for m in marks][1:] == ["CNR Mile 66 Trestle", "CNR Bridge (mile 70.2)", "Greendale Trestle"]


@pytest.mark.needs_bundle
def test_kanaka_is_four_runs_and_its_one_rule_set_on_both_sides_of_the_bridge_reach_is_two_parts(request):
    """T204 -> T517: Kanaka Creek's rule set below the 112 Avenue bridge AND above Cliff Park Falls is TWO parts now
    (one group, runs 0 and 1), the part between meets each at its own mark; the tidal piece is the lowest part."""
    db = _bundle(request)
    (o,) = db.execute("SELECT ord FROM item WHERE item_id = 'gnis:11318'").fetchone()
    parts = db.execute("SELECT part_ix, grp, run, set_id IS NULL FROM part WHERE ord = ? ORDER BY part_ix",
                       (o,)).fetchall()
    assert parts == [(0, 2, 0, 1), (1, 0, 0, 0), (2, 1, 0, 0), (3, 0, 1, 0)]
    marks = db.execute("SELECT label, down_part, up_parts FROM part_mark WHERE ord = ? ORDER BY mark_ix", (o,)).fetchall()
    assert marks == [("CPR Bridge (tidal boundary)", 0, "[1]"), ("112 Avenue bridge", 1, "[2]"),
                     ("Cliff Park Falls", 2, "[3]")]
    assert not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'part_stretch'").fetchone()


@pytest.mark.needs_bundle
def test_the_chilliwack_flows_into_the_vedder_at_vedder_crossing(request):
    db = _bundle(request)
    row = db.execute("SELECT d.item_id, x.down_part, x.label FROM water_exit x JOIN item i ON i.ord = x.ord "
                     "JOIN item d ON d.ord = x.down_ord WHERE i.item_id = 'gnis:8634'").fetchone()
    # (T517) the Vedder's parts are in river order: Vedder Crossing is its TOP part
    assert row == ("gnis:3062", 1, "Vedder River")


@pytest.mark.needs_bundle
def test_the_fraser_reaches_the_sea_and_its_side_channels_flow_into_it(request):
    """T59: the Fraser's exit was Annacis Channel, its own side channel (item_down lists 19 waters the Fraser "flows
    into", every one of them flowing back into it). Reviewed on the live bundle + graph (469 exits change: each old one
    was a water that flows into the water — mostly the headwater lake of a creek that reaches the sea)."""
    db = _bundle(request)

    def exit_of(item):
        return db.execute("SELECT d.item_id FROM water_exit x JOIN item i ON i.ord = x.ord JOIN item d ON "
                          "d.ord = x.down_ord WHERE i.item_id = ?", (item,)).fetchone()
    assert [exit_of(i) for i in ("gnis:39325", "gnis:10494", "gnis:16471", "gnis:17209", "gnis:10631")] == [
        None,                   # Fraser River: the sea
        ("gnis:39325",),        # Annacis Channel: the Fraser
        ("gnis:39325",),        # Nahatlatch River: the Fraser, not Frances Lake (its headwater lake)
        ("gnis:11611",),        # Atnarko River: the Bella Coola
        None]                   # Burman River: the sea, not Burman Lake


@pytest.mark.needs_bundle
def test_every_part_has_a_midpoint_of_its_own_inside_its_box(request):
    db = _bundle(request)
    n_parts = db.execute("SELECT COUNT(*) FROM part").fetchone()[0]
    rows = db.execute("SELECT g.ord, g.part_ix, g.mid_sid, g.mid_lon, g.mid_lat, g.w, g.s, g.e, g.n, "
                      "EXISTS (SELECT 1 FROM part_section ps WHERE ps.ord = g.ord AND ps.part_ix = g.part_ix "
                      "AND ps.sid = g.mid_sid) FROM part_geo g").fetchall()
    assert len(rows) == n_parts
    assert not [r[:3] for r in rows if not r[9]], "mid_sid outside its part"
    eps = 1e-6
    assert not [r[:2] for r in rows if not (r[5] - eps <= r[3] <= r[7] + eps and r[6] - eps <= r[4] <= r[8] + eps)]
    sizes = {t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("part_geo", "part_mark", "water_exit")}
    print("P09 sizes:", sizes)


def test_keep_named_reads_the_1_based_handle_and_the_lake_polygons(tmp_path):
    """sid N is LINE N of section_handles.txt (1-based; 0 is no section). Reading ids[sid] shifted every shape by one
    section (found by the Chilliwack test). A lake's outline comes from waterbody_polys.pkl."""
    import pickle
    from shapely.geometry import LineString, Point
    build = tmp_path / "full"
    build.mkdir()
    (build / "section_handles.txt").write_text("blk:1:0\nblk:1:100\nlake:7\n")
    with (build / "waterbody_polys.pkl").open("wb") as fh:
        pickle.dump({"lake:7": Point(0, 0).buffer(5)}, fh)
    geoms = {"blk:1:0": LineString([(0, 0), (1, 0)]), "blk:1:100": LineString([(0, 0), (9, 0)])}
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE item_section (ord INTEGER, sid INTEGER)")
    db.executemany("INSERT INTO item_section VALUES (1, ?)", [(1,), (2,), (3,)])
    got = T.keep_named(db, build, geoms)
    assert (got[1].length, got[2].length, got[3].geom_type) == (1.0, 9.0, "Polygon")


#: (T517 review finding 10) marks with no words: every one is where a SIDE CHANNEL part meets another part — a channel
#: leaves its river at no named place, so neither side has an end the other shares. Pinned as a KNOWN COUNT on the
#: side build (2026-11-08, r-… T517 fix lane); a change moves this number and must say why. The assert in
#: `topology.mark_words` (a mark's words are the parts' end words) is not weakened: it holds wherever two main stems meet.
UNLABELLED_MARKS = 106                 # 112 before two parts meeting across a lake were said by the lake (5)
#                                        107 -> 106 build 27 (2026-11-11): T637 fix (a) 070230ff5 takes the nameless
#                                        blue line 354141307 out of Quinsam Diversion (a curation-named water keeps only
#                                        its named pieces; listed in that commit's dry count), so its side-channel part
#                                        and the one mark between its two parts are gone (4 sections / 2 parts -> 2 / 1)


@pytest.mark.needs_bundle
def test_every_unlabelled_mark_is_a_side_channel_meeting_and_their_count_is_known(request):
    db = _bundle(request)
    rows = db.execute("SELECT m.ord, m.down_part, m.up_parts FROM part_mark m WHERE m.label IS NULL").fetchall()
    side = set()
    for o, ix, sids in db.execute("SELECT ps.ord, ps.part_ix, group_concat(ps.sid) FROM part_section ps "
                                  "GROUP BY ps.ord, ps.part_ix"):
        offs = [r[0] for r in db.execute(f"SELECT off_stem FROM section_span WHERE sid IN ({sids})")]
        if offs and any(offs):
            side.add((o, ix))
    pairs = [(o, d, u) for o, d, us in rows for u in json.loads(us)]
    assert all((o, d) in side or (o, u) in side for o, d, u in pairs), \
        [p for p in pairs if (p[0], p[1]) not in side and (p[0], p[2]) not in side][:5]
    assert len(pairs) == UNLABELLED_MARKS
