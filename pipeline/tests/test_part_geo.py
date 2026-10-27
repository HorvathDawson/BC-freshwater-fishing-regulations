"""P09: part topology + camera hints (`part_geo`, `part_mark`, `water_exit`), the map navigation's data with no geometry.

No data: the pure rules (km, rank, midpoint walk, mark merge, split labels). On a bundle (`needs_bundle`; point
`UI_EXPORT_BUNDLE` at a SIDE bundle built with `python -m pipeline.deliver bundle --out …/bundle.next.sqlite`, never the
live one before promotion): the Vedder / Chilliwack and the Cowichan against the prototype's water map
(`app/design/regs-v11/watermap.js`, written by `water_map_export` from the same build), and every part's midpoint.
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
    raw = [{"lon": -122.0, "lat": 49.1, "down": 0, "up": 2, "label": None},
           {"lon": -122.0, "lat": 49.1, "down": 0, "up": 1, "label": "Bridge"},
           {"lon": -122.1, "lat": 49.2, "down": 1, "up": 3, "label": None}]
    assert T.merge_marks(raw) == [
        {"lon": -122.0, "lat": 49.1, "down": 0, "down_k": 0, "up": [1, 2], "up_k": [0, 0], "label": "Bridge"},
        {"lon": -122.1, "lat": 49.2, "down": 1, "down_k": 0, "up": [3], "up_k": [0], "label": None}]


def test_two_meeting_places_of_one_pair_of_parts_are_two_marks():
    """T204 (Kanaka Creek): part 0 is below the 112 Avenue bridge AND above Cliff Park Falls, part 1 between them. The
    pair meets twice; each place is its own mark, with the stretch of part 0 that meets there."""
    raw = [{"lon": -122.55, "lat": 49.20, "down": 0, "down_k": 0, "up": 1, "up_k": 0, "label": "112 Avenue bridge"},
           {"lon": -122.53, "lat": 49.21, "down": 1, "down_k": 0, "up": 0, "up_k": 1, "label": "Cliff Park Falls"}]
    assert [(m["down"], m["down_k"], m["up"], m["up_k"], m["label"]) for m in T.merge_marks(raw)] == [
        (0, 0, [1], [0], "112 Avenue bridge"), (1, 0, [0], [1], "Cliff Park Falls")]


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
    t = (REPO_ROOT / "app" / "design" / "regs-v11" / "watermap.js").read_text()
    return json.loads(t[t.index("{"):t.rindex("}") + 1])["waters"]


def _ranked(db, item: str) -> list[int]:
    return [p for (p,) in db.execute("SELECT g.part_ix FROM part_geo g JOIN item i ON i.ord = g.ord "
                                     "WHERE i.item_id = ? ORDER BY g.rank", (item,))]


def _marks(db, item: str) -> list[tuple]:
    return [(d, tuple(json.loads(u)), lab) for d, u, lab in db.execute(
        "SELECT m.down_part, m.up_parts, m.label FROM part_mark m JOIN item i ON i.ord = m.ord "
        "WHERE i.item_id = ? ORDER BY m.mark_ix", (item,))]


@pytest.mark.needs_bundle
def test_the_chilliwack_and_the_cowichan_rank_and_meet_as_the_prototype_draws_them(request):
    db = _bundle(request)
    wm = _watermap()
    for group, item in (("vedder", "gnis:8634"), ("cowichan", "gnis:12227")):
        parts = [p for p in wm[group]["parts"] if p["item"] == item]
        assert _ranked(db, item) == [p["v"] for p in parts], group
        # every boundary the prototype draws between two parts of this water, with its words
        at = {i for i, p in enumerate(wm[group]["parts"]) if p["item"] == item}
        want = sorted((wm[group]["parts"][m["down"]]["v"], (wm[group]["parts"][m["up"]]["v"],), m["label"])
                      for m in wm[group]["marks"] if m["down"] in at and m["up"] in at)
        assert sorted(_marks(db, item)) == want, group


@pytest.mark.needs_bundle
def test_kanaka_has_a_mark_at_the_bridge_and_at_the_falls_and_two_stretches(request):
    """T204: Kanaka Creek's part below the bridge + above the falls is two stretches, and the part between meets it at
    BOTH places — the bridge mark (the lower stretch below it) and the falls mark (the upper stretch above it)."""
    db = _bundle(request)
    o = db.execute("SELECT ord FROM item WHERE item_id = 'gnis:11318'").fetchone()
    marks = db.execute("SELECT label, down_part, up_parts, down_stretch, up_stretches FROM part_mark WHERE ord = ? "
                       "ORDER BY mark_ix", o).fetchall()
    labels = sorted(m[0] for m in marks)
    assert labels == ["112 Avenue bridge", "Cliff Park Falls"], marks
    stretches = db.execute("SELECT part_ix, stretch_ix, rank FROM part_stretch WHERE ord = ? ORDER BY rank",
                           o).fetchall()
    assert len(stretches) == 3 and sorted({p for p, _, _ in stretches}) == [0, 1], stretches
    by = {m[0]: m for m in marks}
    two = [p for p, k, _ in stretches if k == 1][0]           # the part with two stretches
    one = [p for p, _, _ in stretches if p != two][0]
    assert by["112 Avenue bridge"][1:] == (two, json.dumps([one]), 0, None)
    assert by["Cliff Park Falls"][1:] == (one, json.dumps([two]), 0, json.dumps([1]))


@pytest.mark.needs_bundle
def test_the_chilliwack_chain_has_no_stretches_and_one_mark_per_pair(request):
    """T204: a chain of parts (each pair meets once) is unchanged: no part_stretch rows, no stretch columns."""
    db = _bundle(request)
    (o,) = db.execute("SELECT ord FROM item WHERE item_id = 'gnis:8634'").fetchone()
    assert db.execute("SELECT COUNT(*) FROM part_stretch WHERE ord = ?", (o,)).fetchone() == (0,)
    assert db.execute("SELECT COUNT(*) FROM part_mark WHERE ord = ? AND (down_stretch IS NOT NULL "
                      "OR up_stretches IS NOT NULL)", (o,)).fetchone() == (0,)


@pytest.mark.needs_bundle
def test_the_chilliwack_flows_into_the_vedder_at_vedder_crossing(request):
    db = _bundle(request)
    row = db.execute("SELECT d.item_id, x.down_part, x.label FROM water_exit x JOIN item i ON i.ord = x.ord "
                     "JOIN item d ON d.ord = x.down_ord WHERE i.item_id = 'gnis:8634'").fetchone()
    assert row == ("gnis:3062", 0, "Vedder River")


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
