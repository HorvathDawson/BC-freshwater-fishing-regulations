"""T1140 (owner 2026-11-25: "itshould have the real name. there should be no partt x of n nameas anywhewre in the app",
"Named by its ends", "They should always have the cut names or distance it is impossible not to"). A part with NO MAIN
STEM is named by what it is, measured on the ground in the bundle stage (`bundle.side_channels`) and said by the answers
step (`answers.display.builders._names`):

- INSIDE: at least 95 % of it (lines by length, polygons by area, every section measured) in ONE area →
  `near:inside:area:<name>` as both ends, "<water> inside <Area>" — the NAME only for a pair a reviewer has looked at
  (`part.REVIEWED_INSIDE`, a tripwire both ways); (T1172) the SUBTITLE for every such part, reviewed or not;
- NAMED ENDS: the ONE such part of a water whose channels have a named end → `near:side:[[lower, upper, km]…]`, "Side
  channel from <lower> up to <upper>"; a run that names only its own mouth takes the named water its flow reaches;
- JOIN DISTANCE: a channel the flow pointers never walk to the main line gets the measure of the main line's nearest
  point to its downstream end (else its end nearer the main line): its OWN water's main line, within `MAX_SNAP_M`, else
  its nearest point — (T1172) said, with no verb, only where its water has more than one such part.

Synthetic shapes (EPSG:3005 metres) through the real functions; every guard has a test that fails without it."""
import sqlite3
import sys

import pytest
from shapely.geometry import LineString, Polygon, box

from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.display.builders import _flows_into, _names, reviewed_still_said
from pipeline.deliver.answers.display.part import AT, INSIDE, OWN, SIDE, Water, end_words, side_channels
from pipeline.deliver.bundle import side_channels as sc

# --- the bundle step: join distance ------------------------------------------------------------------------------

# a main line of two sections along y = 0: sid 1 from x 0 (the mouth, 0 m) to 1000 (1000 m), sid 2 on to 2000 (2000 m)
STEM = [(0, 1000, LineString([(0, 0), (1000, 0)])), (1000, 2000, LineString([(1000, 0), (2000, 0)]))]


def test_the_join_is_the_main_lines_nearest_point_to_the_channels_nearer_end():
    ch = [LineString([(400, 30), (600, 500)])]                 # its end at x 400 lies 30 m off the main line
    m, snap, k, which = sc.join_measure(ch, STEM)
    assert (round(m), round(snap), k, which) == (400, 30, 0, "nearer")


def test_the_flow_pointer_says_which_end_is_downstream():
    ch = [LineString([(400, 70), (1500, 100)])]                # both ends off the main line, beyond the snap limit
    down = LineString([(1500, 100), (1500, 0)])                # the flow leaves at x 1500
    m, snap, _, which = sc.join_measure(ch, STEM, down)
    assert (round(m), round(snap), which) == (1500, 100, "downstream")
    assert round(sc.join_measure(ch, STEM)[0]) == 400          # with no pointer: the nearer end


def test_a_channel_whose_downstream_end_lies_away_is_measured_where_it_leaves():
    # (T1172, owner 2026-11-26: "Fix the measure in this PR") the flow leaves the channel at x 1500, 100 m off the main
    # line; its other end lies 30 m off it: the channel LEAVES the main line there, and is measured there
    ch = [LineString([(400, 30), (1500, 100)])]
    down = LineString([(1500, 100), (1500, 200)])
    m, snap, _, which = sc.join_measure(ch, STEM, down)
    assert (round(m), round(snap), which) == (400, 30, "leaves")
    # at the limit it is still on the line; one metre beyond it, no end is: the downstream end, as before
    edge = [LineString([(400, sc.MAX_SNAP_M), (1500, 100)])]
    assert sc.join_measure(edge, STEM, down)[3] == "leaves"
    off = [LineString([(400, sc.MAX_SNAP_M + 1), (1500, 100)])]
    assert sc.join_measure(off, STEM, down)[3] == "downstream"
    # its downstream end on the line itself: that end, however near the other
    on = [LineString([(400, 10), (1500, 20)])]
    assert sc.join_measure(on, STEM, LineString([(1500, 20), (1500, 200)]))[:2] == (1500, 20.0)
    # an end given (it touches a water body of the river): always that end
    assert sc.join_measure(ch, STEM, down, end=ch[0].interpolate(1.0, normalized=True))[3] == "given"


def test_the_measure_runs_from_the_mouth_whichever_way_a_section_is_drawn():
    flipped = [(0, 1000, LineString([(1000, 0), (0, 0)])), STEM[1]]
    assert round(sc.join_measure([LineString([(250, 10), (250, 400)])], flipped)[0]) == 250


def test_a_main_line_of_one_section_has_its_mouth_at_its_first_vertex():
    # (T1172) no neighbour on it says which end is the mouth's: FWA's order does — the first vertex is measure 0.
    # The same channel, the main line drawn either way past it
    ch = [LineString([(400, 30), (600, 500)])]
    assert round(sc.join_measure(ch, STEM[:1])[0]) == 400                              # drawn from x 0
    assert round(sc.join_measure(ch, [(0, 1000, LineString([(1000, 0), (0, 0)]))])[0]) == 600   # drawn from x 1000
    # its channel near the other end: the measure follows the first vertex, not the channel
    far = [LineString([(900, 10), (950, 400)])]
    assert round(sc.join_measure(far, STEM[:1])[0]) == 900
    assert round(sc.join_measure(far, [(0, 1000, LineString([(1000, 0), (0, 0)]))])[0]) == 100


def test_a_one_section_main_line_in_pieces_is_read_in_its_drawn_order():
    from shapely.geometry import MultiLineString
    ch = [LineString([(400, 30), (600, 500)])]
    joined = MultiLineString([[(0, 0), (500, 0)], [(500, 0), (1000, 0)]])            # head to tail: one line
    assert round(sc.join_measure(ch, [(0, 1000, joined)])[0]) == 400
    back = MultiLineString([[(1000, 0), (500, 0)], [(500, 0), (0, 0)]])               # head to tail from x 1000
    assert round(sc.join_measure(ch, [(0, 1000, back)])[0]) == 600


def test_a_one_section_main_line_with_no_first_vertex_is_still_no_join():
    # pieces that do not join into one line (a gap; or two drawn against each other): no one first vertex — never
    # guessed, the loud NO JOIN as before
    from shapely.geometry import MultiLineString
    ch = [LineString([(400, 30), (600, 500)])]
    for g in (MultiLineString([[(0, 0), (300, 0)], [(500, 0), (1000, 0)]]),
              MultiLineString([[(0, 0), (500, 0)], [(1000, 0), (500, 0)]])):
        with pytest.raises(sc.SideChannelError, match="one section drawn in pieces"):
            sc.join_measure(ch, [(0, 1000, g)])


def _db(channel_far: float = 20.0, other_item_stem: bool = False):
    """Water 1 (a stream): main line sids 1, 2; one listed part (1) of side channel sid 3 the flow never joins; part 0 is
    the main line. With `other_item_stem` the main line belongs to water 2 instead (never measured against)."""
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);
        CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE section_span (sid INTEGER PRIMARY KEY, lo_m INTEGER, hi_m INTEGER, lo INTEGER, hi INTEGER,
                                   off_stem INTEGER, shape INTEGER);
        CREATE TABLE section_touch (a INTEGER, b INTEGER);
        CREATE TABLE section_down (sid INTEGER PRIMARY KEY, down_sid INTEGER, len INTEGER, lake INTEGER);
        CREATE TABLE part (ord INTEGER, part_ix INTEGER, key_ix INTEGER, tidal INTEGER);
        CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER, m INTEGER);
        CREATE TABLE part_inside (ord INTEGER, part_ix INTEGER, area TEXT, share REAL, how TEXT, cut INTEGER,
                                  PRIMARY KEY (ord, part_ix));
        CREATE TABLE span_end (eid INTEGER PRIMARY KEY, token TEXT);
        CREATE TABLE part_own_name (ord INTEGER, part_ix INTEGER, name TEXT, label TEXT);
        CREATE TABLE section_join (sid INTEGER PRIMARY KEY, m INTEGER, lo TEXT, how TEXT);
    """)
    db.executemany("INSERT INTO item VALUES (?,?,?,?)", [(1, "gnis:1", "One River", "stream"),
                                                        (2, "gnis:2", "Two River", "stream")])
    stem_ord = 2 if other_item_stem else 1
    db.executemany("INSERT INTO item_section VALUES (?,?)", [(stem_ord, 1), (stem_ord, 2), (1, 3)])
    db.executemany("INSERT INTO section_span VALUES (?,?,?,?,?,?,?)",
                   [(1, 0, 1000, 0, 0, 0, 0), (2, 1000, 2000, 0, 0, 0, 0), (3, None, None, 0, 0, 1, 0)])
    db.executemany("INSERT INTO part VALUES (?,?,?,?)", [(1, 0, 1, 0), (1, 1, 2, 0)])
    db.executemany("INSERT INTO part_section VALUES (?,?,?,?)", [(1, 1, 0, None), (1, 2, 0, None), (1, 3, 1, None)])
    geo = {1: STEM[0][2], 2: STEM[1][2], 3: LineString([(700, channel_far), (900, channel_far + 400)])}
    return db, geo


class _Cov:
    def filled(self, *_a):
        pass


def _how(db, sid):
    got = db.execute("SELECT how FROM section_join WHERE sid = ?", (sid,)).fetchone()
    return got[0] if got else None


def _write(db, geo, tmp_path, monkeypatch, unnamed=None, marks=None):
    monkeypatch.setattr(sc, "inside_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(sc, "own_name_rows", lambda *_a, **_k: [])
    log = []
    sc.write(db, geo, tmp_path, _Cov(), log=log.append, added={}, unnamed=unnamed or {}, marks=marks or {})
    assert dict(db.execute("SELECT sid, lo_m FROM section_span WHERE sid = 3")) in ({3: None}, {})   # never written
    return {**{s: None for (s,) in db.execute("SELECT sid FROM section_span")},
            **dict(db.execute("SELECT sid, m FROM section_join WHERE m IS NOT NULL"))}, log


# --- the bundle step: the order a main line is drawn in, checked (T1172) ------------------------------------------

def test_a_stem_section_drawn_backwards_stops_the_build():
    db, geo = _db()
    assert sc.check_stem_order(db, geo, {}, log=lambda *_: None)["below"] == 1           # sid 2 starts at sid 1
    geo[2] = LineString([(2000, 0), (1000, 0)])                                          # drawn from its top
    with pytest.raises(sc.SideChannelError, match=r"drawn backwards.*\n.*gnis:1 \(One River\) section 2: its first "
                                                  r"vertex is 1000 m from the stem section below it \[1\]"):
        sc.check_stem_order(db, geo, {}, log=lambda *_: None)


def _one_stem_db():
    """Water 1: a main line of ONE section (sid 1, 0–1000 m) drawn from x 0, and nothing else."""
    db, geo = _db()
    for t in ("item_section", "section_span", "part_section"):
        db.execute(f"DELETE FROM {t} WHERE sid IN (2, 3)")
    geo.pop(2)
    geo.pop(3)
    return db, geo


def test_a_one_section_main_line_is_held_to_its_junctions_fwa_measures():
    # a tributary joins at FWA measure 200 (800 m below the top): 200 m from the first vertex — drawn from its mouth
    db, geo = _one_stem_db()
    ok = {1: [(200.0, 800.0, 200.0, 0.0)]}
    assert sc.check_stem_order(db, geo, ok, log=lambda *_: None)["marks"] == 1
    geo[1] = LineString([(1000, 0), (0, 0)])         # the same line drawn from its top: the junction 800 m from its start
    with pytest.raises(sc.SideChannelError, match=r"gnis:1 \(One River\) section 1: drawn backwards — 1 junction"):
        sc.check_stem_order(db, geo, ok, log=lambda *_: None)


def test_one_backwards_junction_stops_it_however_many_read_forwards():
    # (review L2) two junctions read from the first vertex, one from the last: stopped — a majority rule could let a
    # turned section through on coincidental readings
    db, geo = _one_stem_db()
    mixed = {1: [(200.0, 800.0, 200.0, 0.0), (300.0, 700.0, 300.0, 0.0), (100.0, 900.0, 900.0, 0.0)]}
    with pytest.raises(sc.SideChannelError, match=r"drawn backwards — 1 junction\(s\) .* \(2 from its first\)"):
        sc.check_stem_order(db, geo, mixed, log=lambda *_: None)


def test_a_junction_that_cannot_tell_the_two_ways_apart_says_nothing():
    db, geo = _one_stem_db()
    # mid-line (both readings 500 m), or off its own measure either way: counted, never a stop or a pass
    n = sc.check_stem_order(db, geo, {1: [(500.0, 500.0, 500.0, 0.0), (200.0, 800.0, 450.0, 0.0)]},
                            log=lambda *_: None)
    assert (n["marks"], n["unchecked"], n["odd"]) == (0, 1, 1)
    assert sc.check_stem_order(db, geo, {}, log=lambda *_: None)["unchecked"] == 1      # no junction: counted


def test_the_junction_measures_are_read_from_the_graph():
    # a junction INTO the lowest stem piece, at FWA measure 200 on its blue line (down_m 0, up_m 1000); a piece with a
    # stem section below it, and an edge with no point, give nothing
    from types import SimpleNamespace as N
    db, _ = _db()
    g = N(nodes={"9:0": N(down_m=0.0, up_m=1000.0), "9:1000": N(down_m=1000.0, up_m=2000.0)},
          edges=[N(at_measure=200.0, x=200.0, y=0.0), N(at_measure=300.0, x=0.0, y=0.0),
                 N(at_measure=1500.0, x=1500.0, y=0.0)],
          up_adj={"9:0": [0, 1], "9:1000": [2]})
    assert sc.stem_marks(db, g, ["9:0", "9:1000", "x"]) == {1: [(200.0, 800.0, 200.0, 0.0)]}


def test_the_bundle_step_refuses_to_run_without_the_junction_measures(tmp_path):
    db, geo = _db()
    with pytest.raises(sc.SideChannelError, match="junction measures"):
        sc.write(db, geo, tmp_path, _Cov(), log=lambda *_: None, added={}, unnamed={})


def test_a_one_section_main_line_with_no_first_vertex_is_a_loud_no_join(tmp_path, monkeypatch):
    from shapely.geometry import MultiLineString
    db, geo = _one_stem_db()
    db.execute("INSERT INTO item_section VALUES (1, 3)")
    db.execute("INSERT INTO section_span VALUES (3, NULL, NULL, 0, 0, 1, 0)")
    db.execute("INSERT INTO part_section VALUES (1, 3, 1, NULL)")
    geo[1] = MultiLineString([[(0, 0), (300, 0)], [(500, 0), (1000, 0)]])
    geo[3] = LineString([(700, 20), (900, 420)])
    lo, log = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] is None and _how(db, 3) is None
    assert any("NO JOIN" in x and "one section drawn in pieces" in x and "One River" in x for x in log)
    assert any("1 in pieces that do not join" in x for x in log)


def test_a_backwards_stem_stops_the_bundle_step(tmp_path, monkeypatch):
    db, geo = _db()
    geo[2] = LineString([(2000, 0), (1000, 0)])
    with pytest.raises(sc.SideChannelError, match="drawn backwards"):
        _write(db, geo, tmp_path, monkeypatch)


def test_a_loose_channel_gets_its_join_written_on_its_sections(tmp_path, monkeypatch):
    db, geo = _db()
    lo, log = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] == 700 and _how(db, 3) == "join"
    assert any("joins at 0.7 km, snap 20 m from its nearer end" in x for x in log)


def test_beyond_the_snap_limit_it_is_said_at_its_nearest_point_not_joining(tmp_path, monkeypatch):
    # (coordinator's proposal) no join to say, but never no distance: the main line's nearest point, `how` 'nearest'
    db, geo = _db(channel_far=sc.MAX_SNAP_M + 1)
    lo, log = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] == 700 and _how(db, 3) == "nearest"
    assert any("NEAREST" in x and f"beyond {sc.MAX_SNAP_M:.0f} m" in x for x in log)
    db, geo = _db(channel_far=sc.MAX_SNAP_M - 1)
    lo, _ = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] == 700 and _how(db, 3) == "join"


def test_never_measured_against_another_waters_main_line(tmp_path, monkeypatch):
    lo, _ = _write(*_db(other_item_stem=True), tmp_path, monkeypatch)
    assert lo[3] is None


def test_an_added_stream_section_is_measured_against_the_water_it_was_added_to(tmp_path, monkeypatch):
    # an added stream (a negative blk: the low sids) is a section of its water like any other: its join is on THAT
    # water's main line, by the same rule and guard
    db, geo = _db()
    db.execute("UPDATE item_section SET sid = -5 WHERE sid = 3")
    for t in ("section_span", "part_section"):
        db.execute(f"UPDATE {t} SET sid = -5 WHERE sid = 3")
    geo[-5] = geo.pop(3)
    lo, _ = _write(db, geo, tmp_path, monkeypatch)
    assert lo[-5] == 700


def test_the_join_is_deterministic(tmp_path, monkeypatch):
    a, la = _write(*_db(), tmp_path, monkeypatch)
    b, lb = _write(*_db(), tmp_path, monkeypatch)
    assert (a, la) == (b, lb)


# --- the bundle step: the touch rule (T1140 proposal: "a consistent way to handle this") --------------------------

def _touch_db(body: str):
    """The channel (sid 3) lies 300 m off the main line, beyond the snap limit, its end at x 700 y 300 touching: `own` a
    polygon section of its own river (sid 4); `lake` a named lake (water 3) its main line runs through; `lake_off` the
    same lake set away from the main line; `other` another river's polygon (water 2); `none` nothing."""
    db, geo = _db(channel_far=300.0)
    db.execute("INSERT INTO span_end VALUES (1, 'mouth')")
    db.execute("UPDATE section_span SET lo = 1, hi = 1")
    db.execute("INSERT INTO item VALUES (3, 'gnis:3', 'Olsen Lake', 'lake')")
    if body == "own":
        db.execute("INSERT INTO item_section VALUES (1, 4)")
        db.execute("INSERT INTO section_span VALUES (4, 0, 1000, 1, 1, 0, 1)")
        geo[4] = Polygon([(600, -10), (800, -10), (800, 300), (600, 300)])
    elif body in ("lake", "lake_off"):
        db.execute("INSERT INTO item_section VALUES (3, 5)")
        # `lake_off`: its box spans the main line's, but the lake meets the line's level only past the river's top (x 2100)
        geo[5] = Polygon([(600, -10), (800, -10), (800, 300), (600, 300)]) if body == "lake" else \
            Polygon([(2100, -50), (2200, -50), (2200, 300), (600, 300), (600, 250), (2100, 250)])
    elif body == "other":
        db.execute("INSERT INTO item_section VALUES (2, 6)")
        db.execute("INSERT INTO section_span VALUES (6, 0, 1000, 1, 1, 0, 1)")
        geo[6] = Polygon([(600, -10), (800, -10), (800, 300), (600, 300)])
    return db, geo


def test_a_channel_touching_an_unnamed_lake_on_its_main_line_joins_there(tmp_path, monkeypatch):
    db, geo = _touch_db("none")
    lakes = {"lake:1": Polygon([(600, -10), (800, -10), (800, 300), (600, 300)])}   # handed in, never kept in a global
    lo, log = _write(db, geo, tmp_path, monkeypatch, unnamed=lakes)
    assert lo[3] == 700 and _how(db, 3) == "join" and any("unnamed lake on its main line lake:1" in x for x in log)
    db, geo = _touch_db("none")                                   # an unnamed lake away from the main line: nothing
    lakes = {"lake:2": Polygon([(2100, -50), (2200, -50), (2200, 300), (600, 300), (600, 250), (2100, 250)])}
    lo, _ = _write(db, geo, tmp_path, monkeypatch, unnamed=lakes)
    assert _how(db, 3) == "nearest"


def test_a_polygon_end_never_breaks_the_tie():
    poly = box(0, 0, 2, 2)
    assert sc.touched([poly], [(4, box(1, 1, 3, 3)), (2, box(1, 1, 3, 3))], [])[:2] == ("own", 2)


def _lo_token(db, sid):
    got = db.execute("SELECT lo FROM section_join WHERE sid = ? AND lo IS NOT NULL", (sid,)).fetchone()
    return got[0] if got else db.execute("SELECT e.token FROM section_span s JOIN span_end e ON e.eid = s.lo "
                                         "WHERE s.sid = ?", (sid,)).fetchone()[0]


def test_a_channel_touching_its_rivers_own_polygon_joins_there_whatever_the_distance(tmp_path, monkeypatch):
    db, geo = _touch_db("own")
    lo, log = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] == 700 and _how(db, 3) == "join"              # 300 m off the centre line, beyond the 50 m limit
    assert any("touching its own polygon 4" in x for x in log)


def test_two_ends_touching_two_bodies_pick_one_by_rule():
    # both ends touch (no flow pointer tells them apart): the lowest id wins, ties on the point's place, never on a Point
    from shapely.geometry import Point
    e1, e2 = Point(0, 0), Point(5, 0)
    own = [(7, box(-1, -1, 1, 1)), (4, box(4, -1, 6, 1))]
    assert sc.touched([e1, e2], own, [])[:2] == ("own", 4)
    assert sc.touched([e1, e2], [(4, box(-1, -1, 6, 1))], [])[2] == e1
    assert sc.touched([e1, e2], own, [("gnis:3", box(4, -1, 6, 1))])[:2] == ("lake", "gnis:3")


def test_a_channel_touching_nothing_keeps_the_snap_limit(tmp_path, monkeypatch):
    db, geo = _touch_db("none")
    lo, _ = _write(db, geo, tmp_path, monkeypatch)
    assert _how(db, 3) == "nearest"


def test_another_waters_polygon_never_counts(tmp_path, monkeypatch):
    db, geo = _touch_db("other")
    lo, _ = _write(db, geo, tmp_path, monkeypatch)
    assert _how(db, 3) == "nearest"


def test_a_channel_into_a_named_lake_on_its_main_line_is_named_by_it(tmp_path, monkeypatch):
    db, geo = _touch_db("lake")
    db.execute("INSERT INTO section_down VALUES (3, 5, 0, 1)")          # its flow pointer: into the lake (Theodosia)
    lo, log = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] is None and _lo_token(db, 3) == "lake_inlet:gnis:3"     # rule A: "side channel into Olsen Lake"
    assert any("flows into gnis:3" in x for x in log)


def test_unnamed_lakes_handed_to_one_write_are_not_remembered_for_the_next(tmp_path, monkeypatch):
    # T1140: the lakes were once kept in a module dict keyed by id(db); a later db reusing a freed id inherited them
    assert not hasattr(sys.modules[__name__], "UNNAMED"), "unnamed lakes must be passed to _write, never kept in a global"
    db, geo = _touch_db("none")
    _write(db, geo, tmp_path, monkeypatch, unnamed={"lake:1": Polygon([(600, -10), (800, -10), (800, 300), (600, 300)])})
    assert _how(db, 3) == "join"
    db, geo = _touch_db("none")
    _write(db, geo, tmp_path, monkeypatch)
    assert _how(db, 3) == "nearest"


def test_a_lake_with_no_flow_pointer_to_say_the_direction_never_names_the_channel(tmp_path, monkeypatch):
    # Robertson Creek: its channel touches Boot Lagoon where it LEAVES it; with no pointer the direction is not known
    db, geo = _touch_db("lake")
    _write(db, geo, tmp_path, monkeypatch)
    assert _lo_token(db, 3) == "mouth" and _how(db, 3) == "nearest"
    db, geo = _touch_db("lake")                          # the pointer leads AWAY from the lake: its far end is downstream
    geo[9] = LineString([(900, 700), (900, 900)])
    db.execute("INSERT INTO section_down VALUES (3, 9, 0, 0)")
    _write(db, geo, tmp_path, monkeypatch)
    assert _lo_token(db, 3) == "mouth"


def test_a_lake_the_main_line_does_not_run_through_never_counts(tmp_path, monkeypatch):
    db, geo = _touch_db("lake_off")
    db.execute("INSERT INTO section_down VALUES (3, 5, 0, 1)")          # into it, but the river does not run through it
    lo, _ = _write(db, geo, tmp_path, monkeypatch)
    assert _how(db, 3) == "nearest" and _lo_token(db, 3) == "mouth"


# --- the bundle step: inside an area -------------------------------------------------------------------------------

def _catalog(tmp_path, areas):
    """The atlas's area catalogue: areas [(name, kind, shape)] cut at, or [(name, kind, shape, cut)]."""
    import geopandas as gpd
    gpd.GeoDataFrame({"area_id": [f"a{i}" for i in range(len(areas))], "name": [a[0] for a in areas],
                      "kind": [a[1] for a in areas], "cut": [bool(a[3]) if len(a) > 3 else True for a in areas]},
                     geometry=[a[2] for a in areas], crs="EPSG:3005").to_file(tmp_path / "area_catalog.gpkg",
                                                                              layer="areas")


def _inside(tmp_path, shapes, areas, iid="gnis:1", log=None):
    """One water (`iid`), one listed part of side channels only (sids 10…), shapes {sid: geometry}."""
    db, _ = _db()
    db.execute("UPDATE item SET item_id = ? WHERE ord = 1", (iid,))
    db.execute("DELETE FROM part_section WHERE part_ix = 1")
    db.executemany("INSERT INTO part_section VALUES (1, ?, 1, NULL)", [(s,) for s in shapes])
    db.executemany("INSERT INTO section_span VALUES (?, NULL, NULL, 0, 0, 1, ?)",
                   [(s, int(g.geom_type == "Polygon")) for s, g in shapes.items()])
    span = {s: (a, b, off, shape) for s, a, b, off, shape in db.execute(
        "SELECT sid, lo_m, hi_m, off_stem, shape FROM section_span")}
    _catalog(tmp_path, areas)
    return sc.inside_rows(db, {**{1: STEM[0][2], 2: STEM[1][2]}, **shapes}, tmp_path, span,
                          log=(log.append if log is not None else lambda *_: None))


RESERVE = ("BAYNES ISLAND ECOLOGICAL RESERVE", "ecological_reserves", box(0, 0, 100, 100))


def test_a_part_wholly_inside_one_area_is_said_inside_it(tmp_path):
    assert _inside(tmp_path, {10: LineString([(10, 10), (90, 90)])}, [RESERVE]) == \
        [(1, 1, "BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "inside", 1)]


def test_lines_by_length_at_95_percent(tmp_path):
    # 95 m of 100 inside: inside; 94 m: not (the Skeena's 71.7 % along the reserve's edge, and anything short of 95 %)
    assert _inside(tmp_path, {10: LineString([(5, 50), (105, 50)])}, [RESERVE])[0][3] == 0.95
    assert _inside(tmp_path, {10: LineString([(6, 50), (106, 50)])}, [RESERVE]) == []


def test_polygons_by_area_and_a_straddling_polygon_is_at_the_area(tmp_path):
    # the Okanagan River Oxbows: its lines all inside, its lake polygon 31.9 % inside — "at" the reserve, not inside it
    lines = {10: LineString([(10, 10), (90, 10)])}
    half = {11: Polygon([(50, 20), (150, 20), (150, 80), (50, 80)])}                     # 50 % of its area inside
    assert _inside(tmp_path, {**lines, **half}, [RESERVE]) == [(1, 1, "BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "at", 1)]
    whole = {11: Polygon([(20, 20), (80, 20), (80, 80), (20, 80)])}
    assert _inside(tmp_path, {**lines, **whole}, [RESERVE])[0][2:] == \
        ("BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "inside", 1)


def test_never_at_an_area_its_lines_leave_or_its_polygon_misses(tmp_path):
    half = {11: Polygon([(50, 20), (150, 20), (150, 80), (50, 80)])}
    out_lines = {10: LineString([(50, 10), (150, 10)])}                                    # its lines half outside
    assert _inside(tmp_path, {**out_lines, **half}, [RESERVE]) == []
    lines = {10: LineString([(10, 10), (90, 10)])}
    away = {11: Polygon([(300, 20), (350, 20), (350, 80), (300, 80)])}                    # a polygon wholly outside
    assert _inside(tmp_path, {**lines, **away}, [RESERVE]) == []
    assert _inside(tmp_path, half, [RESERVE]) == []                                        # no line: never "at"


def test_never_a_region_or_two_areas(tmp_path):
    ch = {10: LineString([(10, 10), (90, 90)])}
    assert _inside(tmp_path, ch, [("Region 2", "region", box(-10, -10, 200, 200))]) == []
    log = []
    assert _inside(tmp_path, ch, [RESERVE, ("PARK", "parks", box(-10, -10, 200, 200))], log=log) == []   # two cut areas
    assert any("INSIDE two areas: gnis:1 part 1" in x for x in log)
    two = [("A PARK", "park", box(0, 0, 100, 100), False), ("B WMA", "wma", box(-10, -10, 200, 200), False)]
    assert _inside(tmp_path, ch, two) == []                                     # two membership-only areas, no cut one


def test_an_unmeasured_section_means_not_inside(tmp_path):
    db, _ = _db()
    _catalog(tmp_path, [RESERVE])
    db.execute("INSERT INTO part_section VALUES (1, 11, 1, NULL)")
    db.execute("INSERT INTO section_span VALUES (11, NULL, NULL, 0, 0, 1, 0)")
    span = {s: (a, b, off, shape) for s, a, b, off, shape in db.execute(
        "SELECT sid, lo_m, hi_m, off_stem, shape FROM section_span")}
    geo = {1: STEM[0][2], 2: STEM[1][2], 3: LineString([(10, 10), (90, 90)])}          # sid 11 has no shape
    assert sc.inside_rows(db, geo, tmp_path, span) == []


# (T1172, owner 2026-11-26: "Yes, but name it by the area"; "The area shows in the app SUBTITLE, universally") a
# MEMBERSHIP-ONLY area (`cut` 0) is measured for every part, reviewed or not (the answers step gates the name); never
# Indigenous land
WMA = ("CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "wma", box(0, 0, 100, 100), False)
CH = {10: LineString([(10, 10), (90, 90)])}


def test_a_membership_only_area_is_measured_for_every_part_reviewed_or_not(tmp_path):
    for iid in ("gnis:2123", "gnis:1"):                                     # a reviewed pair, and one nobody looked at
        assert _inside(tmp_path, CH, [WMA], iid=iid) == [(1, 1, "CRESTON VALLEY WILDLIFE MANAGEMENT AREA", 1.0, "inside", 0)]
    park = ("STRATHCONA - WESTMIN PARK", "park", box(0, 0, 100, 100), False)
    for iid in ("gnis:21681", "gnis:1"):
        assert _inside(tmp_path, CH, [park], iid=iid) == [(1, 1, "STRATHCONA - WESTMIN PARK", 1.0, "inside", 0)]


def test_thelwood_creek_is_said_inside_its_park():
    W = _water([MAIN, LOOSE], iid="gnis:21681", name="Thelwood Creek")
    cr = {("gnis:21681", "inside", 1): ("STRATHCONA - WESTMIN PARK", "inside", False)}
    assert _said(W, cr)[1] == "Thelwood Creek inside Strathcona - Westmin Park"      # the park's own spaced hyphen
    assert _sub(W, cr)[1] == "Side channel inside Strathcona - Westmin Park"


def test_two_rows_of_one_area_are_one_area(tmp_path):
    # (review L3) an area the catalogue keeps in two rows of one name is ONE area, said by its best row; two names stay two
    a = ("CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "wma", box(0, 0, 100, 100), False)
    b = ("CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "wma", box(-10, -10, 200, 200), False)
    assert _inside(tmp_path, CH, [a, b], iid="gnis:2123") == \
        [(1, 1, "CRESTON VALLEY WILDLIFE MANAGEMENT AREA", 1.0, "inside", 0)]
    other = ("SOME OTHER PARK", "park", box(-10, -10, 200, 200), False)
    assert _inside(tmp_path, CH, [a, b, other], iid="gnis:2123") == []          # two names: two areas, no row


def test_indigenous_land_and_other_membership_kinds_never_name_a_part(tmp_path, monkeypatch):
    from pipeline.deliver.answers.display import part as P
    for kind in ("indigenous_land", "permit_land_access", "watershed"):
        monkeypatch.setattr(P, "REVIEWED_INSIDE", P.REVIEWED_INSIDE | {("gnis:1", "HAIDA NATION", "inside")})
        assert _inside(tmp_path, CH, [("HAIDA NATION", kind, box(0, 0, 100, 100), False)]) == []   # never, even reviewed


def test_a_membership_area_never_moves_a_cut_areas_name(tmp_path):
    # Baynes Island inside a park the atlas does not cut at: still said inside the reserve (the atlas's own line first),
    # never dropped for two; the reserve unreviewed stops the build at the answers step, as before
    park = ("SOME PARK", "park", box(-10, -10, 200, 200), False)
    for iid in ("gnis:25671", "gnis:1"):
        assert _inside(tmp_path, CH, [RESERVE, park], iid=iid) == \
            [(1, 1, "BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "inside", 1)]


def test_a_membership_only_area_keeps_the_95_percent_rule(tmp_path):
    wma = ("CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "wma", box(0, 0, 100, 100), False)
    assert _inside(tmp_path, {10: LineString([(5, 50), (105, 50)])}, [wma], iid="gnis:2123")[0][3] == 0.95
    assert _inside(tmp_path, {10: LineString([(6, 50), (106, 50)])}, [wma], iid="gnis:2123") == []


def _old_kootenay(tmp_path):
    """gnis:2123 as the clean clone measures it (2026-11-26): main line 317222 (here sid 1), ONE section of 11,375 m,
    drawn from its mouth (p0, FWA measure 0) to where it leaves the Kootenay; its flow pointer (the gauges' route,
    `section_down`) leads into channel 1 at 588 m — no end at all. Two side channels, one part, neither coming back:
    channel 1 (317223 + 317226, here 3 + 6) leaves at 588 m and flows into Leach Lake (water 3, sid 5), which the main
    line never touches; channel 2 (317225, here 4, 171 m) leaves at 379 m and drains to a polygon no water owns. The
    junctions of the main line's tributaries at their FWA measures (201, 2,076 m, and the Kootenay at its top). The
    Creston Valley WMA (membership-only) holds the channels; its edge runs 3.7 m off the main line."""
    db, geo = _db()
    for t in ("item_section", "section_span", "part_section"):
        db.execute(f"DELETE FROM {t} WHERE sid IN (2, 3)")
    db.execute("UPDATE item SET item_id = 'gnis:2123', name = 'Old Kootenay River Channel' WHERE ord = 1")
    db.execute("UPDATE section_span SET hi_m = 11375 WHERE sid = 1")
    db.execute("INSERT INTO item VALUES (3, 'gnis:3', 'Leach Lake', 'lake')")
    db.executemany("INSERT INTO item_section VALUES (?, ?)", [(1, 3), (1, 4), (1, 6), (3, 5)])
    db.executemany("INSERT INTO section_span VALUES (?, NULL, NULL, 0, 0, 1, 0)", [(3,), (4,), (6,)])
    db.executemany("INSERT INTO part_section VALUES (1, ?, 1, NULL)", [(3,), (4,), (6,)])
    db.executemany("INSERT INTO section_touch VALUES (?, ?)", [(1, 3), (1, 4), (3, 6)])
    db.executemany("INSERT INTO section_down VALUES (?, ?, 0, ?)", [(1, 3, 0), (3, 5, 1), (6, 98, 1), (4, 99, 0)])
    geo.pop(2)
    geo[1] = LineString([(0, 0), (11375, 0)])                               # p0, the mouth, at x 0
    geo[3] = LineString([(588, 0), (588, -1200)])
    geo[6] = LineString([(588, -1200), (520, -1763)])
    geo[5] = Polygon([(400, -1763), (640, -1763), (640, -2000), (400, -2000)])           # Leach Lake
    geo[4] = LineString([(379, 0), (450, -155)])
    marks = {1: [(201.2, 11173.4, 201.2, 0.0), (2076.4, 9298.2, 2076.4, 0.0), (11374.6, 0.0, 11374.6, 0.0)]}
    _catalog(tmp_path, [("CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "wma", box(-500, -5000, 12000, -3.7), False),
                        ("4", "region", box(-1000, -9000, 13000, 9000))])
    return db, geo, marks


def test_the_old_kootenay_river_channel_is_measured_and_named_by_its_area(tmp_path, monkeypatch):
    db, geo, marks = _old_kootenay(tmp_path)
    monkeypatch.setattr(sc, "own_name_rows", lambda *_a, **_k: [])
    log = []
    sc.write(db, geo, tmp_path, _Cov(), log=log.append, added={}, unnamed={}, marks=marks)
    assert not any("NO JOIN" in x for x in log)
    joins = dict(db.execute("SELECT sid, m FROM section_join"))
    # both channels where they leave the main line: channel 2 at 379 m from p0; channel 1 at 588 m — its downstream end
    # lies 1,763 m away in Leach Lake (never the main line's nearest point to that end: 520 m, "nearest")
    assert (joins[4], _how(db, 4)) == (379, "join") and (joins[3], joins[6], _how(db, 3)) == (588, 588, "join")
    assert any("sections 3…(2): joins at 0.6 km, snap 0 m where it leaves the main line" in x for x in log)
    assert any("1 with none below agree with their junctions' FWA measures" in x for x in log)
    assert db.execute("SELECT ord, part_ix, area, how, cut FROM part_inside").fetchall() == \
        [(1, 1, "CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "inside", 0)]
    # the answers step: the area wins over the joins (as the Okanagan River Oxbows' reserve does)
    from pipeline.deliver.answers.display.builders import _crossings
    p = tmp_path / "b.sqlite"
    db.executescript("CREATE TABLE crossing_near (ord INTEGER, token TEXT, km REAL, near TEXT);"
                     "CREATE TABLE part_near (ord INTEGER, part_ix INTEGER, near TEXT);")
    disk = sqlite3.connect(p)
    db.backup(disk)
    disk.close()
    cr = _crossings(str(p))
    W = _water([MAIN, [_run("source", "mouth", joins[3] / 1000, joins[3] / 1000, branch=True),
                       _run("source", "mouth", joins[4] / 1000, joins[4] / 1000, branch=True)]],
               iid="gnis:2123", name="Old Kootenay River Channel")
    W.doc["waters"]["gnis:3"] = {"name": "Leach Lake", "kind": "lake", "parts": []}
    # (T1172, owner 2026-11-26: "inside Creston Valley WMA") the area's short form
    assert _said(W, cr)[1] == "Old Kootenay River Channel inside Creston Valley WMA"
    # its subtitle says the area too, in the name's words; the one such part of its water says no distance
    assert _sub(W, cr)[1] == "2 side channels inside Creston Valley WMA"


def test_the_old_kootenay_river_channel_unreviewed_goes_to_the_next_rung_but_its_subtitle_says_the_area(tmp_path,
                                                                                                        monkeypatch):
    # (T1172) without its REVIEWED_INSIDE entry the WMA is still measured; the NAME takes the next rung (no stop for a
    # membership-only area), the SUBTITLE says the area all the same, and the part is listed
    from pipeline.deliver.answers.display import part as P
    from pipeline.deliver.answers.display.builders import _crossings, inside_candidates
    monkeypatch.setattr(P, "REVIEWED_INSIDE", P.REVIEWED_INSIDE - {("gnis:2123", "CRESTON VALLEY WILDLIFE MANAGEMENT AREA",
                                                                     "inside")})
    db, geo, marks = _old_kootenay(tmp_path)
    monkeypatch.setattr(sc, "own_name_rows", lambda *_a, **_k: [])
    sc.write(db, geo, tmp_path, _Cov(), log=lambda *_: None, added={}, unnamed={}, marks=marks)
    assert db.execute("SELECT area, cut FROM part_inside").fetchall() == [("CRESTON VALLEY WILDLIFE MANAGEMENT AREA", 0)]
    p = tmp_path / "b.sqlite"
    db.executescript("CREATE TABLE crossing_near (ord INTEGER, token TEXT, km REAL, near TEXT);"
                     "CREATE TABLE part_near (ord INTEGER, part_ix INTEGER, near TEXT);")
    disk = sqlite3.connect(p)
    db.backup(disk)
    disk.close()
    cr = _crossings(str(p))
    W = _water([MAIN, [_run("source", "mouth", 0.379, 0.379, branch=True), _run("source", "mouth", 0.588, 0.588, branch=True)]],
               iid="gnis:2123", name="Old Kootenay River Channel")
    W.doc["waters"]["gnis:3"] = {"name": "Leach Lake", "kind": "lake", "parts": []}
    cr[("gnis:2123", "flows_into", 1)] = "lake_inlet:gnis:3"           # (synthetic: the lower channel's flow, into the lake)
    assert _said(W, cr)[1] == "Old Kootenay River Channel side channels: one into Leach Lake; one other"   # the next rung
    assert _sub(W, cr)[1] == "2 side channels inside Creston Valley WMA"
    assert inside_candidates("gnis:2123", W.w, cr) == ["gnis:2123 part 1 inside 'CRESTON VALLEY WILDLIFE MANAGEMENT AREA'"]


# --- the answers step: the names ---------------------------------------------------------------------------------

BAYNES = "area:BAYNES ISLAND ECOLOGICAL RESERVE"
SPLITS = {BAYNES: {"name": "Baynes Island Ecological Reserve boundary", "kind": "area"},
          "squamish__tidal": {"name": "Tidal boundary", "km": 3.0, "water_id": "gnis:1"},
          "area:SKEENA RIVER ECOLOGICAL RESERVE": {"name": "Skeena River Ecological Reserve boundary", "kind": "area"},
          "bc_border": {"name": "B.C. border", "kind": "border"}}


def _run(frm, to, km_from, km_to, branch=False):
    return {"from": frm, "to": to, "km_from": km_from, "km_to": km_to, **({"branch": True} if branch else {})}


def _water(parts, iid="gnis:25671", name="Squamish River"):
    w = {"name": name, "parts": [{"ruleset": i, "sections": 10 - i, "grp": i, "run": 0, "runs": runs}
                                 for i, runs in enumerate(parts)]}
    waters = {iid: w, "gnis:9": {"name": "Exchamsiks River", "parts": []},
              "gnis:8": {"name": "Olsen Lake", "kind": "lake", "parts": []}}
    doc = {"waters": waters, "rulesets": [{} for _ in parts], "rules": {}, "splits": SPLITS}
    return Water(iid, w, doc)


def _said(W, crossings=None):
    out = []
    for n in _names(W, crossings or {}):
        if n["lo"] is None:
            out.append(None)
        elif n["lo"] == n["hi"] and n["lo"].startswith("near:"):
            w = end_words(W.doc, n["lo"], "from")                 # as the app composes it (`spanName`)
            from pipeline.deliver.answers.display.name_check import near_name
            out.append(near_name(W.w["name"], w))
        else:
            out.append(f"{end_words(W.doc, n['lo'], 'to')} → {end_words(W.doc, n['hi'], 'from')}")
    return out


def _sub(W, crossings=None):
    """Each part's subtitle (`runs`, what the app's `subtitleOf` shows), as `produce_parts` makes it (`side_facts`)."""
    from pipeline.deliver.answers.display.builders import side_facts
    from pipeline.deliver.answers.display.part import runs_label, water_ends_km
    w = side_facts(W.id, W.w, crossings or {})
    return [runs_label(W.doc, p, water_ends_km(w)) for p in Water(W.id, w, W.doc).parts]


MAIN = [_run("squamish__tidal", "mouth", 3.0, 0.0), _run("source", "squamish__tidal", 90.0, 3.0)]
LOOSE = [_run("source", "mouth", 11.6, 11.6, branch=True)]


def test_the_squamish_channels_inside_baynes_island_are_said_inside_it():
    W = _water([MAIN, LOOSE])
    cr = {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside", True)}
    got = _names(W, cr)
    assert got[1] == {"lo": INSIDE + BAYNES, "hi": INSIDE + BAYNES}
    assert _said(W, cr)[1] == "Squamish River inside Baynes Island Ecological Reserve"
    assert _sub(W, cr)[1] == "Side channel inside Baynes Island Ecological Reserve"


def test_an_inside_pair_no_reviewer_looked_at_stops_the_build():
    W = _water([MAIN, LOOSE], iid="gnis:777")
    with pytest.raises(AnswersError, match="no reviewer has looked at"):
        _names(W, {("gnis:777", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside", True)})
    W = _water([MAIN, LOOSE])                              # a reviewed water and area, but said another way
    with pytest.raises(AnswersError, match="no reviewer has looked at"):
        _names(W, {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "at", True)})
    # (T1172) an unreviewed MEMBERSHIP-ONLY area never stops it: the name takes its next rung, the subtitle says it
    W = _water([MAIN, LOOSE], iid="gnis:777")
    cr = {("gnis:777", "inside", 1): ("SOME PARK", "inside", False)}
    assert _names(W, cr)[1] == {"lo": None, "hi": None}
    assert _sub(W, cr)[1] == "Side channel inside Some Park"


def test_the_oxbows_are_said_at_hayne_s_lease():
    hl = "area:HAYNE'S LEASE ECOLOGICAL RESERVE"
    SPLITS[hl] = {"name": "Hayne's Lease Ecological Reserve boundary", "kind": "area"}
    try:
        W = _water([MAIN, [_run("source", hl, None, None, branch=True), _run("source", "mouth", None, None, branch=True)]],
                   iid="wsc:300-432687-459615", name="Okanagan River Oxbows")
        cr = {("wsc:300-432687-459615", "inside", 1): ("HAYNE'S LEASE ECOLOGICAL RESERVE", "at", True)}
        assert _names(W, cr)[1] == {"lo": AT + hl, "hi": AT + hl}
        assert _said(W, cr)[1] == "Okanagan River Oxbows at Hayne's Lease Ecological Reserve"
        assert _sub(W, cr)[1] == "2 side channels at Hayne's Lease Ecological Reserve"
    finally:
        del SPLITS[hl]


def test_a_part_said_by_its_channels_named_ends():
    # the Skeena's p4: "Side channel from the Exchamsiks River confluence up to the Skeena River Ecological Reserve
    # boundary" — never its own mouth or source
    W = _water([MAIN, [_run("area:SKEENA RIVER ECOLOGICAL RESERVE", "confluence:gnis:9", 63.2, 63.2, branch=True)]],
               iid="gnis:2936", name="Skeena River")
    n = _names(W, {})[1]
    assert n["lo"] == n["hi"] and n["lo"].startswith(SIDE)
    said = _said(W)[1]
    assert said.startswith("Skeena River side channel from ") and "Exchamsiks" in said and \
        "up to Skeena River Ecological Reserve" in said


def test_a_channel_that_names_only_its_own_mouth_takes_the_water_its_flow_reaches():
    # Theodosia River p1: its run says source → mouth; its flow pointer leaves the river for Olsen Lake
    p = {"runs": [_run("source", "mouth", None, None, branch=True)]}
    assert side_channels(p) is None
    assert side_channels(p, "lake_inlet:gnis:8") == [["lake_inlet:gnis:8", "", None]]
    W = _water([MAIN, [_run("source", "mouth", None, None, branch=True)]], iid="gnis:21687", name="Theodosia River")
    assert _said(W, {("gnis:21687", "flows_into", 1): "lake_inlet:gnis:8"})[1] == \
        "Theodosia River side channel into Olsen Lake"


def test_one_named_channel_and_one_bare_one_say_the_bare_ones_join():
    # Snehumption Creek p1: one channel from the B.C. border, the other joining at its derived km
    chans = side_channels({"runs": [_run("bc_border", "mouth", 2.0, 2.0, branch=True),
                                    _run("source", "mouth", 3.6, 3.6, branch=True)]})
    assert chans == [["", "bc_border", 2.0], ["", "", 3.6]]
    W = _water([MAIN, [_run("bc_border", "mouth", 2.0, 2.0, branch=True), _run("source", "mouth", 3.6, 3.6, branch=True)]],
               iid="gnis:27390", name="Snehumption Creek")
    # (T1172, owner 2026-11-26: "Distance only to tell parts apart") the one such part of its water: the bare channel is
    # counted, never placed
    assert _said(W)[1] == "Snehumption Creek side channels: one up to the B.C. border; one other"
    assert _sub(W)[1] == "2 side channels"


def test_a_channel_with_no_named_end_and_no_distance_is_counted_in_a_named_part():
    # the Okanagan River Oxbows, were it not "at" its reserve: one channel to the boundary, one with no join — (T1172)
    # no distance is said on the one such part of a water, so none is needed (before: a stop)
    hl = "area:HAYNE'S LEASE ECOLOGICAL RESERVE"
    SPLITS[hl] = {"name": "Hayne's Lease Ecological Reserve boundary", "kind": "area"}
    try:
        W = _water([MAIN, [_run("source", hl, None, None, branch=True), _run("source", "mouth", None, None, branch=True)]],
                   iid="wsc:300-432687-459615", name="Okanagan River Oxbows")
        assert _said(W)[1] == "Okanagan River Oxbows side channels: one from Hayne's Lease Ecological Reserve boundary; " \
                              "one other"
    finally:
        del SPLITS[hl]


def test_a_part_said_by_its_label_needs_no_distance_when_it_is_its_waters_only_one():
    # (T1172, owner 2026-11-26: "A unique one is plain") before: "never a part named without its distance", a stop
    for km in (None, 0.7):
        W = _water([MAIN, [_run("source", "mouth", km, km, branch=True)]], iid="gnis:23366", name="Noons Creek")
        assert _names(W, {})[1] == {"lo": None, "hi": None}       # said by its label
        assert _sub(W)[1] == "Side channel"


def test_several_parts_with_no_main_stem_count_only_parts_with_no_ends():
    # two stemless parts → each said near what it lies nearest (`part_near`); an inside part is one of them
    W = _water([MAIN, LOOSE, [_run("source", "mouth", 20.0, 20.0, branch=True)]])
    with pytest.raises(AnswersError, match="one of several parts with no main stem"):
        _names(W, {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside", True)})


def test_a_reviewed_part_no_longer_said_inside_stops_the_build():
    sq = "near:inside:area:BAYNES ISLAND ECOLOGICAL RESERVE"
    reviewed_still_said({"gnis:25671": {"parts": [None, {"lo": sq, "hi": sq}]}})          # still said: passes
    with pytest.raises(AnswersError, match="no longer said inside"):
        reviewed_still_said({"gnis:25671": {"parts": [None, {"lo": None, "hi": None}]}})
    with pytest.raises(AnswersError, match="no longer said inside"):                        # said, but "at" not "inside"
        reviewed_still_said({"gnis:25671": {"parts": [None, {"lo": "near:at:area:BAYNES ISLAND ECOLOGICAL RESERVE"}]}})
    reviewed_still_said({"gnis:1": {"parts": [None]}})                                     # a water not built: no check


def test_the_flow_pointer_names_the_one_water_a_part_of_side_channels_flows_into():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, kind TEXT);
        CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER);
        CREATE TABLE section_span (sid INTEGER PRIMARY KEY, off_stem INTEGER);
        CREATE TABLE section_down (sid INTEGER PRIMARY KEY, down_sid INTEGER);
    """)
    db.executemany("INSERT INTO item VALUES (?,?,?)", [(1, "gnis:21687", "stream"), (2, "gnis:8", "lake"),
                                                      (3, "gnis:9", "stream")])
    # Theodosia: stem 1; part 1 = channel 10 → 11 → Olsen Lake (20); part 2 = channel 12 → stream 30 and lake 20 (two)
    # part 3 = stem 14 and channel 15 → the lake: a part WITH a main stem is said by its ends, never this
    db.executemany("INSERT INTO item_section VALUES (?,?)", [(1, 1), (1, 10), (1, 11), (1, 12), (1, 13), (1, 14), (1, 15),
                                                             (2, 20), (3, 30)])
    db.executemany("INSERT INTO part_section VALUES (1, ?, ?)", [(1, 0), (10, 1), (11, 1), (12, 2), (13, 2), (14, 3), (15, 3)])
    db.executemany("INSERT INTO section_span VALUES (?, ?)", [(1, 0), (10, 1), (11, 1), (12, 1), (13, 1), (14, 0), (15, 1)])
    db.executemany("INSERT INTO section_down VALUES (?, ?)", [(10, 11), (11, 20), (12, 30), (13, 20), (1, 20), (15, 20)])
    got = _flows_into(db)
    assert got == {("gnis:21687", "flows_into", 1): "lake_inlet:gnis:8"}       # part 2 reaches two waters: none; part 0
    # has a main stem: none


# --- a part of added city lines with its own name (T1140 PROPOSAL, coordinator 2026-11-25) --------------------------

def _added_db(rows):
    """Noons Creek (water 1): part 0 its main line (FWA blk 360000001); part 1 the sections `rows` [(sid, blk)]."""
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);
        CREATE TABLE section_measure (sid INTEGER PRIMARY KEY, blk INTEGER, from_m INTEGER, to_m INTEGER);
        CREATE TABLE name_source (item_id TEXT, name TEXT, src TEXT, label TEXT);
        CREATE TABLE part (ord INTEGER, part_ix INTEGER, key_ix INTEGER, tidal INTEGER);
        CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER, m INTEGER);
    """)
    db.execute("INSERT INTO item VALUES (1, 'gnis:23366', 'Noons Creek', 'stream')")
    db.executemany("INSERT INTO name_source VALUES ('gnis:23366', ?, 'municipal', 'City of Port Moody ESA streams layer')",
                   [("Turner Creek",), ("West Noons Creek",)])
    db.executemany("INSERT INTO part VALUES (1, ?, ?, 0)", [(0, 1), (1, 2)])
    db.execute("INSERT INTO section_measure VALUES (1, 360000001, 0, 900)")
    db.execute("INSERT INTO part_section VALUES (1, 1, 0, NULL)")
    for sid, blk in rows:
        if blk is not None:
            db.execute("INSERT INTO section_measure VALUES (?, ?, 0, 100)", (sid, blk))
        db.execute("INSERT INTO part_section VALUES (1, ?, 1, NULL)", (sid,))
    return db


NAMES = {-2200000392: "Turner Creek", -2200000393: "Turner Creek Trib.1", -2200000394: "Noons Creek"}


def test_a_part_of_added_lines_with_its_own_name_is_named_by_it():
    assert sc.own_name_rows(_added_db([(22, -2200000392)]), NAMES) == \
        [(1, 1, "Turner Creek", "Port Moody")]
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, -2200000392)]), NAMES)[0][2] == "Turner Creek"


def test_no_own_name_no_row():
    assert sc.own_name_rows(_added_db([(22, -2200000999)]), NAMES) == []          # an added line with no name
    db = _added_db([(22, -2200000394)])
    db.execute("INSERT INTO name_source VALUES ('gnis:23366', 'Noons Creek', 'municipal', 'City of Port Moody ESA streams layer')")
    assert sc.own_name_rows(db, NAMES) == []                                       # its name is the water's own
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, -2200000999)]), NAMES) == []   # one line with no name
    db = _added_db([(22, -2200000392), (23, -2200000393)])                        # two names, both with words
    db.execute("INSERT INTO name_source VALUES ('gnis:23366', 'Turner Creek Trib.1', 'municipal', 'City of Port Moody')")
    assert sc.own_name_rows(db, NAMES) == []
    assert sc.own_name_rows(_added_db([(22, -2200000393)]), NAMES) == []          # no source words for the name


def test_a_label_naming_no_municipality_gives_no_own_name():
    db = _added_db([(22, -2200000392)])
    db.execute("UPDATE name_source SET label = 'OpenStreetMap' WHERE name = 'Turner Creek'")
    assert sc.own_name_rows(db, NAMES) == []


def test_a_mixed_part_never_takes_the_name():
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, 360000002)]), {**NAMES, 360000002: "Turner Creek"}) == []
    # an FWA line too, even one a names file listed
    assert sc.own_name_rows(_added_db([(22, -2200000392), (24, None)]), NAMES) == []          # a polygon (no line)


def test_the_added_names_are_read_from_the_atlas_graph():
    from types import SimpleNamespace as N
    t = lambda name, note: N(name=name, note=note)                                      # noqa: E731
    g = N(nodes={"a": N(blk="-2200000392", name_tuples=(t("Noons Creek", ""), t("Turner Creek", "added:added"))),
                 "b": N(blk="-5", name_tuples=(t(" ", "added:added"),)),                 # a blank name: none
                 "c": N(blk="-6", name_tuples=(t("A", "added:added"), t("B", "added:added"))),   # two: none
                 "d": N(blk="360000001", name_tuples=(t("Fwa Creek", "added:added"),)),  # an FWA line: never
                 "e": N(blk="", name_tuples=())})
    assert sc.added_stream_names(g) == {-2200000392: "Turner Creek"}


def test_the_bundle_step_refuses_to_run_without_the_added_names(tmp_path):
    db, geo = _db()
    with pytest.raises(sc.SideChannelError, match="added streams"):
        sc.write(db, geo, tmp_path, _Cov(), log=lambda *_: None, unnamed={})


def test_the_part_is_said_by_its_own_name_and_source():
    W = _water([MAIN, [_run("source", "mouth", None, None, branch=True)]], iid="gnis:23366", name="Noons Creek")
    cr = {("gnis:23366", "own", 1): ("Turner Creek", "Port Moody")}
    n = _names(W, cr)[1]
    assert n["lo"] == n["hi"] and n["lo"].startswith(OWN)
    assert _said(W, cr)[1] == "Noons Creek: Turner Creek (Port Moody)"
    assert _names(W, {})[1] == {"lo": None, "hi": None}       # (T1172) with no own name: its label, "Side channel"


def test_the_export_lays_the_joins_over_the_flows_spans():
    from pipeline.deliver.export.read import with_joins
    span = {3: (None, None, "mouth", "source", 1, 0), 4: (5, 9, "mouth", "source", 1, 0), 1: (0, 1000, "mouth", "x", 0, 0)}
    got = with_joins(span, [(3, 700, None), (4, None, "lake_inlet:gnis:8"), (99, 5, None)])
    assert got[3] == (700, 700, "mouth", "source", 1, 0)
    assert got[4] == (5, 9, "lake_inlet:gnis:8", "source", 1, 0)
    assert got[1] == span[1] and 99 not in got and span[3][0] is None             # the input is left as it was


def test_a_city_layers_label_gives_its_municipality():
    assert sc.municipality("City of Port Moody ESA streams layer") == "Port Moody"
    assert sc.municipality("City of Burnaby waterways layer") == "Burnaby"
    assert sc.municipality("District of North Vancouver watercourses") == "North Vancouver"
    assert sc.municipality("OpenStreetMap") is None                                  # names no municipality: no own name


# --- (T1172, owner 2026-11-26) the subtitle: distance only to tell parts apart, no verb, the area always -----------------

def test_a_distance_only_where_the_water_has_more_than_one_such_part():
    # two parts of side channels only: each says its distance (it tells them apart); a water of one: none — one rule
    two = _water([MAIN, [_run("source", "mouth", 5.7, 5.7, branch=True)],
                  [_run("source", "mouth", 6.04, 6.04, branch=True), _run("source", "mouth", 7.0, 7.0, branch=True)]])
    assert _sub(two)[1:] == ["Side channel, 5.7 km from the mouth", "2 side channels, 6–7 km from the mouth"]
    one = _water([MAIN, [_run("source", "mouth", 5.7, 5.7, branch=True)]])
    assert _sub(one)[1] == "Side channel"
    # a part with a main stem, or one outside B.C. (no rule set), is never counted
    out = _water([MAIN, [_run("source", "mouth", 5.7, 5.7, branch=True)], [_run("source", "mouth", 9.0, 9.0, branch=True)]])
    out.w["parts"][2]["ruleset"] = None
    from pipeline.deliver.answers.display.builders import side_facts
    assert side_facts(out.id, out.w, {})["parts"][1]["side_apart"] is False


def test_no_verb_whether_a_channel_joins_leaves_or_is_measured_at_its_nearest_point():
    # the measure is where it joins, where it leaves, or the nearest point (`section_join.how`): one shape for all three
    two = _water([MAIN, [_run("source", "mouth", 5.7, 5.7, branch=True)], [_run("source", "mouth", 9.0, 9.0, branch=True)]])
    assert all("joining" not in t and "leaving" not in t for t in _sub(two)[1:])
    from pipeline.deliver.answers.display import part as P
    assert P._from_mouth([_run("source", "mouth", 1.12, 1.12, branch=True)]) == "1.1 km from the mouth"


def test_the_area_short_form_in_name_and_subtitle_alike():
    from pipeline.deliver.answers.display.part import area_short
    assert area_short("Creston Valley Wildlife Management Area") == "Creston Valley WMA"
    assert area_short("Strathcona - Westmin Park") == "Strathcona - Westmin Park"       # the parks source's own hyphen
    assert area_short("Baynes Island Ecological Reserve") == "Baynes Island Ecological Reserve"
    # a cut WMA (said by its split) takes it too
    SPLITS["area:X WILDLIFE MANAGEMENT AREA"] = {"name": "X Wildlife Management Area boundary", "kind": "area"}
    try:
        W = _water([MAIN, LOOSE], iid="gnis:888", name="X Creek")
        cr = {("gnis:888", "inside", 1): ("X WILDLIFE MANAGEMENT AREA", "inside", True)}
        assert _sub(W, cr)[1] == "Side channel inside X WMA"
    finally:
        del SPLITS["area:X WILDLIFE MANAGEMENT AREA"]


def test_the_subtitle_says_the_area_with_its_distance_when_told_apart():
    W = _water([MAIN, LOOSE, [_run("source", "mouth", 20.0, 20.0, branch=True)]])
    cr = {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside", True)}
    assert _sub(W, cr)[1:] == ["Side channel inside Baynes Island Ecological Reserve, 11.6 km from the mouth",
                               "Side channel, 20 km from the mouth"]


def test_a_rule_set_group_of_side_parts_says_its_area_only_if_all_share_it():
    from pipeline.deliver.answers.display.builders import _by_grp, side_facts
    W = _water([MAIN, LOOSE, [_run("source", "mouth", 20.0, 20.0, branch=True)]])
    for p in W.w["parts"][1:]:
        p["grp"] = 1
    W.w["parts"][2]["run"] = 1
    a = ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside", True)
    def grp1(w):
        Wg = _by_grp(W.id, {**w, "grp_runs": [MAIN, LOOSE + w["parts"][2]["runs"]]}, W.doc)[0]
        return Wg.parts[Wg.export_ix.index(1)]
    one = grp1(side_facts(W.id, W.w, {(W.id, "inside", 1): a}))
    assert one["side_apart"] is True and one["side_area"] is None
    assert grp1(side_facts(W.id, W.w, {(W.id, "inside", 1): a, (W.id, "inside", 2): a}))["side_area"] == list(a)


def test_the_answers_step_reads_the_bundles_new_tables(tmp_path):
    from pipeline.deliver.answers.display.builders import _crossings
    p = tmp_path / "b.sqlite"
    db = sqlite3.connect(p)
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT);
        CREATE TABLE crossing_near (ord INTEGER, token TEXT, km REAL, near TEXT);
        CREATE TABLE part_near (ord INTEGER, part_ix INTEGER, near TEXT);
        CREATE TABLE part_inside (ord INTEGER, part_ix INTEGER, area TEXT, share REAL, how TEXT, cut INTEGER);
        CREATE TABLE part_own_name (ord INTEGER, part_ix INTEGER, name TEXT, label TEXT);
        CREATE TABLE section_join (sid INTEGER PRIMARY KEY, m INTEGER, lo TEXT, how TEXT);
        CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER, m INTEGER);
        CREATE TABLE section_span (sid INTEGER PRIMARY KEY, lo_m INTEGER, hi_m INTEGER, lo INTEGER, hi INTEGER,
                                   off_stem INTEGER, shape INTEGER);
        CREATE TABLE section_down (sid INTEGER PRIMARY KEY, down_sid INTEGER);
        CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        INSERT INTO item VALUES (1, 'gnis:36111', 'Halfway River', 'stream'), (2, 'gnis:23366', 'Noons Creek', 'stream');
        INSERT INTO part_inside VALUES (1, 0, 'X RESERVE', 1.0, 'at', 1);
        INSERT INTO part_own_name VALUES (2, 4, 'Turner Creek', 'Port Moody');
        INSERT INTO part_section VALUES (1, 10, 2, NULL), (1, 11, 2, NULL), (1, 12, 1, NULL);
        INSERT INTO section_join VALUES (10, 1000, NULL, 'join'), (11, 2000, NULL, 'nearest'), (12, 5, NULL, 'join');
    """)
    db.commit()
    db.close()
    got = _crossings(str(p))
    assert got[("gnis:36111", "inside", 0)] == ("X RESERVE", "at", True)
    assert got[("gnis:23366", "own", 4)] == ("Turner Creek", "Port Moody")
