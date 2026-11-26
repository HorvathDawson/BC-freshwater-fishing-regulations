"""T1140 (owner 2026-11-25: "itshould have the real name. there should be no partt x of n nameas anywhewre in the app",
"Named by its ends", "They should always have the cut names or distance it is impossible not to"). A part with NO MAIN
STEM is named by what it is, measured on the ground in the bundle stage (`bundle.side_channels`) and said by the answers
step (`answers.display.builders._names`):

- INSIDE: at least 95 % of it (lines by length, polygons by area, every section measured) in ONE cut area →
  `near:inside:area:<name>` as both ends, "<water> inside <Area>" — only for a pair a reviewer has looked at
  (`part.REVIEWED_INSIDE`, a tripwire both ways);
- NAMED ENDS: the ONE such part of a water whose channels have a named end → `near:side:[[lower, upper, km]…]`, "Side
  channel from <lower> up to <upper>"; a run that names only its own mouth takes the named water its flow reaches;
- JOIN DISTANCE: a channel the flow pointers never walk to the main line gets the measure of the main line's nearest
  point to its downstream end (else its end nearer the main line): its OWN water's main line, within `MAX_SNAP_M`, else
  no distance — and a part whose label then lacks one stops the build, named.

Synthetic shapes (EPSG:3005 metres) through the real functions; every guard has a test that fails without it."""
import sqlite3

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
    m, snap, k = sc.join_measure(ch, STEM)
    assert (round(m), round(snap), k) == (400, 30, 0)


def test_the_flow_pointer_says_which_end_is_downstream():
    ch = [LineString([(400, 30), (1500, 100)])]                # nearer end at x 400; the flow leaves at x 1500
    down = LineString([(1500, 100), (1500, 0)])
    m, snap, _ = sc.join_measure(ch, STEM, down)
    assert (round(m), round(snap)) == (1500, 100)
    assert round(sc.join_measure(ch, STEM)[0]) == 400          # with no pointer: the nearer end


def test_the_measure_runs_from_the_mouth_whichever_way_a_section_is_drawn():
    flipped = [(0, 1000, LineString([(1000, 0), (0, 0)])), STEM[1]]
    assert round(sc.join_measure([LineString([(250, 10), (250, 400)])], flipped)[0]) == 250


def test_a_main_line_of_one_section_cannot_be_oriented():
    with pytest.raises(sc.SideChannelError, match="one section"):
        sc.join_measure([LineString([(400, 30), (600, 500)])], STEM[:1])


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
        CREATE TABLE part_inside (ord INTEGER, part_ix INTEGER, area TEXT, share REAL, how TEXT,
                                  PRIMARY KEY (ord, part_ix));
        CREATE TABLE span_end (eid INTEGER PRIMARY KEY, token TEXT);
        CREATE TABLE part_own_name (ord INTEGER, part_ix INTEGER, name TEXT, label TEXT);
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


def _write(db, geo, tmp_path, monkeypatch):
    monkeypatch.setattr(sc, "inside_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(sc, "own_name_rows", lambda *_a, **_k: [])
    log = []
    sc.write(db, geo, tmp_path, _Cov(), log=log.append, added={})
    return dict(db.execute("SELECT sid, lo_m FROM section_span")), log


def test_a_loose_channel_gets_its_join_written_on_its_sections(tmp_path, monkeypatch):
    lo, log = _write(*_db(), tmp_path, monkeypatch)
    assert lo[3] == 700
    assert any("joins at 0.7 km, snap 20 m from its nearer end" in x for x in log)


def test_beyond_the_snap_limit_there_is_no_join(tmp_path, monkeypatch):
    lo, log = _write(*_db(channel_far=sc.MAX_SNAP_M + 1), tmp_path, monkeypatch)
    assert lo[3] is None
    assert any("NO JOIN" in x and f"beyond {sc.MAX_SNAP_M:.0f} m" in x for x in log)
    lo, _ = _write(*_db(channel_far=sc.MAX_SNAP_M - 1), tmp_path, monkeypatch)
    assert lo[3] == 700


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


def _lo_token(db, sid):
    return db.execute("SELECT e.token FROM section_span s JOIN span_end e ON e.eid = s.lo WHERE s.sid = ?",
                      (sid,)).fetchone()[0]


def test_a_channel_touching_its_rivers_own_polygon_joins_there_whatever_the_distance(tmp_path, monkeypatch):
    lo, log = _write(*_touch_db("own"), tmp_path, monkeypatch)
    assert lo[3] == 700                                        # 300 m off the centre line, beyond the 50 m limit
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
    lo, _ = _write(*_touch_db("none"), tmp_path, monkeypatch)
    assert lo[3] is None


def test_another_waters_polygon_never_counts(tmp_path, monkeypatch):
    lo, _ = _write(*_touch_db("other"), tmp_path, monkeypatch)
    assert lo[3] is None


def test_a_channel_into_a_named_lake_on_its_main_line_is_named_by_it(tmp_path, monkeypatch):
    db, geo = _touch_db("lake")
    lo, log = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] is None and _lo_token(db, 3) == "lake_inlet:gnis:3"     # rule A: "side channel into Olsen Lake"
    assert any("flows into gnis:3" in x for x in log)


def test_a_lake_the_main_line_does_not_run_through_never_counts(tmp_path, monkeypatch):
    db, geo = _touch_db("lake_off")
    lo, _ = _write(db, geo, tmp_path, monkeypatch)
    assert lo[3] is None and _lo_token(db, 3) == "mouth"


# --- the bundle step: inside an area -------------------------------------------------------------------------------

def _catalog(tmp_path, areas):
    import geopandas as gpd
    gpd.GeoDataFrame({"area_id": [f"a{i}" for i in range(len(areas))], "name": [n for n, _k, _g in areas],
                      "kind": [k for _n, k, _g in areas], "cut": [1] * len(areas)},
                     geometry=[g for _n, _k, g in areas], crs="EPSG:3005").to_file(tmp_path / "area_catalog.gpkg",
                                                                                   layer="areas")


def _inside(tmp_path, shapes, areas):
    """One water, one listed part of side channels only (sids 10…), shapes {sid: geometry}."""
    db, _ = _db()
    db.execute("DELETE FROM part_section WHERE part_ix = 1")
    db.executemany("INSERT INTO part_section VALUES (1, ?, 1, NULL)", [(s,) for s in shapes])
    db.executemany("INSERT INTO section_span VALUES (?, NULL, NULL, 0, 0, 1, ?)",
                   [(s, int(g.geom_type == "Polygon")) for s, g in shapes.items()])
    span = {s: (a, b, off, shape) for s, a, b, off, shape in db.execute(
        "SELECT sid, lo_m, hi_m, off_stem, shape FROM section_span")}
    _catalog(tmp_path, areas)
    return sc.inside_rows(db, {**{1: STEM[0][2], 2: STEM[1][2]}, **shapes}, tmp_path, span)


RESERVE = ("BAYNES ISLAND ECOLOGICAL RESERVE", "ecological_reserves", box(0, 0, 100, 100))


def test_a_part_wholly_inside_one_area_is_said_inside_it(tmp_path):
    assert _inside(tmp_path, {10: LineString([(10, 10), (90, 90)])}, [RESERVE]) == \
        [(1, 1, "BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "inside")]


def test_lines_by_length_at_95_percent(tmp_path):
    # 95 m of 100 inside: inside; 94 m: not (the Skeena's 71.7 % along the reserve's edge, and anything short of 95 %)
    assert _inside(tmp_path, {10: LineString([(5, 50), (105, 50)])}, [RESERVE])[0][3] == 0.95
    assert _inside(tmp_path, {10: LineString([(6, 50), (106, 50)])}, [RESERVE]) == []


def test_polygons_by_area_and_a_straddling_polygon_is_at_the_area(tmp_path):
    # the Okanagan River Oxbows: its lines all inside, its lake polygon 31.9 % inside — "at" the reserve, not inside it
    lines = {10: LineString([(10, 10), (90, 10)])}
    half = {11: Polygon([(50, 20), (150, 20), (150, 80), (50, 80)])}                     # 50 % of its area inside
    assert _inside(tmp_path, {**lines, **half}, [RESERVE]) == [(1, 1, "BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "at")]
    whole = {11: Polygon([(20, 20), (80, 20), (80, 80), (20, 80)])}
    assert _inside(tmp_path, {**lines, **whole}, [RESERVE])[0][2:] == ("BAYNES ISLAND ECOLOGICAL RESERVE", 1.0, "inside")


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
    assert _inside(tmp_path, ch, [RESERVE, ("PARK", "parks", box(-10, -10, 200, 200))]) == []


def test_an_unmeasured_section_means_not_inside(tmp_path):
    db, _ = _db()
    _catalog(tmp_path, [RESERVE])
    db.execute("INSERT INTO part_section VALUES (1, 11, 1, NULL)")
    db.execute("INSERT INTO section_span VALUES (11, NULL, NULL, 0, 0, 1, 0)")
    span = {s: (a, b, off, shape) for s, a, b, off, shape in db.execute(
        "SELECT sid, lo_m, hi_m, off_stem, shape FROM section_span")}
    geo = {1: STEM[0][2], 2: STEM[1][2], 3: LineString([(10, 10), (90, 90)])}          # sid 11 has no shape
    assert sc.inside_rows(db, geo, tmp_path, span) == []


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
            w = end_words(W.doc, n["lo"], "from")                 # as a shipped app composes it (`spanName`)
            out.append(f"{W.w['name']} {w[:1].lower() + w[1:]}")
        else:
            out.append(f"{end_words(W.doc, n['lo'], 'to')} → {end_words(W.doc, n['hi'], 'from')}")
    return out


MAIN = [_run("squamish__tidal", "mouth", 3.0, 0.0), _run("source", "squamish__tidal", 90.0, 3.0)]
LOOSE = [_run("source", "mouth", 11.6, 11.6, branch=True)]


def test_the_squamish_channels_inside_baynes_island_are_said_inside_it():
    W = _water([MAIN, LOOSE])
    got = _names(W, {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside")})
    assert got[1] == {"lo": INSIDE + BAYNES, "hi": INSIDE + BAYNES}
    assert _said(W, {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside")})[1] == \
        "Squamish River inside Baynes Island Ecological Reserve"


def test_an_inside_pair_no_reviewer_looked_at_stops_the_build():
    W = _water([MAIN, LOOSE], iid="gnis:777")
    with pytest.raises(AnswersError, match="no reviewer has looked at"):
        _names(W, {("gnis:777", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside")})
    W = _water([MAIN, LOOSE])                              # a reviewed water and area, but said another way
    with pytest.raises(AnswersError, match="no reviewer has looked at"):
        _names(W, {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "at")})


def test_the_oxbows_are_said_at_hayne_s_lease():
    hl = "area:HAYNE'S LEASE ECOLOGICAL RESERVE"
    SPLITS[hl] = {"name": "Hayne's Lease Ecological Reserve boundary", "kind": "area"}
    try:
        W = _water([MAIN, [_run("source", hl, None, None, branch=True), _run("source", "mouth", None, None, branch=True)]],
                   iid="wsc:300-432687-459615", name="Okanagan River Oxbows")
        cr = {("wsc:300-432687-459615", "inside", 1): ("HAYNE'S LEASE ECOLOGICAL RESERVE", "at")}
        assert _names(W, cr)[1] == {"lo": AT + hl, "hi": AT + hl}
        assert _said(W, cr)[1] == "Okanagan River Oxbows at Hayne's Lease Ecological Reserve"
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
    assert _said(W)[1] == "Snehumption Creek side channels: one up to the B.C. border; one joining 3.6 km from the mouth"


def test_a_channel_with_no_named_end_and_no_distance_stops_a_named_part():
    # the Okanagan River Oxbows, were it not "at" its reserve: one channel to the boundary, one with no join
    hl = "area:HAYNE'S LEASE ECOLOGICAL RESERVE"
    SPLITS[hl] = {"name": "Hayne's Lease Ecological Reserve boundary", "kind": "area"}
    try:
        W = _water([MAIN, [_run("source", hl, None, None, branch=True), _run("source", "mouth", None, None, branch=True)]],
                   iid="wsc:300-432687-459615", name="Okanagan River Oxbows")
        with pytest.raises(AnswersError, match="no named end and no join"):
            _names(W, {})
    finally:
        del SPLITS[hl]


def test_a_part_said_by_its_label_must_have_every_channels_distance():
    W = _water([MAIN, [_run("source", "mouth", None, None, branch=True)]], iid="gnis:23366", name="Noons Creek")
    with pytest.raises(AnswersError, match="no join to say"):
        _names(W, {})
    W = _water([MAIN, [_run("source", "mouth", 0.7, 0.7, branch=True)]], iid="gnis:23366", name="Noons Creek")
    assert _names(W, {})[1] == {"lo": None, "hi": None}       # said by its label: "Side channel, joining 0.7 km …"


def test_several_parts_with_no_main_stem_count_only_parts_with_no_ends():
    # two stemless parts → each said near what it lies nearest (`part_near`); an inside part is one of them
    W = _water([MAIN, LOOSE, [_run("source", "mouth", 20.0, 20.0, branch=True)]])
    with pytest.raises(AnswersError, match="one of several parts with no main stem"):
        _names(W, {("gnis:25671", "inside", 1): ("BAYNES ISLAND ECOLOGICAL RESERVE", "inside")})


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
        [(1, 1, "Turner Creek", "City of Port Moody ESA streams layer")]
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, -2200000392)]), NAMES)[0][2] == "Turner Creek"


def test_no_own_name_no_row():
    assert sc.own_name_rows(_added_db([(22, -2200000999)]), NAMES) == []          # an added line with no name
    db = _added_db([(22, -2200000394)])
    db.execute("INSERT INTO name_source VALUES ('gnis:23366', 'Noons Creek', 'municipal', 'City of Port Moody ESA streams layer')")
    assert sc.own_name_rows(db, NAMES) == []                                       # its name is the water's own
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, -2200000999)]), NAMES) == []   # one line with no name
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, -2200000393)]), NAMES) == []    # two names
    assert sc.own_name_rows(_added_db([(22, -2200000393)]), NAMES) == []          # no source words for the name


def test_a_mixed_part_never_takes_the_name():
    assert sc.own_name_rows(_added_db([(22, -2200000392), (23, 360000002)]), {**NAMES, 360000002: "Turner Creek"}) == []
    # an FWA line too, even one a names file listed
    assert sc.own_name_rows(_added_db([(22, -2200000392), (24, None)]), NAMES) == []          # a polygon (no line)


def test_the_added_names_are_read_from_the_streams_by_blk(tmp_path):
    import json
    p = tmp_path / "added_streams.json"
    p.write_text(json.dumps({"streams": [{"blk": "-2200000392", "name": "Turner Creek"}, {"blk": "-5", "name": " "}],
                             "name_variants": [{"kind": "added", "name": "Other", "target_blk": "-2200000392"}]}))
    assert sc.added_stream_names(p) == {-2200000392: "Turner Creek"}


def test_the_part_is_said_by_its_own_name_and_source():
    W = _water([MAIN, [_run("source", "mouth", None, None, branch=True)]], iid="gnis:23366", name="Noons Creek")
    cr = {("gnis:23366", "own", 1): ("Turner Creek", "City of Port Moody ESA streams layer")}
    n = _names(W, cr)[1]
    assert n["lo"] == n["hi"] and n["lo"].startswith(OWN)
    assert _said(W, cr)[1] == "Noons Creek — Turner Creek (City of Port Moody ESA streams layer)"
    with pytest.raises(AnswersError, match="no join to say"):           # with no own name it is a named STOP
        _names(W, {})
