"""T173 — the fish obstacles tile layer: what is kept, what is one obstacle, and that the app knows every type."""

import re
from pathlib import Path

from shapely.geometry import LineString

from pipeline.deliver.tiles import obstacles as O
from pipeline.deliver.tiles.layers import BY_NAME

ROOT = Path(__file__).resolve().parents[2]


def test_types_by_name_not_code():
    # `PD` is both "Private Dam" and "Persistent debris": mapped by name, one kept and one dropped
    got = [O.fiss_type(n) for n in ("Falls", "Cascade or Chute", "Private Dam", "Persistent debris; present for several years",
                                    "BEAVER DAM", "Log jam", "Rocks", "Velocity Barrier", "Culvert", "Weir", None)]
    assert got == ["falls", "cascade", "dam", None, None, None, None, "velocity", "culvert", "dam", None]
    assert set(O.FISS_TYPES.values()) == set(O.OBSTACLE_TYPES)


def test_heights_drop_placeholders():
    assert [O.height(v) for v in (4, "1.55", 0, -1000, 9999, float("nan"), None, 440)] == \
        [4.0, 1.6, None, None, None, None, None, 440.0]


def test_years():
    import pandas as pd
    assert [O.year_of(v) for v in (pd.Timestamp("2002-10-14"), "1996-08-01Z", 1973.0, float("nan"), None)] == \
        [2002, 1996, 1973, None, None]


def _o(n, t, x, y, h=None, year=None):
    return {"obstacle_id": f"fiss:{n}", "type": t, "height_m": h, "year": year, "x": x, "y": y, "_n": n}


def test_dedupe_same_place_same_type_is_one():
    got = O.dedupe([_o(1, "falls", 0, 0, year=1996), _o(2, "falls", 3, 0, h=2.0, year=1990),
                    _o(3, "falls", 6, 0, year=2001),            # chained within 10 m of 2: the same falls
                    _o(4, "cascade", 1, 0),                     # another type at the same place: kept
                    _o(5, "falls", 50, 0)], [])                 # 44 m away: another falls
    # the one kept has a height (2), over a later survey without one (3)
    assert sorted(o["obstacle_id"] for o in got) == ["fiss:2", "fiss:4", "fiss:5"]


def test_dedupe_a_fiss_dam_on_a_wris_crest_is_that_dam():
    crest = LineString([(1000, 0), (1000, 100)])
    got = O.dedupe([_o(1, "dam", 1150, 50), _o(2, "dam", 1300, 50), _o(3, "falls", 1001, 50)], [crest])
    assert sorted(o["obstacle_id"] for o in got) == ["fiss:2", "fiss:3"]


def test_layer_spec_and_app_types():
    spec = BY_NAME["obstacle"]
    assert (spec.geometry, spec.attrs[0], spec.minzoom, spec.decorative, spec.mvt_id) == \
        ("point", "obstacle_id", 10, False, False)
    ts = (ROOT / "app/packages/core/src/obstacles.ts").read_text(encoding="utf-8")
    m = re.search(r"export const OBSTACLE_TYPES = \[([^\]]*)\]", ts)
    assert m and tuple(re.findall(r'"(\w+)"', m.group(1))) == O.OBSTACLE_TYPES


# --- PSCIS (stream-crossing assessments; owner treats as OGL-BC, Q165) -------------------------------------------------

def _p(n, x, y, barrier="barrier", ct="CBS", water=None):
    return {"obstacle_id": f"pscis:{n}", "type": "culvert", "name": None, "water": water, "height_m": None,
            "source": "pscis", "year": 2010, "barrier": barrier, "habitat": None, "road": None, "date": "2010-07-01",
            "crossing_type": ct, "x": x, "y": y}


def test_pscis_shown_only_unpassable_culverts():
    got = O.merge_pscis([], [_p(1, 0, 0), _p(2, 1000, 0, "potential"), _p(3, 2000, 0, "unknown"),
                             _p(4, 3000, 0, "passable"),                  # a passable culvert: noise
                             _p(5, 4000, 0, "passable", "OBS"),           # a bridge
                             _p(6, 5000, 0, "unknown", "OTHER")])         # a ford
    assert [o["obstacle_id"] for o in got] == ["pscis:1", "pscis:2", "pscis:3"]
    assert O.PSCIS_SHOW == ("barrier", "potential", "unknown") and O.PSCIS_TYPES == ("CBS",)


def test_a_fiss_culvert_at_a_pscis_crossing_is_that_crossing():
    fiss = [{**_o(1, "culvert", 30, 0), "water": "Rocky Creek"},   # 30 m from pscis:1 -> replaced, fills its water
            {**_o(2, "culvert", 3040, 0), "water": "Elk Creek"},   # 40 m from a PASSABLE crossing -> gone, not shown
            {**_o(3, "culvert", 6000, 0), "water": "Far Creek"},   # 1 km from anything -> kept
            _o(4, "falls", 10, 0)]                                 # not a culvert -> kept
    got = O.merge_pscis(fiss, [_p(1, 0, 0), _p(2, 3000, 0, "passable"), _p(3, 4000, 0, water="Own Name")])
    ids = sorted(o["obstacle_id"] for o in got)
    assert ids == ["fiss:3", "fiss:4", "pscis:1", "pscis:3"]
    by = {o["obstacle_id"]: o for o in got}
    assert by["pscis:1"]["water"] == "Rocky Creek" and by["pscis:3"]["water"] == "Own Name"
    assert O.PSCIS_MATCH_M == 50.0
    # beyond the match radius a FISS culvert stays its own mark
    assert len(O.merge_pscis([_o(9, "culvert", 60, 0)], [_p(1, 0, 0)])) == 2


def test_pscis_record_reads_the_assessment_view():
    import pandas as pd
    from shapely.geometry import Point
    from types import SimpleNamespace as R
    r = R(geometry=Point(1, 2), STREAM_CROSSING_ID=13, ASSESSMENT_DATE=pd.Timestamp("2008-08-17"), STREAM_NAME="Unnamed",
          ROAD_NAME=" Brush  Main ", CURRENT_BARRIER_RESULT_CODE="POTENTIAL", HABITAT_VALUE_CODE=None,
          CURRENT_CROSSING_TYPE_CODE="CBS")
    m = O.pscis_record(r)
    assert (m["obstacle_id"], m["water"], m["road"], m["barrier"], m["habitat"], m["date"], m["year"]) == \
        ("pscis:13", None, "Brush Main", "potential", None, "2008-08-17", 2008)
    assert O.pscis_record(R(**{**vars(r), "geometry": None})) is None
    assert [O.date_of(v) for v in ("2008-08-17Z", pd.NaT, None, "2008")] == ["2008-08-17", None, None, None]
    assert [O._name(v) for v in ("n/a", "UNKNOWN", "-", "Trib to Beaver Ck")] == [None, None, None, "Trib to Beaver Ck"]


def test_pscis_attrs_and_app_barrier_words():
    spec = BY_NAME["obstacle"]
    assert spec.attrs[-4:] == ("barrier", "habitat", "road", "date")
    ts = (ROOT / "app/packages/core/src/obstacles.ts").read_text(encoding="utf-8")
    m = re.search(r"export const OBSTACLE_BARRIERS = \[([^\]]*)\]", ts)
    assert m and set(re.findall(r'"(\w+)"', m.group(1))) == set(O.PSCIS_BARRIER.values())
    assert '"pscis"' in ts
