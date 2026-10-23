"""T78 — tide and backwater on a level gauge (audit N20).

OWNER 2026-10-22 ("a water's own gauge comes first"): "In the Level view, check and disclose tidal or backwater
influence where the record shows it (Port Coquitlam is tidal, with Fraser backwater)." The pipeline flags such
stations from two sources — HYDAT's own station remarks (the record) and an authored list
(`data/curated/gauges/tidal.json`) — the bundle writes `gauge.tidal`, the release carries it, and core says one
sentence in the Level view.

The remark strings below are HYDAT's own, copied verbatim (HYDAT release in data/source/hydat.release), so the pins
are about the words the record actually uses.
"""

from __future__ import annotations

import inspect
import json
import re
import sqlite3
import textwrap
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, SOURCE
from pipeline.gauges.consume import tidal as T
from pipeline.tests.conftest import need
from pipeline.gauges.consume.tidal import TIDAL_REMARK, load_curated, remark_evidence, tidal_stations

#: (station, remark) exactly as HYDAT's STN_REMARKS holds them.
MISSION = ("08MH024", "DISCHARGE NOT PUBLISHED FOR GAUGE HEIGHTS BELOW 3.0M DUE TO TIDAL EFFECTS.")
PORT_MANN = ("08MH126", "THIS STATION IS SUBJECT TO TIDAL EFFECT.")
#: "Backwater" in the record that is NOT a standing tide or a receiving river: ice, beavers, a landslide, a lake's
#: variable level. Every one of these is episodic, and flagging it would say every winter gauge is tidal.
NOT_TIDAL = [
    ("07FA006", "MAX. INST. WATER LEVEL AFFECTED BY BACKWATER FROM ICE"),
    ("07FD004", "BACKWATER CAUSED BY BEAVER ACTIVITY DURING OPEN WATER PERIODS. MAX. INST. FROM HIGH WATER MARK - "
                "TIME UNKNOWN."),
    ("08HB074", "STATION SUBJECT TO VARIABLE BACKWATER FROM COMOX LAKE, NECESSITATING EXTENSIVE ESTIMATED PERIODS"),
    ("08MF074", "A landslide occurred on 2024-07-30 that blocked the Chilcotin River, forming a backwater lake."),
    ("08MH044", "MINIMUM INSTANTANEOUS AND DAILY WATER LEVEL NOT VALID -  EQUIPMENT MALFUNCTION."),
]
#: Unaffected stations: the Fraser at Hope (above the tide) and the Chilliwack at Vedder Crossing.
UNAFFECTED = ("08MF005", "08MH001")


def _hydat(tmp_path: Path, rows) -> Path:
    p = tmp_path / "hydat.sqlite3"
    with sqlite3.connect(p) as con:
        con.execute("CREATE TABLE STN_REMARKS (STATION_NUMBER TEXT, REMARK_TYPE_CODE INTEGER, YEAR INTEGER, "
                    "REMARK_EN TEXT, REMARK_FR TEXT)")
        con.executemany("INSERT INTO STN_REMARKS VALUES (?,?,?,?,NULL)",
                        [(st, 2, 2000 + i, text) for i, (st, text) in enumerate(rows)])
    return p


def _curated(tmp_path: Path, stations: dict) -> Path:
    p = tmp_path / "tidal.json"
    p.write_text(json.dumps({"_about": "test", "stations": stations}), encoding="utf-8")
    return p


# ------------------------------------------------------------------ the record

def test_the_record_flags_a_tidal_remark_and_nothing_else(tmp_path):
    got = remark_evidence(_hydat(tmp_path, [MISSION, PORT_MANN, *NOT_TIDAL]))
    assert got == {"08MH024": "HYDAT: " + MISSION[1], "08MH126": "HYDAT: " + PORT_MANN[1]}


def test_a_station_with_several_tidal_remarks_cites_the_first_by_year(tmp_path):
    later = ("08MH126", "TIDAL EFFECT AT LOW FLOW.")
    got = remark_evidence(_hydat(tmp_path, [PORT_MANN, later]))
    assert got == {"08MH126": "HYDAT: " + PORT_MANN[1]}


@pytest.mark.parametrize("text", ["SUBJECT TO TIDES", "TIDE AFFECTED", "tidal influence at low water"])
def test_the_tide_is_read_in_any_of_its_forms(text):
    assert TIDAL_REMARK.search(text)


@pytest.mark.parametrize("text", ["TIDEWATER GLACIER", "STATIDAL", "BACKWATER FROM ICE"])
def test_words_that_only_contain_tide_are_not_the_tide(text):
    assert not TIDAL_REMARK.search(text)


# ------------------------------------------------------------------ the authored list

def test_the_authored_list_is_read_whole(tmp_path):
    p = _curated(tmp_path, {"08MH035": "owner 2026-10-22", "08MH044": "tidal"})
    assert load_curated(p) == {"08MH035": "owner 2026-10-22", "08MH044": "tidal"}


def test_a_missing_authored_list_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_curated(tmp_path / "absent.json")


@pytest.mark.parametrize("bad", [{"8MH035": "x"}, {"08MH035": ""}, {"08MH035": None}])
def test_an_authored_row_without_a_station_id_or_a_reason_is_refused(tmp_path, bad):
    with pytest.raises(ValueError):
        load_curated(_curated(tmp_path, bad))


def test_the_authored_list_is_declared_in_config():
    assert CURATED.gauges.tidal is not None
    assert CURATED.gauges.tidal.name == "tidal.json"
    assert T.TIDAL_FILE == CURATED.gauges.tidal       # the default the bundle's call reads


# ------------------------------------------------------------------ both, and the pins

def test_both_sources_are_joined_and_the_authored_reason_wins(tmp_path):
    hy = _hydat(tmp_path, [MISSION, PORT_MANN, *NOT_TIDAL])
    cur = _curated(tmp_path, {"08MH035": "owner", "08MH024": "authored"})
    assert tidal_stations(hy, cur) == {"08MH024": "authored", "08MH035": "owner",
                                       "08MH126": "HYDAT: " + PORT_MANN[1]}


def test_a_missing_hydat_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        tidal_stations(tmp_path / "absent.sqlite3", _curated(tmp_path, {}))


def test_the_named_stations_are_flagged_and_an_unaffected_one_is_not(tmp_path):
    """THE ACCEPTANCE PIN: 08MH035 (the Pitt near Port Coquitlam), Whonock, Mission and Port Mann are flagged; the
    Fraser at Hope and the Chilliwack at Vedder Crossing are not — against the REAL authored list and the record's own
    remark strings."""
    got = tidal_stations(_hydat(tmp_path, [MISSION, PORT_MANN, *NOT_TIDAL]), CURATED.gauges.tidal)
    # WHOLE VALUE (review): the named four, the estuary three from the list, and nothing else — so an ice, beaver,
    # slide or lake station, or Hope / Vedder, cannot slip in unseen.
    assert sorted(got) == ["08MH024", "08MH028", "08MH032", "08MH035", "08MH044", "08MH053", "08MH126"]
    assert got["08MH024"] == "HYDAT: " + MISSION[1] and got["08MH126"] == "HYDAT: " + PORT_MANN[1]
    assert not (set(UNAFFECTED) | {s for s, _ in NOT_TIDAL if s != "08MH044"}) & set(got)


def test_the_real_authored_list_is_exactly_the_lower_fraser():
    assert sorted(load_curated(CURATED.gauges.tidal)) == ["08MH028", "08MH032", "08MH035", "08MH044", "08MH053"]


@pytest.mark.needs_source
def test_the_live_record_flags_mission_and_port_mann_and_not_hope(request):
    """WHOLE VALUE against the live HYDAT (review 2026-10-22): the 7 lower-Fraser roster stations plus the 4 the
    record calls tidal outside the roster (08MH025 a DFO tide station; 10LC002, 10LC021, 10MC011 Mackenzie delta).
    A HYDAT refresh that adds a tidal remark fails here, so the new flag is looked at before it ships."""
    hydat = need(request, "source", Path(SOURCE) / "hydat.sqlite3", "python data/fetch_data.py --layers hydat")
    got = tidal_stations(hydat)            # the bundle's own call: the authored list by default
    assert sorted(got) == ["08MH024", "08MH025", "08MH028", "08MH032", "08MH035", "08MH044", "08MH053", "08MH126",
                           "10LC002", "10LC021", "10MC011"]
    assert not set(UNAFFECTED) & set(got)


# ------------------------------------------------------------------ mutation pins

def _mutant_pattern(old: str, new: str) -> re.Pattern:
    src = textwrap.dedent(inspect.getsource(T))
    line = next(ln for ln in src.splitlines() if ln.startswith("TIDAL_REMARK = "))
    assert line.count(old) == 1, f"{old!r} moved; update this pin"
    ns: dict = {"re": re}
    exec(line.replace(old, new), ns)
    return ns["TIDAL_REMARK"]


def test_the_tide_only_pattern_is_what_keeps_backwater_from_ice_unflagged(tmp_path, monkeypatch):
    """Widen the pattern to every "backwater" the record mentions and an ice / beaver / landslide station is flagged
    tidal: the unaffected pin above is what catches it."""
    mutant = _mutant_pattern("TID(", "BACKWATER|TID(")
    monkeypatch.setattr(T, "TIDAL_REMARK", mutant)
    got = remark_evidence(_hydat(tmp_path, [MISSION, PORT_MANN, *NOT_TIDAL]))
    assert {"07FA006", "07FD004", "08HB074", "08MF074"} <= set(got)


def test_the_word_boundary_is_what_keeps_tidewater_unflagged():
    mutant = _mutant_pattern('r"\\bTID', 'r"TID')
    assert mutant.search("STATIDAL")


# ------------------------------------------------------------------ the bundle and the release carry it

def test_the_bundle_gauge_table_carries_the_flag_last_with_a_default_and_the_release_contract_matches():
    from pipeline.deliver.release.contract import FILES
    schema = (Path(__file__).resolve().parents[1] / "deliver" / "bundle" / "schema.sql").read_text(encoding="utf-8")
    con = sqlite3.connect(":memory:")
    con.executescript(schema)
    cols = [(r[1], r[2], r[3], r[4]) for r in con.execute("PRAGMA table_info(gauge)")]
    assert [c[0] for c in cols] == FILES["conditions"]["tables"]["gauge"]
    # T160 added `backwater` after `tidal`, last and defaulted the same way: still conditions/1
    assert cols[-2:] == [("tidal", "INTEGER", 1, "0"), ("backwater", "INTEGER", 1, "0")]
    assert FILES["conditions"]["format"] == "conditions/1"


# ------------------------------------------------------------------ a lake's standing backwater (T160)
#
# MUTATION PINS — each mutation of `backwater_stations` / `load_backwater` and the test it fails:
#   * drop the check against the record (`not in said[st]`)   -> test_a_row_the_record_does_not_state_is_refused
#   * drop the tidal refusal (`both`)                          -> test_a_station_flagged_tidal_cannot_also_be_backwater
#   * drop the lake-name / BACKWATER check on the quote       -> test_an_authored_backwater_row_must_say_what_it_claims
#   * drop the `from` must-be-a-lake check                   -> test_an_authored_backwater_row_must_say_what_it_claims[…Puntledge…]
#   * read BACKWATER from the remarks by pattern instead      -> test_the_backwater_flag_is_exactly_the_authored_rows

COMOX = ("08HB074", "STATION SUBJECT TO VARIABLE BACKWATER FROM COMOX LAKE, NECESSITATING EXTENSIVE ESTIMATED PERIODS")
COMOX_ROW = {"from": "Comox Lake", "remark": COMOX[1].lower(), "why": "the gauge sits where the river enters the lake"}


def _backwater(tmp_path: Path, stations: dict) -> Path:
    p = tmp_path / "backwater.json"
    p.write_text(json.dumps({"_about": "test", "stations": stations}), encoding="utf-8")
    return p


def test_the_backwater_list_is_declared_in_config():
    assert CURATED.gauges.backwater is not None and CURATED.gauges.backwater.name == "backwater.json"
    assert T.BACKWATER_FILE == CURATED.gauges.backwater


def test_the_real_backwater_list_is_exactly_comox_lake_on_the_cruickshank():
    got = T.load_backwater(CURATED.gauges.backwater)
    assert sorted(got) == ["08HB074"]
    assert got["08HB074"]["from"] == "Comox Lake" and got["08HB074"]["remark"] == COMOX[1]


def test_the_backwater_flag_is_exactly_the_authored_rows(tmp_path):
    """WHOLE VALUE: the ice, beaver and slide backwater in the record stays unread — only the authored row."""
    hy = _hydat(tmp_path, [MISSION, PORT_MANN, *NOT_TIDAL])
    got = T.backwater_stations(hy, _backwater(tmp_path, {"08HB074": COMOX_ROW}), _curated(tmp_path, {}))
    assert got == {"08HB074": f"Comox Lake (HYDAT: {COMOX[1]}): the gauge sits where the river enters the lake"}


def test_a_row_the_record_does_not_state_is_refused(tmp_path):
    hy = _hydat(tmp_path, [MISSION, ("08HB074", "MAX. INST. WATER LEVEL AFFECTED BY BACKWATER FROM ICE")])
    with pytest.raises(ValueError, match="HYDAT holds no station remark"):
        T.backwater_stations(hy, _backwater(tmp_path, {"08HB074": COMOX_ROW}), _curated(tmp_path, {}))


def test_a_station_flagged_tidal_cannot_also_be_backwater(tmp_path):
    hy = _hydat(tmp_path, [COMOX])
    with pytest.raises(ValueError, match="also flagged tidal"):
        T.backwater_stations(hy, _backwater(tmp_path, {"08HB074": COMOX_ROW}),
                             _curated(tmp_path, {"08HB074": "a mistaken tidal row"}))


@pytest.mark.parametrize("bad", [
    {"8HB074": COMOX_ROW},
    {"08HB074": "Comox Lake"},
    {"08HB074": {**COMOX_ROW, "why": ""}},
    {"08HB074": {**COMOX_ROW, "extra": "x"}},
    {"08HB074": {**COMOX_ROW, "from": "Puntledge River",                        # not a lake, though the
                 "remark": "SUBJECT TO BACKWATER FROM PUNTLEDGE RIVER"}},       # quote says so
    {"08HB074": {**COMOX_ROW, "from": "Buttle Lake"}},                         # the quote names another lake
    {"08HB074": {**COMOX_ROW, "remark": "NECESSITATING EXTENSIVE ESTIMATED PERIODS COMOX LAKE"}},  # no backwater
])
def test_an_authored_backwater_row_must_say_what_it_claims(tmp_path, bad):
    with pytest.raises(ValueError):
        T.load_backwater(_backwater(tmp_path, bad))


def test_a_missing_backwater_list_or_hydat_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        T.load_backwater(tmp_path / "absent.json")
    with pytest.raises(FileNotFoundError):
        T.backwater_stations(tmp_path / "absent.sqlite3", _backwater(tmp_path, {}), _curated(tmp_path, {}))


@pytest.mark.needs_source
def test_the_live_record_states_comox_lakes_backwater(request):
    """The real row against the live HYDAT — the bundle's own call."""
    hydat = need(request, "source", Path(SOURCE) / "hydat.sqlite3", "python data/fetch_data.py --layers hydat")
    got = T.backwater_stations(hydat)
    assert sorted(got) == ["08HB074"] and got["08HB074"].startswith("Comox Lake (HYDAT: STATION SUBJECT TO VARIABLE")
    assert not set(got) & set(tidal_stations(hydat))
