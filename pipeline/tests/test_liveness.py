"""ONE STATION LIVENESS (T122, owner 2026-10-22: "unify checks that decide the same thing"). "May this station speak at
all, and is it speaking now?" is answered by `pipeline.gauges.liveness` alone: `may_speak` (ECCC's Active status) and
`speaks_now` (ECCC's real-time list — this run's, else the roster's snapshot of it). The bundle's shed filter, the
shed's tie-break, the atlas's gauge cuts, the feed's stations to ask and the transmitting counts each read one of
them; nothing else reads `active`, `realtime` or the list.

No data, no network. Mutations each test catches (run by hand, T122):
  * `may_speak` reading `realtime` (the traffic, not the status): `test_may_speak_is_ecccs_status_not_todays_traffic`.
  * `speaks_now` ignoring the feed (the roster's snapshot always): `test_speaks_now_is_todays_list_else_the_snapshot`.
  * `speaks_now` gated on `may_speak`: `test_speaks_now_is_todays_list_else_the_snapshot` (a listed inactive row),
    `test_the_feed_asks_todays_list_and_says_how_far_the_roster_drifted` (X leaves the drift).
  * `listed_stations` returning an empty set for a bad answer: `test_the_list_is_none_not_empty_when_it_says_nothing`.
  * a reader spelling `s.get("realtime")` / `s.get("active")` or parsing the list again (e.g. in `shed.py`, `cuts.py`,
    `build.py`, `fetch_data.py`): `test_nothing_else_reads_liveness`.
"""
from __future__ import annotations

import re
from pathlib import Path

from pipeline.gauges import liveness as LV

ROOT = Path(__file__).resolve().parents[2]

#: active and transmitting / active and silent (a seasonal gauge) / discontinued / listed while discontinued
ROWS = [{"station": "A", "active": True, "realtime": True}, {"station": "S", "active": True, "realtime": False},
        {"station": "D", "active": False, "realtime": False}, {"station": "X", "active": False, "realtime": True}]
LIST = ("ID,Name / Nom,Latitude,Longitude,Prov/Terr,Timezone / Fuseau horaire\n"
        "A,A RIVER,49,-123,BC,UTC-08:00\nS,S RIVER,49,-123,BC,UTC-08:00\nN,NEW CREEK,49,-123,BC,UTC-08:00\n"
        "Y,YUKON RIVER,60,-135,YT,UTC-08:00\n , BLANK,49,-123,BC,UTC-08:00\n")


def test_may_speak_is_ecccs_status_not_todays_traffic():
    assert [LV.may_speak(r) for r in ROWS] == [True, True, False, False]
    assert LV.may_speak({"station": "Q"}) is False


def test_speaks_now_is_todays_list_else_the_snapshot():
    assert [LV.speaks_now(r) for r in ROWS] == [True, False, False, True]               # the roster's snapshot
    feed = LV.listed_stations(LIST)
    assert [LV.speaks_now(r, feed) for r in ROWS] == [True, True, False, False]         # today's list wins
    assert [LV.speaks_now(r, set()) for r in ROWS] == [False] * 4                       # an empty list is a list


def test_the_list_is_none_not_empty_when_it_says_nothing():
    assert LV.listed_stations(LIST) == {"A", "S", "N"}                                 # BC only, no blank id
    assert [LV.listed_stations(t) for t in (None, "", "<html>maintenance</html>",
                                            "ID,Name / Nom,Latitude,Longitude,Prov/Terr\n")] == [None] * 4


def test_the_feed_asks_todays_list_and_says_how_far_the_roster_drifted():
    feed = LV.listed_stations(LIST)
    assert (LV.stations_to_ask(ROWS, feed), LV.roster_drift(ROWS, feed)) == (
        (["A", "N", "S"], "eccc"), {"joined": ["N", "S"], "left": ["X"]})
    assert (LV.stations_to_ask(ROWS, None), LV.roster_drift(ROWS, None)) == ((["A", "X"], "roster"), None)


def test_the_feed_and_the_roster_writer_use_this_list():
    from pipeline.gauges.feed import publish as pub
    assert (pub.STATION_LIST, pub.listed_stations, pub.stations_to_ask, pub.roster_drift) == (
        LV.STATION_LIST, LV.listed_stations, LV.stations_to_ask, LV.roster_drift)
    assert "from pipeline.gauges.liveness import STATION_LIST, listed_stations, speaks_now" in (
        ROOT / "data" / "fetch_data.py").read_text(encoding="utf-8")


#: A READ of a roster row's liveness, or the ECCC list parsed or addressed. The roster WRITER's dict literal
#: (`"realtime": sid in speaking`) is no read. T122 review added: a SUBSCRIPT of `active` (the answers' unrelated
#: `["vessel"]["active"]` excepted) and `getattr`. A field read through a VARIABLE key (`check_curated` read
#: `r.get(a.roster_where)` with `roster_where="active"`) no grep can see: that one is pinned by identity in
#: `test_check_curated.py::test_the_gauge_queue_is_committed_curated_data` (its predicate IS `may_speak`).
_READS = re.compile(r"""\.get\(\s*["'](?:realtime|active)["']\s*[,)]"""
                    r"""|(?<!\["vessel"\])\[\s*["'](?:realtime|active)["']\s*\]"""
                    r"""|getattr\([^)]*["'](?:realtime|active)["']"""
                    r"""|hydrometric_StationList|Prov/Terr""")


def _offenders(root: Path) -> list[str]:
    out = []
    files = sorted((root / "pipeline").rglob("*.py")) + sorted((root / "data").glob("*.py"))
    for p in files:
        rel = p.relative_to(root)
        if "tests" in rel.parts or "__pycache__" in rel.parts or rel.as_posix() == "pipeline/gauges/liveness.py":
            continue
        if _READS.search(p.read_text(encoding="utf-8")):
            out.append(rel.as_posix())
    return out


def test_the_pin_leaves_the_answers_vessel_alone(tmp_path):
    d = tmp_path / "pipeline" / "deliver"
    d.mkdir(parents=True)
    (d / "x.py").write_text('out.update(g["vessel"]["active"])\nans = {"active": vessel}\n')
    assert _offenders(tmp_path) == []


def test_nothing_else_reads_liveness():
    """THE GREP PIN: `realtime`, `active` and ECCC's list are read in `liveness.py` alone."""
    assert _offenders(ROOT) == []
    assert _READS.search((ROOT / "pipeline/gauges/liveness.py").read_text(encoding="utf-8"))


def test_the_pin_would_see_a_second_reader(tmp_path):
    for i, line in enumerate(('live = {s["station"] for s in stations if s.get("realtime")}',
                              'active = {s["station"] for s in stations if s.get("active")}',
                              'if (row.get("Prov/Terr") or "").strip() == "BC":', 'n = sum(r["realtime"] for r in out)',
                              'need = {r["station"] for r in rows if r["active"]}', 'ok = getattr(row, "realtime")')):
        d = tmp_path / str(i) / "pipeline" / "gauges"
        d.mkdir(parents=True)
        (d / "x.py").write_text(line + "\n")
        assert _offenders(tmp_path / str(i)) == ["pipeline/gauges/x.py"], line
