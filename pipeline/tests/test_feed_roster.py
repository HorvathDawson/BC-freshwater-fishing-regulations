"""Which stations the feed asks for: ECCC's real-time list TODAY, not the roster's snapshot.

Finding 2026-10-22: 28 stations the bundle matches to ~3,450 sections had no feed file.
18 were `realtime: false` in `bc_hydrometric_stations.json`; 10 were `realtime: true` there
but ECCC had since dropped them from its real-time station list (the roster was fetched on
1 September and is frozen until deleted). Meanwhile 08NM139 and 10CD005 began transmitting
and the publisher never asked for them, because the snapshot said they did not.

So the publisher reads ECCC's own list every run (one request), asks exactly those stations,
falls back to the roster only when the list cannot be fetched, says when the roster has
drifted, and publishes the list in `index.json` so a reader can tell "not a real-time gauge"
from "a real-time gauge that is quiet today". Absence from `stations` keeps its meaning:
nothing to show (spots and Conditions already treat it as settled).

Fixtures are recorded ECCC files (2026-10-22): a trimmed `hydrometric_StationList.csv` and
08MG014's hourly CSV, which is level-only. No network.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline.gauges.feed import publish as pub

FIX = Path(__file__).parent / "fixtures" / "gauge_feed"
LIST_CSV = (FIX / "eccc_stationlist_2026-10-22.csv").read_text(encoding="utf-8-sig")
HOURLY_08MG014 = (FIX / "eccc_hourly_08MG014_2026-10-22.csv").read_text(encoding="utf-8-sig")
NOW = datetime(2026, 10, 22, 21, 0, tzinfo=timezone.utc)

# The roster as fetched 1 Sep: 08MG014 and 08MH001 live, 08EG012 live then (dropped since),
# 08NM139 and 10CD005 not live then (transmitting now), 08NP003 never real-time.
ROSTER = [
    {"station": "08MG014", "realtime": True, "active": True},
    {"station": "08MH001", "realtime": True, "active": True},
    {"station": "08EG012", "realtime": True, "active": True},
    {"station": "08NM139", "realtime": False, "active": True},
    {"station": "10CD005", "realtime": False, "active": True},
    {"station": "08NP003", "realtime": False, "active": True},
]


def test_the_list_is_bc_only():
    assert pub.listed_stations(LIST_CSV) == {"07FA008", "08MG014", "08MH001", "08NM139", "10CD005"}


def test_an_unreadable_list_is_unknown_not_empty():
    assert pub.listed_stations("") is None
    assert pub.listed_stations("<html>maintenance</html>") is None
    # Headers only: ECCC answered with a list holding no BC station — not something to
    # believe over the roster (it would empty the feed).
    assert pub.listed_stations("ID,Name / Nom,Latitude,Longitude,Prov/Terr\n") is None


def test_the_feed_asks_what_eccc_lists_today():
    ids, source = pub.stations_to_ask(ROSTER, pub.listed_stations(LIST_CSV))
    assert source == "eccc"
    # 08NM139 and 10CD005 are asked though the roster says not live; 08EG012 is not asked
    # though the roster says live; a listed station not in the roster at all is asked too.
    assert ids == ["07FA008", "08MG014", "08MH001", "08NM139", "10CD005"]


def test_without_the_list_the_roster_still_answers():
    ids, source = pub.stations_to_ask(ROSTER, None)
    assert source == "roster"
    assert ids == ["08EG012", "08MG014", "08MH001"]


def test_roster_drift_names_both_directions():
    drift = pub.roster_drift(ROSTER, pub.listed_stations(LIST_CSV))
    assert drift == {"joined": ["07FA008", "08NM139", "10CD005"], "left": ["08EG012"]}
    assert pub.roster_drift(ROSTER, None) is None


def test_a_level_only_station_is_read():
    # 08MG014 (Harrison at Harrison Mills) measures stage only; every discharge cell is empty.
    got = pub._parse_hourly("08MG014", HOURLY_08MG014)
    assert got is not None
    assert got["discharge"] is None
    assert got["level"] == pytest.approx(6.414)
    assert got["at"] == "2026-10-22T12:30:00-08:00"


@pytest.fixture
def net(monkeypatch):
    served = {f"{pub.BASE}/hourly/BC_08MG014_hourly_hydrometric.csv": HOURLY_08MG014}
    monkeypatch.setattr(pub, "_get", lambda url: served.get(url))
    monkeypatch.setattr(pub, "_sleep", lambda s: None)


def _index(out: Path) -> dict:
    return json.loads((out / "index.json").read_text())


def test_the_index_publishes_the_list(tmp_path, net):
    listed = pub.listed_stations(LIST_CSV)
    pub.publish(tmp_path, ["08MG014", "10CD005"], now=NOW, listed=listed)
    idx = _index(tmp_path)
    # Listed and reading: in `stations`. Listed and silent this run (10CD005 served nothing):
    # absent from `stations` — the existing "nothing to show" — but still in `listed`.
    assert set(idx["stations"]) == {"08MG014"}
    assert idx["listed"] == sorted(listed)
    assert (tmp_path / "08MG014.json").exists()
    assert not (tmp_path / "10CD005.json").exists()


def test_no_list_means_no_listed_key(tmp_path, net):
    # Unknown is absent, never an empty list: an empty list would claim no BC gauge is real-time.
    pub.publish(tmp_path, ["08MG014"], now=NOW, listed=None)
    assert "listed" not in _index(tmp_path)
