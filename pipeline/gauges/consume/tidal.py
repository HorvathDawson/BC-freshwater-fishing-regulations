"""TIDE AND BACKWATER ON A LEVEL GAUGE (T78, audit N20).

OWNER 2026-10-22 ("a water's own gauge comes first", handoff/PLAN/app-integration/decisions.md): "In the Level view,
check and disclose tidal or backwater influence where the record shows it (Port Coquitlam is tidal, with Fraser
backwater)." The own-gauge rule made the lower Fraser's Level answer lead with the gauges at Whonock, Mission and Port
Mann, and the Pitt's with 08MH035 — every one of them a stage that rises and falls with the tide, or with the Fraser
backing up into it. A reader shown "higher than usual" there has to be told the river may not be carrying more water.

TWO SOURCES, JOINED:

    the record    HYDAT's own station remarks (`STN_REMARKS`). Mission: "DISCHARGE NOT PUBLISHED FOR GAUGE HEIGHTS
                  BELOW 3.0M DUE TO TIDAL EFFECTS." Port Mann: "THIS STATION IS SUBJECT TO TIDAL EFFECT." Only the
                  TIDE is read here. "Backwater" in the record is almost always episodic — ice (07FA006, 07FD010),
                  beavers (07FD004, 08HD023), the 2024 Chilcotin landslide (08MF073-076), a lake's variable level
                  (08HB074) — and flagging it would call every winter gauge tidal (`test_tidal_gauges` pins this).
    the list      `data/curated/gauges/tidal.json` (authored): stations the record does not say so about but that are
                  known to be tidal or backed up by a tidal river — the Pitt near Port Coquitlam (owner), Whonock, and
                  the Fraser below New Westminster. Each row carries its reason; the authored reason wins over the
                  record's where both name a station.

ONE FLAG, NO KIND. Every station flagged is tidal or backed up by a tidal river, and what a reader must be told is the
same for both: the level moves with something other than this river's own flow. `gauge.tidal` (0/1) is all the bundle
and the release carry; the reason stays here, for the review.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

#: The tide, as the record writes it: TIDAL, TIDE, TIDES — a whole word, in any case. Not "TIDEWATER" (a glacier
#: word) and never "BACKWATER" (see the module note).
TIDAL_REMARK = re.compile(r"\bTID(?:AL|ES?)\b", re.IGNORECASE)

_STATION = re.compile(r"^\d{2}[A-Z]{2}\d{3}$")


def remark_evidence(hydat: Path) -> dict[str, str]:
    """`station -> "HYDAT: <remark>"` for every station whose own remarks name the tide. The earliest such remark
    (by year, then type) is the one cited, so the citation does not move when HYDAT adds a later one."""
    if not Path(hydat).exists():                 # P2: no missing-file fallbacks
        raise FileNotFoundError(f"tidal: no {hydat} (python data/fetch_data.py --layers hydat)")
    with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as con:
        rows = con.execute("SELECT STATION_NUMBER, REMARK_EN FROM STN_REMARKS "
                           "ORDER BY STATION_NUMBER, YEAR, REMARK_TYPE_CODE").fetchall()
    out: dict[str, str] = {}
    for station, text in rows:
        if station not in out and text and TIDAL_REMARK.search(text):
            out[station] = "HYDAT: " + " ".join(text.split())
    return out


def load_curated(path: Path) -> dict[str, str]:
    """The authored list, `station -> reason`. RAISES on a missing file (AGENTS 37) and on a row without a station id
    or a reason — a flag nobody can explain is not one a reader should be shown."""
    blob = json.loads(Path(path).read_text(encoding="utf-8"))
    stations = blob["stations"]
    for st, why in stations.items():
        if not _STATION.match(st):
            raise ValueError(f"tidal: {st!r} is not a station id ({path})")
        if not isinstance(why, str) or not why.strip():
            raise ValueError(f"tidal: {st} carries no reason ({path})")
    return dict(sorted(stations.items()))


def tidal_stations(hydat: Path, curated: Path) -> dict[str, str]:
    """Every station whose level the tide or a tidal river's backwater moves, `station -> why`: the record's tidal
    remarks joined with the authored list, the authored reason winning."""
    return dict(sorted({**remark_evidence(hydat), **load_curated(curated)}.items()))
