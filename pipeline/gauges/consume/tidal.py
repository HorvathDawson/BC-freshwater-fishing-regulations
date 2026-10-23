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

ONE FLAG FOR THE TIDE. Every station flagged tidal is tidal or backed up by a tidal river, and what a reader must be told
is the same for both: the level moves with something other than this river's own flow. `gauge.tidal` (0/1) is all the
bundle and the release carry; the reason stays here, for the review.

A LAKE'S STANDING BACKWATER IS ITS OWN FLAG (T160; Q49 decided 2026-10-23 under the owner's T78 rule). 08HB074
(Cruickshank River near the mouth) carries HYDAT's standing remark "STATION SUBJECT TO VARIABLE BACKWATER FROM COMOX
LAKE" — not the tide, so not the tidal sentence. It is AUTHORED (`data/curated/gauges/backwater.json`), never read from
the remarks by pattern (the episodic ice / beaver / slide backwater stays unread), and each row is CHECKED against the
record: it quotes the station's own remark and names the lake (`backwater_stations`). `gauge.backwater` (0/1); the app
says its own Level sentence.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

from pipeline.common.curated import CURATED

#: The tide, as the record writes it: TIDAL, TIDE, TIDES — a whole word, in any case. Not "TIDEWATER" (a glacier
#: word) and never "BACKWATER" (see the module note).
TIDAL_REMARK = re.compile(r"\bTID(?:AL|ES?)\b", re.IGNORECASE)

_STATION = re.compile(r"^\d{2}[A-Z]{2}\d{3}$")

#: The authored list, owned here the way `pipeline.gauges.matches.MATCH_FILE` owns the frozen match: the bundle asks
#: `tidal_stations` and never names a curated file itself (`test_one_data_flow`: the bundle reads no curated file).
TIDAL_FILE = CURATED.gauges.tidal


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


def load_curated(path: Path | None = None) -> dict[str, str]:
    """The authored list, `station -> reason`. RAISES on a missing file (AGENTS 37) and on a row without a station id
    or a reason — a flag nobody can explain is not one a reader should be shown."""
    path = path or TIDAL_FILE
    blob = json.loads(Path(path).read_text(encoding="utf-8"))
    stations = blob["stations"]
    for st, why in stations.items():
        if not _STATION.match(st):
            raise ValueError(f"tidal: {st!r} is not a station id ({path})")
        if not isinstance(why, str) or not why.strip():
            raise ValueError(f"tidal: {st} carries no reason ({path})")
    return dict(sorted(stations.items()))


def tidal_stations(hydat: Path, curated: Path | None = None) -> dict[str, str]:
    """Every station whose level the tide or a tidal river's backwater moves, `station -> why`: the record's tidal
    remarks joined with the authored list, the authored reason winning."""
    return dict(sorted({**remark_evidence(hydat), **load_curated(curated)}.items()))


# ------------------------------------------------------------------ a lake's standing backwater (T160)

#: The authored list of level gauges a LAKE below them backs up (T160; Q49 decided 2026-10-23 under the owner's T78
#: rule). Owned here like `TIDAL_FILE`: the bundle asks `backwater_stations` and never names a curated file itself.
BACKWATER_FILE = CURATED.gauges.backwater


def _norm(text: str) -> str:
    return " ".join(str(text or "").split()).upper()


def load_backwater(path: Path | None = None) -> dict[str, dict]:
    """The authored list, `station -> {from, remark, why}`. RAISES on a missing file (AGENTS 37) and on a row without a
    station id, a lake (`from`), a quoted remark or a reason, or whose quote does not name its lake or a backwater."""
    path = path or BACKWATER_FILE
    blob = json.loads(Path(path).read_text(encoding="utf-8"))
    stations = blob["stations"]
    for st, row in stations.items():
        if not _STATION.match(st):
            raise ValueError(f"backwater: {st!r} is not a station id ({path})")
        if not isinstance(row, dict) or set(row) != {"from", "remark", "why"}:
            raise ValueError(f"backwater: {st} must carry exactly from / remark / why ({path})")
        for k in ("from", "remark", "why"):
            if not isinstance(row[k], str) or not row[k].strip():
                raise ValueError(f"backwater: {st} carries no {k} ({path})")
        lake = _norm(row["from"])
        if not (lake.endswith(" LAKE") or lake.startswith("LAKE ")):
            raise ValueError(f"backwater: {st}: {row['from']!r} is not a lake — the Level sentence says a lake "
                             f"backs the river up ({path})")
        quote = _norm(row["remark"])
        if "BACKWATER" not in quote or lake not in quote:
            raise ValueError(f"backwater: {st}: the quoted remark must say BACKWATER and name {row['from']} ({path})")
    return {st: dict(stations[st]) for st in sorted(stations)}


def backwater_stations(hydat: Path, curated: Path | None = None, tidal: Path | None = None) -> dict[str, str]:
    """Every station a lake below it backs up, `station -> why` — the authored list, EACH ROW CHECKED AGAINST THE
    RECORD: its quoted remark must be one of the station's own HYDAT `STN_REMARKS` (case and spacing aside), so a row
    cannot claim a backwater the record does not state. A station also flagged tidal (`tidal_stations`, `tidal` the
    authored tidal list) is refused: a gauge gets one Level sentence, never two."""
    if not Path(hydat).exists():                 # P2: no missing-file fallbacks
        raise FileNotFoundError(f"backwater: no {hydat} (python data/fetch_data.py --layers hydat)")
    rows = load_backwater(curated)
    said: dict[str, set[str]] = {}
    with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as con:
        for st in rows:
            said[st] = {_norm(t) for (t,) in con.execute(
                "SELECT REMARK_EN FROM STN_REMARKS WHERE STATION_NUMBER = ?", (st,))}
    for st, row in rows.items():
        if _norm(row["remark"]) not in said[st]:
            raise ValueError(f"backwater: {st}: HYDAT holds no station remark {row['remark']!r} — the row's reason "
                             f"is not the record's")
    both = sorted(set(rows) & set(tidal_stations(hydat, tidal)))
    if both:
        raise ValueError(f"backwater: {both} also flagged tidal — one Level sentence per gauge")
    return {st: f"{row['from']} (HYDAT: {_norm(row['remark'])}): {row['why']}" for st, row in rows.items()}
