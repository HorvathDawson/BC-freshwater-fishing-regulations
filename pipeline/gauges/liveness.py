"""STATION LIVENESS — the one place a reader learns whether a gauge station may speak, and whether it is speaking (T122,
owner 2026-10-22: "unify checks that decide the same thing").

TWO QUESTIONS, NEVER ONE. "Live" meant three things across `pipeline/gauges` and the bundle — ECCC's `active` status,
the roster's `realtime` flag, ECCC's real-time station list — and each reader picked one:

  may_speak(station)        MAY THIS STATION SPEAK FOR A WATER AT ALL? ECCC's own durable STATUS (`active`: the OGC
                            API's `STATUS_EN == "Active"`, `data/fetch_data.py`). A seasonal gauge shut for the winter
                            is active and silent; one discontinued in 1974 is neither. The bundle builds gauge sheds
                            from these only (`build.py` → `build_gauge_sheds(prefer=…)`): filtering on today's traffic
                            would delete a river's gauge every autumn and rebuild it every spring.
  speaks_now(station, feed) IS IT TRANSMITTING NOW? Membership of ECCC's real-time station list (`STATION_LIST`) — the
                            list as fetched THIS RUN (`feed`, from `listed_stations`) when there is one, else the
                            roster's `realtime` flag, which is that same list as `data/fetch_data.py` read it on the
                            day the roster was fetched (a snapshot: `roster_drift` says how far it has moved).

They are independent facts. On the roster of 2026-10-23 every transmitting station is active (428 of 466), and 38
active stations are silent (seasonal, or dropped from the list); ECCC's list that day held 4 stations the roster has
never seen (07FA008, 08DA013, 08DB015, 08FB012) — the feed asks them, the bundle cannot place them.

WHO ASKS WHICH. May speak: the bundle's shed filter. Speaks now: the shed's tie-break between two stations of equal
ratio (the roster snapshot), the atlas's gauge cuts (`consume.cuts.split_defs`, the roster snapshot — see T122's owner
question), the feed's list of stations to ask (`stations_to_ask`, today's list), and the transmitting counts the
bundle build and `generate.promote` print. Nothing else in `pipeline/` or `data/fetch_data.py` reads `active`,
`realtime` or the ECCC list (`test_liveness.py`, the grep pin).

No I/O: the feed fetches `STATION_LIST` and hands the text to `listed_stations`, so this module is importable from
`feed/`, `consume/`, `generate/` and the bundle alike.
"""
from __future__ import annotations

import csv
import io
from typing import Iterable, Mapping, Optional

#: ECCC's list of stations publishing real-time data today. `data/fetch_data.py` reads it once to set the roster's
#: `realtime` flag; the feed reads it every run.
STATION_LIST = "https://dd.weather.gc.ca/today/hydrometric/doc/hydrometric_StationList.csv"


def may_speak(station: Mapping) -> bool:
    """May this roster row speak for a water at all: ECCC's own status is Active (`active`)."""
    return bool(station.get("active"))


def speaks_now(station: Mapping, feed: Optional[set[str]] = None) -> bool:
    """Is this roster row transmitting now: on ECCC's real-time list `feed` (this run's, `listed_stations`), or — with
    no list — on the roster's snapshot of it (`realtime`)."""
    if feed is not None:
        return station.get("station") in feed
    return bool(station.get("realtime"))


def listed_stations(text: Optional[str]) -> Optional[set[str]]:
    """The BC stations ECCC's real-time list (`STATION_LIST`, decoded text) names, or None when it says nothing usable.

    NONE, NOT EMPTY, for a failed or empty answer: an empty set would claim no gauge in the province is transmitting and
    empty the feed on the strength of one bad response."""
    if not text:
        return None
    out = {(r.get("ID") or "").strip() for r in csv.DictReader(io.StringIO(text))
           if (r.get("Prov/Terr") or "").strip() == "BC"}
    out.discard("")
    return out or None


def stations_to_ask(roster: Iterable[Mapping], listed: Optional[set[str]]) -> tuple[list[str], str]:
    """`(ids, source)`: the stations that speak now — ECCC's list when there is one (`"eccc"`), else the roster's
    snapshot (`"roster"`).

    The list wins outright, including over a station the roster never heard of — the feed is addressed by station id
    and knows nothing else. A station the roster calls live and ECCC no longer lists is not asked: its answer would be
    a 404."""
    if listed is not None:
        return sorted(listed), "eccc"
    return sorted(s["station"] for s in roster if speaks_now(s)), "roster"


def roster_drift(roster: Iterable[Mapping], listed: Optional[set[str]]) -> Optional[dict]:
    """How far the roster's snapshot is from ECCC's list today. None without the list.

    `joined`: listed now, not live in the roster. `left`: live in the roster, no longer listed. The donor panels no
    longer read the roster flag (owner 2026-10-22, "chosen live": which members answer is decided from the feed at
    read time), so drift costs only the roster's own accuracy — said every run, like a stale HYDAT."""
    if listed is None:
        return None
    was = {s["station"] for s in roster if speaks_now(s)}
    return {"joined": sorted(listed - was), "left": sorted(was - listed)}
