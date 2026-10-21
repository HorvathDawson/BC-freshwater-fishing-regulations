"""Fill this year's daily record back to 1 January — the part the 30-day archive cannot reach.

    python -m pipeline.gauges.feed.year_backfill --out <feed dir> [--stations 08MH001 ...]

WHY THIS EXISTS (ticket 2026-10-21, "gaps in the flow data on 'this year'"). The publisher
grows each station's `daily` one run at a time and can refill only what ECCC's 30-day
datamart archive still holds (`publish.daily_gaps`). Everything older than that — the feed's
own record for 08MH001 began 27 August — was simply absent, and the chart drew nothing for
January to August.

TWO OFFICIAL SOURCES, BOTH ECCC'S WATER OFFICE, tried in this order:

1. HYDAT DAILY MEANS — `services/daily_data/csv/inline`. ECCC's archive of APPROVED data,
   already daily, many stations per request. It is not a year behind for every station:
   on 2026-10-21 it held 08MH001 through 8 June 2026 and 08LF051 through 7 January. Rows
   from it are tagged "final".
2. REAL-TIME UNIT VALUES — `services/real_time_data/csv/inline`, parameters 46 (level)
   and 47 (discharge), 5-minute readings kept for about eighteen months. Averaged here into
   daily means in the station's local standard time (the same day boundary the datamart
   files and HYDAT use). Each reading carries ECCC's own Approval column; a day is tagged
   "final" only if every reading in it is, else "provisional".

THE TAG IS A FOURTH ELEMENT, `[day, level, discharge, "final" | "provisional"]`, on the rows
this writes and no others. Readers destructure three and are unaffected; a row without one
is the publisher's own (datamart real-time, i.e. provisional).

IT ONLY FILLS. A day the record already holds is never touched, and a day no source answers
for stays absent — the chart breaks its line there rather than drawing across it.

TERMS. Both services are ECCC's, under the Environment and Climate Change Canada Data
Servers End-use Licence (https://eccc-msc.github.io/open-data/licence/readme_en/), and the
Water Office's disclaimer (https://wateroffice.ec.gc.ca/disclaimer_info_e.html): public,
may be redistributed with an acknowledgement of Environment Canada, provisional data "have
been transmitted automatically with limited verification". No rate limit is published; the
service does answer bursts with a bare 422 that succeeds on retry, so requests here are
serial, spaced POLITE_S apart, and retried with backoff.

ONE AT A TIME WITH THE PUBLISHER. Both rewrite the station files; run this between publishes,
never during one, or the publish that read the file first writes back the old record.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from pipeline.gauges.feed.publish import UA, daily_gaps, day_runs

HYDAT_DAILY = "https://wateroffice.ec.gc.ca/services/daily_data/csv/inline"
REALTIME = "https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline"

#: Stations per HYDAT request. The response is daily, so ten stations for most of a year is
#: a few thousand rows.
HYDAT_BATCH = 10
#: Days per real-time request: two parameters at 5-minute steps is ~18,000 rows a month,
#: which the service answers; three months it refuses outright (422, every time).
CHUNK_DAYS = 31
#: Seconds between two requests to the Water Office. Serial, never parallel.
POLITE_S = 2.0
#: Tries per request; the wait doubles from BACKOFF_S. A bare 422 is the service's answer to
#: a burst as often as to a bad request, and it succeeds on the next try.
TRIES = 4
BACKOFF_S = 10.0

_LEVEL_RT, _FLOW_RT = "46", "47"
_SENTINEL = 99_000.0
PACIFIC = timedelta(hours=-8)

Fetch = Callable[[str], "tuple[int, str | None]"]


def _fetch(url: str) -> tuple[int, str | None]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=120) as fh:
            return fh.status, fh.read().decode("utf-8-sig", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, None
    except (urllib.error.URLError, OSError, TimeoutError):
        return 0, None


class Polite:
    """Serial requests, POLITE_S apart, retried with backoff. Counts what it asked."""

    def __init__(self, fetch: Fetch = _fetch, sleep: Callable[[float], None] = time.sleep,
                 polite_s: float = POLITE_S, tries: int = TRIES, backoff_s: float = BACKOFF_S):
        self.fetch, self.sleep = fetch, sleep
        self.polite_s, self.tries, self.backoff_s = polite_s, tries, backoff_s
        self.requests = 0
        self.failed: list[str] = []
        self._first = True

    def get(self, url: str) -> str | None:
        for i in range(self.tries):
            if not self._first:
                self.sleep(self.polite_s if i == 0 else self.backoff_s * 2 ** (i - 1))
            self._first = False
            self.requests += 1
            status, text = self.fetch(url)
            if status == 200 and text is not None:
                return text
        self.failed.append(url)
        return None


def _num(v: str | None) -> float | None:
    try:
        x = float(v) if v not in (None, "") else None
    except ValueError:
        return None
    return None if x is None or x >= _SENTINEL else x


def hydat_daily(text: str) -> dict[str, dict[str, tuple[float | None, float | None]]]:
    """`{station: {day: (level, discharge)}}` from the Water Office daily_data CSV."""
    out: dict[str, dict[str, list[float | None]]] = {}
    rows = csv.reader(io.StringIO(text))
    next(rows, None)
    for r in rows:
        if len(r) < 4:
            continue
        stn, day, param, v = r[0].strip(), r[1].strip()[:10], r[2].strip().lower(), _num(r[3])
        if not stn or not day or v is None:
            continue
        cell = out.setdefault(stn, {}).setdefault(day, [None, None])
        if param.startswith("water level"):
            cell[0] = v
        elif param.startswith("discharge"):
            cell[1] = v
    return {s: {d: (c[0], c[1]) for d, c in days.items()} for s, days in out.items()}


def realtime_daily(text: str, offset: timedelta
                   ) -> dict[str, dict[str, tuple[float | None, float | None, str]]]:
    """`{station: {local day: (level, discharge, "final"|"provisional")}}` from unit values.

    The mean of every reading on the station's LOCAL STANDARD day (`offset` from UTC) —
    the day the datamart files and HYDAT both use, so a backfilled day and a published one
    are means over the same 24 hours.
    """
    acc: dict[str, dict[str, list]] = {}
    rows = csv.reader(io.StringIO(text))
    next(rows, None)
    for r in rows:
        if len(r) < 7:
            continue
        stn, param, v = r[0].strip(), r[2].strip(), _num(r[3])
        if v is None or param not in (_LEVEL_RT, _FLOW_RT):
            continue
        try:
            t = datetime.fromisoformat(r[1].strip().replace("Z", "+00:00"))
        except ValueError:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        day = (t.astimezone(timezone.utc) + offset).date().isoformat()
        cell = acc.setdefault(stn, {}).setdefault(day, [[], [], True])
        cell[0 if param == _LEVEL_RT else 1].append(v)
        cell[2] = cell[2] and r[6].strip().lower().startswith("final")
    mean = lambda xs: round(sum(xs) / len(xs), 4) if xs else None      # noqa: E731
    return {s: {d: (mean(c[0]), mean(c[1]), "final" if c[2] else "provisional")
                for d, c in days.items()}
            for s, days in acc.items()}


def chunks(days: list[str], size: int = CHUNK_DAYS) -> list[tuple[str, str]]:
    """Missing days as request windows: each consecutive run, cut to at most `size` days."""
    out: list[tuple[str, str]] = []
    for a, b in day_runs(days):
        d, end = date.fromisoformat(a), date.fromisoformat(b)
        while d <= end:
            last = min(end, d + timedelta(days=size - 1))
            out.append((d.isoformat(), last.isoformat()))
            d = last + timedelta(days=1)
    return out


def _offset(at: str | None) -> timedelta:
    """The station's standard-time offset, read off its own last reading (-08:00 / -07:00)."""
    try:
        off = datetime.fromisoformat(str(at)).utcoffset()
    except ValueError:
        off = None
    return off if off is not None else PACIFIC


def _realtime_url(station: str, a: str, b: str, offset: timedelta) -> str:
    # Local midnight to local 23:59:59, said in UTC, so every day asked for is whole.
    start = datetime.fromisoformat(a) - offset
    end = datetime.fromisoformat(b) + timedelta(hours=23, minutes=59, seconds=59) - offset
    return REALTIME + "?" + urllib.parse.urlencode(
        [("stations[]", station), ("parameters[]", _LEVEL_RT), ("parameters[]", _FLOW_RT),
         ("start_date", start.strftime("%Y-%m-%d %H:%M:%S")),
         ("end_date", end.strftime("%Y-%m-%d %H:%M:%S"))])


def _hydat_url(stations: list[str], a: str, b: str) -> str:
    return HYDAT_DAILY + "?" + urllib.parse.urlencode(
        [("stations[]", s) for s in stations]
        + [("parameters[]", "level"), ("parameters[]", "flow"),
           ("start_date", a), ("end_date", b)])


def backfill(out: Path, today: date | None = None, stations: list[str] | None = None,
             net: Polite | None = None, use_realtime: bool = True) -> dict:
    """Fill each station file's missing days older than the archive window. Returns a summary."""
    net = net or Polite()
    files = ([out / f"{s}.json" for s in stations] if stations
             else sorted(p for p in out.glob("*.json")
                         if p.name not in ("index.json", "temp_history.json")))
    docs: dict[str, dict] = {}
    want: dict[str, set[str]] = {}
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        st = str(doc.get("station") or f.stem)
        day = today or date.fromisoformat(str((doc.get("now") or {}).get("at") or "")[:10]
                                          or datetime.now(timezone.utc).date().isoformat())
        beyond = daily_gaps(doc.get("daily") or [], day)[1]
        if beyond:
            docs[st], want[st] = doc, set(beyond)

    got: dict[str, dict[str, list]] = {st: {} for st in want}

    # 1. HYDAT, approved, many stations a request.
    ids = sorted(want)
    for i in range(0, len(ids), HYDAT_BATCH):
        batch = ids[i:i + HYDAT_BATCH]
        a = min(min(want[s]) for s in batch)
        b = max(max(want[s]) for s in batch)
        text = net.get(_hydat_url(batch, a, b))
        for st, days in hydat_daily(text or "").items():
            for d, (lv, q) in days.items():
                if st in want and d in want[st] and (lv is not None or q is not None):
                    got[st][d] = [d, lv, q, "final"]
                    want[st].discard(d)
    from_hydat = sum(len(v) for v in got.values())

    # 2. Real-time unit values, one station and at most a month a request.
    if use_realtime:
        for st in ids:
            off = _offset((docs[st].get("now") or {}).get("at"))
            for a, b in chunks(sorted(want[st])):
                text = net.get(_realtime_url(st, a, b, off))
                for d, (lv, q, tag) in (realtime_daily(text or "", off).get(st) or {}).items():
                    if d in want[st] and (lv is not None or q is not None):
                        got[st][d] = [d, lv, q, tag]
                        want[st].discard(d)

    for st, rows in got.items():
        if not rows:
            continue
        doc = docs[st]
        have = {str(r[0]): r for r in doc.get("daily") or []}
        for d, row in rows.items():
            old = have.get(d)
            # Never overwrite a day already held; a row with no value in it is not one.
            if old is None or (old[1] is None and old[2] is None):
                have[d] = row
        doc["daily"] = [have[d] for d in sorted(have)]
        (out / f"{st}.json").write_text(json.dumps(doc, separators=(",", ":")),
                                        encoding="utf-8")

    filled = sum(len(v) for v in got.values())
    return {"stations": len(ids), "requests": net.requests, "failed": len(net.failed),
            "filled": filled, "fromHydat": from_hydat, "fromRealtime": filled - from_hydat,
            "provisional": sum(1 for v in got.values() for r in v.values() if r[3] == "provisional"),
            "stillMissing": {st: day_runs(sorted(w)) for st, w in want.items() if w}}


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill this year's daily gauge record")
    ap.add_argument("--out", required=True, type=Path, help="the feed directory")
    ap.add_argument("--stations", nargs="*", help="only these station ids")
    ap.add_argument("--hydat-only", action="store_true",
                    help="skip the real-time unit values (one request per station-month)")
    args = ap.parse_args()
    s = backfill(args.out, stations=args.stations or None, use_realtime=not args.hydat_only)
    print(f"  ✅ {s['stations']} station records had days older than the archive missing; "
          f"filled {s['filled']} ({s['fromHydat']} HYDAT final, {s['fromRealtime']} real-time, "
          f"{s['provisional']} of them provisional) in {s['requests']} requests, "
          f"{s['failed']} failed")
    left = s["stillMissing"]
    if left:
        print(f"     {len(left)} still lack days no source holds — the chart leaves them empty")


if __name__ == "__main__":
    main()
