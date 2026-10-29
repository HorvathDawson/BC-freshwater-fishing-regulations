"""T274: HYDAT's DAILY MEANS per published station, as static files the app reads from OUR data host.

    python -m pipeline.gauges.feed.hydat_daily --hydat data/source/hydat.sqlite3 \
        --feeds data/generated/gauges/feeds --out data/generated/gauges/hydat_daily

The app used to ask ECCC's GeoMet API from the device for a past day's flow (`dayFlow.ts`). Owner 2026-10-29: "we should
have it stored". HYDAT changes once a release (quarterly), so this is a build step beside the climatology, uploaded to
R2 `feeds/hydat/` with the other feed seeds — not a worker job.

ONE FILE PER STATION THE FEED PUBLISHES (`<station>.json` beside `index.json`; only stations with any daily discharge):
    {"format": "hydat-daily/1", "station": "08MH024", "release": "202607171779871", "from": 1996,
     "q": [[366 values for 1996], ...]}          one array per year from `from` to the last year HYDAT holds, 366 slots
                                                 (day of year, leap day always slot 59), null = no mean that day
Values are 4 significant figures (what a chart can tell), flow only: a level never stands in for a flow.
THE WINDOW is the newest WINDOW_YEARS years of record (a spot's trip is recent); older days answer "no record".
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date
from pathlib import Path

from pipeline.gauges.feed.climatology import _sig4

FORMAT = "hydat-daily/1"
WINDOW_YEARS = 30
SKIP = {"index", "clim", "temp_history"}


def feed_stations(feeds: Path) -> list[str]:
    return sorted(p.stem for p in feeds.glob("*.json") if p.stem not in SKIP and not p.stem.startswith("feed"))


def build(hydat: Path, stations: list[str], release: str) -> dict[str, dict]:
    db = sqlite3.connect(f"file:{hydat}?mode=ro", uri=True)
    cols = ", ".join(f"FLOW{d}" for d in range(1, 32))
    by: dict[str, dict[int, list]] = {}
    for i in range(0, len(stations), 500):
        part = stations[i:i + 500]
        q = f"SELECT STATION_NUMBER, YEAR, MONTH, {cols} FROM DLY_FLOWS WHERE STATION_NUMBER IN ({','.join('?' * len(part))})"
        for row in db.execute(q, part):
            st, yr, mo = row[0], row[1], row[2]
            for d, v in enumerate(row[3:], start=1):
                if v is None:
                    continue
                try:
                    yday = date(2000, mo, d).timetuple().tm_yday      # a fixed leap year: Feb 29 is always slot 60
                except ValueError:
                    continue
                by.setdefault(st, {}).setdefault(yr, [None] * 366)[yday - 1] = _sig4(float(v))
    db.close()
    out: dict[str, dict] = {}
    for st, years in by.items():
        last = max(years)
        first = max(min(years), last - WINDOW_YEARS + 1)
        out[st] = {"format": FORMAT, "station": st, "release": release, "from": first,
                   "q": [years.get(y, [None] * 366) for y in range(first, last + 1)]}
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hydat", type=Path, default=Path("data/source/hydat.sqlite3"))
    ap.add_argument("--feeds", type=Path, default=Path("data/generated/gauges/feeds"))
    ap.add_argument("--out", type=Path, default=Path("data/generated/gauges/hydat_daily"))
    a = ap.parse_args()
    release = (a.hydat.parent / "hydat.release").read_text().strip()
    files = build(a.hydat, feed_stations(a.feeds), release)
    a.out.mkdir(parents=True, exist_ok=True)
    for old in a.out.glob("*.json"):
        if old.stem not in files:
            old.unlink()
    total = 0
    for st, doc in files.items():
        s = json.dumps(doc, separators=(",", ":"))
        (a.out / f"{st}.json").write_text(s)
        total += len(s)
    print(f"{len(files)} stations, {total / 1e6:.1f} MB (release {release})")


if __name__ == "__main__":
    main()
