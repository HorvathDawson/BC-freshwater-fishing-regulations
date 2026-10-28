"""T225 gauge parity fixtures: build the FULL seed (from the live dev feed) and cut the SMALL committed set from a full
recording.

    .venv/bin/python app/workers/feeds/parity/gauges/fixtures.py seed-full --feed data/generated/gauges/feeds \
        --out app/workers/feeds/fixtures-full/gauges/seed
    .venv/bin/python app/workers/feeds/parity/gauges/harness.py record --fixtures app/workers/feeds/fixtures-full/gauges \
        --seed app/workers/feeds/fixtures-full/gauges/seed
    .venv/bin/python app/workers/feeds/parity/gauges/fixtures.py small --full app/workers/feeds/fixtures-full/gauges \
        --out app/workers/feeds/test/fixtures/gauges

THE SEED IS THE LIVE FEED WITH HOLES CUT IN IT, so one run exercises every path: two stations with no file (a first
run: the whole 30-day archive), three with days missing inside the archive window (a refill), five with their
forecast runs dropped (a series fetch instead of a carry), and the runs archive as it stands.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import shutil
import sys
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO))


def seed_full(feed: Path, runs: Path, out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    (out / "feed").mkdir(parents=True)
    (out / "priv" / "runs").mkdir(parents=True)
    for f in sorted(feed.glob("*.json")):
        if f.name == "stats.json":
            continue
        shutil.copy(f, out / "feed" / f.name)
    for f in sorted(runs.glob("*.jsonl")):
        shutil.copy(f, out / "priv" / "runs" / f.name)
    idx = json.loads((out / "feed" / "index.json").read_text())
    st = sorted(idx["stations"])
    today = date.fromisoformat(idx["fetchedAt"][:10])
    edits = {"noFile": [st[10], st[200]], "holes": [st[20], st[150], st[300]], "noForecast": []}
    for s in edits["noFile"]:
        (out / "feed" / f"{s}.json").unlink()
    holes = {(today - timedelta(days=k)).isoformat() for k in (3, 4, 5, 12)}
    for s in edits["holes"]:
        p = out / "feed" / f"{s}.json"
        d = json.loads(p.read_text())
        d["daily"] = [r for r in d["daily"] if r[0] not in holes]
        p.write_text(json.dumps(d, separators=(",", ":")))
    for s in st:
        if len(edits["noForecast"]) >= 5:
            break
        p = out / "feed" / f"{s}.json"
        if not p.exists() or s in edits["holes"]:
            continue
        d = json.loads(p.read_text())
        if any((r or {}).get("series") for r in (d.get("forecasts") or {}).values()) and st.index(s) % 7 == 0:
            d["forecasts"] = {}
            p.write_text(json.dumps(d, separators=(",", ":")))
            edits["noForecast"].append(s)
    (out / "edits.json").write_text(json.dumps(edits, indent=1))
    print(json.dumps(edits))


# ------------------------------------------------------------------------------------------------ small set ----

def _body(full: Path, man: dict, url: str) -> bytes:
    return (full / "bodies" / man["urls"][url]["file"]).read_bytes()


def small(full: Path, out: Path, n_extra: int) -> None:
    from pipeline.gauges.feed import publish
    from pipeline.gauges.liveness import STATION_LIST
    man = json.loads((full / "manifest.json").read_text())
    U = man["urls"]
    edits = json.loads((full / "seed" / "edits.json").read_text())
    listed_text = _body(full, man, STATION_LIST).decode("utf-8-sig")
    hourly = {u.split("/BC_")[1].split("_")[0]: u for u in U if "/hourly/" in u}
    daily = {u.split("/BC_")[1].split("_")[0]: u for u in U if "/daily/" in u}
    answered = {}
    for s, u in hourly.items():
        if U[u]["status"] == 200:
            r = publish._parse_hourly(s, _body(full, man, u).decode("utf-8-sig", "replace"))
            if r:
                answered[s] = r
    silent = sorted(s for s, u in hourly.items() if s not in answered)
    temps_rows: dict[str, list[str]] = {}
    temp_urls = [u for u in U if u.startswith(publish._WATEROFFICE)]
    for u in temp_urls:
        if U[u]["status"] != 200:
            continue
        for line in _body(full, man, u).decode("utf-8-sig", "replace").splitlines():
            row = next(csv.reader([line]), [])
            if row and row[0].strip() in answered:
                temps_rows.setdefault(row[0].strip(), []).append(line)
    temp_head = None
    for u in temp_urls:
        lines = _body(full, man, u).decode("utf-8-sig", "replace").splitlines()
        if lines and lines[0].startswith(("ID", " ID", "﻿ID")):
            temp_head = lines[0]
            break
    series = {}
    for u in U:
        if "bcrfc.env.gov.bc.ca" in u:
            series.setdefault(u, U[u])
    forecasted = set()
    for u in series:
        name = u.rsplit("/", 1)[1]
        s = name.replace("COFFEE_", "").split("_")[0].split(".")[0]
        forecasted.add(s)

    seed_feed = full / "seed" / "feed"
    prev = {}
    for s in answered:
        p = seed_feed / f"{s}.json"
        prev[s] = json.loads(p.read_text()) if p.exists() else None

    def has_level_only(s):
        r = answered[s]
        return r["discharge"] is None and r["level"] is not None

    pick: list[str] = []

    def add(s):
        if s and s not in pick:
            pick.append(s)

    # one of each cut (an archive file is ~450 KB), plus a station whose archive 404s if the recording has one
    for s in edits["noFile"][:1] + edits["holes"][:1] + edits["noForecast"]:
        if s in answered:
            add(s)
    for s, u in sorted(daily.items()):
        if U[u]["status"] != 200 and s in answered:
            add(s)
            break
    for s in sorted(forecasted & set(answered))[:4]:
        add(s)
    for s in [s for s in sorted(answered) if has_level_only(s)][:3]:
        add(s)
    for s in [s for s in sorted(answered) if s in temps_rows][:3]:
        add(s)
    models = {}
    for s in sorted(answered):
        p = prev.get(s) or {}
        for m in (p.get("forecasts") or {}):
            models.setdefault(m, []).append(s)
    for m, ss in sorted(models.items()):
        for s in ss[:2]:
            add(s)
    for s in sorted(answered)[::max(1, len(answered) // n_extra)][:n_extra]:
        add(s)
    pick = sorted(pick)
    # one listed station that does not answer: a real one when the recording has it, else a made-up id whose hourly
    # file is a 404 (ECCC lists stations that are silent today)
    ghost = silent[0] if silent else "08ZZ999"
    ask = sorted(set(pick) | {ghost})

    # roster: picked stations live; one live-in-roster-not-listed ("left"); one listed-not-live ("joined")
    roster = json.loads((full / "roster.json").read_text())
    by = {r["station"]: r for r in roster}
    keep = []
    for s in ask:
        r = dict(by.get(s) or {"station": s, "name": s, "lon": 0, "lat": 0, "realtime": True, "active": True})
        r["realtime"] = True
        keep.append(r)
    keep[0]["realtime"] = False                      # joined
    gone = next(r for r in roster if r["station"] not in hourly and r.get("realtime") is not None)
    keep.append({**gone, "realtime": True})          # left
    out.mkdir(parents=True, exist_ok=True)
    if (out / "bodies").exists():
        shutil.rmtree(out / "bodies")
    (out / "bodies").mkdir()
    (out / "roster.json").write_text(json.dumps(keep, indent=0))

    urls: dict[str, dict] = {}

    def put(url: str, status: int, body: bytes):
        import hashlib
        name = hashlib.sha1(url.encode()).hexdigest()[:20]
        (out / "bodies" / name).write_bytes(body)
        urls[url] = {"status": status, "file": name}

    # ECCC list: header + the asked rows
    lines = listed_text.splitlines()
    rows = [lines[0]] + [ln for ln in lines[1:] if next(csv.reader([ln]), [""])[0].strip() in ask]
    if ghost not in hourly:
        lines.append(f'{ghost},"MADE-UP SILENT STATION (parity fixture)",49.0,-123.0,BC,UTC-08:00')
        rows.append(lines[-1])
        put(f"{publish.BASE}/hourly/BC_{ghost}_hourly_hydrometric.csv", 404, b"Not Found")
    put(STATION_LIST, 200, ("\n".join(rows) + "\n").encode())
    for s in ask:
        if s not in hourly:
            continue
        u = hourly[s]
        put(u, U[u]["status"], _body(full, man, u))
        if s in daily:
            put(daily[s], U[daily[s]]["status"], _body(full, man, daily[s]))
    for u in U:
        if "Hydat" in u or "hydrometrics/www" in u:
            put(u, U[u]["status"], _body(full, man, u))
        if "arcgis.com" in u:
            d = json.loads(_body(full, man, u))
            d["features"] = [f for f in d.get("features", [])
                             if str((f.get("attributes") or {}).get("Station_ID") or "").strip() in ask]
            put(u, U[u]["status"], json.dumps(d).encode())
        if "bcrfc.env.gov.bc.ca" in u:
            name = u.rsplit("/", 1)[1]
            s = name.replace("COFFEE_", "").split("_")[0].split(".")[0]
            if s in ask:
                put(u, U[u]["status"], _body(full, man, u))
    # the one temperature batch the small run asks: the answered picked stations, at the recording's clock
    now = datetime.fromisoformat(man["now"].replace("Z", "+00:00"))
    got = [s for s in ask if s in answered]
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")
    q = urllib.parse.urlencode([("stations[]", s) for s in got]
                               + [("parameters[]", publish._TEMP_PARAM), ("start_date", start), ("end_date", end)])
    tl = ([temp_head] if temp_head else []) + [ln for s in got for ln in temps_rows.get(s, [])]
    put(f"{publish._WATEROFFICE}?{q}", 200, ("\n".join(tl) + "\n").encode())
    (out / "manifest.json").write_text(json.dumps({"now": man["now"], "urls": dict(sorted(urls.items()))}, indent=0))

    # seed: the picked stations' files, a trimmed index/clim/temp history/runs archive
    seed = out / "seed"
    if seed.exists():
        shutil.rmtree(seed)
    (seed / "feed").mkdir(parents=True)
    (seed / "priv" / "runs").mkdir(parents=True)
    for s in ask:
        p = seed_feed / f"{s}.json"
        if p.exists():
            shutil.copy(p, seed / "feed" / p.name)
    idx = json.loads((seed_feed / "index.json").read_text())
    (seed / "feed" / "index.json").write_text(json.dumps(
        {"fetchedAt": idx["fetchedAt"], "maintenance": idx.get("maintenance"), "stations": {}},
        separators=(",", ":")))
    clim = json.loads((seed_feed / "clim.json").read_text())
    small_clim = {k: v for k, v in clim.items() if k not in ("stations", "stats", "recentYears")}
    small_clim["release"] = "20250117"               # an older envelope: the run raises a `hydat` notice
    small_clim["stations"] = {s: clim["stations"][s] for s in ask if s in clim["stations"]}
    small_clim["stats"] = {s: clim["stats"][s] for s in ask if s in clim.get("stats", {})}
    small_clim["recentYears"] = {s: clim["recentYears"][s] for s in ask if s in clim.get("recentYears", {})}
    (seed / "feed" / "clim.json").write_text(json.dumps(small_clim, separators=(",", ":")))
    th = json.loads((seed_feed / "temp_history.json").read_text())
    (seed / "feed" / "temp_history.json").write_text(json.dumps(
        {s: v for s, v in th.items() if s in ask}, separators=(",", ":")))
    for f in sorted((full / "seed" / "priv" / "runs").glob("*.jsonl")):
        kept = [ln for ln in f.read_text().splitlines() if ln and json.loads(ln).get("station") in ask]
        (seed / "priv" / "runs" / f.name).write_text("".join(ln + "\n" for ln in kept))
    (seed / "priv" / "notified.json").write_text(json.dumps({"sent": ["roster:+07FA008"]}))
    (out / "stations.json").write_text(json.dumps({"ask": ask, "silent": [s for s in ask if s not in answered]}))
    if (out / "_work").exists():
        shutil.rmtree(out / "_work")
    print(f"small set: {len(ask)} stations, {len(urls)} urls")


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("seed-full")
    a.add_argument("--feed", type=Path, required=True)
    a.add_argument("--out", type=Path, required=True)
    b = sub.add_parser("small")
    b.add_argument("--full", type=Path, required=True)
    b.add_argument("--out", type=Path, required=True)
    b.add_argument("--extra", type=int, default=6)
    args = ap.parse_args()
    if args.cmd == "seed-full":
        seed_full(args.feed, Path(str(args.feed) + ".runs"), args.out)
    else:
        small(args.full, args.out, args.extra)


if __name__ == "__main__":
    main()
