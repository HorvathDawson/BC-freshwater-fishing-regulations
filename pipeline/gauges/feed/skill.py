"""THE MODELS' OWN TRACK RECORD (T177): every forecast run the feed fetched, kept small, and scored against what the
river then did.

    python -m pipeline.gauges.feed.skill --feed data/generated/gauges/feeds          # the skill table, per lead day

WHY THIS EXISTS. The owner asked which of CLEVER and COFFEE is better, to weigh them where both forecast a day
(Q160). Nobody publishes a head-to-head verification of the two (T144's research; T177 searched again), and the feed
kept only the newest run per model — it overwrites the run it scored nothing against. So the only way to ever have an
answer is to start keeping the runs. Measured on 2026-10-24 the machine held runs from TWO issue days that can be
scored (the test fixtures of 2026-10-15 and 2026-10-22): five stations paired at lead 1, one at leads 1–4. Too few to
weigh anything (T177 ticket has the table), so the combination stays EQUAL (`change.FLOOD_WEIGHTS`) until this file says
otherwise.

THE ARCHIVE: `<feed dir>.runs/<YYYY-MM>.jsonl`, BESIDE the feed directory, never inside it — the feed directory is
what is published (the notifier's state lives beside it for the same reason, `notify.state_path`). One line per run,
appended the first time the feed sees its (station, model, issue time); a run carried forward unchanged is not written
twice. A line is the run's DAILY MEANS (`change.day_means`, the numbers every ranked day is made of) for today and the
AHEAD_DAYS after it, the gauge's reading when it was archived, and `scale` — what `change.gauge_scale` multiplies the
model by before ranking (COFFEE's peaks onto the gauge). So the score ranks exactly what the feed would have shown.
Bounded: ARCHIVE_MONTHS months are kept, older ones deleted by the publisher; a month older than the last two is
gzipped (`<YYYY-MM>.jsonl.gz`). Measured on the scratch republish of 2026-10-24 (T177): 825 runs, 270,632 bytes (328 B
a run), 42,494 gzipped (6.4x) — about 8 MB a month raw at ~825 new runs a day, ~1.3 MB gzipped, so fourteen months is
~2 x 8 + 12 x 1.3 ≈ 32 MB.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from pipeline.gauges.feed.change import (AHEAD_DAYS, CENTRE_ZONE, FLOOD_MODELS, LOW_WATER_MODEL, combine_day,
                                         day_means, gauge_scale, stale)
from pipeline.gauges.feed.forecast import instant

#: Month files kept. Fourteen: a whole year of seasons (freshet, low water, fall storms) plus the month on either side,
#: so a season can be scored against the same season's runs a year later.
ARCHIVE_MONTHS = 14

#: The lead days scored: the strip's coming days (1–5) and the map's last horizon (10).
LEADS = (1, 2, 3, 4, 5, 10)

#: The band the app shows (core `standing`, collapsed to three): below p25 low, p75 and over high, else normal.
LOW, HIGH = 0.25, 0.75


def archive_dir(out: Path) -> Path:
    """`<feed dir>.runs`, beside the published directory (resolved first: `--out .` must not name a dir inside it)."""
    out = out.resolve()
    return out.parent / f"{out.name}.runs"


def run_record(station: str, model: str, run: dict, now: datetime, observed: float | None) -> dict | None:
    """One run as the archive keeps it, or None when it has nothing to keep (no series, stale, no day ahead)."""
    series = (run or {}).get("series")
    if not series or stale(run, now):
        return None
    today = now.astimezone(CENTRE_ZONE).date()
    last = today + timedelta(days=AHEAD_DAYS)
    days = {d: round(v, 4) for d, v in sorted(day_means(series).items()) if today.isoformat() <= d <= last.isoformat()}
    if not days:
        return None
    s = gauge_scale(model, run, now, observed)
    return {"station": station, "model": model, "issuedAt": run.get("issuedAt"),
            "at": now.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "observed": observed, "scale": None if s is None else round(s, 6), "days": days}


def _key(r: dict) -> tuple[str, str, str]:
    return (str(r.get("station")), str(r.get("model")), str(r.get("issuedAt")))


def _month(f: Path) -> str:
    return f.name.split(".", 1)[0]


def _months(folder: Path) -> dict[str, list[Path]]:
    got: dict[str, list[Path]] = defaultdict(list)
    for f in sorted([*folder.glob("*.jsonl"), *folder.glob("*.jsonl.gz")]):
        got[_month(f)].append(f)
    return dict(sorted(got.items()))


def read_runs(folder: Path, months: Iterable[str] | None = None) -> list[dict]:
    """Every archived run (or those of `months`, `YYYY-MM`), oldest month first; gzipped months too. A torn line is
    skipped."""
    if not folder.is_dir():
        return []
    want = set(months) if months is not None else None
    out: list[dict] = []
    for month, files in _months(folder).items():
        if want is not None and month not in want:
            continue
        for f in files:
            text = (gzip.decompress(f.read_bytes()).decode("utf-8") if f.suffix == ".gz"
                    else f.read_text(encoding="utf-8"))
            for line in text.splitlines():
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    return out


def append_runs(out: Path, forecast: dict | None, now: datetime, observed: dict[str, float | None]) -> dict:
    """Append every run not yet archived; gzip months older than the last two; delete months past ARCHIVE_MONTHS.
    Returns `{added, file, bytes}`.

    A run is new when its (station, model, issuedAt) is in neither this month's file nor last month's (a run issued on
    the 31st is first published on the 1st). The publisher catches anything this raises: the archive is a by-product."""
    folder = archive_dir(out)
    folder.mkdir(parents=True, exist_ok=True)
    month = now.astimezone(CENTRE_ZONE).strftime("%Y-%m")
    first = now.astimezone(CENTRE_ZONE).date().replace(day=1)
    prev = (first - timedelta(days=1)).strftime("%Y-%m")
    seen = {_key(r) for r in read_runs(folder, (month, prev))}
    lines: list[str] = []
    for st, runs in sorted((forecast or {}).items()):
        for model, run in sorted((runs or {}).items()):
            rec = run_record(st, model, run, now, observed.get(st))
            if rec is None or _key(rec) in seen:
                continue
            seen.add(_key(rec))
            lines.append(json.dumps(rec, separators=(",", ":"), sort_keys=True))
    path = folder / f"{month}.jsonl"
    if lines:
        with path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    months = _months(folder)
    for m in list(months)[:-ARCHIVE_MONTHS] if len(months) > ARCHIVE_MONTHS else []:
        for f in months.pop(m):
            f.unlink()
    for m, files in months.items():
        if m in (month, prev):
            continue
        for f in files:
            if f.suffix == ".jsonl":                       # a closed month: gzipped once, the plain file removed
                (folder / f"{m}.jsonl.gz").write_bytes(gzip.compress(f.read_bytes(), mtime=0))
                f.unlink()
    return {"added": len(lines), "file": str(path), "bytes": path.stat().st_size if path.exists() else 0}


# ---------------------------------------------------------------------------------------------------- scoring ----

Rank = Callable[[float, date], "float | None"]


def band(p: float) -> str:
    return "low" if p < LOW else "high" if p >= HIGH else "normal"


def _issue_day(r: dict) -> date | None:
    t = instant(r.get("issuedAt"))
    return None if t is None else t.astimezone(CENTRE_ZONE).date()


def cases(records: list[dict], observed: dict[str, dict[str, float]], before: date) -> list[dict]:
    """`[{station, issued, lead, day, obs, by: {model: value on the gauge's scale}}]` — one per station, issue day and
    lead, each model's LAST run of that issue day, and only days that are over (`< before`) with an observed mean."""
    last: dict[tuple[str, date, str], dict] = {}
    for r in records:
        d0 = _issue_day(r)
        if d0 is None or r.get("scale") is None:
            continue
        k = (r["station"], d0, r["model"])
        if k not in last or str(r.get("issuedAt")) > str(last[k].get("issuedAt")):
            last[k] = r
    grouped: dict[tuple[str, date], dict[str, dict]] = defaultdict(dict)
    for (st, d0, m), r in last.items():
        grouped[(st, d0)][m] = r
    out: list[dict] = []
    for (st, d0), runs in sorted(grouped.items()):
        for lead in range(1, AHEAD_DAYS + 1):
            day = d0 + timedelta(days=lead)
            obs = (observed.get(st) or {}).get(day.isoformat())
            if day >= before or obs is None or obs <= 0:
                continue
            by = {m: r["days"][day.isoformat()] * r["scale"] for m, r in runs.items()
                  if day.isoformat() in r["days"] and r["days"][day.isoformat()] > 0}
            if by:
                out.append({"station": st, "issued": d0.isoformat(), "lead": lead, "day": day.isoformat(),
                            "obs": obs, "by": by})
    return out


def _stats(pairs: list[tuple[float, float, float | None, float | None]]) -> dict:
    """`pairs` of (forecast, observed, forecast percentile, observed percentile)."""
    if not pairs:
        return {"n": 0}
    e = [math.log2(f / o) for f, o, _, _ in pairs]
    ranked = [(pf, po) for _, _, pf, po in pairs if pf is not None and po is not None]
    return {"n": len(pairs), "bias": round(sum(e) / len(e), 3), "mae": round(sum(abs(x) for x in e) / len(e), 3),
            **({"pmae": round(sum(abs(a - b) for a, b in ranked) / len(ranked), 3),
                "hit": round(sum(band(a) == band(b) for a, b in ranked) / len(ranked), 3), "nr": len(ranked)}
               if ranked else {})}


def score(found: list[dict], rank_for: Callable[[str], Rank | None], leads: Iterable[int] = LEADS) -> dict:
    """The table: per lead day, each model on every case it answered (`own`), and on the cases where CLEVER and COFFEE
    BOTH answered (`paired`) CLEVER, COFFEE and their combination (`combine_day`, the feed's own) side by side.
    Errors in log2 (bias: mean signed, + = too high; mae: mean absolute — 1.0 is a factor of two), `pmae` on the
    percentile scale the app shows (0–1), `hit` the share in the same low/normal/high band as what happened."""
    table: dict = {}
    for lead in leads:
        at = [c for c in found if c["lead"] == lead]
        if not at:
            continue

        def row(c: dict, f: float) -> tuple[float, float, float | None, float | None]:
            rank = rank_for(c["station"])
            day = date.fromisoformat(c["day"])
            return (f, c["obs"], rank(f, day) if rank else None, rank(c["obs"], day) if rank else None)

        own = {m: _stats([row(c, c["by"][m]) for c in at if m in c["by"]]) for m in FLOOD_MODELS + (LOW_WATER_MODEL,)}
        both = [c for c in at if all(m in c["by"] for m in FLOOD_MODELS)]
        paired = {m: _stats([row(c, c["by"][m]) for c in both]) for m in FLOOD_MODELS}
        paired["combined"] = _stats([row(c, combine_day(c["by"])[1]) for c in both])  # type: ignore[index]
        if any(LOW_WATER_MODEL in c["by"] for c in both):
            paired[LOW_WATER_MODEL] = _stats([row(c, c["by"][LOW_WATER_MODEL]) for c in both if LOW_WATER_MODEL in c["by"]])
        # THE HEAD-TO-HEAD: per paired case, CLEVER's absolute log2 error less COFFEE's (+ = COFFEE closer)
        diffs = [abs(math.log2(c["by"][FLOOD_MODELS[0]] / c["obs"])) - abs(math.log2(c["by"][FLOOD_MODELS[1]] / c["obs"]))
                 for c in both]
        if len(diffs) >= 2:
            mean = sum(diffs) / len(diffs)
            sd = math.sqrt(sum((x - mean) ** 2 for x in diffs) / (len(diffs) - 1))
            paired["difference"] = {"n": len(diffs), "mean": round(mean, 3), "se": round(sd / math.sqrt(len(diffs)), 3)}
        table[str(lead)] = {"own": own, "paired": paired,
                            "stations": len({c["station"] for c in at}), "pairedStations": len({c["station"] for c in both}),
                            "issueDays": len({c["issued"] for c in at})}
    return table


#: WHEN THE SCORE MAY MOVE THE WEIGHTS. Pairs on one issue day share one storm, so they are not independent: the
#: score needs both many pairs and many issue days. 200 pairs puts the standard error of a mean difference near 0.035
#: log2 at the spread seen so far (sd about 0.5 without the one 10x miss); 20 issue days is about four weeks of
#: COFFEE's storm season (it runs only when rain is on or forecast).
MIN_PAIRS = 200
MIN_ISSUE_DAYS = 20


def verdict(row: dict) -> dict:
    """THE RULE (T177) for one lead day's row of `score`: `{"weights": {model: w}, "why": ...}`.

      - too few paired cases or issue days -> EQUAL (what the feed ships, `change.FLOOD_WEIGHTS`);
      - the mean paired difference within two standard errors of zero -> EQUAL: neither is better;
      - else INVERSE ERROR: each model weighted by 1 / its paired mean absolute log2 error, the better one at 1.
    The feed never re-weights itself (a forecast must not change because a file grew overnight): the CLI prints this,
    and a person moves `FLOOD_WEIGHTS` with the table in the T177 ticket."""
    c, f = FLOOD_MODELS
    equal = {c: 1.0, f: 1.0}
    d = (row.get("paired") or {}).get("difference")
    n, days = (d or {}).get("n", 0), row.get("issueDays", 0)
    if n < MIN_PAIRS or days < MIN_ISSUE_DAYS:
        return {"weights": equal, "why": f"too few: {n} paired cases over {days} issue day(s) "
                                         f"(needs {MIN_PAIRS} over {MIN_ISSUE_DAYS})"}
    if abs(d["mean"]) <= 2 * d["se"]:
        return {"weights": equal, "why": f"neither better: difference {d['mean']} within 2 x {d['se']}"}
    inv = {m: 1.0 / max(row["paired"][m]["mae"], 1e-6) for m in (c, f)}
    top = max(inv.values())
    return {"weights": {m: round(v / top, 2) for m, v in inv.items()},
            "why": f"{c if d['mean'] < 0 else f} better: difference {d['mean']}, se {d['se']}"}


def feed_observed(feed: Path) -> dict[str, dict[str, float]]:
    """`{station: {day: discharge daily mean}}` from the feed's station files (`daily`, this year's record)."""
    out: dict[str, dict[str, float]] = {}
    for f in sorted(feed.glob("*.json")):
        if f.name in ("index.json", "clim.json", "stats.json", "temp_history.json"):
            continue
        try:
            rows = json.loads(f.read_text(encoding="utf-8")).get("daily") or []
        except ValueError:
            continue
        got = {str(r[0]): float(r[2]) for r in rows if len(r) >= 3 and r[2] is not None}
        if got:
            out[f.stem] = got
    return out


def clim_ranks(clim: dict | None) -> Callable[[str], Rank | None]:
    """A station's day-ranking against its discharge envelope — the publisher's `rank_day`, the same two functions."""
    from pipeline.gauges.feed.publish import band_for, percentile_of     # noqa: PLC0415 — publish imports this file
    stations = (clim or {}).get("stations") or {}

    def of(st: str) -> Rank | None:
        env = (stations.get(st) or {}).get("discharge")
        if not env:
            return None
        return lambda v, d: percentile_of(v, band_for(env, datetime(d.year, d.month, d.day, 12, tzinfo=timezone.utc)))
    return of


def main() -> None:
    ap = argparse.ArgumentParser(description="Score the archived forecast runs against what happened")
    ap.add_argument("--feed", required=True, type=Path)
    ap.add_argument("--clim", type=Path, help="default: <feed>/clim.json")
    a = ap.parse_args()
    clim_path = a.clim or a.feed / "clim.json"
    clim = json.loads(clim_path.read_text(encoding="utf-8")) if clim_path.exists() else None
    records = read_runs(archive_dir(a.feed))
    today = datetime.now(timezone.utc).astimezone(CENTRE_ZONE).date()
    found = cases(records, feed_observed(a.feed), today)
    table = score(found, clim_ranks(clim))
    print(json.dumps({"runs": len(records), "cases": len(found), "table": table,
                      "verdict": {lead: verdict(row) for lead, row in table.items()}}, indent=1))


if __name__ == "__main__":
    main()
