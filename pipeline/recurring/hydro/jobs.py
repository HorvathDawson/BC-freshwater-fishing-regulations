"""hydro.jobs — the single portable entrypoint for the (now unified) hydro cron.

    python -m pipeline.recurring.hydro.jobs run            # the one cron
    python -m pipeline.recurring.hydro.jobs run --local DIR  # dry run vs a local dir

ONE job does everything, gating internally by cheap change-detection instead of
running on separate cadences:

  * EVERY run  — fetch latest conditions/readings/forecasts and (re)write
    ``stations.json`` (the live map index), ``gauges.geojson`` (map points), and
    ``recent/<id>.json`` (the chart time-series). The cron trigger cadence (~30 min)
    sets how fresh these are.
  * DAILY      — merge fresh daily means into ``history/<id>.json`` (pull-merge-push,
    idempotent by date), gated by the ``history_date.json`` marker.
  * HYDAT      — rebuild ``climatology/<id>.json`` + refresh station coords, gated by
    comparing the latest HYDAT *listing date* (no 266 MB download to check) to the
    ``hydat_release.json`` marker.

STATE lives in the JSON artifacts themselves — there is NO persisted database in
R2. Each run uses an ephemeral ``/tmp`` sqlite purely as scratch (so it can reuse
hydro_poc's fetchers + export_hydro's shapers), seeding the station roster/coords
from the prior ``stations.json`` and merging history at the JSON level. First run
(no prior ``stations.json``) does a full bootstrap incl. HYDAT.

R2 credentials (env): ``R2_S3_ENDPOINT`` (or ``CLOUDFLARE_ACCOUNT_ID``),
``R2_ACCESS_KEY_ID``, ``R2_SECRET_ACCESS_KEY``, ``R2_BUCKET`` (or ``DEPLOY_ENV``).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipeline.recurring.r2_storage import UPLOAD_WORKERS, put_tree, storage_from_env

from . import export_hydro
from .export_hydro import HISTORY_DAYS, _write, build_climatology, build_history
from .hydro_poc import connect, run_bootstrap, run_update

KEY_PREFIX = "cron/hydro"


# ── JSON storage helpers ─────────────────────────────────────────────────────

def _get_json(storage, key: str, workdir: Path) -> Optional[dict]:
    """Pull a JSON object from storage, or None if absent."""
    tmp = workdir / ("_dl_" + key.replace("/", "_"))
    if not storage.get(key, tmp):
        return None
    try:
        return json.loads(tmp.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"WARN: could not parse {key}: {e}")
        return None


def _selection_args() -> argparse.Namespace:
    """The station-selection Namespace hydro_poc's fetchers expect (all BC)."""
    return argparse.Namespace(
        stations=None, all=False, bc=True, province=None, limit=5, no_series=False,
    )


def _seed_stations(conn: sqlite3.Connection, index: dict) -> int:
    """Seed the scratch DB's ``stations`` table (roster + coords) from a prior
    ``stations.json`` so a non-first run can fetch/export without re-pulling the
    266 MB HYDAT metadata. Coords are authoritative until the next HYDAT refresh.
    """
    rows = index.get("stations", [])
    conn.executemany(
        """INSERT OR REPLACE INTO stations
             (station_id, name, province, lat, lon, hyd_status,
              drainage_area_gross_km2, coord_source)
           VALUES (?, ?, 'BC', ?, ?, ?, ?, ?)""",
        [
            (s["id"], s.get("name"), s.get("lat"), s.get("lon"), s.get("hyd_status"),
             s.get("drainage_area_km2"), s.get("coord_source"))
            for s in rows
        ],
    )
    conn.commit()
    return len(rows)


def _clim_ids_from_index(index: Optional[dict]) -> set:
    """station_ids the prior index flagged as having a climatology envelope."""
    if not index:
        return set()
    return {s["id"] for s in index.get("stations", []) if s.get("has_climatology")}


def _merge_daily(existing: list, fresh: list, keep_days: int, today: str) -> list:
    """Merge two [date, value] daily series: fresh overrides existing on a shared
    date, then trim to the last ``keep_days`` and sort. Idempotent — re-running
    with the same fresh series yields the same result (no dupes)."""
    from datetime import date, timedelta

    merged: dict[str, object] = {}
    for d, v in existing or []:
        merged[d] = v
    for d, v in fresh or []:
        merged[d] = v  # fresh wins
    cutoff = (date.fromisoformat(today) - timedelta(days=keep_days)).isoformat()
    return sorted(([d, v] for d, v in merged.items() if d >= cutoff), key=lambda p: p[0])


def _merge_history(storage, conn, out: Path, key_prefix: str, workdir: Path, today: str) -> int:
    """For every station: pull its existing history/<id>.json, merge the freshly
    fetched (last ~14 d) daily means into it, trim to HISTORY_DAYS, write to out.
    The JSON files ARE the persisted record (no DB in R2), so this is the merge
    that keeps the ~18-month window without re-fetching it each run."""
    sids = [r[0] for r in conn.execute("SELECT station_id FROM stations ORDER BY station_id")]
    n = 0
    for sid in sids:
        fresh = build_history(conn, sid)  # DB only has the last ~14 d after `update`
        prior = _get_json(storage, f"{key_prefix}/history/{sid}.json", workdir) or {}
        merged = {
            "id": sid,
            "discharge_daily": _merge_daily(
                prior.get("discharge_daily"), fresh.get("discharge_daily"), HISTORY_DAYS, today),
            "level_daily": _merge_daily(
                prior.get("level_daily"), fresh.get("level_daily"), HISTORY_DAYS, today),
        }
        _write(out / "history" / f"{sid}.json", merged)
        n += 1
    print(f"history/: merged {n} files")
    return n


def _export_climatology(conn, out: Path) -> int:
    """Write climatology/<id>.json for every station that has an envelope (built
    into the scratch DB by the HYDAT sync). Only runs on a HYDAT-change run."""
    sids = [r[0] for r in conn.execute("SELECT station_id FROM stations ORDER BY station_id")]
    n = 0
    for sid in sids:
        payload = build_climatology(conn, sid)
        if payload is None:
            continue
        _write(out / "climatology" / f"{sid}.json", payload)
        n += 1
    print(f"climatology/: {n} files")
    return n


def _write_version_marker(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "version.json").write_text(
        json.dumps({"v": datetime.now(timezone.utc).isoformat()}), encoding="utf-8")


def _upload_tree(storage, out_dir: Path, key_prefix: str) -> None:
    """Upload every file under out_dir to key_prefix/... concurrently, version.json
    LAST so the webapp can fence on a consistent artifact set. The parallel PUT
    logic (450+ independent uploads, latency-bound on CI) lives in r2_storage."""
    n = put_tree(storage, out_dir, key_prefix)
    print(f"uploaded {n} files → {key_prefix}/ ({UPLOAD_WORKERS}-way parallel)")


# ── Timing + run summary ─────────────────────────────────────────────────────

@contextlib.contextmanager
def _timed(timings: "list[tuple[str, float]]", label: str):
    """Record wall-clock for a phase into ``timings`` and echo it to the log."""
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        timings.append((label, dt))
        print(f"⏱  {label}: {dt:.1f}s")


def _write_run_summary(out: Path, did: dict, timings: "list[tuple[str, float]]") -> None:
    """Emit a Markdown run summary to $GITHUB_STEP_SUMMARY (if running in CI) so
    the Actions run page shows what the job did and how long each phase took."""
    def _count(sub: str) -> int:
        d = out / sub
        return sum(1 for p in d.rglob("*.json") if p.is_file()) if d.is_dir() else 0

    total = sum(dt for _, dt in timings)
    mode = "first-run bootstrap" if did.get("first_run") else "incremental"
    lines = [
        "## Hydro cron summary",
        "",
        f"**Mode:** {mode} · "
        f"**history:** {'✅' if did.get('do_history') else '—'} · "
        f"**HYDAT/climatology:** {'✅' if did.get('do_hydat') else '—'}",
        "",
        "| Tier | Files |",
        "|------|------:|",
        f"| recent/ | {_count('recent')} |",
        f"| history/ | {_count('history')} |",
        f"| climatology/ | {_count('climatology')} |",
        "",
        "| Phase | Duration |",
        "|-------|---------:|",
    ]
    lines += [f"| {label} | {dt:.1f}s |" for label, dt in timings]
    lines += [f"| **total** | **{total:.1f}s** |", ""]

    md = "\n".join(lines)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with contextlib.suppress(OSError):
            with open(summary_path, "a", encoding="utf-8") as fh:
                fh.write(md + "\n")
    print(md)


# ── The one job ──────────────────────────────────────────────────────────────

def run_job(storage, workdir: Path, key_prefix: str = KEY_PREFIX,
            upload: bool = True, force: bool = False) -> Path:
    db = workdir / "hydro.db"          # ephemeral scratch — never persisted to R2
    out = workdir / "out"
    conn = connect(db)
    timings: list[tuple[str, float]] = []
    did: dict = {}
    try:
        args = _selection_args()

        prior_index = _get_json(storage, f"{key_prefix}/stations.json", workdir)
        first_run = prior_index is None
        clim_ids: Optional[set] = _clim_ids_from_index(prior_index)

        # HYDAT change detection — cheap listing date, no 266 MB download.
        from . import fetch_hydat
        latest_release = None
        try:
            latest_release, _ = fetch_hydat.find_latest_release()
        except Exception as e:  # noqa: BLE001 — non-fatal; skip the HYDAT path
            print(f"WARN: could not check latest HYDAT release: {e}")
        rel_marker = _get_json(storage, f"{key_prefix}/hydat_release.json", workdir) or {}
        do_hydat = bool(force) or first_run or (
            latest_release is not None and latest_release != rel_marker.get("release_date"))

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        hist_marker = _get_json(storage, f"{key_prefix}/history_date.json", workdir) or {}
        do_history = bool(force) or first_run or hist_marker.get("date") != today
        did.update(first_run=first_run, do_history=do_history, do_hydat=do_hydat)

        if first_run:
            print("=== first run: full bootstrap (roster + HYDAT + 18mo history + climatology) ===")
            with _timed(timings, "bootstrap fetch (roster + HYDAT + history)"):
                run_bootstrap(conn, args)
            with _timed(timings, "export (all tiers)"):
                export_hydro.export(conn, out, "all")
            latest_release = _current_release(conn) or latest_release
        else:
            print(f"=== incremental run (do_history={do_history}, do_hydat={do_hydat}) ===")
            seeded = _seed_stations(conn, prior_index)
            print(f"seeded {seeded} stations from prior stations.json")
            with _timed(timings, "fetch latest (conditions + readings + forecasts)"):
                run_update(conn, args)
            if do_hydat:
                print("HYDAT release changed — syncing metadata + climatology")
                with _timed(timings, "HYDAT sync (266 MB)"):
                    fetch_hydat.sync(conn)
                clim_ids = None  # DB now has the authoritative climatology set
                latest_release = _current_release(conn) or latest_release
            # Index + geojson + recent every run (has_climatology from prior index
            # unless the HYDAT sync just rebuilt it in-DB).
            with _timed(timings, "export (index + geojson + recent)"):
                export_hydro.export(conn, out, "realtime", clim_station_ids=clim_ids)
            if do_history:
                with _timed(timings, "history merge (pull + merge + write)"):
                    _merge_history(storage, conn, out, key_prefix, workdir, today)
            if do_hydat:
                with _timed(timings, "climatology export"):
                    _export_climatology(conn, out)

        # Markers: only (re)written on the run that refreshed their tier, so a plain
        # run doesn't churn them (and their absence-of-write leaves R2's copy intact).
        if do_history:
            _write(out / "history_date.json", {"date": today})
        if do_hydat and latest_release:
            _write(out / "hydat_release.json", {"release_date": latest_release})

        _write_version_marker(out)
        if upload:
            with _timed(timings, "upload to R2"):
                _upload_tree(storage, out, key_prefix)
        _write_run_summary(out, did, timings)
        return out
    finally:
        conn.close()


def _current_release(conn) -> Optional[str]:
    """Most recent HYDAT release_date recorded in the scratch DB, if any."""
    try:
        row = conn.execute(
            "SELECT release_date FROM hydat_sync ORDER BY synced_at DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Unified hydro cron job (portable R2 entrypoint).")
    p.add_argument("job", nargs="?", default="run", choices=["run"])
    p.add_argument("--local", type=Path, default=None,
                   help="use a local dir as the storage backend instead of R2 (dry run)")
    p.add_argument("--workdir", type=Path, default=None,
                   help="scratch dir (default: a temp dir, cleaned up)")
    p.add_argument("--key-prefix", default=KEY_PREFIX)
    p.add_argument("--no-upload", action="store_true", help="skip the upload step")
    p.add_argument("--force", action="store_true",
                   help="force the history + HYDAT/climatology tiers this run")
    args = p.parse_args(argv)

    storage = storage_from_env(args.local)

    cleanup = args.workdir is None
    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="hydro-job-"))
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        out = run_job(storage, workdir, key_prefix=args.key_prefix,
                      upload=not args.no_upload, force=args.force)
        print(f"hydro run done → {out}")
        return 0
    finally:
        if cleanup:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
