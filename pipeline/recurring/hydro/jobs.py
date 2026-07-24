"""hydro.jobs — the single portable entrypoint for the hydro crons.

    python -m pipeline.recurring.hydro.jobs realtime   # frequent (~15 min)
    python -m pipeline.recurring.hydro.jobs nightly     # nightly history + climatology

This module owns all the orchestration AND its own R2 (S3-compatible) GET/PUT via
boto3, so the ONLY GitHub-Actions-specific artifact is the workflow YAML — the same
entrypoint runs unchanged in a Cloudflare Container triggered by a Cron Trigger
(the ~68 s / ~1825-request fetch is container-shaped, not a Worker isolate).

Design invariants (see plan Parts C + G):
  * realtime is STATELESS for heavy readings — it pulls only the small seed DB
    (``cron/hydro/hydro_seed.db``), re-fetches the last 14 days fresh, exports,
    discards. It pulls just the fid shards its gauges need (not all 4096).
  * nightly is STATEFUL — it keeps the full ``hydro.db`` in R2, refreshes daily
    means + climatology, exports history + climatology, rebuilds the seed, and
    pushes the full DB back for the next night.
  * a ``version.json`` marker is written/uploaded LAST so the webapp can fence on
    a consistent artifact set (avoids reading a new stations.json against a stale
    recent/*.json mid-upload).

R2 credentials (env): ``R2_S3_ENDPOINT`` (or ``CLOUDFLARE_ACCOUNT_ID`` →
``https://<id>.r2.cloudflarestorage.com``), ``R2_ACCESS_KEY_ID``,
``R2_SECRET_ACCESS_KEY``, ``R2_BUCKET`` (or derived from ``DEPLOY_ENV``).
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipeline.deploy.r2_sharder import shard_prefix
from pipeline.recurring.r2_storage import storage_from_env

from .seed import build_seed_db

KEY_PREFIX = "cron/hydro"
STREAM_LAYERS = ("streams", "under_lake_streams")


# ── Helpers ─────────────────────────────────────────────────────────────────

def _shard_version() -> int:
    from project_config import ProjectConfig

    v = ProjectConfig().config.get("output", {}).get("pipeline", {}).get("shard_version")
    return int(v) if v is not None else 2


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _ensure_gauge_fwa_match(storage, db: Path, workdir: Path, key_prefix: str) -> None:
    """Make sure ``gauge_fwa_match`` (+ summary) exist in ``db``.

    The nightly job builds hydro.db via ``hydro_poc bootstrap`` (WSC roster +
    readings + climatology) which never runs the atlas-time ``match_fwa`` step,
    so its db has no gauge→FWA link table. The authoritative copy lives in the
    enrich-built ``hydro_seed.db``; pull it and copy the tables across so the
    nightly export can still resolve gauge reach_ids. Degrades to a no-op (with
    a warning) if the seed isn't in storage — ``_gauge_fids`` then returns empty.
    """
    conn = sqlite3.connect(db)
    try:
        if _has_table(conn, "gauge_fwa_match"):
            return
        seed = workdir / "hydro_seed.db"
        if not storage.get(f"{key_prefix}/hydro_seed.db", seed):
            print("gauge_fwa_match missing and no hydro_seed.db in storage — "
                  "gauge FWA links will be skipped this run")
            return
        conn.execute("ATTACH DATABASE ? AS seed", (str(seed),))
        restored = []
        for tbl in ("gauge_fwa_match", "gauge_fwa_match_summary"):
            if conn.execute(
                "SELECT 1 FROM seed.sqlite_master WHERE type='table' AND name=?", (tbl,)
            ).fetchone():
                conn.execute(f"CREATE TABLE {tbl} AS SELECT * FROM seed.{tbl}")
                restored.append(tbl)
        conn.commit()
        conn.execute("DETACH DATABASE seed")
        print(f"restored {', '.join(restored)} from hydro_seed.db")
    finally:
        conn.close()


def _gauge_fids(db: Path) -> set[str]:
    """Distinct stream fids from the seed's gauge_fwa_match (primary matches).

    Guards the table's absence (matching export_hydro's own guard) — a db built
    without the atlas-time match_fwa step simply yields no gauge fids.
    """
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        if not _has_table(conn, "gauge_fwa_match"):
            return set()
        rows = conn.execute(
            "SELECT DISTINCT fwa_id FROM gauge_fwa_match "
            "WHERE is_primary=1 AND layer IN (?, ?) AND fwa_id IS NOT NULL",
            STREAM_LAYERS,
        ).fetchall()
    finally:
        conn.close()
    return {str(r[0]) for r in rows if r[0] is not None}


def _pull_fid_shards(storage, db: Path, workdir: Path) -> Path:
    """Download only the ``shards/v{N}/fids/{prefix}.json`` buckets the gauges need.

    Returns the shard root (``.../shards/v{N}``) to hand to the exporter via
    ``HYDRO_SHARD_ROOT`` — the fid→reach_id resolver reads local disk, so this is
    the step that stops the realtime export emitting null reach_ids (review G1).
    """
    version = _shard_version()
    shard_root = workdir / "shards" / f"v{version}"
    (shard_root / "fids").mkdir(parents=True, exist_ok=True)
    prefixes = {shard_prefix(fid) for fid in _gauge_fids(db)}
    got = 0
    for prefix in sorted(prefixes):
        key = f"shards/v{version}/fids/{prefix}.json"
        if storage.get(key, shard_root / "fids" / f"{prefix}.json"):
            got += 1
    print(f"pulled {got}/{len(prefixes)} fid shard buckets for {len(prefixes)} prefixes")
    return shard_root


def _run(argv: list[str], env: dict) -> None:
    print(f"$ {' '.join(argv)}")
    subprocess.run(argv, env=env, check=True)


def _write_version_marker(out_dir: Path) -> Path:
    marker = out_dir / "version.json"
    marker.write_text(
        f'{{"v":"{datetime.now(timezone.utc).isoformat()}"}}', encoding="utf-8"
    )
    return marker


def _upload_tree(storage, out_dir: Path, key_prefix: str) -> None:
    """Upload every file under out_dir to key_prefix/..., version.json LAST."""
    files = [p for p in out_dir.rglob("*") if p.is_file()]
    version_files = [p for p in files if p.name == "version.json"]
    data_files = [p for p in files if p.name != "version.json"]
    for p in data_files:
        rel = p.relative_to(out_dir).as_posix()
        ct = "application/json" if p.suffix == ".json" else None
        storage.put(p, f"{key_prefix}/{rel}", content_type=ct)
    for p in version_files:  # fence marker last
        rel = p.relative_to(out_dir).as_posix()
        storage.put(p, f"{key_prefix}/{rel}", content_type="application/json")
    print(f"uploaded {len(data_files)} data files + version marker → {key_prefix}/")


# ── Jobs ────────────────────────────────────────────────────────────────────

def realtime_job(storage, workdir: Path, key_prefix: str = KEY_PREFIX, upload: bool = True) -> Path:
    db = workdir / "hydro.db"
    if not storage.get(f"{key_prefix}/hydro_seed.db", db):
        raise RuntimeError(
            f"seed DB {key_prefix}/hydro_seed.db not found in storage — run the "
            "nightly job (or a full enrich seed) first."
        )

    shard_root = _pull_fid_shards(storage, db, workdir)
    out = workdir / "out"
    env = {
        **os.environ,
        "HYDRO_DB_PATH": str(db),
        "HYDRO_SHARD_ROOT": str(shard_root),
        "HYDRO_OUT_DIR": str(out),
    }
    _run([sys.executable, "-m", "pipeline.recurring.hydro.hydro_poc", "update", "--bc"], env)
    _run(
        [sys.executable, "-m", "pipeline.recurring.hydro.export_hydro",
         "--scope", "realtime", "--db", str(db), "--out", str(out)],
        env,
    )
    _write_version_marker(out)
    if upload:
        _upload_tree(storage, out, key_prefix)
    return out


def nightly_job(storage, workdir: Path, key_prefix: str = KEY_PREFIX, upload: bool = True) -> Path:
    db = workdir / "hydro.db"
    env_base = {**os.environ, "HYDRO_DB_PATH": str(db)}

    if storage.get(f"{key_prefix}/hydro.db", db):
        # Refresh the persisted DB: recent readings/forecasts + HYDAT climatology.
        _run([sys.executable, "-m", "pipeline.recurring.hydro.hydro_poc", "update", "--bc"], env_base)
        _run([sys.executable, "-m", "pipeline.recurring.hydro.fetch_hydat"], env_base)
    else:
        # First run: full bootstrap (roster + 18mo history + HYDAT + climatology).
        _run([sys.executable, "-m", "pipeline.recurring.hydro.hydro_poc", "bootstrap", "--bc"], env_base)

    out = workdir / "out"
    env = {**env_base, "HYDRO_OUT_DIR": str(out)}
    # No shard pull here: history/climatology don't resolve reach_ids; but the
    # 'all' scope also re-writes stations.json, so pull shards to keep fwa links.
    # bootstrap/update build a db with no gauge_fwa_match — restore it from the
    # enrich-built seed first so _gauge_fids (and stations.json fwa links) work.
    _ensure_gauge_fwa_match(storage, db, workdir, key_prefix)
    shard_root = _pull_fid_shards(storage, db, workdir)
    env["HYDRO_SHARD_ROOT"] = str(shard_root)
    _run(
        [sys.executable, "-m", "pipeline.recurring.hydro.export_hydro",
         "--scope", "all", "--db", str(db), "--out", str(out)],
        env,
    )
    build_seed_db(db, out / "hydro_seed.db")
    _write_version_marker(out)
    if upload:
        _upload_tree(storage, out, key_prefix)
        storage.put(db, f"{key_prefix}/hydro.db")  # persist for next night
    return out


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Hydro cron jobs (portable R2 entrypoint).")
    p.add_argument("job", choices=["realtime", "nightly"])
    p.add_argument("--local", type=Path, default=None,
                   help="use a local dir as the storage backend instead of R2 (dry run)")
    p.add_argument("--workdir", type=Path, default=None,
                   help="scratch dir (default: a temp dir, cleaned up)")
    p.add_argument("--key-prefix", default=KEY_PREFIX)
    p.add_argument("--no-upload", action="store_true", help="skip the upload step")
    args = p.parse_args(argv)

    storage = storage_from_env(args.local)

    cleanup = args.workdir is None
    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="hydro-job-"))
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        job = realtime_job if args.job == "realtime" else nightly_job
        out = job(storage, workdir, key_prefix=args.key_prefix, upload=not args.no_upload)
        print(f"{args.job} job done → {out}")
        return 0
    finally:
        if cleanup:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
