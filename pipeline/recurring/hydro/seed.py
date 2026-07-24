"""Build the small hydro *seed* DB.

The frequent (~15 min) realtime cron is stateless for the heavy raw readings — it
re-fetches the last 14 days fresh from ECCC each run and never persists them. But
the realtime export still needs the small, slowly-changing tables to emit the
``fwa`` link (``gauge_fwa_match``) and the ``has_climatology`` flag + station
roster/metadata. Those live in this seed DB (~a few MB), which the nightly job
rebuilds and pushes to R2; the frequent job pulls it, attaches fresh readings,
exports, and discards.

Explicitly EXCLUDED from the seed: ``readings`` (the ~1.3 GB raw 5-min feed),
``forecasts``, ``forecast_series`` — all re-fetched by the frequent ``update``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# Small, slowly-changing tables the realtime export reads. Copied verbatim.
# NB: the big per-doy ``flow_climatology`` table (~273k rows) is deliberately
# NOT seeded — the realtime scope only needs ``flow_climatology_meta`` for the
# ``has_climatology`` flag; the full envelope is a history-scope concern the
# nightly job serves from the complete hydro.db. Excluding it keeps the seed <1 MB.
SEED_TABLES = [
    "stations",
    "gauge_fwa_match",
    "gauge_fwa_match_summary",
    "flow_climatology_meta",
    "current_conditions",
    "hydat_sync",
]


def build_seed_db(src_db: Path, dst_db: Path) -> dict:
    """Copy the SEED_TABLES from a full ``hydro.db`` into a fresh seed DB.

    Returns a per-table row-count summary. Tables absent in the source are
    skipped (e.g. a DB built before a given feature landed).
    """
    src_db = Path(src_db)
    dst_db = Path(dst_db)
    if not src_db.exists():
        raise FileNotFoundError(f"source hydro.db not found: {src_db}")

    dst_db.parent.mkdir(parents=True, exist_ok=True)
    if dst_db.exists():
        dst_db.unlink()

    counts: dict = {}
    con = sqlite3.connect(dst_db)
    try:
        con.execute("ATTACH DATABASE ? AS src", (str(src_db),))
        for table in SEED_TABLES:
            ddl = con.execute(
                "SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not ddl or not ddl[0]:
                continue  # table doesn't exist in this source DB
            con.execute(ddl[0])
            con.execute(f"INSERT INTO main.{table} SELECT * FROM src.{table}")
            counts[table] = con.execute(
                f"SELECT COUNT(*) FROM main.{table}"
            ).fetchone()[0]
        con.commit()
        con.execute("DETACH DATABASE src")
        con.execute("VACUUM")
        con.commit()
    finally:
        con.close()
    return counts


def main(argv: list[str] | None = None) -> int:
    import argparse

    from .hydro_poc import DB_PATH

    p = argparse.ArgumentParser(description="Build the small hydro seed DB.")
    p.add_argument("--src", type=Path, default=DB_PATH, help="full hydro.db")
    p.add_argument(
        "--dst",
        type=Path,
        default=None,
        help="seed DB output (default: config output.pipeline.hydro_seed)",
    )
    args = p.parse_args(argv)

    dst = args.dst
    if dst is None:
        from project_config import ProjectConfig

        dst = ProjectConfig().get_path(
            "output", "pipeline", "hydro_seed",
            default="output/pipeline/hydro/hydro_seed.db",
        )

    counts = build_seed_db(args.src, dst)
    print(f"seed DB → {dst}")
    for table, n in counts.items():
        print(f"  {table}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
