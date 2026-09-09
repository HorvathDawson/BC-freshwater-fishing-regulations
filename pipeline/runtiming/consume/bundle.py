"""The frozen index -> the four bundle tables.

    run_clim(cuid, pentad, pct)             the curve, on gauge_clim's own 73-pentad axis
    run_profile(profile_id, cuid)           which runs cover this kind of reach
    section_run(section_id, profile_id)     one row per reach
    run_cube(slot_count, pentads, blob)     the flattened colour lookup, one row

WHY BOTH THE TABLES AND THE CUBE. They answer different questions and only one of them can
be answered by a byte — see `generate/profiles.py`. The sheet needs to NAME the runs on a
reach ("summer-run and winter-run Steelhead"); the map needs one number to colour with.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS run_clim(
  cuid INTEGER NOT NULL, pentad INTEGER NOT NULL, pct REAL NOT NULL,
  PRIMARY KEY(cuid, pentad)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS run_cu(
  cuid INTEGER PRIMARY KEY, name TEXT NOT NULL, species TEXT NOT NULL,
  region TEXT, label TEXT, form TEXT, quality INTEGER, quality_word TEXT,
  peak_date TEXT, span_start TEXT, span_end TEXT);

CREATE TABLE IF NOT EXISTS run_profile(
  profile_id INTEGER NOT NULL, cuid INTEGER NOT NULL,
  PRIMARY KEY(profile_id, cuid)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS section_run(
  section_id TEXT PRIMARY KEY, profile_id INTEGER NOT NULL) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS section_run_profile ON section_run(profile_id);

-- PASSAGE: the runs that CROSS this reach on the way in, and how far up it sits.
-- A separate profile space from `section_run`, because a reach's spawning runs and the
-- runs passing through it are different sets and must render differently.
CREATE TABLE IF NOT EXISTS passage_profile(
  profile_id INTEGER NOT NULL, cuid INTEGER NOT NULL,
  PRIMARY KEY(profile_id, cuid)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS section_passage(
  section_id TEXT PRIMARY KEY, profile_id INTEGER NOT NULL,
  km_to_sea REAL) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS section_passage_profile ON section_passage(profile_id);

CREATE TABLE IF NOT EXISTS run_cube(
  relation TEXT PRIMARY KEY, slots TEXT NOT NULL, profiles INTEGER NOT NULL,
  pentads INTEGER NOT NULL, blob BLOB NOT NULL);

CREATE TABLE IF NOT EXISTS run_gap(
  item_id TEXT NOT NULL, species TEXT NOT NULL, reason TEXT, PRIMARY KEY(item_id, species))
  WITHOUT ROWID;
"""


def main(gen: Path, db_path: Path, review: Path | None) -> None:
    runs = json.loads((gen / "runs.json").read_text())["runs"]
    meta = json.loads((gen / "profiles_meta.json").read_text())
    db = sqlite3.connect(db_path)
    db.executescript(DDL)
    for t in ("run_clim", "run_cu", "run_profile", "section_run", "run_cube", "run_gap",
              "passage_profile", "section_passage"):
        db.execute(f"DELETE FROM {t}")

    db.executemany(
        "INSERT INTO run_cu VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        [(r["cuid"], r["name"], r["species"], r["region"], r["label"], r["form"],
          r["quality"], r["quality_word"], r["peak_date"],
          (r["span"] or [None, None])[0], (r["span"] or [None, None])[1]) for r in runs])
    db.executemany("INSERT INTO run_clim VALUES(?,?,?)",
                   [(r["cuid"], w, v) for r in runs for w, v in enumerate(r["pentads"])])

    with open(gen / "run_profile.csv") as f:
        rd = csv.reader(f); next(rd)
        db.executemany("INSERT INTO run_profile VALUES(?,?)",
                       [(int(a), int(b)) for a, b in rd])
    with open(gen / "section_profile.csv") as f:
        rd = csv.reader(f); next(rd)
        db.executemany("INSERT INTO section_run VALUES(?,?)",
                       [(a, int(b)) for a, b in rd])

    db.execute("INSERT INTO run_cube VALUES(?,?,?,?,?)",
               ("spawning", json.dumps(meta["slots"]), meta["profiles"], 73,
                (gen / "run_cube.bin").read_bytes()))

    if (gen / "passage_cube.bin").is_file():
        db.execute("INSERT INTO run_cube VALUES(?,?,?,?,?)",
                   ("passage", json.dumps(meta["slots"]), meta["passage_profiles"], 73,
                    (gen / "passage_cube.bin").read_bytes()))
        with open(gen / "passage_profile.csv") as f:
            rd = csv.reader(f); next(rd)
            db.executemany("INSERT INTO passage_profile VALUES(?,?)",
                           [(int(a), int(b)) for a, b in rd])
        with open(gen / "section_passage.csv") as f:
            rd = csv.reader(f); next(rd)
            db.executemany("INSERT INTO section_passage VALUES(?,?,?)",
                           [(a, int(b), float(c)) for a, b, c in rd])

    if review and Path(review).is_file():
        gaps = json.loads(Path(review).read_text()).get("coverage_gaps", [])
        db.executemany("INSERT INTO run_gap VALUES(?,?,?)",
                       [(g["item_id"], g["species"], g["reason"]) for g in gaps])

    db.commit()
    for t in ("run_cu", "run_clim", "run_profile", "section_run",
              "passage_profile", "section_passage", "run_gap"):
        print(f"  {t:14}{db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:>10,}")
    print(f"  run_cube      {len((gen / 'run_cube.bin').read_bytes())/1024:>9.1f} KB")
    if (gen / "passage_cube.bin").is_file():
        print(f"  passage_cube  {len((gen / 'passage_cube.bin').read_bytes())/1024:>9.1f} KB")
    db.close()


if __name__ == "__main__":
    from pipeline.common.curated import CURATED, GENERATED
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gen", type=Path, default=GENERATED.runtiming)
    ap.add_argument("--db", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--review", type=Path, default=CURATED.runtiming.review)
    a = ap.parse_args()
    main(a.gen, a.db, a.review)
