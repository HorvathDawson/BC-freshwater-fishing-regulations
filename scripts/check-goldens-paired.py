"""GOLDENS PAIRED WITH LATEST (T483 fast-fail, AGENTS 20a): the app's release-backed golden fixtures were cut from the
release data/release/LATEST names.  .venv/bin/python scripts/check-goldens-paired.py   (stdlib only, ~0.1 s)

caught (2026-11-09): after LATEST moved (r-d14bdb180f55), `pnpm check` failed in conformance until
`node app/tools/regs-recut/sync.mjs` re-cut the fixtures. The pairing is what the conformance suites hold: taps.json
`meta.store` (answers_sha256, reach_digest, section_handles, rule_ids_sha256 — what taps.py read off the release's own
store) against the same keys in the `meta` table of LATEST's `water` file.

A machine with no data/release/LATEST (a fresh clone, CI) has nothing to pair against: said, not failed.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "data/release"
TAPS = ROOT / "app/fixtures/golden/taps.json"
KEYS = ("answers_sha256", "reach_digest", "section_handles", "rule_ids_sha256")
SYNC = "node app/tools/regs-recut/sync.mjs"


def store_meta(release_dir: Path) -> dict:
    """The `meta` keys of the release's water file (the store taps.py reads)."""
    m = json.loads((release_dir / "manifest.json").read_text(encoding="utf-8"))
    water = release_dir.parent / m["files"]["water"]["url"]
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "water.sqlite"
        db.write_bytes(gzip.decompress(water.read_bytes()))
        con = sqlite3.connect(db)
        try:
            return {k: v for k, v in con.execute("SELECT k, v FROM meta") if k in KEYS}
        finally:
            con.close()


def problems(latest: str, live: dict, cut: dict) -> list[str]:
    off = [k for k in KEYS if live.get(k) != cut.get(k)]
    if not off:
        return []
    said = ", ".join(f"{k} {str(cut.get(k))[:16]} (LATEST {str(live.get(k))[:16]})" for k in off)
    return [f"the app's golden fixtures (app/fixtures/golden/taps.json) were cut from another release than LATEST "
            f"{latest}: {said}. Re-cut them: `{SYNC}`, then commit the fixtures with the release id"]


def main() -> int:
    ptr = RELEASE / "LATEST"
    if not ptr.is_file():
        print("   (no data/release/LATEST on this machine: nothing to pair the goldens with)")
        return 0
    latest = ptr.read_text().strip()
    cut = json.loads(TAPS.read_text(encoding="utf-8"))["meta"]["store"]
    bad = problems(latest, store_meta(RELEASE / latest), cut)
    for b in bad:
        print(f"fast-fail: {b}", file=sys.stderr)
    if not bad:
        print(f"   taps.json is cut from LATEST {latest}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
