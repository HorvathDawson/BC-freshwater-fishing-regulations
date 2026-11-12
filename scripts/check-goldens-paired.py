"""GOLDENS PAIRED WITH LATEST (T483 fast-fail, AGENTS 20a): the app's release-backed golden fixtures were cut from the
release data/release/LATEST names.  .venv/bin/python scripts/check-goldens-paired.py   (stdlib only, ~0.1 s)

caught (2026-11-09): after LATEST moved (r-d14bdb180f55), `pnpm check` failed in conformance until
`node app/tools/regs-recut/sync.mjs` re-cut the fixtures. The pairing is what the conformance suites hold: taps.json
`meta.store` (answers_sha256, reach_digest, section_handles, rule_ids_sha256 — what taps.py read off the release's own
store) against the same keys in the `meta` table of LATEST's `water` file.

A machine with no data/release/LATEST (a fresh clone, CI) has nothing to pair against: said, not failed.

(T666 item 2) THE RELEASE UNDER TEST: with RELEASE_DIR set (repo-relative or absolute; `pipeline release` sets it to its
CANDIDATE once it has re-cut the fixtures on it, before the promote) the goldens are paired with THAT release. A
RELEASE_DIR naming no release fails; it is never passed over for LATEST.
"""
from __future__ import annotations

import gzip
import json
import os
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


def problems(latest: str, live: dict, cut: dict, which: str = "LATEST") -> list[str]:
    off = [k for k in KEYS if live.get(k) != cut.get(k)]
    if not off:
        return []
    said = ", ".join(f"{k} {str(cut.get(k))[:16]} ({which} {str(live.get(k))[:16]})" for k in off)
    sync = SYNC if which == "LATEST" else f"{SYNC} --release data/release/{latest}"
    return [f"the app's golden fixtures (app/fixtures/golden/taps.json) were cut from another release than {which} "
            f"{latest}: {said}. Re-cut them: `{sync}`, then commit the fixtures with the release id"]


def under_test(env=os.environ) -> tuple[Path | None, str]:
    """(the release folder to pair with, what names it): RELEASE_DIR's, else LATEST's; (None, …) when this machine has
    no LATEST. Raises LookupError for a RELEASE_DIR that names no release."""
    named = env.get("RELEASE_DIR")
    if named:
        d = Path(named) if Path(named).is_absolute() else ROOT / named
        if not (d / "manifest.json").is_file():
            raise LookupError(f"RELEASE_DIR={named}: no release there ({d}/manifest.json)")
        return d, "RELEASE_DIR"
    ptr = RELEASE / "LATEST"
    if not ptr.is_file():
        return None, "LATEST"
    return RELEASE / ptr.read_text().strip(), "LATEST"


def main() -> int:
    try:
        rel, which = under_test()
    except LookupError as e:
        print(f"fast-fail: {e}", file=sys.stderr)
        return 1
    if rel is None:
        print("   (no data/release/LATEST on this machine: nothing to pair the goldens with)")
        return 0
    cut = json.loads(TAPS.read_text(encoding="utf-8"))["meta"]["store"]
    bad = problems(rel.name, store_meta(rel), cut, which)
    for b in bad:
        print(f"fast-fail: {b}", file=sys.stderr)
    if not bad:
        print(f"   taps.json is cut from {which} {rel.name}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
