"""THE FIXTURE RELEASE (P11): the release's BUNDLE half — paint, flow-area, core, near, conditions — cut from the app's
development fixture bundle (`app/packages/data/dev/bundle.sqlite`) by the release builder's own code
(`pipeline.deliver.release.build.bundle_files` + `ship`), so the app's conformance suite can run against a release in
`pnpm check` with no data and no regs store.

    python -m pipeline.tools.fixture_release --bundle app/packages/data/dev/bundle.sqlite --out <dir> [--valid-until D]

What it is NOT: a whole release. `water`, `regs-<region>` and `regs-sid` come from a regs store, which the fixture has
none of (`python -m pipeline.deliver release` needs one); the app's RegsSource reads none of them, and the loader
refuses a file the manifest does not list. Two inputs the release formats require and the fixture bundle does not
carry are DERIVED, never invented:
  section_handles   the fixture's handle-table id ("fixture-588ceed04ef45f27") is not the 16 hex digits the status
                    index and every manifest pattern use: its sha256's first 16 hex digits stand in for it.
  status index      empty (no rules in the fixture): every section reads base, which is what the index's absence says.
`--valid-until` is the edition expiry for the manifest (the dev bundle's own is empty); the app test passes the
fixture source's, so the two fixtures state one edition.
"""
from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Optional, Sequence

from pipeline.deliver import status_index as SI
from pipeline.deliver.release import build as B

_HEX16 = re.compile(r"^[0-9a-f]{16}$")


def hex16(text: str) -> str:
    """A 16-hex-digit id for a handle table or reach digest: itself when it already is one, else its sha256's head."""
    return text if _HEX16.match(text) else hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def build(bundle: Path, out_root: Path, valid_until: Optional[str] = None) -> Path:
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        meta = dict(b.execute("SELECT k, v FROM meta"))
        handles = hex16(meta["section_handles"])
        reach = meta.get("reach_digest") or ""
        index = SI.encode({"handles": handles, "reach_digest": hex16(reach), "profiles": [], "sections": {},
                           "items": {}})
        out_root.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix=".release-", dir=out_root))
        try:
            files = B.bundle_files(work, bundle, b, {"section_handles": handles, "reach_digest": reach}, index)
            return B.ship(work, files, handles, out_root, valid_until=valid_until)
        finally:
            shutil.rmtree(work, ignore_errors=True)
    finally:
        b.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bundle", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True, help="the release root: <out>/<release id>/ is written")
    ap.add_argument("--valid-until", default=None)
    a = ap.parse_args(argv)
    print(build(a.bundle, a.out, a.valid_until))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
