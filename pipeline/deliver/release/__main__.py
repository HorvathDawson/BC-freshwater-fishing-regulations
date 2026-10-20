"""python -m pipeline.deliver.release [--bundle FILE] [--store FILE] [--status-index FILE] [--export-dir DIR]
                                    [--out-root DIR] [--data-release ID]

Writes `<out-root>/<data_release>/` (default `data/release/`), prints the sizes and fails on a size gate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED


def main(argv=None) -> int:
    from pipeline.deliver.release import build as B
    from pipeline.deliver.release.manifest import tiles_block
    from pipeline.deliver.store import OUT as STORE
    from pipeline.tools.export_ui_rules import OUT as EXPORT
    ap = argparse.ArgumentParser(prog="pipeline.deliver.release", description=__doc__.split("\n\n")[0])
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--store", type=Path, default=STORE)
    ap.add_argument("--status-index", type=Path, default=GENERATED.bundle / "status_index.bin")
    ap.add_argument("--export-dir", type=Path, default=EXPORT.parent)
    ap.add_argument("--out-root", type=Path, default=GENERATED.release)
    ap.add_argument("--data-release", default=None, help="default: r-<sha12 of the files>")
    a = ap.parse_args(argv)
    import sqlite3
    handles = dict(sqlite3.connect(f"file:{a.bundle}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))[
        "section_handles"]
    d = B.build(bundle=a.bundle, store=a.store, status_index=a.status_index, export_dir=a.export_dir,
                out_root=a.out_root, data_release=a.data_release, tiles=tiles_block(GENERATED.release, handles))
    sz = B.sizes(d)
    print(json.dumps({"release": str(d), "gz_total": sum(v["gz"] for v in sz.values()), "files": sz}, indent=1))
    bad = B.gate_problems(d)
    for p in bad:
        print(f"SIZE GATE: {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
