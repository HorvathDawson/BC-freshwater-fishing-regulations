"""python -m pipeline.deliver.release [--bundle FILE] --store FILE [--status-index FILE] [--export-dir DIR]
                                    [--out-root DIR] [--data-release ID] [--lake-facts FILE]
                                    [--tiles DIR] [--tiles-release-root DIR]

Writes `<out-root>/<data_release>/` (default `data/release/`), prints the sizes and fails on a size gate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED, SOURCE


def main(argv=None) -> int:
    from pipeline.deliver.release import build as B
    from pipeline.deliver.answers.common import BOOK_TITLE
    from pipeline.deliver.release.manifest import basemap_block, sources_block, tiles_block
    from pipeline.deliver.export.contract import OUT as EXPORT
    ap = argparse.ArgumentParser(prog="pipeline.deliver.release", description=__doc__.split("\n\n")[0])
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--store", type=Path, required=True,
                    help="the regs store to package (deliver all makes it in its staging directory; P20: none is kept)")
    ap.add_argument("--status-index", type=Path, default=GENERATED.bundle / "status_index.bin")
    ap.add_argument("--export-dir", type=Path, default=EXPORT.parent)
    ap.add_argument("--out-root", type=Path, default=GENERATED.release)
    ap.add_argument("--data-release", default=None, help="default: r-<sha12 of the files>")
    ap.add_argument("--tiles", type=Path, default=GENERATED.tiles,
                    help="the tile set this release names (its atlas.meta.json; default: the live tiles)")
    ap.add_argument("--tiles-release-root", type=Path, default=GENERATED.release,
                    help="where that tile set's released folder (tiles/<version>/) is (default: data/release)")
    ap.add_argument("--lake-facts", type=Path, default=None,
                    help="a lakes/1 file (pipeline/deliver/lake_facts.py), shipped as `lakes`; none is made yet, so "
                         "by default the release ships no `lakes` (owner 2026-10-21: prepared, not fed)")
    ap.add_argument("--synopsis", type=Path, default=None,
                    help="the synopsis PDF this release ships for the highlighted 'open the PDF' (T215; default: the repo copy)")
    a = ap.parse_args(argv)
    from pipeline.common.synopsis_pages import default_pdf
    syn = a.synopsis or default_pdf()
    import sqlite3
    handles = dict(sqlite3.connect(f"file:{a.bundle}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))[
        "section_handles"]
    d = B.build(bundle=a.bundle, store=a.store, status_index=a.status_index, export_dir=a.export_dir,
                out_root=a.out_root, data_release=a.data_release, lakes=a.lake_facts, synopsis=syn if syn.exists() else None, tiles=tiles_block(a.tiles_release_root, handles, a.tiles / "atlas.meta.json"),
                # T75: the released basemap (its sidecar beside the tiles), so a native install can fetch the ground
                basemap=basemap_block(a.tiles_release_root, a.tiles / "basemap.meta.json"),
                # the date each source was last updated, read from the fetched inputs (the app's Data sources screen)
                sources=sources_block(SOURCE, synopsis_title=BOOK_TITLE, dfo_rules=GENERATED.regs.dfo_salmon / "rules"))
    sz = B.sizes(d)
    print(json.dumps({"release": str(d), "gz_total": sum(v["gz"] for v in sz.values()), "files": sz}, indent=1))
    bad = B.gate_problems(d)
    for p in bad:
        print(f"SIZE GATE: {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
