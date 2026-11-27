"""python -m pipeline.deliver.release [--bundle FILE] --store FILE [--status-index FILE] [--export-dir DIR]
                                    [--out-root DIR] [--data-release ID] [--lake-facts FILE]
                                    [--tiles DIR] [--tiles-release-root DIR] [--reports DIR] [--previous DIR]

Writes `<out-root>/<data_release>/` (default `data/release/`), prints the sizes and fails on a size gate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED, ROOTS, SOURCE


def live_root(out_root: Path) -> bool:
    """(T788 #5) is `out_root` the live release root (`ROOTS.release`, data/release: LATEST's folder, what the
    uploads read), by its resolved path, so `./data/release` or a symlink to it is the same root"""
    return Path(out_root).resolve() == Path(ROOTS.release).resolve()


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
    ap.add_argument("--out-root", type=Path, default=ROOTS.release)
    ap.add_argument("--data-release", default=None, help="default: r-<sha12 of the files>")
    ap.add_argument("--tiles", type=Path, default=GENERATED.tiles,
                    help="the tile set this release names (its atlas.meta.json; default: the live tiles)")
    ap.add_argument("--tiles-release-root", type=Path, default=ROOTS.release,
                    help="where that tile set's released folder (tiles/<version>/) is (default: data/release)")
    ap.add_argument("--lake-facts", type=Path, default=None,
                    help="a lakes/1 file (pipeline/deliver/lake_facts.py), shipped as `lakes`; none is made yet, so "
                         "by default the release ships no `lakes` (owner 2026-10-21: prepared, not fed)")
    ap.add_argument("--synopsis", type=Path, default=None,
                    help="the synopsis PDF this release ships for the highlighted 'open the PDF' (T215; default: the repo copy)")
    # (T667 slice 8) a release cut for tests on invented data (the mini release) names its fetched inputs and ships no
    # ground and no book, each by an explicit flag; with none, every input is the live one and a basemap is still
    # required (T416)
    ap.add_argument("--reports", type=Path, default=None,
                    help="(T936) the build reports' root: the release's own lists go to <reports>/<release id>/ "
                         "(deliver all: data/generated/release/checks, beside the release command's checks); none: printed only")
    ap.add_argument("--previous", type=Path, default=None,
                    help="(T1030) the release this one replaces (deliver all: the one LATEST names): the manifest's "
                         "changed_waters counts against its per-water digest list; none: no count (said)")
    ap.add_argument("--no-basemap", action="store_true",
                    help="ship no basemap (manifest basemap: null): only for a release no device installs")
    ap.add_argument("--no-synopsis", action="store_true",
                    help="ship no synopsis PDF, and read or write no synopsis cache")
    ap.add_argument("--source", type=Path, default=SOURCE,
                    help="the fetched source tree the source dates are read from (default: roots.source)")
    ap.add_argument("--dfo-rules", type=Path, default=GENERATED.regs.dfo_salmon / "rules",
                    help="the parsed DFO salmon pages the DFO date is read from (default: the live ones)")
    a = ap.parse_args(argv)
    if a.no_synopsis and a.synopsis:
        ap.error("--no-synopsis and --synopsis name opposite things")
    # (T788 #5) a release cut into the LIVE release root is one a device may install: it ships its ground and its book.
    # The two flags are for a release no device installs (the mini release, a test's), cut into a root of its own.
    # `deliver all` cuts into a staging folder and promotes into the live root: either root being the live one is live.
    live = [f"--{n.replace('_', '-')} {p}" for n, p in (("out_root", a.out_root),
                                                         ("tiles_release_root", a.tiles_release_root)) if live_root(p)]
    if live and (a.no_basemap or a.no_synopsis):
        ap.error(f"{'--no-basemap' if a.no_basemap else '--no-synopsis'} is only for a release no device installs, cut "
                 f"with its own --out-root and --tiles-release-root; {' and '.join(live)} is the live release root "
                 f"({ROOTS.release})")
    if not Path(a.source).is_dir():
        ap.error(f"--source {a.source} is not a directory")
    from pipeline.common.synopsis_pages import default_pdf
    syn = None if a.no_synopsis else (a.synopsis or default_pdf())
    if syn is not None and syn.exists():
        from pipeline.deliver.release.synopsis import shipped
        syn = shipped(syn)              # (T354) the downsampled copy, render-gated, cached by the source's digest
    import sqlite3
    handles = dict(sqlite3.connect(f"file:{a.bundle}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))[
        "section_handles"]
    d = B.build(bundle=a.bundle, store=a.store, status_index=a.status_index, export_dir=a.export_dir,
                out_root=a.out_root, data_release=a.data_release, lakes=a.lake_facts,
                synopsis=syn if syn is not None and syn.exists() else None, reports=a.reports, previous=a.previous,
                tiles=tiles_block(a.tiles_release_root, handles, a.tiles / "atlas.meta.json"),
                # T75: the released basemap (its sidecar beside the tiles), so a native install can fetch the ground
                basemap=None if a.no_basemap else basemap_block(a.tiles_release_root, a.tiles / "basemap.meta.json"),
                # the date each source was last updated, read from the fetched inputs (the app's Data sources screen)
                sources=sources_block(a.source, synopsis_title=BOOK_TITLE, dfo_rules=a.dfo_rules))
    sz = B.sizes(d)
    print(json.dumps({"release": str(d), "gz_total": sum(v["gz"] for v in sz.values()), "files": sz}, indent=1))
    bad = B.gate_problems(d)
    for p in bad:
        print(f"SIZE GATE: {p}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
