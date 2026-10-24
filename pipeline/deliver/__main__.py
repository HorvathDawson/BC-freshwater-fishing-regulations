"""ONE DELIVERY: bundle -> verdicts -> status index -> UI export, each cut from the one before.

    python -m pipeline.deliver                       # every step, canonical paths
    python -m pipeline.deliver bundle [--build DIR] [--reaches DIR] [--out FILE]
    python -m pipeline.deliver verdicts [--bundle FILE] [--out FILE] [--workers N]
    python -m pipeline.deliver answers [--bundle FILE] [--export-dir DIR] [--out FILE]
    python -m pipeline.deliver status_index [--bundle FILE] [--out FILE]
    python -m pipeline.deliver card_check [--bundle FILE] [--export-dir DIR]
    python -m pipeline.deliver store [--bundle FILE] [--export-dir DIR] [--out FILE] [--blobs CODEC]
    python -m pipeline.deliver export [--bundle FILE] [--out FILE]
    python -m pipeline.deliver all --out-dir DIR [--build DIR] [--reaches DIR] [--entries DIR] [--tiles DIR] [--debug-json]

The artifacts a client reads — the bundle, the status index, and the export pair
(`ui-rules-export.json` + `ui-rules-guide.json`, one run) — are three commands
(`pipeline.deliver.bundle`, `pipeline.deliver.status_index`, `pipeline.tools.export_ui_rules`),
and each still runs alone — iterating on the export rebuilds only the export. The REGS STORE
(`pipeline.deliver.store`) is the release's input: the answers and the export words as SQLite, built from the files
beside it and proved to decode back to them byte for byte. NO `regs.sqlite` IS KEPT (P20): `all` builds it in its
staging directory, the release ships its tables (water, regs-<region>, regs-sid), and every reader reads those
(`pipeline.deliver.store.release.province_store`); `--debug-json` keeps a copy at `store.DEBUG_OUT`. What this adds is
the CHAIN: `all` writes them all into one directory from one bundle, and every step reads its input from the previous step's
output and nothing else, so the index and the export are cut from the bundle bytes beside them.
The vintage line (`pipeline.common.vintage`) and the two digests every artifact carries
(`section_handles`, `reach_digest`; the status index header holds both since version 2) are how
a reader proves it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED


def _bundle(a) -> Path:
    from pipeline.deliver.bundle.build import build
    build_dir = a.build or GENERATED.require_build()
    out = a.out or (GENERATED.bundle / "bundle.sqlite")
    print(f"bundling {build_dir} -> {out}")
    build(build_dir, out, reaches=a.reaches, entries=a.entries, tiles=a.tiles)
    return out


def _verdicts(bundle: Path, out: Path, workers: int) -> Path:
    from pipeline.deliver.verdicts.build import build
    build(str(bundle), out, workers=workers)
    return out


def _status_index(bundle: Path, out: Path) -> Path:
    from pipeline.deliver import status_index as si
    si.main(["--bundle", str(bundle), "--out", str(out)])
    return out


def _export(bundle: Path, out: Path) -> Path:
    from pipeline.tools import export_ui_rules as ex
    # a REFUSED export (problems, a lossy or dangling encoding) leaves the old pair on disk and
    # must fail the command, never exit 0
    if ex.main(["--bundle", str(bundle), "--out", str(out)]):
        raise SystemExit(f"pipeline.deliver: the export was refused — {out} was not written")
    return out


def _step(args: list) -> None:
    """One step of the chain, as `python -m pipeline.deliver <step> …` in a fresh process."""
    import subprocess
    print(f"pipeline.deliver: {' '.join(args)}", flush=True)
    subprocess.run([sys.executable, "-m", "pipeline.deliver", *args], check=True)


def canonical_homes(debug: bool = False) -> dict:
    """Where each file `deliver all` writes lives when no --out-dir is given: {file name: its live path}. The regs store
    has no home (P20): it is kept only with `debug` (`--debug-json`), at `store.DEBUG_OUT`."""
    from pipeline.deliver.answers.card_check import STAMP_NAME
    from pipeline.deliver.store import DEBUG_OUT
    from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
    b, r = GENERATED.bundle, _EXPORT_OUT.parent
    return {"bundle.sqlite": b / "bundle.sqlite", "verdicts.sqlite": b / "verdicts.sqlite",
            "status_index.bin": b / "status_index.bin",
            "ui-rules-export.json": _EXPORT_OUT, "ui-rules-guide.json": r / "ui-rules-guide.json",
            "ui-rules-answers.json": r / "ui-rules-answers.json", STAMP_NAME: r / STAMP_NAME,
            **({"regs.sqlite": DEBUG_OUT} if debug else {})}


def _all(a) -> int:
    """THE CHAIN, ATOMIC (P07). Every step writes into a staging directory beside the destination (on the same disk, so
    `os.replace` is a rename). Only when the last step AND `vintage.report_all(strict)` pass is each file moved into
    place: into --out-dir, or to its canonical home (`canonical_homes`). A failed step or a vintage mismatch moves
    nothing, and the staging directory is removed either way, so the live files are never a mix of two runs.

    `--tiles DIR` (T136): the tile set the chain is cut against — the bundle's access measure, the vintage check and
    the release's tile block — instead of the live `GENERATED.tiles`; its released folder is looked up in the
    release root this run writes to (`<out-dir>/release`, or the live one). A scratch rebuild (a review rehearsed on
    copies) then reads and writes no live file."""
    import os
    import shutil
    import tempfile
    from pipeline.common import vintage
    dest = a.out_dir
    base = dest.parent if dest else GENERATED.bundle.parent
    base.mkdir(parents=True, exist_ok=True)
    d = Path(tempfile.mkdtemp(prefix=".deliver-staging-", dir=base))
    try:
        bundle = d / "bundle.sqlite"
        # EACH STEP IN ITS OWN PROCESS (DATAFLOW P1b, M10.1): CPython rarely hands a fragmented
        # heap back, so a step run in-process inherited the bundle build's peak. A step that
        # fails stops the chain (`check=True`); each still runs alone.
        b = ["bundle", "--out", str(bundle)]
        for flag, v in (("--build", a.build), ("--reaches", a.reaches), ("--entries", a.entries),
                        ("--tiles", a.tiles)):
            if v is not None:
                b += [flag, str(v)]
        _step(b)
        _step(["verdicts", "--bundle", str(bundle), "--out", str(d / "verdicts.sqlite")])
        _step(["status_index", "--bundle", str(bundle), "--out", str(d / "status_index.bin")])
        _step(["export", "--bundle", str(bundle), "--out", str(d / "ui-rules-export.json")])
        _step(["answers", "--bundle", str(bundle), "--export-dir", str(d)])
        # THE CARD GATE (P05): every number on every card of the answers just written, proved
        # against the bundle's rules; a problem stops the chain here, before anything is packaged
        _step(["card_check", "--bundle", str(bundle), "--export-dir", str(d)])
        _step(["store", "--bundle", str(bundle), "--export-dir", str(d), "--out", str(d / "regs.sqlite")])
        # THE SHIPPED SET (P08): data/release/<data_release>/, from the files just made
        rel_root = (dest / "release") if dest else GENERATED.release
        tiles = ["--tiles", str(a.tiles), "--tiles-release-root", str(rel_root)] if a.tiles else []
        _step(["release", "--bundle", str(bundle), "--store", str(d / "regs.sqlite"), "--status-index",
               str(d / "status_index.bin"), "--export-dir", str(d), "--out-root", str(d / "release"), *tiles])
        ok, msg = vintage.report_all(a.tiles or GENERATED.tiles, vintage.Artifacts.in_dir(d), strict=True)
        print(msg)
        if not ok:
            print("pipeline.deliver: vintage refused — nothing was promoted", file=sys.stderr)
            return 1
        debug = a.debug_json
        if not debug:                        # P20: the store was the release's input; the release holds its tables
            (d / "regs.sqlite").unlink(missing_ok=True)
        homes = canonical_homes(debug)
        # the release folder(s) go to data/release/ (or <out-dir>/release/), whole
        releases = sorted(p for p in (d / "release").iterdir()) if (d / "release").exists() else []
        names = sorted(p.name for p in d.iterdir() if p.name != "release")
        unknown = [n for n in names if dest is None and n not in homes]
        if unknown:
            raise SystemExit(f"pipeline.deliver: no home for {unknown} — nothing was promoted")
        if dest:
            dest.mkdir(parents=True, exist_ok=True)
        for n in names:
            target = dest / n if dest else homes[n]
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(d / n, target)
        for r in releases:
            rel_root.mkdir(parents=True, exist_ok=True)
            if (rel_root / r.name).exists():          # the same content id: identical bytes, replaced whole
                shutil.rmtree(rel_root / r.name)
            os.replace(r, rel_root / r.name)
        print(f"pipeline.deliver: promoted {len(names)} files to {dest or 'their canonical homes'}"
              + (f" and release {', '.join(r.name for r in releases)} to {rel_root}" if releases else ""))
        return 0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["release"]:                 # its flags are its own module's (argparse cannot pass them through)
        from pipeline.deliver.release.__main__ import main as release_main
        if release_main(argv[1:]):
            raise SystemExit("pipeline.deliver: the release was refused (a size gate) — nothing was packaged")
        return 0
    ap = argparse.ArgumentParser(prog="pipeline.deliver", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="step")
    b = sub.add_parser("bundle", help="the SQLite bundle from an atlas and its reach run")
    b.add_argument("--build", type=Path, default=None)
    b.add_argument("--reaches", type=Path, default=None)
    b.add_argument("--entries", type=Path, default=None)
    b.add_argument("--out", type=Path, default=None)
    b.add_argument("--tiles", type=Path, default=None,
                   help="the tile set the access measure reads (default: the live tiles)")
    v = sub.add_parser("verdicts", help="the reader's every answer, once: verdicts.sqlite")
    v.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    v.add_argument("--out", type=Path, default=None,
                   help="default: verdicts.sqlite beside the bundle")
    v.add_argument("--workers", type=int, default=4)
    s = sub.add_parser("status_index", help="the status index from a bundle")
    s.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    s.add_argument("--out", type=Path, default=GENERATED.bundle / "status_index.bin")
    e = sub.add_parser("export", help="the UI rules export from a bundle")
    e.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    e.add_argument("--out", type=Path, default=None,
                   help="the data file; ui-rules-guide.json is written beside it")
    an = sub.add_parser("answers", help="the answers file (answers/2) beside the export pair")
    an.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    an.add_argument("--export-dir", type=Path, default=None,
                    help="the export pair's directory (default: the canonical export's)")
    an.add_argument("--out", type=Path, default=None,
                    help="default: ui-rules-answers.json in the export's directory")
    an.add_argument("--workers", type=int, default=4)
    cc = sub.add_parser("card_check", help="the card gate on the shipped answers; writes card-check.json")
    cc.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    cc.add_argument("--export-dir", type=Path, default=None,
                    help="the answers' directory (default: the canonical export's)")
    sub.add_parser("release", help="the shipped set: data/release/<data_release>/ (P08); its own flags: "
                                   "python -m pipeline.deliver.release --help")
    st = sub.add_parser("store", help="the regs store (regs.sqlite) from the answers + export pair — a debug copy: "
                                      "`all` makes it in staging for the release and keeps none (P20)")
    st.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    st.add_argument("--export-dir", type=Path, default=None,
                    help="the export pair's and the answers' directory (default: the canonical export's)")
    st.add_argument("--out", type=Path, default=None,
                    help="default: pipeline.deliver.store.DEBUG_OUT (data/generated/debug/regs.sqlite)")
    st.add_argument("--blobs", default="zdict", choices=("json", "deflate", "zdict"))
    al = sub.add_parser("all", help="bundle, then the index and the export from THAT bundle")
    al.add_argument("--build", type=Path, default=None)
    al.add_argument("--reaches", type=Path, default=None)
    al.add_argument("--entries", type=Path, default=None)
    al.add_argument("--tiles", type=Path, default=None,
                    help="the tile set to cut against (default: the live tiles); its released folder is looked up "
                         "in this run's release root")
    al.add_argument("--out-dir", type=Path, default=None,
                    help="write bundle.sqlite, status_index.bin, ui-rules-export.json, "
                         "ui-rules-guide.json, ui-rules-answers.json (and, with --debug-json, regs.sqlite) here "
                         "(default: the canonical paths)")
    al.add_argument("--debug-json", action="store_true",
                    help="also keep the regs store the release was cut from (regs.sqlite: in --out-dir, else "
                         "pipeline.deliver.store.DEBUG_OUT); by default none is kept (P20)")
    a = ap.parse_args(argv)
    if a.step in (None, "all"):
        if a.step is None:
            a.build = a.reaches = a.entries = a.tiles = None
            a.out_dir = None
            a.debug_json = False
        return _all(a)
    if a.step == "bundle":
        _bundle(a)
    elif a.step == "verdicts":
        _verdicts(a.bundle, a.out or a.bundle.with_name("verdicts.sqlite"), a.workers)
    elif a.step == "status_index":
        _status_index(a.bundle, a.out)
    elif a.step == "answers":
        from pipeline.deliver.answers.cli import main as answers_main
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        out = a.out or exp / "ui-rules-answers.json"
        rc = answers_main(["build", "--bundle", str(a.bundle), "--export-dir", str(exp),
                           "--out", str(out), "--workers", str(a.workers)])
        if rc:
            raise SystemExit(f"pipeline.deliver: the answers were refused — {out} was not written")
    elif a.step == "card_check":
        from pipeline.deliver.answers.card_check import main as card_main
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        if card_main(["--all", "--bundle", str(a.bundle), "--export-dir", str(exp)]):
            raise SystemExit(f"pipeline.deliver: the card gate found problems in {exp / 'ui-rules-answers.json'} "
                             "— nothing was packaged")
    elif a.step == "store":
        from pipeline.deliver.store import DEBUG_OUT
        from pipeline.deliver.store.__main__ import main as store_main
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        out = a.out or DEBUG_OUT
        if store_main(["build", "--bundle", str(a.bundle), "--export-dir", str(exp),
                       "--out", str(out), "--blobs", a.blobs]):
            raise SystemExit(f"pipeline.deliver: the store was refused — {out} was not written")
    elif a.step == "export":
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        _export(a.bundle, a.out or _EXPORT_OUT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
