"""ONE DELIVERY: bundle -> verdicts -> status index -> UI export, each cut from the one before.

    python -m pipeline.deliver                       # every step, canonical paths
    python -m pipeline.deliver bundle [--build DIR] [--reaches DIR] [--out FILE]
    python -m pipeline.deliver verdicts [--bundle FILE] [--out FILE] [--workers N]
    python -m pipeline.deliver answers [--bundle FILE] [--export-dir DIR] [--out FILE]
    python -m pipeline.deliver status_index [--bundle FILE] [--out FILE]
    python -m pipeline.deliver store [--bundle FILE] [--export-dir DIR] [--out FILE] [--blobs CODEC]
    python -m pipeline.deliver export [--bundle FILE] [--out FILE]
    python -m pipeline.deliver all --out-dir DIR [--build DIR] [--reaches DIR]

The artifacts a client reads — the bundle, the status index, and the export pair
(`ui-rules-export.json` + `ui-rules-guide.json`, one run) — are three commands
(`pipeline.deliver.bundle`, `pipeline.deliver.status_index`, `pipeline.tools.export_ui_rules`),
and each still runs alone — iterating on the export rebuilds only the export. The REGS STORE
(`regs.sqlite`, `pipeline.deliver.store`) is the last step: the answers and the export words as
SQLite, built from the files beside it and proved to decode back to them byte for byte. What this adds is
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
    build(build_dir, out, reaches=a.reaches, entries=a.entries)
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.deliver", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="step")
    b = sub.add_parser("bundle", help="the SQLite bundle from an atlas and its reach run")
    b.add_argument("--build", type=Path, default=None)
    b.add_argument("--reaches", type=Path, default=None)
    b.add_argument("--entries", type=Path, default=None)
    b.add_argument("--out", type=Path, default=None)
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
    st = sub.add_parser("store", help="the regs store (regs.sqlite) from the answers + export pair")
    st.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    st.add_argument("--export-dir", type=Path, default=None,
                    help="the export pair's and the answers' directory (default: the canonical export's)")
    st.add_argument("--out", type=Path, default=None,
                    help="default: pipeline.deliver.store.OUT (regs.sqlite beside the bundle)")
    st.add_argument("--blobs", default="zdict", choices=("json", "deflate", "zdict"))
    al = sub.add_parser("all", help="bundle, then the index and the export from THAT bundle")
    al.add_argument("--build", type=Path, default=None)
    al.add_argument("--reaches", type=Path, default=None)
    al.add_argument("--entries", type=Path, default=None)
    al.add_argument("--out-dir", type=Path, default=None,
                    help="write bundle.sqlite, status_index.bin, ui-rules-export.json, "
                         "ui-rules-guide.json, ui-rules-answers.json and regs.sqlite here "
                         "(default: the canonical paths)")
    a = ap.parse_args(argv)
    if a.step in (None, "all"):
        if a.step is None:
            a.build = a.reaches = a.entries = None
            a.out_dir = None
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        d = a.out_dir
        bundle = (d / "bundle.sqlite") if d else (GENERATED.bundle / "bundle.sqlite")
        # EACH STEP IN ITS OWN PROCESS (DATAFLOW P1b, M10.1): CPython rarely hands a fragmented
        # heap back, so a step run in-process inherited the bundle build's peak. A step that
        # fails stops the chain (`check=True`); each still runs alone.
        b = ["bundle", "--out", str(bundle)]
        for flag, v in (("--build", a.build), ("--reaches", a.reaches), ("--entries", a.entries)):
            if v is not None:
                b += [flag, str(v)]
        _step(b)
        _step(["verdicts", "--bundle", str(bundle), "--out", str(bundle.with_name("verdicts.sqlite"))])
        _step(["status_index", "--bundle", str(bundle), "--out",
               str((d / "status_index.bin") if d else GENERATED.bundle / "status_index.bin")])
        export_out = (d / "ui-rules-export.json") if d else _EXPORT_OUT
        _step(["export", "--bundle", str(bundle), "--out", str(export_out)])
        _step(["answers", "--bundle", str(bundle), "--export-dir", str(export_out.parent)])
        from pipeline.deliver.store import OUT as _STORE_OUT
        _step(["store", "--bundle", str(bundle), "--export-dir", str(export_out.parent),
               "--out", str((d / "regs.sqlite") if d else _STORE_OUT)])
        from pipeline.common.vintage import report
        ok, msg = report(GENERATED.tiles, bundle)
        print(msg)
        return 0 if ok else 1
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
    elif a.step == "store":
        from pipeline.deliver.store import OUT as _STORE_OUT
        from pipeline.deliver.store.__main__ import main as store_main
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        out = a.out or _STORE_OUT
        if store_main(["build", "--bundle", str(a.bundle), "--export-dir", str(exp),
                       "--out", str(out), "--blobs", a.blobs]):
            raise SystemExit(f"pipeline.deliver: the store was refused — {out} was not written")
    elif a.step == "export":
        from pipeline.tools.export_ui_rules import OUT as _EXPORT_OUT
        _export(a.bundle, a.out or _EXPORT_OUT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
