"""CLI for the regs store (no credits; nothing here calls the claude CLI).

  python -m pipeline.deliver.store build [--bundle FILE] [--export-dir DIR] [--answers FILE]
                                         [--out FILE] [--blobs json|deflate|zdict]
  python -m pipeline.deliver.store check --store FILE [--answers FILE] [--export-dir DIR]
      decode the store and compare: the answers bytes identical, the export subset equal
  python -m pipeline.deliver.store measure --store FILE [--variants DIR …build inputs]
      sizes per table and compressed; with --variants, build every blob codec into DIR and
      measure each
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED
from pipeline.deliver.store import OUT
from pipeline.deliver.store.common import CODECS, StoreError


def _inputs(p: argparse.ArgumentParser) -> None:
    from pipeline.tools.export_ui_rules import OUT as EXPORT_OUT
    p.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    p.add_argument("--export-dir", type=Path, default=EXPORT_OUT.parent)
    p.add_argument("--answers", type=Path, default=None,
                   help="default: ui-rules-answers.json in the export's directory")


def build_cmd(a) -> int:
    from pipeline.deliver.store.build import build
    answers = a.answers or a.export_dir / "ui-rules-answers.json"
    try:
        counts = build(answers, a.export_dir, a.bundle, a.out, blobs=a.blobs)
    except StoreError as e:
        print(f"refused: {e}", file=sys.stderr)
        return 2
    print(json.dumps({"file": str(a.out), "bytes": a.out.stat().st_size, "blobs": a.blobs,
                      "counts": counts}))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.deliver.store", description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    _inputs(b)
    b.add_argument("--out", type=Path, default=OUT)
    b.add_argument("--blobs", choices=CODECS, default="zdict")
    c = sub.add_parser("check")
    _inputs(c)
    c.add_argument("--store", type=Path, default=OUT)
    m = sub.add_parser("measure")
    _inputs(m)
    m.add_argument("--store", type=Path, default=OUT)
    m.add_argument("--variants", type=Path, default=None,
                   help="build regs.<codec>.sqlite for every blob codec here and measure each")
    a = ap.parse_args(argv)
    if a.cmd == "build":
        return build_cmd(a)
    if a.cmd == "check":
        from pipeline.deliver.store.build import load_inputs
        from pipeline.deliver.store.common import export_subset
        from pipeline.deliver.store.decode import Store
        from pipeline.tools.export_codec import expand
        raw, _A, E, G = load_inputs(a.answers or a.export_dir / "ui-rules-answers.json",
                                    a.export_dir)
        s = Store(a.store)
        try:
            same = s.answers_bytes(check=False) == raw
            sub_ok = s.export_subset(check=False) == export_subset(expand(E, G))
        finally:
            s.close()
        print(json.dumps({"answers_identical": same, "export_subset_equal": sub_ok}))
        return 0 if same and sub_ok else 1
    from pipeline.deliver.store.measure import compressed, measure
    from pipeline.deliver.store.build import build
    answers = a.answers or a.export_dir / "ui-rules-answers.json"
    report = {"inputs": {"answers": compressed(answers.read_bytes())}}
    if a.variants:
        a.variants.mkdir(parents=True, exist_ok=True)
        for codec in CODECS:
            out = a.variants / f"regs.{codec}.sqlite"
            build(answers, a.export_dir, a.bundle, out, blobs=codec)
            report[codec] = measure(out)
    else:
        report["store"] = measure(a.store)
    print(json.dumps(report, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
