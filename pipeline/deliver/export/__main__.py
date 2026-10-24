"""The UI data export: every regulation record in the bundle, and a guide to reading it.

    .venv/bin/python -m pipeline.deliver.export [--bundle B] [--out OUT] [--guide-out GUIDE] [--pretty]
        [--verdicts V]

Builds the model (`model.build`), refuses it on any `checks.problems`, encodes it (`files.encode`), proves the
encoded pair decodes back to exactly the model (`codec.expand`), and only then writes both files.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from pipeline.deliver.export import codec as K
from pipeline.deliver.export.contract import BUNDLE, GUIDE_NAME, OUT
from pipeline.deliver.export.model import build
from pipeline.deliver.export.checks import problems
from pipeline.deliver.export.verdicts import verdicts_of
from pipeline.deliver.export.files import encode, guide_path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bundle", type=Path, default=BUNDLE)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--guide-out", type=Path, default=None,
                    help=f"the guide file (default: {GUIDE_NAME} beside --out)")
    ap.add_argument("--pretty", action="store_true",
                    help="indent both files for a human (the shipped files are not indented)")
    ap.add_argument("--verdicts", type=Path, default=None,
                    help="the bundle's verdicts (default: verdicts.sqlite beside the bundle)")
    a = ap.parse_args(argv)
    verdicts_of(a.bundle, a.verdicts)          # opened and held to the bundle's digests, once
    doc = build(a.bundle)
    bad = problems(doc)
    try:
        data, gd = encode(doc)
    except K.EncodeError as e:
        bad.append(f"the encoding refuses the model: {e}")
        data = gd = None
    if data is not None:
        bad += K.wire_problems(data, gd)
        # LOSSLESS, proved on what is written: the pair must decode to exactly the model the
        # checks above passed (through JSON, as a reader gets it)
        if K.expand(json.loads(K.dumps(data)), json.loads(K.dumps(gd))) != doc:
            bad.append("the encoded pair does not decode to the model (codec.expand)")
    if bad:
        print(f"pipeline.deliver.export: REFUSED — {len(bad)} problem(s) in the output:", file=sys.stderr)
        for p in bad[:40]:
            print(f"  {p}", file=sys.stderr)
        return 1
    for r in doc["about"]["unresolved_references"]:
        print(f"  corpus reference does not resolve: {r}", file=sys.stderr)
    g_out = a.guide_out or guide_path(a.out)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    g_out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(K.dumps(data, a.pretty), encoding="utf-8")
    g_out.write_text(K.dumps(gd, a.pretty), encoding="utf-8")
    c = doc["about"]["counts"]
    print(f"wrote {a.out} ({os.path.getsize(a.out) / 1e6:.2f} MB) and {g_out} "
          f"({os.path.getsize(g_out) / 1e6:.2f} MB)")
    for k, v in c.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
