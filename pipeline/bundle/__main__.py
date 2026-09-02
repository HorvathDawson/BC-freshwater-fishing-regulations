"""CLI: python -m pipeline.bundle [--build DIR] [--out FILE]"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.bundle.build import build

ROOT = Path(__file__).resolve().parents[2]

ap = argparse.ArgumentParser(prog="pipeline.bundle", description=__doc__)
ap.add_argument("--build", type=Path, default=ROOT / "output" / "v2" / "full",
                help="a completed build directory (contains registry.json)")
ap.add_argument("--out", type=Path, default=ROOT / "output" / "bundle" / "bundle.sqlite")
a = ap.parse_args()
print(f"bundling {a.build} -> {a.out}")
build(a.build, a.out, data_dir=ROOT / "data")
