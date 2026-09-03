"""CLI: python -m pipeline.deliver.bundle [--build DIR] [--out FILE]"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.deliver.bundle.build import build
from pipeline.common.curated import REPO_ROOT

ROOT = REPO_ROOT

ap = argparse.ArgumentParser(prog="pipeline.deliver.bundle", description=__doc__)
ap.add_argument("--build", type=Path, default=ROOT / "output" / "v2" / "full",
                help="a completed build directory (contains registry.json)")
ap.add_argument("--out", type=Path, default=ROOT / "output" / "bundle" / "bundle.sqlite")
a = ap.parse_args()
print(f"bundling {a.build} -> {a.out}")
build(a.build, a.out, data_dir=ROOT / "data")
