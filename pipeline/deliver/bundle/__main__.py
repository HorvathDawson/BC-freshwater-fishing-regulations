"""CLI: python -m pipeline.deliver.bundle [--build DIR] [--out FILE]"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.deliver.bundle.build import build
from pipeline.common.curated import GENERATED

ap = argparse.ArgumentParser(prog="pipeline.deliver.bundle", description=__doc__)
# `require_build` and not `build`: bundling reads a build, and a reader that invents its
# own directory is how this file already went wrong once — it overrode build()'s data_dir
# default and made the fetched-source move invisible. A missing build now names itself.
ap.add_argument("--build", type=Path, default=None,
                help="a completed build directory (contains registry.json); "
                     "default: the `full` build under generated.atlas.builds")
ap.add_argument("--out", type=Path,
                default=GENERATED.bundle / "bundle.sqlite")
a = ap.parse_args()
if a.build is None:
    a.build = GENERATED.require_build()
print(f"bundling {a.build} -> {a.out}")
# No data_dir: it comes from config. Passing `ROOT / "data"` here is what made the
# fetched-source move invisible — build() had the right default and this overrode it.
build(a.build, a.out)
