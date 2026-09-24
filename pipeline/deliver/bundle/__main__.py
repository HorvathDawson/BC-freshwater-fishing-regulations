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
ap.add_argument("--reaches", type=Path, default=None,
                help="a reach run directory to read instead of the newest one under "
                     "generated.reaches that names this atlas (it must name it too)")
ap.add_argument("--entries", type=Path, default=None,
                help="a directory of entry sources to read instead of the curated ones — for a "
                     "side build against a reach run made with `reach.cli --entries`")
a = ap.parse_args()
if a.build is None:
    a.build = GENERATED.require_build()
print(f"bundling {a.build} -> {a.out}")
# No data_dir: it comes from config. Passing `ROOT / "data"` here is what made the
# fetched-source move invisible — build() had the right default and this overrode it.
build(a.build, a.out, reaches=a.reaches, entries=a.entries)

# THE PAIR THAT SHIPS, checked here because this is where it changes. A bundle built to a
# side path is a normal thing to do — but the app opens the canonical one, and building a
# new bundle while leaving the old one in place is exactly how the tiles and the
# regulations came apart last time.
from pipeline.common.vintage import report                                  # noqa: E402

_canonical = GENERATED.bundle / "bundle.sqlite"
_ok, _msg = report(GENERATED.tiles, _canonical)
print(_msg)
if a.out.resolve() != _canonical.resolve():
    print(f"  note: this build wrote {a.out}, which is NOT the bundle the app opens.\n"
          f"        Promote it when you are satisfied:  cp {a.out} {_canonical}")

