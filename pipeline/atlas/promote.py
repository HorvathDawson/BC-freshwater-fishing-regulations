"""Promote a side atlas build to the one the pipeline serves.

    python -m pipeline.atlas.promote data/generated/atlas/full_next            # -> full, full -> full.prev
    python -m pipeline.atlas.promote data/generated/atlas/full_next --dry-run  # parity report only

A PROMOTED ATLAS IS IMMUTABLE (`pipeline.common.vintage.promoted_atlas`): the bundle, the status
index, the export and the tiles all key sections by its `section_handles.txt` digest, and the atlas
build is not deterministic, so nothing may build INTO the promoted directory. A new atlas is built
beside it (`<name>_next`) and takes its place here, by rename — after `pipeline.tools.build_parity`
has said what changed between the two registries. The previous atlas is kept as `<name>.prev` until
the next promotion, so the derivatives that still carry its digest can be rebuilt or compared.

Renames, never copies: an atlas is ~9 GB. Refuses a candidate with no handle table, a candidate
that IS the current build, and a `.prev` that a shipped artifact still carries (it would be
deleted).
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED
from pipeline.common.section_handles import FILENAME as HANDLES


def promote(candidate: Path, name: str | None = None, *, dry_run: bool = False,
            log=print) -> dict:
    """Make `candidate` the build called `name` (default: the configured default build). Returns
    {"from", "to", "prev", "parity"}; `parity` is build_parity's report text."""
    candidate = Path(candidate).resolve()
    target = GENERATED.build(name).resolve()
    prev = target.with_name(target.name + ".prev")
    if not (candidate / HANDLES).exists() or not (candidate / "registry.json").exists():
        raise SystemExit(f"promote: {candidate} is not a finished atlas (no {HANDLES} / "
                         f"registry.json)")
    if candidate == target:
        raise SystemExit(f"promote: {candidate} already is the promoted build")
    parity = ""
    if target.is_dir():
        got = subprocess.run([sys.executable, "-m", "pipeline.tools.build_parity",
                              str(target), str(candidate)],
                             capture_output=True, text=True, check=False)
        parity = (got.stdout + got.stderr).strip()
        log(parity)
    if dry_run:
        return {"from": str(candidate), "to": str(target), "prev": str(prev), "parity": parity,
                "dry_run": True}
    if prev.exists():
        from pipeline.common.vintage import promoted_atlas
        carrier = promoted_atlas(prev, GENERATED.tiles, GENERATED.bundle / "bundle.sqlite")
        if carrier:
            raise SystemExit(f"promote: {prev} is still what {carrier} was cut from — rebuild the "
                             f"shipped artifacts from {target} before promoting again")
        shutil.rmtree(prev)
    if target.is_dir():
        target.rename(prev)
        log(f"  {target.name} -> {prev.name}")
    candidate.rename(target)
    log(f"  {candidate.name} -> {target.name}")
    return {"from": str(candidate), "to": str(target), "prev": str(prev), "parity": parity}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.atlas.promote",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("candidate", type=Path, help="the finished side build (e.g. .../full_next)")
    ap.add_argument("--to", default=None, help="the build name to become (default: the "
                                                 "configured default build)")
    ap.add_argument("--dry-run", action="store_true", help="print the parity report and stop")
    a = ap.parse_args(argv)
    promote(a.candidate, a.to, dry_run=a.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
