"""Promote a side atlas build to the one the pipeline serves.

    python -m pipeline.atlas.promote data/generated/atlas/full_next            # -> full, full -> full.prev
    python -m pipeline.atlas.promote data/generated/atlas/full_next --dry-run  # parity report only

A PROMOTED ATLAS IS IMMUTABLE (`pipeline.common.vintage.promoted_atlas`): the bundle, the status
index, the export and the tiles all key sections by its `section_handles.txt` digest, and the atlas
build is not deterministic, so nothing may build INTO the promoted directory. A new atlas is built
beside it (`<name>_next`) and takes its place here, by rename — after `pipeline.tools.audit.build_parity`
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
            log=print, target: Path | None = None, tiles: Path | None = None,
            bundle: Path | None = None) -> dict:
    """Make `candidate` the build called `name` (default: the configured default build) — or the
    build directory `target` (T136: a scratch atlas a review rehearsal serves). `tiles` / `bundle`
    are the shipped artifacts whose atlas the `.prev` may still be (default: the live ones). Returns
    {"from", "to", "prev", "parity"}; `parity` is build_parity's report text."""
    if target is not None and name is not None:
        raise SystemExit("promote: name a build (--to) or a directory (--target), not both")
    candidate = Path(candidate).resolve()
    target = Path(target).resolve() if target is not None else GENERATED.build(name).resolve()
    prev = target.with_name(target.name + ".prev")
    if not (candidate / HANDLES).exists() or not (candidate / "registry.json").exists():
        raise SystemExit(f"promote: {candidate} is not a finished atlas (no {HANDLES} / "
                         f"registry.json)")
    if candidate == target:
        raise SystemExit(f"promote: {candidate} already is the promoted build")
    parity = ""
    if target.is_dir():
        got = subprocess.run([sys.executable, "-m", "pipeline.tools.audit.build_parity",
                              str(target), str(candidate)],
                             capture_output=True, text=True, check=False)
        parity = (got.stdout + got.stderr).strip()
        log(parity)
    if dry_run:
        return {"from": str(candidate), "to": str(target), "prev": str(prev), "parity": parity,
                "dry_run": True}
    if prev.exists():
        from pipeline.common.vintage import promoted_atlas
        carrier = promoted_atlas(prev, Path(tiles) if tiles else GENERATED.tiles,
                                 Path(bundle) if bundle else GENERATED.bundle / "bundle.sqlite")
        # the current build carries the SAME sid space (a promote that moved only the registry): it becomes the new
        # `.prev` and still is what the shipped artifacts were cut from, so the old `.prev` holds nothing they need
        if carrier and _same_handles(prev, target):
            carrier = None
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


def _same_handles(a: Path, b: Path) -> bool:
    """Whether two atlas directories have one handle table (`section_handles.txt` digest)."""
    from pipeline.common.section_handles import digest_for
    try:
        return (a / HANDLES).exists() and (b / HANDLES).exists() and digest_for(a) == digest_for(b)
    except Exception:            # noqa: BLE001 — unreadable: not provably the same
        return False


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.atlas.promote",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("candidate", type=Path, help="the finished side build (e.g. .../full_next)")
    ap.add_argument("--to", default=None, help="the build name to become (default: the "
                                                 "configured default build)")
    ap.add_argument("--target", type=Path, default=None,
                    help="the build DIRECTORY to become (a scratch atlas), instead of a name under the live builds")
    ap.add_argument("--tiles", type=Path, default=None,
                    help="the tile set the .prev may still be the atlas of (default: the live tiles)")
    ap.add_argument("--bundle", type=Path, default=None,
                    help="the bundle the .prev may still be the atlas of (default: the live bundle)")
    ap.add_argument("--dry-run", action="store_true", help="print the parity report and stop")
    a = ap.parse_args(argv)
    promote(a.candidate, a.to, dry_run=a.dry_run, target=a.target, tiles=a.tiles, bundle=a.bundle)
    return 0


if __name__ == "__main__":
    sys.exit(main())
