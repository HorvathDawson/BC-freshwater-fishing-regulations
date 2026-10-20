"""THE TILE VERSION (P18, MINIMAL-PACKAGING §6.2): tiles are geometry only and versioned on their own.

    version = "<handles16>.<tilebuild12>"

`handles16` is the atlas's `section_handles` digest: the sid space, which is the ONLY thing a data file must agree on with
the tiles. `tilebuild12` is the sha256 of the archive's bytes as tippecanoe wrote them, BEFORE this module stamps the
version into the metadata (a version cannot contain the hash of the bytes that contain it). Two tile builds of one atlas
are two versions with one `handles16`, and a data release declares `tiles.compatible: ["<handles16>.*"]`.

`stamp` writes `tile_version`, `section_handles` and `labels_registry` (informational: which registry the labels came
from) into the pmtiles metadata, so an archive swapped under a manifest cannot pass (audit F6). `to_release` copies the
stamped archive, the outside-BC mask and the self-hosted map assets (`assets.py`) into `data/release/tiles/<version>/`; the mask is a separate file, never a tile
layer (owner, MINIMAL-PACKAGING §1).
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


def sha256_file(p: Path, n: int | None = None) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:n] if n else h.hexdigest()


def version_id(handles16: str, tilebuild12: str) -> str:
    if len(handles16) != 16 or len(tilebuild12) != 12:
        raise ValueError(f"tile version: want 16 + 12 hex digits, got {handles16!r}.{tilebuild12!r}")
    return f"{handles16}.{tilebuild12}"


def stamped_metadata(meta: dict, version: str, handles16: str, labels_registry: str | None) -> dict:
    """The archive's metadata with the version keys set (and nothing else changed)."""
    return {**meta, "tile_version": version, "section_handles": handles16, "labels_registry": labels_registry}


def _pmtiles(*args: str) -> str:
    r = subprocess.run(["pmtiles", *args], capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"pmtiles {args[0]} failed ({r.returncode}): {r.stderr[-2000:]}")
    return r.stdout


def read_metadata(archive: Path) -> dict:
    return json.loads(_pmtiles("show", "--metadata", str(archive)) or "{}")


def stamp(archive: Path, build_dir: Path) -> str:
    """Stamp the version into `archive` (in place) and return it."""
    from pipeline.common.section_handles import digest_for, registry_digest_for
    handles16 = digest_for(build_dir)
    version = version_id(handles16, sha256_file(archive, 12))
    meta = stamped_metadata(read_metadata(archive), version, handles16, registry_digest_for(build_dir))
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(meta, fh)
    try:
        _pmtiles("edit", str(archive), "--metadata", fh.name)
    finally:
        Path(fh.name).unlink(missing_ok=True)
    got = read_metadata(archive)
    if got.get("tile_version") != version:
        raise RuntimeError(f"tiles: the version stamp did not stick in {archive} ({got.get('tile_version')!r})")
    return version


def to_release(archive: Path, version: str, release_root: Path, boundary: Path) -> Path:
    """`data/release/tiles/<version>/`: `atlas-<version>.pmtiles` + `bc_outside.geojson`. A version folder is immutable
    (its URL is published), so an existing one is refused rather than overwritten."""
    from pipeline.deliver.tiles import boundary as B
    final = release_root / "tiles" / version
    if final.exists():
        raise SystemExit(f"tiles: {final} already exists — a released tile version is immutable")
    stage = final.with_name(f".{version}.staging")
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True)
    try:
        shutil.copy2(archive, stage / f"atlas-{version}.pmtiles")
        B.build(boundary, stage / "bc_outside.geojson")
        from pipeline.deliver.tiles import assets
        assets.write(stage)                                  # map-assets-<n>/: self-hosted glyphs + sprites
        stage.rename(final)
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    return final
