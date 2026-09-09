"""Do the shipped tiles and the shipped bundle come from one atlas?

A section is an integer handle — an index into the atlas's `section_handles.txt` — carried by
BOTH artifacts. Build them from different atlases and they do not miss each other: they agree
on a number that means two different rivers, so every lookup succeeds and every answer is
about the wrong water.

THIS IS NOT HYPOTHETICAL. The tiles were rebuilt with new handles while `bundle.sqlite` was
left behind at an older build, and the app's Conditions map painted every river as
unmeasured — a state it draws on purpose, so it read as a quiet feed rather than a broken
pair. Nothing errored at any layer.

The app refuses to colour a mismatched pair (see `useVintage`). This is the same check one
step earlier, against what is actually ON DISK at the paths that get shipped, so a build says
so at the moment the pair diverges rather than a person finding out by looking at a map.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def tile_vintage(tiles_dir: Path) -> str | None:
    """The handle digest the tile sidecar publishes, or None if there is no sidecar."""
    p = Path(tiles_dir) / "atlas.meta.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8")).get("section_handles") or None
    except (OSError, ValueError):
        return None


def bundle_vintage(bundle: Path) -> str | None:
    """The handle digest a built bundle records in `meta`, or None if it predates the field."""
    p = Path(bundle)
    if not p.exists():
        return None
    try:
        db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            row = db.execute("SELECT v FROM meta WHERE k = 'section_handles'").fetchone()
        finally:
            db.close()
        return (row[0] or None) if row else None
    except sqlite3.Error:
        return None


def report(tiles_dir: Path, bundle: Path) -> tuple[bool, str]:
    """`(ok, message)` for the pair that would actually ship.

    `ok` is True only when BOTH artifacts exist and agree, or when one of them has not been
    built at all — a checkout that has not produced both is not a mismatch.

    ONE DIGEST AND NOT THE OTHER IS A MISMATCH, not an absence. That is the shape the real
    failure took: the tiles were rebuilt carrying handles while the shipped bundle still
    keyed sections by string, and treating "no digest" as "nothing to compare" would let
    exactly that pair through. An artifact with no digest predates the handle scheme, so it
    cannot be the partner of one that has it.
    """
    tp, bp = Path(tiles_dir) / "atlas.pmtiles", Path(bundle)
    if not tp.exists() or not bp.exists():
        missing = tp if not tp.exists() else bp
        return True, f"  vintage: not checked — {missing} has not been built"
    t, b = tile_vintage(tiles_dir), bundle_vintage(bundle)
    if t is None and b is None:
        return True, "  vintage: not checked — neither artifact records a handle digest"
    if t is None or b is None:
        stale = "tiles" if t is None else "bundle"
        return False, (
            f"  ⚠️  VINTAGE MISMATCH — the {stale} predates the section handle table\n"
            f"        tiles  {tp}  {t or 'no digest'}\n"
            f"        bundle {bp}  {b or 'no digest'}\n"
            f"      One of these keys sections by an integer handle and the other by a\n"
            f"      string. Rebuild the older one.")
    if t == b:
        return True, f"  vintage: tiles and bundle agree ({t})"
    return False, (
        f"  ⚠️  VINTAGE MISMATCH — these two must not ship together\n"
        f"        tiles  {Path(tiles_dir) / 'atlas.pmtiles'}  {t}\n"
        f"        bundle {bundle}  {b}\n"
        f"      A section is an index into the atlas's handle table, so a mixed pair does\n"
        f"      not fail to match — it matches the WRONG SECTION. Rebuild whichever is\n"
        f"      older, or copy the bundle you just built over the one that ships.")
