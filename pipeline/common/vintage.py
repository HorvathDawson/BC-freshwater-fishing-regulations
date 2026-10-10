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


def registry_vintages(tiles_dir: Path, bundle: Path) -> tuple[str | None, str | None]:
    """The registry digests (P2) the tile sidecar and the bundle record — None where absent."""
    t = b = None
    p = Path(tiles_dir) / "atlas.meta.json"
    if p.exists():
        try:
            t = json.loads(p.read_text(encoding="utf-8")).get("registry") or None
        except (OSError, ValueError):
            t = None
    if Path(bundle).exists():
        try:
            db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
            try:
                row = db.execute("SELECT v FROM meta WHERE k = 'registry_digest'").fetchone()
            finally:
                db.close()
            b = (row[0] or None) if row else None
        except sqlite3.Error:
            b = None
    return t, b


def report(tiles_dir: Path, bundle: Path, *, strict: bool = False) -> tuple[bool, str]:
    """`(ok, message)` for the pair that would actually ship.

    `ok` is True only when BOTH artifacts exist and agree, or when one of them has not been
    built at all — a checkout that has not produced both is not a mismatch. `strict` (P2: the
    rebuild command's last stage, where both were just built) refuses that too: a missing
    artifact, or a pair where neither records a digest, is a failure, never "not checked".

    ONE DIGEST AND NOT THE OTHER IS A MISMATCH, not an absence. That is the shape the real
    failure took: the tiles were rebuilt carrying handles while the shipped bundle still
    keyed sections by string, and treating "no digest" as "nothing to compare" would let
    exactly that pair through. An artifact with no digest predates the handle scheme, so it
    cannot be the partner of one that has it.
    """
    tp, bp = Path(tiles_dir) / "atlas.pmtiles", Path(bundle)
    if not tp.exists() or not bp.exists():
        missing = tp if not tp.exists() else bp
        return not strict, f"  vintage: not checked — {missing} has not been built"
    t, b = tile_vintage(tiles_dir), bundle_vintage(bundle)
    if t is None and b is None:
        return not strict, "  vintage: not checked — neither artifact records a handle digest"
    if t is None or b is None:
        stale = "tiles" if t is None else "bundle"
        return False, (
            f"  ⚠️  VINTAGE MISMATCH — the {stale} predates the section handle table\n"
            f"        tiles  {tp}  {t or 'no digest'}\n"
            f"        bundle {bp}  {b or 'no digest'}\n"
            f"      One of these keys sections by an integer handle and the other by a\n"
            f"      string. Rebuild the older one.")
    if t == b:
        # THE REGISTRY TOO (P2): one handle table, but names and kinds from another registry
        # still label and draw the wrong water.
        rt, rb = registry_vintages(tiles_dir, bundle)
        if rt and rb and rt != rb:
            return False, (
                f"  ⚠️  VINTAGE MISMATCH — one handle table ({t}), two registries\n"
                f"        tiles  registry {rt}\n        bundle registry {rb}\n"
                f"      Rebuild whichever is older against the atlas's current registry.json.")
        if strict and not (rt and rb):
            return False, (f"  vintage: handles agree ({t}) but the registry is not recorded by "
                           f"{'the tiles' if not rt else 'the bundle'} — rebuild it")
        return True, f"  vintage: tiles and bundle agree ({t}, registry {rt or rb or 'unrecorded'})"
    return False, (
        f"  ⚠️  VINTAGE MISMATCH — these two must not ship together\n"
        f"        tiles  {Path(tiles_dir) / 'atlas.pmtiles'}  {t}\n"
        f"        bundle {bundle}  {b}\n"
        f"      A section is an index into the atlas's handle table, so a mixed pair does\n"
        f"      not fail to match — it matches the WRONG SECTION. Rebuild whichever is\n"
        f"      older, or copy the bundle you just built over the one that ships.")


def shipped_digests(tiles_dir: Path, bundle: Path) -> dict[str, str]:
    """{handle digest: the shipped artifact carrying it} — the atlases a derivative on disk was
    cut from. Empty when neither artifact exists or records one."""
    out: dict[str, str] = {}
    t = tile_vintage(tiles_dir)
    if t:
        out[t] = str(Path(tiles_dir) / "atlas.pmtiles")
    b = bundle_vintage(bundle)
    if b:
        out.setdefault(b, str(bundle))
    return out


def promoted_atlas(build_dir: Path, tiles_dir: Path, bundle: Path) -> str | None:
    """The shipped artifact (tiles or bundle) that carries the handle digest of the atlas at
    `build_dir`, or None when nothing shipped was cut from it.

    A PROMOTED ATLAS IS IMMUTABLE. Every derivative — the reach run, the bundle, the status index,
    the export, the tiles — is keyed by the atlas's `section_handles.txt` digest, and the atlas
    build is not deterministic (memory `atlas-build-not-deterministic`): rebuilding INTO the
    directory a shipped bundle or tile set names rewrites the handle table under them. Afterwards
    the tiles and the bundle still agree with each other, and both silently disagree with the atlas
    on disk, until the next bundle build finds no reach run for it — hours later, as the only
    symptom. So a build refuses such an `--out` (`pipeline.atlas.build --force` overrides), and the
    curation app builds to `<build>_next` and promotes by an explicit step (`pipeline.atlas.promote`).
    """
    from pipeline.common.section_handles import FILENAME, digest_for
    build_dir = Path(build_dir)
    if not (build_dir / FILENAME).exists():
        return None
    try:
        have = digest_for(build_dir)
    except Exception:            # noqa: BLE001 — an unreadable table is not a promoted atlas
        return None
    return shipped_digests(tiles_dir, bundle).get(have)
