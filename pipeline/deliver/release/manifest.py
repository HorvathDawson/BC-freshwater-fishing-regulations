"""THE MANIFEST (`app-manifest/2`, MINIMAL-PACKAGING §6.2): what an app reads first, on every launch.

    {"format": "app-manifest/2", "data_release": "<id>",
     "tiles":   {"compatible": ["<handles16>.*"], "current": {id, url, bytes, sha256} | null, "assets": …, "mask": …},
     "basemap": {id, url, bytes, sha256} | null,
     "files":   {"<name>": {url, bytes, gz, sha256, format, section_handles}, …},
     "regulations": {"valid_until": "<date>" | null, "versions": {…}}}

A data release built on an atlas is compatible with EVERY tile build of that atlas: `compatible` names the sid space
only. `tiles.current` is filled from the tile version folder when one exists (P18; the tile rebuild is human-run), else
null: the app then keeps the tiles it holds when their handles match.
"""
from __future__ import annotations

from typing import Dict, Optional

FORMAT = "app-manifest/2"


def manifest(data_release: str, files: Dict[str, dict], handles16: str, *, tiles: Optional[dict] = None,
             basemap: Optional[dict] = None, valid_until: Optional[str] = None,
             regs_versions: Optional[dict] = None) -> dict:
    t = {"compatible": [f"{handles16}.*"], "current": None, "assets": None, "mask": None, **(tiles or {})}
    return {"format": FORMAT, "data_release": data_release, "tiles": t, "basemap": basemap, "files": files,
            "regulations": {"valid_until": valid_until, "versions": regs_versions}}


def tiles_block(release_root, handles16: str, tiles_meta) -> Optional[dict]:
    """`tiles.current/assets/mask` from the tile set the PROMOTED tiles are, or None (unstamped or not built yet).

    Which set is current is what `tiles_meta` (data/generated/tiles/atlas.meta.json) says it is, never "the last folder
    by name": the build half of a version is a hash, so name order is arbitrary. A stamped version from another sid
    space, or one whose release folder is missing, is refused — the release would otherwise name tiles the app cannot
    pair or cannot fetch.
    """
    import hashlib
    import json
    from pathlib import Path
    meta = Path(tiles_meta)
    if not meta.exists():
        return None
    version = json.loads(meta.read_text()).get("tile_version")
    if not version:
        return None
    if not version.startswith(handles16 + "."):
        raise SystemExit(f"release: the promoted tiles are {version}, not this sid space ({handles16}.*)")
    v = Path(release_root) / "tiles" / version
    if not (v / f"atlas-{version}.pmtiles").exists():
        raise SystemExit(f"release: the promoted tiles are {version} but {v} holds no atlas — "
                         "run the tile step's release copy (python -m pipeline.deliver.tiles) first")

    def entry(p: Path) -> dict:
        return {"url": f"tiles/{v.name}/{p.relative_to(v)}", "bytes": p.stat().st_size,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
    out = {"current": {"id": v.name, **entry(v / f"atlas-{v.name}.pmtiles")}, "mask": entry(v / "bc_outside.geojson")}
    assets = sorted(v.glob("map-assets-*/assets.json"))
    if assets:
        from pipeline.deliver.tiles.assets import files_sha256
        a = assets[-1]
        out["assets"] = {"id": a.parent.name, "url": f"tiles/{v.name}/{a.parent.name}/",
                         "files_sha256": files_sha256(a), "pin": json.loads(a.read_text())["pin"]}
    return out
