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


def tiles_block(release_root, handles16: str) -> Optional[dict]:
    """`tiles.current/assets/mask` from the newest tile version folder of this sid space, or None (not built yet)."""
    import hashlib
    import json
    from pathlib import Path
    root = Path(release_root) / "tiles"
    if not root.exists():
        return None
    versions = sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith(handles16 + "."))
    if not versions:
        return None
    v = versions[-1]

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
