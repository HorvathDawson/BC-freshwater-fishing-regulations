"""THE MANIFEST (`app-manifest/2`, MINIMAL-PACKAGING §6.2): what an app reads first, on every launch.

    {"format": "app-manifest/2", "data_release": "<id>",
     "tiles":   {"compatible": ["<handles16>.*"], "current": {id, url, bytes, sha256} | null, "assets": …, "mask": …},
     "basemap": {id, url, bytes, sha256} | null,
     "files":   {"<name>": {url, bytes, gz, sha256, format, section_handles}, …},
     "regulations": {"valid_until": "<date>" | null, "versions": {…}},
     "sources": {"synopsis": {title, date}, "dfo"|"fwa"|"basemap"|"hydat": {date}}}   (only when the build passes it)

A data release built on an atlas is compatible with EVERY tile build of that atlas: `compatible` names the sid space
only. `tiles.current` is filled from the tile version folder when one exists (P18; the tile rebuild is human-run), else
null: the app then keeps the tiles it holds when their handles match.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict, Optional

FORMAT = "app-manifest/2"

#: THE SOURCES WHOSE DATE A RELEASE STATES (`sources`), in the order the app lists them (emitted to the app's
#: pack.generated.ts). The live sources (the gauge feed, the weather, the online imagery) are dated by the app from
#: what it fetched, the tiles by their version. Owner 2026-10-21: "the date each source of info was updated".
SOURCE_KEYS = ("synopsis", "dfo", "fwa", "basemap", "hydat")


def manifest(data_release: str, files: Dict[str, dict], handles16: str, *, tiles: Optional[dict] = None,
             basemap: Optional[dict] = None, valid_until: Optional[str] = None,
             regs_versions: Optional[dict] = None, sources: Optional[dict] = None) -> dict:
    t = {"compatible": [f"{handles16}.*"], "current": None, "assets": None, "mask": None, **(tiles or {})}
    out = {"format": FORMAT, "data_release": data_release, "tiles": t, "basemap": basemap, "files": files,
           "regulations": {"valid_until": valid_until, "versions": regs_versions}}
    if sources is not None:
        out["sources"] = sources
    return out


_DAY = re.compile(r"^(\d{4})-?(\d{2})-?(\d{2})")


def _day(text: Optional[str]) -> Optional[str]:
    """"20260717" / "2026-07-15T23:21:10Z" -> "2026-07-17" / "2026-07-15"; anything else is not a date (None)."""
    m = _DAY.match((text or "").strip())
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def _gpkg_day(gpkg: Path, table: str) -> Optional[str]:
    """The day a GeoPackage layer was last written (`gpkg_contents.last_change`, stamped by the fetch)."""
    if not gpkg.exists():
        return None
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    try:
        row = con.execute("SELECT last_change FROM gpkg_contents WHERE table_name = ?", (table,)).fetchone()
    finally:
        con.close()
    return _day(row[0]) if row else None


def _pmtiles_osm_day(archive: Path) -> Optional[str]:
    """The OpenStreetMap data a Protomaps build was cut from (`planetiler:osm:osmosisreplicationtime` in the archive's
    metadata — PMTiles v3: the header names the metadata's offset, length and compression). Reads the header and the
    metadata only; anything that is not a v3 archive, or states no replication time, is no date."""
    import gzip
    import json
    import struct
    if not archive.exists():
        return None
    with archive.open("rb") as f:
        h = f.read(127)
        if len(h) < 127 or h[:7] != b"PMTiles" or h[7] != 3:
            return None
        offset, length = struct.unpack("<QQ", h[24:40])
        f.seek(offset)
        raw = f.read(length)
    try:
        meta = json.loads(gzip.decompress(raw) if h[97] == 2 else raw)
    except (OSError, ValueError):
        return None
    return _day(meta.get("planetiler:osm:osmosisreplicationtime")) if isinstance(meta, dict) else None


def _dfo_day(rules_dir: Path) -> Optional[str]:
    """The newest "Date modified" DFO prints on the region salmon pages the release's federal rules were parsed from
    (`<dfo_salmon>/rules/region-*.json` `date_modified`); a page that states none is skipped."""
    import json
    days = []
    for p in sorted(Path(rules_dir).glob("region-*.json")) if Path(rules_dir).is_dir() else []:
        d = _day(json.loads(p.read_text(encoding="utf-8")).get("date_modified"))
        if d:
            days.append(d)
    return max(days) if days else None


def sources_block(data_dir, *, synopsis_title: str, dfo_rules) -> dict:
    """`sources`: each source's last update, READ FROM THE INPUTS, never the clock of this run (AGENTS 19: two runs on
    the same inputs write the same bytes). A source whose inputs record no date is null, and the app says it is not
    recorded rather than guessing.

      synopsis   the printed edition (its title; the repo PDF carries no date of its own)
      dfo        the newest "Date modified" on the DFO region salmon pages the federal rules were parsed from
      fwa        the day the FWA streams layer was fetched (`bc_fisheries_data.gpkg`): the atlas every water is drawn from
      basemap    the OpenStreetMap replication the Protomaps basemap was cut from (`bc.pmtiles` metadata)
      hydat      the HYDAT release the gauges' record years (the climatology) were built from (`hydat.release`)
    """
    d = Path(data_dir)
    hydat = d / "hydat.release"
    return {"synopsis": {"title": synopsis_title, "date": None},
            "dfo": {"date": _dfo_day(Path(dfo_rules))},
            "fwa": {"date": _gpkg_day(d / "bc_fisheries_data.gpkg", "streams")},
            "basemap": {"date": _pmtiles_osm_day(d / "bc.pmtiles")},
            "hydat": {"date": _day(hydat.read_text()) if hydat.exists() else None}}


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
