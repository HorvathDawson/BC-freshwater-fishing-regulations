"""THE MANIFEST (`app-manifest/2`, MINIMAL-PACKAGING §6.2): what an app reads first, on every launch.

    {"format": "app-manifest/2", "data_release": "<id>",
     "tiles":   {"compatible": ["<handles16>.*"], "current": {id, url, bytes, sha256} | null, "assets": …, "mask": …},
     "basemap": {id, url, bytes, sha256} | null,
     "files":   {"<name>": {url, bytes, gz, sha256, format, section_handles}, …},
     "regulations": {"valid_until": "<date>" | null, "versions": {…}},
     "synopsis": {id, url, bytes, sha256} | null,   (T215: OUR copy of the synopsis PDF, raw, beside the data files)
     "sources": {"synopsis": {title, date}, "dfo"|"fwa"|"basemap"|"hydat": {date}},   (only when the build passes it)
     "bundle":  {"entries_digest": "<16 hex>", "reach_digest": "<16 hex>"}}           (only when the build passes it)

`bundle` (T133) names the corpus and the reach run the release was cut from — its bundle's `meta` digests — so the
curation-review tool can tell that a release IS the bundle beside it (and so carries each entry's digest, the bundle's
`entry_digests`) without opening a shipped file. The app ignores it.

A data release built on an atlas is compatible with EVERY tile build of that atlas: `compatible` names the sid space
only. `tiles.current` is filled from the tile version folder when one exists (P18; the tile rebuild is human-run), else
null: the app then keeps the tiles it holds when their handles match.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

FORMAT = "app-manifest/2"

#: THE SOURCES WHOSE DATE A RELEASE STATES (`sources`), in the order the app lists them (emitted to the app's
#: pack.generated.ts). The live sources (the gauge feed, the weather, the online imagery) are dated by the app from
#: what it fetched, the tiles by their version. Owner 2026-10-21: "the date each source of info was updated".
SOURCE_KEYS = ("synopsis", "dfo", "fwa", "basemap", "hydat")


def manifest(data_release: str, files: Dict[str, dict], handles16: str, *, tiles: Optional[dict] = None,
             basemap: Optional[dict] = None, valid_until: Optional[str] = None,
             regs_versions: Optional[dict] = None, sources: Optional[dict] = None,
             bundle: Optional[dict] = None, synopsis: Optional[dict] = None) -> dict:
    t = {"compatible": [f"{handles16}.*"], "current": None, "assets": None, "mask": None, **(tiles or {})}
    out = {"format": FORMAT, "data_release": data_release, "tiles": t, "basemap": basemap, "files": files,
           "regulations": {"valid_until": valid_until, "versions": regs_versions}}
    if sources is not None:
        out["sources"] = sources
    if synopsis is not None:
        out["synopsis"] = synopsis
    if bundle is not None:
        out["bundle"] = bundle_block(bundle)
    return out


#: the bundle `meta` digests a release states (`bundle`), and nothing else of the bundle's meta
BUNDLE_KEYS = ("entries_digest", "reach_digest")


def bundle_block(bundle_meta: dict) -> dict:
    """`bundle`: the corpus and reach-run digests of the bundle a release is cut from (its `meta`). A bundle that
    states either as empty is refused: a release says what it was made from, or nothing."""
    out = {k: bundle_meta.get(k) for k in BUNDLE_KEYS}
    missing = [k for k, v in out.items() if not v]
    if missing:
        raise SystemExit(f"release: the bundle states no {missing} — rebuild it: python -m pipeline.deliver bundle")
    return out


LATEST = "LATEST"


def write_latest(release_root, rid: str) -> None:
    """T72: name `rid` as THE release under `release_root` (one line, the id). Written by the release step only — a
    folder touched later (a copy, a test, an inspection) never becomes current by its mtime."""
    tmp = Path(release_root) / f".{LATEST}.tmp"
    tmp.write_text(rid + "\n")
    tmp.replace(Path(release_root) / LATEST)


def latest(release_root) -> Optional[Path]:
    """THE RELEASE ON DISK: the release `release_root/LATEST` names (T72), else — a root the release step never wrote
    a pointer into (a test's, an old checkout's) — the newest `r-*` folder holding a manifest (by the manifest's
    mtime; a release id is a content hash, so name order is arbitrary), or None when there is none. One answer for
    every reader that asks "which release is current" (publish, the curation-review tool's vintage, the fixture tools)."""
    root = Path(release_root)
    pointer = root / LATEST
    if pointer.is_file():
        named = root / pointer.read_text().strip()
        if not (named / "manifest.json").is_file():
            raise SystemExit(f"release: {pointer} names {named.name}, which has no manifest — re-run the release step")
        return named
    dirs = releases(root)
    return dirs[0] if dirs else None


def releases(release_root) -> List[Path]:
    """Every `r-*` folder under `release_root` holding a manifest, newest manifest first (the prune's order too)."""
    root = Path(release_root)
    dirs = [d for d in root.glob("r-*") if (d / "manifest.json").is_file()] if root.is_dir() else []
    return sorted(dirs, key=lambda d: ((d / "manifest.json").stat().st_mtime_ns, d.name), reverse=True)


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


def basemap_block(release_root, basemap_meta) -> Optional[dict]:
    """`basemap`: `{id, url, bytes, sha256}` of the released basemap (T75), or None when none has been released yet
    (no `basemap.meta.json` beside the tiles: the app then keeps the ground it holds, or none).

    Which basemap is current is what the sidecar the basemap step wrote says (`tiles.basemap.to_release`), never "the
    last folder". Its released copy is hashed again here, so a manifest can never name bytes it does not point at; a
    sidecar naming a folder that is missing or holds other bytes is refused."""
    import json
    from pipeline.deliver.tiles.version import sha256_file
    meta = Path(basemap_meta)
    if not meta.exists():
        return None
    m = json.loads(meta.read_text())
    vid = m.get("basemap_version")
    if not vid:
        return None
    p = Path(release_root) / "basemap" / vid / f"{vid}.pmtiles"
    if not p.exists():
        raise SystemExit(f"release: the basemap is {vid} but {p} is missing — "
                         "python -m pipeline.deliver.tiles.basemap --release-only")
    sha = sha256_file(p)
    if sha != m.get("sha256") or p.stat().st_size != m.get("bytes"):
        raise SystemExit(f"release: {p} is not the basemap {meta} describes — re-run the basemap release")
    return {"id": vid, "url": f"basemap/{vid}/{vid}.pmtiles", "bytes": p.stat().st_size, "sha256": sha}
