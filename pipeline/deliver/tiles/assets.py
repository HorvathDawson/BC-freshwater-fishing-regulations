"""SELF-HOSTED MAP ASSETS (P18, owner Q18): the glyphs and sprites the style names, shipped beside the tiles so the app
fetches nothing from a CDN at run time (and the always-offline native app has them at all).

    data/release/tiles/<tile_version>/map-assets-<ASSETS_VERSION>/
        fonts/<fontstack>/<start>-<end>.pbf      256 ranges per stack, MapLibre's `glyphs` template
        sprites/v4/<light|dark>[@2x].{json,png}  the basemap's icons, MapLibre's `sprite` URL
        assets.json                              {id, pin, files: {path: sha256}}: the manifest's `files_sha256`
    data/release/tiles/<tile_version>/map-assets-<ASSETS_VERSION>.tar.zlib
                                                 (T919 c) THE SAME FILES AS ONE DOWNLOAD (`write_pack`): the manifest's
                                                 `tiles.assets.pack` {url, bytes, sha256}

THE PACK (T919 c, owner 2026-11-19 20:31: "one packed download"): a first install fetched the folder as 777 requests paced
under the data host's per-IP limit (35-160 s on the benches). A phone app that reads `pack` downloads the folder as ONE
file through the system downloader beside the big files, held to the manifest's bytes + sha256, then unpacks it and holds
`assets.json` to `files_sha256` and every file to its sha256 (`app/packages/data/src/release/assets.ts` `unpackAssets`).
The folder stays published beside it: an installed app that predates the pack reads `url` + `files_sha256` and fetches
the files one by one, as before (the manifest only GAINS a key; no minAppVersion change). A plain ustar tar, deflated in
a ZLIB stream (RFC 1950), NOT gzip: every release download is held to its bytes after `ungzip` (fetchVerified decodes a body
that starts with the gzip magic), so a gzip pack would be decoded on one path and not on another. `assets.json` first, then
every file in sorted order, regular files only, every stamp zeroed, so one folder gives one pack (6.1 MB for the 13.8 MB
live folder, 2026-11-22).

THE SOURCE is the Protomaps basemaps-assets repository at a PINNED commit (the glyph PBFs and sprite sheets the
@protomaps/basemaps layers were drawn with). It is fetched ONCE into `data/source/map-assets/<pin>/` (fetched data, not
generated; never re-fetched while the cache is complete) and copied from there by every tile release. Bump `PIN` and
`ASSETS_VERSION` together.

WHICH FONTS: every `text-font` our style (app/packages/map/style/style.json) names, plus the three the basemap's label
layers use (`BASEMAP_FONTS`, read from @protomaps/basemaps: Regular, Medium, Italic; no other language is enabled).
"""
from __future__ import annotations

import hashlib
import io
import json
import shutil
import tarfile
import zlib
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pipeline.common.curated import REPO_ROOT, SOURCE

ASSETS_VERSION = 1
PIN = "028c18f713baecad011301ff7a69acc39bcc2ae7"           # protomaps/basemaps-assets main, 2025-10-31
RAW = f"https://raw.githubusercontent.com/protomaps/basemaps-assets/{PIN}"
BASEMAP_FONTS = ("Noto Sans Regular", "Noto Sans Medium", "Noto Sans Italic")
SPRITES = tuple(f"sprites/v4/{flavour}{scale}.{ext}" for flavour in ("light", "dark") for scale in ("", "@2x")
                for ext in ("json", "png"))
RANGES = tuple(f"{i * 256}-{i * 256 + 255}.pbf" for i in range(256))
STYLE = REPO_ROOT / "app" / "packages" / "map" / "style" / "style.json"


def style_fonts(style: Path = STYLE) -> set[str]:
    """Every font stack a `text-font` in the style names."""
    out: set[str] = set()

    def walk(v):
        if isinstance(v, dict):
            for k, x in v.items():
                if k == "text-font" and isinstance(x, list):
                    out.update(f for f in x if isinstance(f, str))
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)
    walk(json.loads(Path(style).read_text(encoding="utf-8")))
    return out


def wanted() -> list[str]:
    """Every file the assets folder holds, as a path under it."""
    fonts = sorted(style_fonts() | set(BASEMAP_FONTS))
    return [f"fonts/{f}/{r}" for f in fonts for r in RANGES] + list(SPRITES)


def cache_dir() -> Path:
    return SOURCE / "map-assets" / PIN


def fetch(workers: int = 8) -> Path:
    """Fill the cache with every wanted file (only the missing ones are downloaded). A file the source does not have
    fails the fetch, naming it: a missing glyph range would leave labels undrawn with no error anywhere."""
    d = cache_dir()
    todo = [p for p in wanted() if not (d / p).exists()]

    def get(rel: str) -> str | None:
        url = f"{RAW}/{urllib.parse.quote(rel)}"
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                body = r.read()
        except Exception as e:                                   # noqa: BLE001 — reported, then the fetch fails
            return f"{rel}: {e}"
        out = d / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(out.name + ".part")
        tmp.write_bytes(body)
        tmp.rename(out)
        return None

    if todo:
        print(f"map assets: fetching {len(todo)} files @ {PIN[:8]} into {d}")
        with ThreadPoolExecutor(workers) as ex:
            bad = [e for e in ex.map(get, todo) if e]
        if bad:
            raise SystemExit(f"map assets: {len(bad)} files could not be fetched: " + "; ".join(bad[:5]))
    return d


def write(into: Path) -> Path:
    """Copy the assets into `<into>/map-assets-<ASSETS_VERSION>/` with their `assets.json` and its pack beside it
    (`write_pack`), and return that folder."""
    src = fetch()
    out = into / f"map-assets-{ASSETS_VERSION}"
    files = {}
    for rel in wanted():
        dst = out / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src / rel, dst)
        files[rel] = hashlib.sha256(dst.read_bytes()).hexdigest()
    (out / "assets.json").write_text(json.dumps(
        {"id": f"map-assets-{ASSETS_VERSION}", "pin": PIN, "files": files}, indent=0, sort_keys=True) + "\n")
    write_pack(out)                                          # (T919 c) the same files as one download
    return out


def pack_path(folder: Path) -> Path:
    """The pack of an assets folder: `<tile version>/map-assets-<n>.tar.zlib`, beside the folder."""
    return Path(folder).with_name(f"{Path(folder).name}.tar.zlib")


def write_pack(folder: Path) -> Path:
    """(T919 c) Pack an assets folder into ONE file beside it, every byte held to its `assets.json` first (a folder whose
    files are not the list's is refused, naming the file: the pack must never carry bytes the list does not name).
    Deterministic: the same folder gives the same bytes (sorted members, zeroed stamps; a zlib stream has no name or
    time). Written to `.part` and renamed."""
    folder = Path(folder)
    listing = folder / "assets.json"
    files = json.loads(listing.read_text())["files"]
    on_disk = sorted(str(p.relative_to(folder)) for p in folder.rglob("*") if p.is_file() and p != listing)
    if on_disk != sorted(files):
        extra, missing = sorted(set(on_disk) - set(files)), sorted(set(files) - set(on_disk))
        raise SystemExit(f"map assets: {folder} is not its assets.json (extra {extra[:3]}, missing {missing[:3]})")
    members = [("assets.json", listing.read_bytes())]
    for rel in sorted(files):
        b = (folder / rel).read_bytes()
        if hashlib.sha256(b).hexdigest() != files[rel]:
            raise SystemExit(f"map assets: {folder / rel} is not the sha256 its assets.json names")
        members.append((rel, b))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, b in members:
            ti = tarfile.TarInfo(name)
            ti.size, ti.mtime, ti.mode, ti.uid, ti.gid, ti.uname, ti.gname = len(b), 0, 0o644, 0, 0, "", ""
            tar.addfile(ti, io.BytesIO(b))
    out = pack_path(folder)
    tmp = out.with_name(out.name + ".part")
    tmp.write_bytes(zlib.compress(raw.getvalue(), 9))
    tmp.rename(out)
    return out


def pack_problems(folder: Path) -> list[str]:
    """(T919 c) What is wrong with the pack beside an assets folder, against that folder's `assets.json`; [] when it is
    the folder's. Read as the app reads it: one zlib stream, a tar of regular files, `assets.json` first and byte for
    byte the folder's, then every file the list names in sorted order, each at its sha256, and nothing else. The tar is
    compared, not the deflate bytes (another zlib build writes other bytes for the same members)."""
    folder = Path(folder)
    p = pack_path(folder)
    if not p.is_file():
        return [f"{p} is missing"]
    listing = (folder / "assets.json").read_bytes()
    files = json.loads(listing)["files"]
    try:
        raw = zlib.decompress(p.read_bytes())
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as tar:
            members = tar.getmembers()
            if not all(m.isreg() for m in members):
                return [f"{p}: holds a member that is not a regular file"]
            got = [(m.name, tar.extractfile(m).read()) for m in members]
    except (zlib.error, tarfile.TarError, OSError) as e:
        return [f"{p} would not read as a zlib tar ({e})"]
    want = ["assets.json", *sorted(files)]
    names = [n for n, _ in got]
    if names != want:
        extra, missing = sorted(set(names) - set(want)), sorted(set(want) - set(names))
        return [f"{p} is not its folder's assets.json (extra {extra[:3]}, missing {missing[:3]}"
                + (", out of order" if not extra and not missing else "") + ")"]
    out = []
    if got[0][1] != listing:
        out.append(f"{p}: its assets.json is not the folder's")
    out += [f"{p}: {n} is not the sha256 the folder's assets.json names" for n, b in got[1:]
            if hashlib.sha256(b).hexdigest() != files[n]]
    return out


def unpacked_bytes(folder: Path) -> int:
    """(T919 c) What the pack's members take once unpacked (`assets.json` and every file it names): the room an app
    needs for the folder beside the pack (the manifest's `tiles.assets.pack.unpacked`)."""
    folder = Path(folder)
    files = json.loads((folder / "assets.json").read_text())["files"]
    return (folder / "assets.json").stat().st_size + sum((folder / rel).stat().st_size for rel in files)


def ensure_pack(folder: Path) -> Path:
    """(T919 c) The folder's pack, written when it is missing (a tile version released before the pack existed gains it:
    a new file beside the folder, no published byte changed). An existing pack is kept only when it IS the folder
    (`pack_problems`); one that is not is refused, naming what differs: its url may be published already, so it is
    never rewritten in place by itself (delete it to have it written again)."""
    p = pack_path(folder)
    if not p.exists():
        return write_pack(folder)
    bad = pack_problems(folder)
    if bad:
        raise SystemExit("map assets: the pack beside the folder is not the folder (delete it to write it again): "
                         + "; ".join(bad[:5]))
    return p


def files_sha256(assets_json: Path) -> str:
    """One digest over the whole assets folder (the manifest's `tiles.assets.files_sha256`)."""
    files = json.loads(Path(assets_json).read_text())["files"]
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
