"""SELF-HOSTED MAP ASSETS (P18, owner Q18): the glyphs and sprites the style names, shipped beside the tiles so the app
fetches nothing from a CDN at run time (and the always-offline native app has them at all).

    data/release/tiles/<tile_version>/map-assets-<ASSETS_VERSION>/
        fonts/<fontstack>/<start>-<end>.pbf      256 ranges per stack, MapLibre's `glyphs` template
        sprites/v4/<light|dark>[@2x].{json,png}  the basemap's icons, MapLibre's `sprite` URL
        assets.json                              {id, pin, files: {path: sha256}}: the manifest's `files_sha256`

THE SOURCE is the Protomaps basemaps-assets repository at a PINNED commit (the glyph PBFs and sprite sheets the
@protomaps/basemaps layers were drawn with). It is fetched ONCE into `data/source/map-assets/<pin>/` (fetched data, not
generated; never re-fetched while the cache is complete) and copied from there by every tile release. Bump `PIN` and
`ASSETS_VERSION` together.

WHICH FONTS: every `text-font` our style (app/packages/map/style/style.json) names, plus the three the basemap's label
layers use (`BASEMAP_FONTS`, read from @protomaps/basemaps: Regular, Medium, Italic; no other language is enabled).
"""
from __future__ import annotations

import hashlib
import json
import shutil
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
    """Copy the assets into `<into>/map-assets-<ASSETS_VERSION>/` with their `assets.json`, and return that folder."""
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
    return out


def files_sha256(assets_json: Path) -> str:
    """One digest over the whole assets folder (the manifest's `tiles.assets.files_sha256`)."""
    files = json.loads(Path(assets_json).read_text())["files"]
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
