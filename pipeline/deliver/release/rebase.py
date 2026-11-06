"""(T280) A RELEASE THAT DIFFERS FROM ANOTHER ONLY IN ITS BASEMAP — without re-running the data build.

    python -m pipeline.deliver.release.rebase [--from r-<id>] [--basemap-meta data/generated/tiles/basemap.meta.json]

The basemap is cut from the OSM source on its own schedule (`pipeline.deliver.tiles.basemap`); the data release names
it (`basemap: {id, url, bytes, sha256}`) and its id covers that block (`build.ship`). So a new basemap needs a new
release id, but none of the release's files change. This copies the release's files under the id `ship` would give
the same inputs with the new basemap block, after proving that `ship`'s id is reproduced for the release it starts
from (a release whose id cannot be recomputed from its manifest is refused, not guessed at). `published` and
`minAppVersion` are kept: they are publish stamps, outside every hash, and the regulations did not change.

It never moves `LATEST`; name the new id to `deploy/staging/upload.sh <id>` or promote it like any release.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from pipeline.common.curated import GENERATED


def said_of(m: dict) -> dict:
    """What `build.ship` hashes besides the files, rebuilt from its manifest (the inverse of `manifest.manifest`)."""
    syn = m.get("synopsis")
    if syn is not None:
        syn = {**syn, "url": syn["url"].split("/", 1)[1]}
    tiles = {k: v for k, v in m["tiles"].items() if k != "compatible"}
    said = {"tiles": tiles, "basemap": m.get("basemap"), "valid_until": m["regulations"]["valid_until"],
            "regs_versions": m["regulations"]["versions"], "sources": m.get("sources"), "synopsis": syn}
    if m.get("bundle") is not None:
        said["bundle"] = m["bundle"]
    return said


def id_of(files: dict, said: dict) -> str:
    """THE RELEASE ID FORMULA, the one copy (T435): `build.ship` mints with it, `release_id` recomputes with it.
    `files` = {name: {sha256, ...}}; `said` = everything else the manifest says (`said_of`)."""
    return "r-" + hashlib.sha256(json.dumps({"files": {k: v["sha256"] for k, v in sorted(files.items())},
                                             "said": said}, sort_keys=True).encode()).hexdigest()[:12]


def release_id(m: dict) -> str:
    """The id `build.ship` gives the release `m` describes."""
    return id_of(m["files"], said_of(m))


def rebase(src: Path, basemap: dict, out_root: Path | None = None) -> Path:
    """`src` (a release folder) -> `<out_root>/<new id>/`: the same files, the manifest naming `basemap`."""
    m = json.loads((src / "manifest.json").read_text())
    old = m["data_release"]
    if release_id(m) != old or src.name != old:
        raise SystemExit(f"rebase: {src} does not recompute to its own id ({release_id(m)} vs {old}) — refused")
    if m.get("basemap") == basemap:
        raise SystemExit(f"rebase: {old} already names {basemap.get('id')}")
    new = {**m, "basemap": basemap}
    rid = release_id(new)
    new["data_release"] = rid
    move = lambda url: f"{rid}/{url.split('/', 1)[1]}"  # noqa: E731
    new["files"] = {k: {**v, "url": move(v["url"])} for k, v in m["files"].items()}
    if m.get("synopsis") is not None:
        new["synopsis"] = {**m["synopsis"], "url": move(m["synopsis"]["url"])}
    out_root = Path(out_root or src.parent)
    final = out_root / rid
    if final.exists():
        raise SystemExit(f"rebase: {final} exists")
    stage = out_root / f".{rid}.staging"
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True)
    try:
        for e in [*m["files"].values(), *([m["synopsis"]] if m.get("synopsis") else [])]:
            shutil.copy2(src / e["url"].split("/", 1)[1], stage / e["url"].split("/", 1)[1])
        (stage / "manifest.json").write_text(json.dumps(new, indent=1, sort_keys=True) + "\n")
        stage.rename(final)
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    return final


def main(argv=None) -> int:
    from pipeline.deliver.release.manifest import basemap_block, latest
    ap = argparse.ArgumentParser(prog="python -m pipeline.deliver.release.rebase", description=__doc__.split("\n\n")[0])
    ap.add_argument("--from", dest="src", help="the release to start from (default: LATEST)")
    ap.add_argument("--release-root", type=Path, default=GENERATED.release)
    ap.add_argument("--basemap-meta", type=Path, default=GENERATED.tiles / "basemap.meta.json")
    a = ap.parse_args(argv)
    src = a.release_root / a.src if a.src else latest(a.release_root)
    bm = basemap_block(a.release_root, a.basemap_meta)        # refuses a missing basemap itself (T416)
    out = rebase(src, bm, a.release_root)
    print(f"{src.name} -> {out.name}  (basemap {bm['id']}, {bm['bytes']:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
