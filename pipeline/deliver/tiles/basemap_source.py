"""THE BASEMAP'S SOURCE STAGE (T280): the raw Protomaps extract for BC, fetched from the upstream build and kept as is.

Two stages, owner 2026-10-29 ("basemap in data that we get from pmtile directly then a translation from that to the one
we ship ... make sure it uses proper range requests"):

    1. SOURCE  (this module)  upstream daily build --range requests--> data/source/bc.pmtiles + bc.pmtiles.source.json
    2. SHIP    (`basemap.py`) data/source/bc.pmtiles --trim, grid, simplify--> data/generated/tiles/basemap.pmtiles

The source is never edited. It changes only when someone fetches on purpose; the ship stage reads it, checks it against
its sidecar (sha256) and stamps the source's provenance into the shipped archive's metadata.

THE FETCH reads the upstream planet archive (~140 GB) with HTTP range requests only, through `pmtiles extract <url>`:
the header, the root and leaf directories, then the BBOX's tile runs at each zoom (merged into few requests by
`--overfetch`). Nothing else of the planet is downloaded. It is RESUMABLE by zoom band: each band (ZOOM_PARTS) is
extracted to its own part file, verified (`pmtiles verify`) and marked done; a re-run skips the done bands, and refuses
to resume against a different upstream build. The bands are joined with `pmtiles merge` (they are disjoint by zoom),
verified again and moved into place with the sidecar:

    {upstream_url, upstream_version, osm_replication_time, bbox, maxzoom, fetched_at, bytes, sha256}

Run:
    python -m pipeline.deliver.tiles.basemap_source fetch --build latest   (or --build 20261029; ~4 GB)
    python -m pipeline.deliver.tiles.basemap_source check                  the source matches its sidecar
    python -m pipeline.deliver.tiles.basemap_source describe --url URL     write the sidecar for a file already there

Upstream: https://build.protomaps.com/<YYYYMMDD>.pmtiles, listed at BUILDS. Old builds are pruned upstream, so the
sidecar, not the URL, is what identifies a source.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import urllib.request
from pathlib import Path

from pipeline.common.curated import SOURCE

SOURCE_FILE = SOURCE / "bc.pmtiles"
SIDECAR_SUFFIX = ".source.json"
UPSTREAM = "https://build.protomaps.com/{build}.pmtiles"
BUILDS = "https://build-metadata.protomaps.dev/builds.json"
# The fetch box: the province with a margin (the ship stage clips to the buffered outline). Matches the source fetched
# 2026-03 (`bounds` of data/source/bc.pmtiles).
BBOX = (-148.0, 45.0, -108.0, 63.5)
MAXZOOM = 15
# Resumable bands: the low zooms together, then one band per deep zoom (z15 is about half the bytes).
ZOOM_PARTS = ((0, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15))
THREADS = 4


def sidecar_of(archive: Path) -> Path:
    return archive.with_name(archive.name + SIDECAR_SUFFIX)


def _run(cmd: list[str]) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:2])} failed ({r.returncode})\n{r.stderr[-3000:]}")
    return r.stdout


def identity(path_or_url: str) -> dict:
    """What names a Protomaps build: its schema version and the OSM replication it was cut at (`pmtiles show`)."""
    meta = json.loads(_run(["pmtiles", "show", "--metadata", str(path_or_url)]))
    return {"upstream_version": meta.get("version"),
            "osm_replication_time": meta.get("planetiler:osm:osmosisreplicationtime"),
            "osm_replication_seq": meta.get("planetiler:osm:osmosisreplicationseq")}


def latest_build() -> str:
    req = urllib.request.Request(BUILDS, headers={"User-Agent": "bc-fishing-regs basemap_source"})  # bare urllib: 403
    with urllib.request.urlopen(req, timeout=60) as r:
        builds = json.load(r)
    return max(b["key"] for b in builds).removesuffix(".pmtiles")


def _sha256(path: Path) -> str:
    from pipeline.deliver.tiles.version import sha256_file
    return sha256_file(path)


def _parts_cmds(url: str, parts_dir: Path, bbox: tuple, maxzoom: int, threads: int):
    bb = ",".join(str(v) for v in bbox)
    for lo, hi in ZOOM_PARTS:
        if lo > maxzoom:
            break
        hi = min(hi, maxzoom)
        part = parts_dir / f"z{lo}-{hi}.pmtiles"
        yield part, ["pmtiles", "extract", url, str(part.with_suffix(".partial")), f"--bbox={bb}",
                     f"--minzoom={lo}", f"--maxzoom={hi}", f"--download-threads={threads}"]


def fetch(url: str, out: Path = SOURCE_FILE, bbox: tuple = BBOX, maxzoom: int = MAXZOOM, threads: int = THREADS,
          replace: bool = False) -> dict:
    """`url` (a remote PMTiles archive) -> `out` over `bbox` to `maxzoom`, by range requests, resumable by zoom band.
    Returns the sidecar written beside `out`."""
    if out.exists() and not replace:
        raise SystemExit(f"basemap source: {out} exists — pass --replace to fetch over it (the old file is kept as "
                         f"{out.name}.prev until the new one is verified)")
    who = identity(url)
    parts_dir = out.with_name(out.name + ".parts")
    parts_dir.mkdir(parents=True, exist_ok=True)
    state_file = parts_dir / "state.json"
    state = {"upstream_url": url, **who, "bbox": list(bbox), "maxzoom": maxzoom}
    if state_file.exists():
        prev = json.loads(state_file.read_text())
        if prev != state:
            raise SystemExit(f"basemap source: {parts_dir} holds a fetch of another build or box ({prev}); "
                             f"remove it to start over")
    state_file.write_text(json.dumps(state, indent=2) + "\n")
    parts = []
    for part, cmd in _parts_cmds(url, parts_dir, bbox, maxzoom, threads):
        done = part.with_suffix(".done")
        parts.append(part)
        if part.exists() and done.exists():
            print(f"  {part.name}: done, skipped")
            continue
        print(f"  {part.name}: {' '.join(cmd[4:])}")
        _run(cmd)
        _run(["pmtiles", "verify", str(cmd[3])])
        Path(cmd[3]).rename(part)
        done.write_text(dt.datetime.now(dt.timezone.utc).isoformat() + "\n")
    merged = out.with_name(out.name + ".merging")
    merged.unlink(missing_ok=True)
    if len(parts) == 1:
        shutil.copyfile(parts[0], merged)
    else:
        _run(["pmtiles", "merge", *map(str, parts), str(merged)])
    _run(["pmtiles", "verify", str(merged)])
    if identity(str(merged)) != who:
        raise SystemExit("basemap source: the merged archive does not carry the upstream build's metadata")
    if out.exists():
        out.rename(out.with_name(out.name + ".prev"))
    merged.rename(out)
    side = describe(out, url, fetched_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                    bbox=bbox, maxzoom=maxzoom)
    shutil.rmtree(parts_dir)
    print(f"  ✅ {out}  {side['bytes'] / 1e9:.2f} GB  {side['sha256'][:12]}")
    return side


def describe(archive: Path = SOURCE_FILE, url: str | None = None, fetched_at: str | None = None,
             bbox: tuple | None = None, maxzoom: int | None = None, note: str | None = None) -> dict:
    """Write `archive`'s sidecar: where it came from and the bytes it is."""
    from pipeline.deliver.tiles.basemap_roads import header_and_metadata
    hdr, _meta = header_and_metadata(archive)
    side = {"upstream_url": url, **identity(str(archive)),
            "bbox": list(bbox) if bbox else list(hdr["bounds"]),
            "maxzoom": maxzoom if maxzoom is not None else hdr["maxzoom"],
            "fetched_at": fetched_at, "bytes": archive.stat().st_size, "sha256": _sha256(archive)}
    if note:
        side["note"] = note
    sidecar_of(archive).write_text(json.dumps(side, indent=2) + "\n", encoding="utf-8")
    return side


def check(archive: Path = SOURCE_FILE) -> dict:
    """The source's sidecar, after proving the file is the bytes it describes. The ship stage refuses a source that is
    not (an edited or half-copied source would build a basemap nobody can reproduce)."""
    side_path = sidecar_of(archive)
    if not side_path.exists():
        raise SystemExit(f"basemap source: {archive} has no {side_path.name} — fetch it "
                         f"(python -m pipeline.deliver.tiles.basemap_source fetch) or describe it")
    side = json.loads(side_path.read_text())
    if archive.stat().st_size != side["bytes"] or _sha256(archive) != side["sha256"]:
        raise SystemExit(f"basemap source: {archive} is not the file its sidecar describes")
    return side


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.deliver.tiles.basemap_source")
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="fetch the BC extract from an upstream build (range requests, resumable)")
    f.add_argument("--build", default="latest", help="YYYYMMDD of the upstream daily build, or latest")
    f.add_argument("--url", help="a PMTiles URL instead of a Protomaps build")
    f.add_argument("--out", type=Path, default=SOURCE_FILE)
    f.add_argument("--maxzoom", type=int, default=MAXZOOM)
    f.add_argument("--threads", type=int, default=THREADS)
    f.add_argument("--replace", action="store_true")
    d = sub.add_parser("describe", help="write the sidecar for a source already on disk")
    d.add_argument("--archive", type=Path, default=SOURCE_FILE)
    d.add_argument("--url")
    d.add_argument("--fetched-at")
    d.add_argument("--note")
    c = sub.add_parser("check", help="the source is the file its sidecar describes")
    c.add_argument("--archive", type=Path, default=SOURCE_FILE)
    a = ap.parse_args(argv)
    if a.cmd == "fetch":
        url = a.url or UPSTREAM.format(build=latest_build() if a.build == "latest" else a.build)
        fetch(url, a.out, maxzoom=a.maxzoom, threads=a.threads, replace=a.replace)
    elif a.cmd == "describe":
        print(json.dumps(describe(a.archive, a.url, a.fetched_at, note=a.note), indent=2))
    else:
        print(json.dumps(check(a.archive), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
