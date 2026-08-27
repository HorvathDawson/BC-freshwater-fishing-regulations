"""One-time OSM waterway sweep over a bbox -> per-channel candidate GeoJSON for human allowlisting.

Fetches `waterway` ways from Overpass, merges same-name connected ways into channels
(`merge.merge_channels`, minting a `-min(way_id)` blk each), and writes:
  - output/added_candidates.geojson  (one Feature per channel; edit + copy the keepers into
                                       pipeline/added_streams.geojson, adding `connect_to`)
  - output/added_candidates.md       (a review table)

HUMAN-RUN: this hits the public Overpass API (external network). It is NOT part of the build — the
build only reads the curated pipeline/added_streams.geojson. Reuses the paced/multi-endpoint pattern
from pipeline/oneoff/resolvers/osm_batch.py.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.added_streams.fetch_osm \
        --bbox -123.02 49.22 -122.88 49.28 --out output/added_candidates.geojson
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from pathlib import Path

from shapely.geometry import mapping

from pipeline.added_streams.merge import merge_channels

_HDR = {"User-Agent": "bc-fishing-reg-curation/1.0 (horvath.dawson@gmail.com)"}
_ENDPOINTS = ["https://overpass.kumi.systems/api/interpreter",
              "https://overpass-api.de/api/interpreter",
              "https://overpass.private.coffee/api/interpreter"]
_WATERWAY = "stream|river|tidal_channel|ditch|canal"
_last = [0.0]


def _fetch(query: str) -> dict | None:
    if time.time() - _last[0] < 1.5:
        time.sleep(1.5 - (time.time() - _last[0]))
    for att in range(3):
        ep = _ENDPOINTS[att % len(_ENDPOINTS)]
        try:
            body = ("data=" + query).encode()
            txt = urllib.request.urlopen(
                urllib.request.Request(ep, data=body, headers=_HDR), timeout=60).read().decode()
            _last[0] = time.time()
            if txt.startswith("{"):
                return json.loads(txt)
        except Exception as e:                       # noqa: BLE001 — try the next endpoint
            print(f"   fetch err {ep.split('/')[2]} {type(e).__name__}", flush=True)
        time.sleep(3)
    _last[0] = time.time()
    return None


def fetch_waterways(bbox: tuple[float, float, float, float]) -> list[dict]:
    """Return GeoJSON Feature dicts (lon/lat LineStrings) for `waterway` ways in bbox
    (minlon, minlat, maxlon, maxlat)."""
    minlon, minlat, maxlon, maxlat = bbox
    q = (f'[out:json][timeout:120];way[waterway~"{_WATERWAY}"]'
         f'({minlat},{minlon},{maxlat},{maxlon});out geom tags;')
    data = _fetch(q) or {}
    feats: list[dict] = []
    for e in data.get("elements", []):
        geom = e.get("geometry") or []
        if len(geom) < 2:
            continue
        feats.append({
            "type": "Feature",
            "properties": {"source": "osm", "osm_way_id": e["id"],
                           "name": (e.get("tags") or {}).get("name", ""),
                           "waterway": (e.get("tags") or {}).get("waterway", "")},
            "geometry": {"type": "LineString",
                         "coordinates": [[p["lon"], p["lat"]] for p in geom]},
        })
    return feats


def _channel_features(channels) -> list[dict]:
    out = []
    for ch in channels:
        out.append({
            "type": "Feature",
            "properties": {"source": ch.source, "blk": ch.blk, "name": ch.name,
                           "osm_way_ids": list(ch.members), "connect_to": None,
                           **ch.overrides},
            "geometry": mapping(ch.geometry),
        })
    return out


def _review_md(channels) -> str:
    rows = ["| blk | name | ways | vertices |", "|---|---|---|---|"]
    for ch in sorted(channels, key=lambda c: (c.name or "~", c.blk)):
        rows.append(f"| {ch.blk} | {ch.name or '(unnamed)'} | {len(ch.members)} "
                    f"| {len(ch.geometry.coords)} |")
    return f"# OSM waterway candidates ({len(channels)} channels)\n\n" + "\n".join(rows) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description="One-time OSM waterway sweep -> candidate channels.")
    ap.add_argument("--bbox", nargs=4, type=float, required=True,
                    metavar=("MINLON", "MINLAT", "MAXLON", "MAXLAT"))
    ap.add_argument("--out", default="output/added_candidates.geojson")
    args = ap.parse_args()

    print(f"== fetch_osm: waterways in bbox {args.bbox} (Overpass; external network) ==")
    feats = fetch_waterways(tuple(args.bbox))
    print(f"  {len(feats)} way(s) fetched")
    channels = merge_channels(feats)
    print(f"  {len(channels)} channel(s) after same-name merge")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"type": "FeatureCollection",
                               "_about": "OSM waterway candidates — review, add connect_to, and copy "
                               "keepers into pipeline/added_streams.geojson.",
                               "features": _channel_features(channels)}, indent=1), encoding="utf-8")
    md = out.with_suffix(".md")
    md.write_text(_review_md(channels), encoding="utf-8")
    print(f"  wrote {out}  and  {md}")


if __name__ == "__main__":
    main()
