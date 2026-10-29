"""THE BINDING BASE (`runtiming-base/1`): the atlas reduced to what the run-timing binding reads. OWNER-RUN, on the Mac,
once per atlas — it needs `graph.pkl` and the tiles' own `layers/{stream,lake}.geojsonl` (~3 GB streamed).

    python -m pipeline.runtiming.feed.base [--build DIR] [--tiles DIR] [--release DIR] [--out DIR] [--upload] [--yes]

Writes `<out>/base-<section_handles>.json.gz` (default out: GENERATED.runtiming/feed). `--upload` puts it at the
STAGING bucket's `private/runtiming/base/<handles>.json.gz` (`--yes` to send; without it, the plan), where the
workflow finds it by the live release's handles.

KEPT: a stream of Strahler order >= `MIN_ORDER` (where a run can spawn, and everything below it: a walk down from order
3 never meets a lower order), a lake that drains, and every NAMED water (a DFO survey site binds a stream by its code
and name, whatever its order), and every section below a kept one (the way to the sea stays whole). Dropped: ~1.3 M unnamed first- and second-order trickles and closed ponds no relation
can reach. Columns, one row per kept section in sid order:

    sid (delta)  k 0 stream / 1 lake   o order   len metres   wsc (string table)   blk   m0 m1 (measures, m)
    wbk (lake key, string table)   nm (name keys, string table)   dn (the kept sections it flows into)
    it (its water's item_id, string table; -1 none)   x y (a point: a line's midpoint, a polygon's ring centre, 1e-5 deg)
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import math
import pickle
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

FORMAT = "runtiming-base/1"
MIN_ORDER = 3
LAYERS = ("stream", "lake")
BUCKET = "bc-fishing-regulations-staging"       # STAGING ONLY: this module never names a production bucket
KEY = "private/runtiming/base/{handles}.json.gz"


def name_key(name: Optional[str]) -> str:
    """A water's name for comparison: lower case, one space; a DFO stretch's suffix dropped ("BABINE RIVER - SECTION 4"
    is the Babine River). The base stores these keys; `bind.observed` compares a DFO site's name by the same rule."""
    return " ".join((name or "").split(" - ")[0].lower().replace("'", "").split())


# ---- geometry, cheaply ---------------------------------------------------------------------------------------------

def _line_mid(coords: Sequence[Sequence[float]]) -> Tuple[float, float]:
    """The point half way ALONG a line (planar degrees: a section is a few km, the error is metres)."""
    if len(coords) == 1:
        return coords[0][0], coords[0][1]
    seg = [math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in zip(coords, coords[1:])]
    half, acc = sum(seg) / 2, 0.0
    for (a, b), d in zip(zip(coords, coords[1:]), seg):
        if acc + d >= half and d > 0:
            t = (half - acc) / d
            return a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])
        acc += d
    return coords[-1][0], coords[-1][1]


def point_of(geom: dict) -> Optional[Tuple[float, float]]:
    """A point for a tile feature: a line's midpoint (the longest part of a multi-line), a polygon's ring centre."""
    t, c = geom.get("type"), geom.get("coordinates")
    if not c:
        return None
    if t == "LineString":
        return _line_mid(c)
    if t == "MultiLineString":
        return _line_mid(max(c, key=len))
    if t == "Polygon":
        ring = c[0]
    elif t == "MultiPolygon":
        ring = max((p[0] for p in c), key=len)
    else:
        return None
    return sum(p[0] for p in ring) / len(ring), sum(p[1] for p in ring) / len(ring)


def section_points(layers: Path, want: set) -> Iterator[Tuple[int, float, float]]:
    """(sid, lon, lat) for every wanted section the tiles draw — streamed, a line at a time (the stream layer is
    ~2.5 GB). The first feature of a section wins (a section is one feature per layer)."""
    import orjson
    for name in LAYERS:
        p = layers / f"{name}.geojsonl"
        if not p.exists():
            raise SystemExit(f"base: {p} not found — the base is bound to the tiles' own sections")
        with open(p, "rb") as f:
            for line in f:
                # cheap reject before parsing the geometry: the id sits in the properties
                d = orjson.loads(line)
                sid = d["properties"].get("section_id")
                if sid is None or int(sid) not in want:
                    continue
                pt = point_of(d["geometry"])
                if pt is not None:
                    yield int(sid), pt[0], pt[1]


# ---- the base ------------------------------------------------------------------------------------------------------

def item_of(release: Path) -> Tuple[str, Dict[int, str]]:
    """(section_handles, {sid: item_id}) from the release's core (`item_section` + `item`)."""
    core = sorted(release.glob("core-*.sqlite.gz"))
    if len(core) != 1:
        raise SystemExit(f"base: expected one core-*.sqlite.gz in {release}, found {[p.name for p in core]}")
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "core.sqlite"
        db.write_bytes(gzip.decompress(core[0].read_bytes()))
        c = sqlite3.connect(db)
        meta = dict(c.execute("SELECT k, v FROM meta"))
        out = {sid: iid for sid, iid in c.execute(
            "SELECT s.sid, i.item_id FROM item_section s JOIN item i ON i.ord = s.ord ORDER BY s.sid, i.item_id")}
        c.close()
    return meta["section_handles"], out


def build(build_dir: Path, tiles: Path, release: Path) -> Tuple[dict, dict]:
    """(the base, a report)."""
    from pipeline.common.section_handles import digest_for, read as read_handles
    from pipeline.common.utils.wsc import trim_wsc
    handles = digest_for(build_dir)
    meta = json.loads((tiles / "atlas.meta.json").read_text())
    if meta.get("section_handles") != handles:
        raise SystemExit(f"base: the tiles at {tiles} carry handles {meta.get('section_handles')}, the atlas {handles}")
    core_handles, items = item_of(release)
    if core_handles != handles:
        raise SystemExit(f"base: the release at {release} carries handles {core_handles}, the atlas {handles}")
    _, by_id = read_handles(build_dir)
    with open(build_dir / "graph.pkl", "rb") as fh:
        graph = pickle.load(fh)
    rows: Dict[int, dict] = {}
    for nid, node in graph.nodes.items():
        h = by_id.get(nid)
        if h is None or node.not_fishing:
            continue
        lake = getattr(node.kind, "value", node.kind) == "lake"
        names = sorted({k for k in (name_key(n) for n in (node.display_name, *(t.name for t in node.name_tuples)))
                        if k})
        rows[h] = {"k": 1 if lake else 0, "o": node.stream_order or 0, "len": int(round(node.length_m or 0)),
                   "wsc": trim_wsc(node.wsc or ""), "blk": node.blk or "", "m0": int(node.down_m or 0),
                   "m1": int(round(node.up_m or 0)), "wbk": node.wbk or "", "nm": names}
    down: Dict[int, List[int]] = {}
    for nid, idxs in graph.down_adj.items():
        h = by_id.get(nid)
        if h is None or h not in rows:
            continue
        outs = sorted({by_id[graph.edges[i].to_node] for i in idxs if graph.edges[i].to_node in by_id})
        if outs:
            down[h] = outs
    del graph
    keep = {h for h, r in rows.items()
            if r["nm"] or (r["k"] == 0 and r["o"] >= MIN_ORDER) or (r["k"] == 1 and down.get(h))}
    # and everything below them: a draining lake or a named creek may reach its river through an unnamed trickle, and
    # a passage walk that met a dropped section would stop short of the sea
    stack = sorted(keep)
    while stack:
        for d in down.get(stack.pop(), ()):
            if d not in keep:
                keep.add(d)
                stack.append(d)
    point: Dict[int, Tuple[float, float]] = {}
    for sid, lon, lat in section_points(tiles / "layers", keep):
        point.setdefault(sid, (lon, lat))
    keep = {h for h in keep if h in point}                     # a section the tiles do not draw takes no run
    sids = sorted(keep)
    strings: Dict[str, Dict[str, int]] = {"wsc": {}, "wbk": {}, "nm": {}, "it": {}}

    def ix(table: str, v: str) -> int:
        t = strings[table]
        return t.setdefault(v, len(t))

    cols: Dict[str, list] = {c: [] for c in ("sid", "k", "o", "len", "wsc", "blk", "m0", "m1", "wbk", "nm", "dn",
                                              "it", "x", "y")}
    prev = 0
    for s in sids:
        r = rows[s]
        cols["sid"].append(s - prev)
        prev = s
        cols["k"].append(r["k"])
        cols["o"].append(r["o"])
        cols["len"].append(r["len"])
        cols["wsc"].append(ix("wsc", r["wsc"]))
        cols["blk"].append(r["blk"])
        cols["m0"].append(r["m0"])
        cols["m1"].append(r["m1"])
        cols["wbk"].append(ix("wbk", r["wbk"]))
        cols["nm"].append([ix("nm", n) for n in r["nm"]])
        cols["dn"].append([d for d in down.get(s, ()) if d in keep])
        it = items.get(s)
        cols["it"].append(ix("it", it) if it else -1)
        x, y = point[s]
        cols["x"].append(int(round(x * 1e5)))
        cols["y"].append(int(round(y * 1e5)))
    base = {"format": FORMAT, "section_handles": handles, "tile_version": meta.get("tile_version"),
            "built_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "min_order": MIN_ORDER, "count": len(sids),
            "strings": {k: [s for s, _ in sorted(v.items(), key=lambda kv: kv[1])] for k, v in strings.items()},
            "cols": cols}
    report = {"sections": len(rows), "kept": len(sids), "lakes": sum(cols["k"]), "with_item": sum(1 for i in cols["it"]
                                                                                                   if i >= 0)}
    return base, report


def write(base: dict, out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    p = out / f"base-{base['section_handles']}.json.gz"
    p.write_bytes(gzip.compress(json.dumps(base, separators=(",", ":")).encode(), mtime=0))
    return p


def read(path: Path) -> dict:
    """The base, or refused (wrong format, a column of the wrong length)."""
    b = json.loads(gzip.decompress(Path(path).read_bytes()))
    if b.get("format") != FORMAT:
        raise SystemExit(f"{path}: format {b.get('format')!r}, not {FORMAT}")
    n = b["count"]
    bad = [k for k, v in b["cols"].items() if len(v) != n]
    if bad:
        raise SystemExit(f"{path}: columns {bad} are not {n} long")
    return b


def upload(path: Path, handles: str, yes: bool) -> None:
    key = KEY.format(handles=handles)
    cmd = ["npx", "--prefix", "app", "wrangler", "r2", "object", "put", f"{BUCKET}/{key}", "--file", str(path),
           "--content-type", "application/gzip", "--remote"]
    if not yes:
        print("plan (add --yes to send):", " ".join(cmd))
        return
    subprocess.run(cmd, check=True)
    print(f"base: {path.name} -> {BUCKET}/{key}")


def main(argv=None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(prog="pipeline.runtiming.feed.base", description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", type=Path, default=None, help="the atlas build (default: the live one)")
    ap.add_argument("--tiles", type=Path, default=None, help="the tiles dir (default: GENERATED.tiles)")
    ap.add_argument("--release", type=Path, default=None, help="the release whose core names the items (default: LATEST)")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--upload", action="store_true", help="put it on the STAGING bucket's private/runtiming/base/")
    ap.add_argument("--yes", action="store_true", help="with --upload: send it (without: print the plan)")
    a = ap.parse_args(argv)
    release = a.release or GENERATED.release / (GENERATED.release / "LATEST").read_text().strip()
    base, report = build(a.build or GENERATED.require_build(), a.tiles or GENERATED.tiles, release)
    p = write(base, a.out or GENERATED.runtiming / "feed")
    print(f"base: {json.dumps(report, sort_keys=True)} -> {p} ({p.stat().st_size:,} B)")
    if a.upload:
        upload(p, base["section_handles"], a.yes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
