"""OLD -> NEW split accuracy audit: for every split in pipeline/splits.json, resolve it and measure
how far the RESOLVED cut landed from the CURATED coord (`_coord`) in waterbody-splits.json (ground
truth). The curated coords were hand-verified, so a large distance flags a bad conversion.

Per waterbody: bbox = union(_coord of its splits) ∪ applies_to target bounds (200km-capped),
buffered 3km. Resolve; recover each SplitPoint's lon/lat via chain.geometry.interpolate(measure);
distance (m, EPSG:3005) to the split's `_coord`. Offset splits store `_coord` inconsistently (some =
base feature, some = final post-offset location), so the score is min(raw, |raw - offset|).

Run:  python -m pipeline.hack.audit_splits [--json]
This reads small bbox windows but touches the full GPKG per waterbody, so it takes a couple minutes.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

from pyproj import Transformer
from shapely.geometry import Point

from project_config import get_config
from data.data_extractor import FWADataAccessor
from pipeline.splits.splits import load_split_defs
from pipeline.splits.anchors import resolve_split_defs
from pipeline.graph.blk_chains import load_stream_fids, build_blk_chains
from pipeline.build import get_lake_wbk_kind

ROOT = get_config().project_root
_DATA = str(ROOT / "data/bc_fisheries_data.gpkg")
_TR = Transformer.from_crs(4326, 3005, always_xy=True)


def _target_bounds(fwa: FWADataAccessor, applies: Optional[dict]):
    applies = applies or {}
    try:
        if applies.get("gnis_id"):
            g = fwa.get_features_by_attribute("streams", "GNIS_ID", str(applies["gnis_id"]))
        elif applies.get("gnis_ids"):
            g = fwa.get_features_by_attribute("streams", "GNIS_ID", [str(x) for x in applies["gnis_ids"]])
        elif applies.get("blk"):
            g = fwa.get_features_by_attribute("streams", "BLUE_LINE_KEY", int(applies["blk"]))
        else:
            return None
    except Exception:
        return None
    return None if (g is None or g.empty) else tuple(g.total_bounds)


def audit_splits(splits_path: Optional[Path] = None) -> list[dict]:
    """Return one row per split: {id, wb, type, dist_m|None, offset_m|None, resolved}."""
    splits_path = splits_path or (ROOT / "pipeline/splits.json")
    fwa = FWADataAccessor(_DATA)
    defs = {d.id: d for d in load_split_defs(str(splits_path))}
    j = json.loads(Path(splits_path).read_text())
    rows: list[dict] = []

    for wb in j["waterbodies"]:
        sp = wb["splits"]
        xs: list[float] = []; ys: list[float] = []
        for s in sp:
            c = s.get("_coord") or s.get("_coord_cache")
            if c:
                x, y = _TR.transform(c[0], c[1]); xs.append(x); ys.append(y)
        tb = _target_bounds(fwa, wb.get("applies_to"))
        if tb and (tb[2] - tb[0]) < 200000 and (tb[3] - tb[1]) < 200000:
            xs += [tb[0], tb[2]]; ys += [tb[1], tb[3]]
        if not xs:
            for s in sp:
                rows.append({"id": s["id"], "wb": wb["name"], "type": s["anchor"]["type"],
                             "dist_m": None, "offset_m": s.get("_offset_m"), "resolved": False})
            continue
        pad = 3000.0
        bbox = (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)
        try:
            chains = build_blk_chains(load_stream_fids(_DATA, bbox=bbox), get_lake_wbk_kind(fwa, bbox))
        except Exception:
            for s in sp:
                rows.append({"id": s["id"], "wb": wb["name"], "type": s["anchor"]["type"],
                             "dist_m": None, "offset_m": s.get("_offset_m"), "resolved": False})
            continue
        by_blk = {c.blk: c for c in chains}
        pts = resolve_split_defs([defs[s["id"]] for s in sp if s["id"] in defs], chains)
        by_split: dict = defaultdict(list)
        for p in pts:
            by_split[p.split_id].append(p)
        for s in sp:
            c = s.get("_coord") or s.get("_coord_cache")
            got = by_split.get(s["id"], [])
            best = None
            if got and c:
                cx, cy = _TR.transform(c[0], c[1]); cp = Point(cx, cy)
                for p in got:
                    ch = by_blk.get(p.blk)
                    if ch is None or ch.geometry is None:
                        continue
                    m = max(0.0, min(p.route_measure - ch.mouth_measure, ch.geometry.length))
                    d = ch.geometry.interpolate(m).distance(cp)
                    best = d if best is None else min(best, d)
            rows.append({"id": s["id"], "wb": wb["name"], "type": s["anchor"]["type"],
                         "dist_m": best, "offset_m": s.get("_offset_m"), "resolved": bool(got)})
    return rows


def score(r: dict) -> Optional[float]:
    """Distance metric, offset-accounted (accurate if near either _coord or _coord-shifted-by-offset)."""
    d = r["dist_m"]
    if d is None:
        return None
    return min(d, abs(d - r["offset_m"])) if r["offset_m"] else d


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit resolved splits vs curated coords")
    ap.add_argument("--json", action="store_true", help="dump the per-split rows as JSON")
    args = ap.parse_args()
    rows = audit_splits()
    if args.json:
        print(json.dumps(rows, indent=1)); return

    res = [r for r in rows if r["resolved"] and r["dist_m"] is not None]
    unresolved = [r for r in rows if not r["resolved"]]
    buck: Counter = Counter()
    for r in res:
        s = score(r)
        buck["<=25" if s <= 25 else "25-100" if s <= 100 else "100-500" if s <= 500 else ">500"] += 1
    print(f"TOTAL {len(rows)} | resolved(with dist) {len(res)} | unresolved {len(unresolved)}")
    print(f"best-of-two buckets: {dict(buck)}")
    print("\nworst 15 (score, offset-accounted):")
    for r in sorted(res, key=lambda r: -score(r))[:15]:
        tag = f" [off {r['offset_m']}, raw {r['dist_m']:.0f}]" if r["offset_m"] else ""
        print(f"  {score(r):7.0f}m  {r['id'][:38]:38s} {r['type']:11s} {r['wb'][:26]}{tag}")
    print(f"\nunresolved ({len(unresolved)}):")
    for r in unresolved:
        print(f"  {r['id']:36s} {r['type']:12s} {r['wb'][:34]}")


if __name__ == "__main__":
    main()
