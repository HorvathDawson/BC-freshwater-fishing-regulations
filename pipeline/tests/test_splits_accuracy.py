"""Accuracy guard: resolved cuts must land near their curated `_coord` (ground truth).

A fast, single-bbox slice of pipeline.hack.audit_splits over the Bella Coola / Atnarko system
(mainstem cuts + the three self-declaring tributary cards). The full sweep lives in
`python -m pipeline.hack.audit_splits`; this keeps a regression tripwire in the suite.
"""

import json
import os

import pytest
from pyproj import Transformer
from shapely.geometry import Point
from pipeline.curated import CURATED, SOURCE

_DATA = str(SOURCE / "bc_fisheries_data.gpkg")
_needs_data = pytest.mark.skipif(not os.path.exists(_DATA), reason="needs data/bc_fisheries_data.gpkg")

# split_id -> max acceptable distance (m) from its curated _coord (offset-accounted).
# Confluences/points on-channel resolve to ~0; a generous cap catches wrong-channel regressions.
_EXPECT = {
    "burnt_bridge_creek__sitkatapa_confluence": 100,           # confluence (curated label)
    "atnarko_bella_coola_rivers_includes_tributaries_exce__goat_creek_into_atnarko_river": 100,       # confluence
    "atnarko_bella_coola_rivers_includes_tributaries_exce__talchako_river_into_bella_coola_river": 150,  # confluence
    "hunlen_creek__hunlen_falls": 200,                         # point (obstacle-grounded)
    "young_creek__young_hwy20": 250,                           # point (Hwy 20 crossing)
}


@_needs_data
def test_bella_coola_splits_resolve_near_curated_coords():
    from data.data_extractor import FWADataAccessor
    from pipeline.splits.splits import load_split_defs
    from pipeline.splits.anchors import resolve_split_defs
    from pipeline.graph.blk_chains import load_stream_fids, build_blk_chains
    from pipeline.build import get_lake_wbk_kind

    defs = {d.id: d for d in load_split_defs(str(CURATED.waters.splits))}
    j = json.loads(CURATED.waters.splits.read_text())
    coords = {s["id"]: s.get("_coord") for wb in j["waterbodies"] for s in wb["splits"]}
    for sid in _EXPECT:
        assert sid in defs, f"{sid} missing from splits.json"
        assert coords.get(sid), f"{sid} has no _coord to check against"

    tr = Transformer.from_crs(4326, 3005, always_xy=True)
    xs, ys = [], []
    for sid in _EXPECT:
        x, y = tr.transform(*coords[sid]); xs.append(x); ys.append(y)
    pad = 4000.0
    bbox = (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)

    fwa = FWADataAccessor(_DATA)
    chains = build_blk_chains(load_stream_fids(_DATA, bbox=bbox), get_lake_wbk_kind(fwa, bbox))
    by_blk = {c.blk: c for c in chains}
    pts = resolve_split_defs([defs[sid] for sid in _EXPECT], chains)

    for sid, tol in _EXPECT.items():
        got = [p for p in pts if p.split_id == sid]
        assert got, f"{sid} produced no cut"
        cx, cy = tr.transform(*coords[sid]); cp = Point(cx, cy)
        best = min(
            by_blk[p.blk].geometry.interpolate(
                max(0.0, min(p.route_measure - by_blk[p.blk].mouth_measure, by_blk[p.blk].geometry.length))
            ).distance(cp)
            for p in got if p.blk in by_blk
        )
        assert best <= tol, f"{sid} resolved {best:.0f} m from its curated coord (tol {tol} m)"
