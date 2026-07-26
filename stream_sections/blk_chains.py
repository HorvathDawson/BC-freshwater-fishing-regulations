"""Step 1 (03 S1): load FWA stream fids and merge them into per-BLK chains.

`load_stream_fids` reads the `streams` layer once (via FWADataAccessor) into lightweight
FidRow records (with computed endpoint node ids); both `build_blk_chains` and
`graph.build_stream_graph` consume that single load. Chains are sorted mouth->source by
DOWNSTREAM_ROUTE_MEASURE, geometry stitched, under-lake runs recorded. Sentinel 999-999999
WSC/BLKs are skipped. Names beyond the direct gazette tuple are added by names.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from data.data_extractor import FWADataAccessor
from pipeline.utils.wsc import trim_wsc

from . import cutting
from .models import BlkChain, FidSpan, NameSource, NameTuple, WaterbodyRun

_STREAM_COLUMNS = [
    "LINEAR_FEATURE_ID", "BLUE_LINE_KEY", "FWA_WATERSHED_CODE", "GNIS_ID", "GNIS_NAME",
    "WATERBODY_KEY", "STREAM_ORDER", "STREAM_MAGNITUDE", "EDGE_TYPE",
    "DOWNSTREAM_ROUTE_MEASURE", "UPSTREAM_ROUTE_MEASURE", "LENGTH_METRE",
]
_SENTINEL_WSC = "999-999999"


@dataclass
class FidRow:
    """One FWA linear feature, build-time only (carries geometry + topology fields)."""
    fid: str
    blk: str
    wsc: str            # trim_wsc'd
    edge_type: str
    wbk: str
    gnis_id: str
    gnis_name: str
    stream_order: Optional[int]
    stream_magnitude: Optional[int]
    down_m: float
    up_m: float
    geometry: Any
    down_node: str      # mouth end  (coords[0])
    up_node: str        # source end (coords[-1])


def load_stream_fids(gpkg_path: str, bbox: Optional[tuple] = None,
                     streams_layer: str = "streams") -> list[FidRow]:
    """Read stream fids into FidRow records. Skips sentinel WSC and degenerate geometry."""
    fwa = FWADataAccessor(gpkg_path)
    gdf = fwa.get_layer(streams_layer, columns=_STREAM_COLUMNS, bbox=bbox)
    rows: list[FidRow] = []
    for r in gdf.itertuples():
        wsc_raw = r.FWA_WATERSHED_CODE or ""
        if wsc_raw.startswith(_SENTINEL_WSC):
            continue
        down_node, up_node = cutting.blk_endpoints(r.geometry)
        if down_node is None or up_node is None:
            continue
        down_m = float(r.DOWNSTREAM_ROUTE_MEASURE)
        up_m = float(r.UPSTREAM_ROUTE_MEASURE)
        rows.append(FidRow(
            fid=r.LINEAR_FEATURE_ID, blk=r.BLUE_LINE_KEY, wsc=trim_wsc(wsc_raw),
            edge_type=r.EDGE_TYPE or "", wbk=r.WATERBODY_KEY or "",
            gnis_id=r.GNIS_ID or "", gnis_name=r.GNIS_NAME or "",
            stream_order=r.STREAM_ORDER, stream_magnitude=r.STREAM_MAGNITUDE,
            down_m=down_m, up_m=up_m, geometry=r.geometry,
            down_node=down_node, up_node=up_node,
        ))
    return rows


def _max_opt(a: Optional[int], b: Optional[int]) -> Optional[int]:
    vals = [v for v in (a, b) if v is not None]
    return max(vals) if vals else None


def build_blk_chains(fid_rows: list[FidRow], lake_wbk_kind: dict[str, str]) -> list[BlkChain]:
    """Merge FidRows into one BlkChain per blue line. ``lake_wbk_kind`` maps wbk -> lake|manmade."""
    by_blk: dict[str, list[FidRow]] = {}
    for row in fid_rows:
        by_blk.setdefault(row.blk, []).append(row)

    chains: list[BlkChain] = []
    for blk, group in by_blk.items():
        group.sort(key=lambda r: r.down_m)
        fids = tuple(FidSpan(fid=r.fid, down_m=r.down_m, up_m=r.up_m) for r in group)
        geometry = cutting.merge_ordered([r.geometry for r in group])
        mouth_measure = group[0].down_m
        length_m = group[-1].up_m - mouth_measure

        gnis_id = next((r.gnis_id for r in group if r.gnis_id), "")
        gnis_name = next((r.gnis_name for r in group if r.gnis_name), "")
        order = None
        magnitude = None
        for r in group:
            order = _max_opt(order, r.stream_order)
            magnitude = _max_opt(magnitude, r.stream_magnitude)
        # Preserve distinct edge types: the merge would otherwise lose per-fid EDGE_TYPE, and
        # 2300 (artificial connector) must survive to act as a tributary-walk barrier (S2).
        edge_types = tuple(sorted({r.edge_type for r in group if r.edge_type}))

        runs = tuple(
            WaterbodyRun(wbk=r.wbk, down_m=r.down_m, up_m=r.up_m,
                         kind=lake_wbk_kind[r.wbk])
            for r in group if r.wbk in lake_wbk_kind
        )
        name_tuples = ((NameTuple(gnis_name, NameSource.gazette),) if gnis_name else ())

        chains.append(BlkChain(
            blk=blk, fwa_watershed_code=group[0].wsc, fids=fids, geometry=geometry,
            mouth_measure=mouth_measure, length_m=length_m, name_tuples=name_tuples,
            gnis_id=gnis_id, gnis_name=gnis_name, stream_order=order,
            stream_magnitude=magnitude, waterbody_runs=runs, edge_types=edge_types,
        ))
    return chains
