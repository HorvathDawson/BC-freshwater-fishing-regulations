"""
BaseEntryBuilder — natural gazetteer search over synopsis rows.

Reads synopsis_raw_data.json and builds one BaseEntry per synopsis row using
pure name-based gazetteer search.  No overrides, no manual corrections.

The output JSON is the transparent name+region+mus → gnis_ids table — the
document you diff year-over-year to see exactly what changed in the PDF.

To apply corrections at runtime, layer OverrideEntry objects on top via
MatchTable(bases, overrides).

CLI
---
    python -m pipeline.matching.base_entry_builder
    python -m pipeline.matching.base_entry_builder \\
        --raw path/to/raw.json --out path/to/out.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import logging
import pickle
import yaml
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union
from typing_extensions import TypedDict

from data.data_extractor import FWADataAccessor
from shapely.geometry import LineString
from shapely.ops import unary_union
from shapely.prepared import prep
from shapely.strtree import STRtree
from tqdm import tqdm

from pipeline.utils.wsc import trim_wsc

from .match_table import BaseEntry, OverrideEntry, MatchTable
from .reg_models import MatchCriteria

logger = logging.getLogger(__name__)


class FeatureType(Enum):
    """FWA hydrological layer classification — used as metadata dict keys."""

    STREAM = "streams"
    LAKE = "lakes"
    WETLAND = "wetlands"
    MANMADE = "manmade"


_HYDRO_TYPES = (
    FeatureType.STREAM,
    FeatureType.LAKE,
    FeatureType.WETLAND,
    FeatureType.MANMADE,
)


# Shared type alias for a single feature record (stream or polygon)
class StreamFeature(TypedDict):
    gnis_id: str
    gnis_name: str
    edge_ids: List[str]
    fwa_watershed_codes: List[str]
    blue_line_keys: List[str]
    zones: List[str]
    mgmt_units: List[str]


class PolyFeature(TypedDict):
    waterbody_key: str
    gnis_name: str
    gnis_id: str
    gnis_name_2: str
    gnis_id_2: str
    poly_ids: List[str]
    zones: List[str]
    mgmt_units: List[str]


FeatureRecord = Union[StreamFeature, PolyFeature]
# Name → feature records lookup
NameIndex = Dict[str, List[FeatureRecord]]


class FeatureTypeData(TypedDict):
    features: Dict[str, FeatureRecord]
    name_index: Dict[str, List[str]]


Metadata = Dict[FeatureType, FeatureTypeData]


class SynopsisRow(TypedDict, total=False):
    water: str
    region: str
    mu: List[str]
    raw_regs: str


# ---------------------------------------------------------------------------
# Natural search (no manual corrections — that is what overrides are for)
# ---------------------------------------------------------------------------


def _ref_identity(ref: FeatureRecord) -> tuple:
    """Stable deduplication key for a raw feature dict."""
    wscs = ref.get("fwa_watershed_codes", [])
    if wscs:
        return ("wsc", wscs[0])
    gnis_id = ref.get("gnis_id", "")
    if gnis_id:
        return ("gnis", gnis_id)
    wbk = ref.get("waterbody_key", "")
    if wbk:
        return ("wbk", wbk)
    blks = ref.get("blue_line_keys", [])
    if blks:
        return ("blk", blks[0])
    pids = ref.get("poly_ids", [])
    if pids:
        return ("poly", pids[0])
    return ("unknown", ref.get("gnis_name", ""))


def _name_variations(name: str) -> List[str]:
    """Return name as-is — no parenthetical stripping.

    Parenthetical qualifiers are preserved in the base table.
    Name→feature resolution for entries with parens is handled
    by OverrideEntry records in overrides.json.
    """
    return [name]


def _zone_from_region(region: Optional[str]) -> Optional[str]:
    """Extract zone number from a region string ('Region 4' → '4')."""
    if not region:
        return None
    parts = region.split()
    return parts[-1] if parts else None


def _build_name_index(metadata: Metadata) -> NameIndex:
    """Build title-cased name → [feature_dict, ...] lookup from raw metadata."""
    index: NameIndex = {}
    for ftype in _HYDRO_TYPES:
        ftype_data = metadata.get(ftype, {})
        features = ftype_data.get("features", {})
        is_stream = ftype == FeatureType.STREAM
        for fid, data in features.items():
            _add_name(index, data.get("gnis_name"), data)
            if not is_stream:
                _add_name(index, data.get("gnis_name_2"), data)
    return index


def _add_name(index: NameIndex, name: object, data: FeatureRecord) -> None:
    if isinstance(name, str) and name.strip():
        index.setdefault(name.strip().title(), []).append(data)


def _search_index(
    index: NameIndex,
    name: str,
    zone: Optional[str],
) -> List[FeatureRecord]:
    """Name lookup with optional zone filter."""
    hits = index.get(name.strip().title(), [])
    if zone:
        hits = [r for r in hits if zone in r.get("zones", [])]
    return hits


def _natural_search(
    name_index: NameIndex,
    name: str,
    zone: Optional[str],
    mus: List[str],
) -> Tuple[List[FeatureRecord], str]:
    """Search the name index and return (feature_dicts, link_method).

    Handles name variant fallback, cross-zone fallback, MU overlap filter,
    and deduplication by canonical identifier.
    """
    candidates: List[FeatureRecord] = []

    for search_name in _name_variations(name):
        hits = _search_index(name_index, search_name, zone)
        if not hits and zone:
            hits = _search_index(name_index, search_name, None)
        if hits:
            candidates = hits
            break

    if not candidates:
        return [], "not_found"

    # MU overlap filter
    if mus:
        candidates = [
            r for r in candidates if any(mu in r.get("mgmt_units", []) for mu in mus)
        ]

    if not candidates:
        return [], "not_found"

    # Deduplicate by canonical identifier
    seen: set = set()
    refs: List[FeatureRecord] = []
    for ref in candidates:
        identity = _ref_identity(ref)
        if identity not in seen:
            seen.add(identity)
            refs.append(ref)

    return refs, "natural_search"


def _extract_gnis_ids(refs: List[FeatureRecord]) -> List[str]:
    """Extract unique GNIS IDs from feature records (streams + polygons).

    Checks both gnis_id (primary) and gnis_id_2 (secondary name on polygons)
    to avoid silent data loss for features matched via their secondary name.
    """
    ids: List[str] = []
    seen: set = set()
    for ref in refs:
        for key in ("gnis_id", "gnis_id_2"):
            gid = ref.get(key, "")
            if gid and gid not in seen:
                seen.add(gid)
                ids.append(gid)
    return ids


# ---------------------------------------------------------------------------
# Metadata builder (streams by gnis_id, polygons by waterbody_key)
# ---------------------------------------------------------------------------

_POLY_LAYERS: list[tuple[FeatureType, str]] = [
    (FeatureType.LAKE, "lakes"),
    (FeatureType.WETLAND, "wetlands"),
    (FeatureType.MANMADE, "manmade_water"),
]


def _build_metadata(graph_path: Path, gpkg_path: Path) -> Metadata:
    """Build FWA name-matching metadata in-memory.

    Streams grouped by gnis_id, polygons by waterbody_key.
    Zone/MU assignment via exact spatial intersection.
    """
    accessor = FWADataAccessor(gpkg_path)

    # -- Zone index --
    zone_gdf = accessor.get_layer("wmu")
    if zone_gdf.crs and zone_gdf.crs.to_epsg() != 3005:
        zone_gdf = zone_gdf.to_crs(epsg=3005)
    zone_gdf["zone"] = zone_gdf["REGION_RESPONSIBLE_ID"]
    zone_tree = STRtree(zone_gdf.geometry)
    zone_preps = [prep(geom) for geom in zone_gdf.geometry]
    logger.info(f"Indexed {len(zone_gdf):,} WMU rows.")

    def assign_zones(groups: dict, desc: str) -> tuple[dict, dict]:
        g_zones: dict = {}
        g_mus: dict = {}
        for gk, geoms in tqdm(
            groups.items(), desc=desc, unit="grp", leave=False, disable=False
        ):
            zones, mus = set(), set()
            union = unary_union(geoms)
            for idx in zone_tree.query(union):
                if zone_preps[idx].intersects(union):
                    zones.add(zone_gdf.iloc[idx]["zone"])
                    mus.add(zone_gdf.iloc[idx]["WILDLIFE_MGMT_UNIT_ID"])
            g_zones[gk] = sorted(zones)
            g_mus[gk] = sorted(mus)
        return g_zones, g_mus

    metadata: Metadata = {ft: {} for ft in FeatureType}  # type: ignore[misc]

    # -- Streams (grouped by gnis_id) --
    with open(graph_path, "rb") as f:
        dump = pickle.load(f)
    edge_attrs, node_coords = dump["edge_attrs"], dump["node_coords"]
    logger.info(f"Loaded {len(edge_attrs):,} edges.")

    stream_groups: dict = {}
    skipped = no_gnis = 0
    for key, attrs in edge_attrs.items():
        gnis_id = str(attrs.get("gnis_id") or "")
        if not gnis_id:
            no_gnis += 1
            continue
        try:
            u, v = node_coords[attrs["source"]], node_coords[attrs["target"]]
        except KeyError:
            skipped += 1
            continue
        grp = stream_groups.setdefault(
            gnis_id,
            {
                "gnis_name": attrs.get("gnis_name") or "",
                "edge_ids": [],
                "wscs": set(),
                "blks": set(),
                "geoms": [],
            },
        )
        grp["edge_ids"].append(str(key))
        grp["geoms"].append(LineString([u, v]))
        wsc = trim_wsc(attrs.get("fwa_watershed_code") or "")
        blk = attrs.get("blue_line_key") or ""
        if wsc:
            grp["wscs"].add(wsc)
        if blk:
            grp["blks"].add(blk)

    logger.info(
        f"Grouped {len(stream_groups):,} GNIS streams ({no_gnis:,} unnamed, {skipped:,} skipped)."
    )
    sz, sm = assign_zones(
        {gid: g["geoms"] for gid, g in stream_groups.items()}, "Streams"
    )

    s_features: dict = {}
    s_name_idx: dict = {}
    for gid, g in stream_groups.items():
        s_features[gid] = {
            "gnis_id": gid,
            "gnis_name": g["gnis_name"],
            "edge_ids": g["edge_ids"],
            "fwa_watershed_codes": sorted(g["wscs"]),
            "blue_line_keys": sorted(g["blks"]),
            "zones": sz[gid],
            "mgmt_units": sm[gid],
        }
        name = g["gnis_name"].strip().title()
        if name:
            s_name_idx.setdefault(name, []).append(gid)
    metadata[FeatureType.STREAM] = {"features": s_features, "name_index": s_name_idx}
    logger.info(f"Stored {len(s_features):,} stream features.")

    # -- Polygons (lakes, wetlands, manmade — grouped by waterbody_key) --
    available = set(accessor.list_layers())
    for ftype, layer in _POLY_LAYERS:
        if layer not in available:
            logger.warning(f"Layer '{layer}' not in GPKG — skipped.")
            continue
        gdf = accessor.get_layer(layer)
        poly_groups: dict = {}
        for _, row in gdf.iterrows():
            pid = str(row.get("WATERBODY_POLY_ID") or "")
            if not pid:
                continue
            wbk = str(row.get("WATERBODY_KEY") or pid)
            grp = poly_groups.setdefault(
                wbk,
                {
                    "gnis_name": row.get("GNIS_NAME_1") or row.get("GNIS_NAME") or "",
                    "gnis_id": str(row.get("GNIS_ID_1") or row.get("GNIS_ID") or ""),
                    "gnis_name_2": row.get("GNIS_NAME_2") or "",
                    "gnis_id_2": str(row.get("GNIS_ID_2") or ""),
                    "poly_ids": [],
                    "geoms": [],
                },
            )
            grp["poly_ids"].append(pid)
            grp["geoms"].append(row.geometry)

        pz, pm = assign_zones(
            {w: g["geoms"] for w, g in poly_groups.items()}, ftype.name
        )

        p_features: dict = {}
        p_name_idx: dict = {}
        for wbk, g in poly_groups.items():
            p_features[wbk] = {
                "waterbody_key": wbk,
                "gnis_name": g["gnis_name"],
                "gnis_id": g["gnis_id"],
                "gnis_name_2": g["gnis_name_2"],
                "gnis_id_2": g["gnis_id_2"],
                "poly_ids": g["poly_ids"],
                "zones": pz[wbk],
                "mgmt_units": pm[wbk],
            }
            for n in (g["gnis_name"], g["gnis_name_2"]):
                n = (n or "").strip().title()
                if n:
                    p_name_idx.setdefault(n, []).append(wbk)
        metadata[ftype] = {"features": p_features, "name_index": p_name_idx}
        logger.info(
            f"Stored {len(p_features):,} {ftype.name} "
            f"({sum(len(g['poly_ids']) for g in poly_groups.values()):,} polys)."
        )

    return metadata


# ---------------------------------------------------------------------------
# V2 Linker
# ---------------------------------------------------------------------------


class BaseEntryBuilder:
    """Builds a static BaseEntry table from synopsis_raw_data.json.

    Builds FWA metadata in-memory from graph + GPKG, then runs pure
    name-based search -- no overrides, no corrections.
    """

    def __init__(self, graph_path: Path, gpkg_path: Path) -> None:
        print("Building FWA metadata...", flush=True)
        metadata = _build_metadata(graph_path, gpkg_path)
        self._name_index = _build_name_index(metadata)
        total = sum(len(v.get("features", {})) for v in metadata.values())
        print(
            f"Name index ready ({len(self._name_index):,} names, {total:,} features).",
            flush=True,
        )

    def link_row(self, row: SynopsisRow) -> BaseEntry:
        """Search one synopsis row and return a BaseEntry."""
        name: str = row["water"]
        region: Optional[str] = row.get("region")
        mus: List[str] = row.get("mu") or []

        criteria = MatchCriteria(name_verbatim=name, region=region, mus=mus)
        zone = _zone_from_region(region)
        refs, method = _natural_search(self._name_index, name, zone, mus)
        gnis_ids = _extract_gnis_ids(refs)
        return BaseEntry(criteria=criteria, gnis_ids=gnis_ids, link_method=method)

    def build_table(self, raw_rows: List[SynopsisRow]) -> List[BaseEntry]:
        """Process all synopsis rows; return one BaseEntry per row.

        Each entry represents exactly one synopsis row with the verbatim
        name from the PDF.  No synthetic base-name entries are created —
        parenthetical qualifier resolution is handled by OverrideEntry
        records in overrides.json (with name_variants for the stripped
        form so lookups by base name still resolve).
        """
        entries: List[BaseEntry] = []
        for row in raw_rows:
            entry = self.link_row(row)
            entries.append(entry)
        return entries


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def load_synopsis_rows(path: Path) -> List[SynopsisRow]:
    """Flatten synopsis_raw_data.json pages → individual row dicts."""
    with open(path, encoding="utf-8") as f:
        pages = json.load(f)
    rows: List[SynopsisRow] = []
    for page in pages:
        rows.extend(page.get("rows", []))
    return rows


def load_overrides(path: Optional[Path] = None) -> List[OverrideEntry]:
    """Load a JSON list of OverrideEntry dicts.

    Defaults to the canonical overrides.json shipped next to match_table.py.
    """
    from .match_table import OVERRIDES_PATH

    path = path or OVERRIDES_PATH
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return [OverrideEntry.from_dict(d) for d in data]


def save_table(entries: List[BaseEntry], path: Path) -> None:
    """Write the base entry table to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump([e.to_dict() for e in entries], f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(entries)} entries → {path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_summary(entries: List[BaseEntry], rows: List[SynopsisRow]) -> None:
    """Print a structured match summary to stdout."""
    total = len(entries)
    matched = sum(1 for e in entries if e.gnis_ids)
    not_found = total - matched
    pct = 100 * matched / total if total else 0.0

    # Count by link_method
    from collections import Counter

    method_counts: Counter = Counter(e.link_method for e in entries)

    # Collect unmatched names with their region for top-N display
    unmatched: Counter = Counter()
    for e in entries:
        if not e.gnis_ids:
            label = e.criteria.name_verbatim
            if e.criteria.region:
                label = f"{label}  ({e.criteria.region})"
            unmatched[label] += 1

    print()
    print("=" * 60)
    print("  MATCH SUMMARY")
    print("=" * 60)
    print(f"  Total rows   : {total:>6}")
    print(f"  Matched      : {matched:>6}  ({pct:.1f}%)")
    print(f"  Not found    : {not_found:>6}  ({100 - pct:.1f}%)")
    print()
    print("  By link method:")
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        print(f"    {method:<20} {count:>6}")
    if unmatched:
        print()
        print("  Top unmatched names (by frequency):")
        for label, count in unmatched.most_common(15):
            print(f"    [{count:>3}]  {label}")
    print("=" * 60)
    print()


def main() -> None:
    from project_config import get_config

    parser = argparse.ArgumentParser(description="Build BaseEntry table from synopsis.")
    parser.add_argument("--raw", help="Path to synopsis_raw_data.json")
    parser.add_argument("--graph", help="Path to FWA graph pickle")
    parser.add_argument("--gpkg", help="Path to bc_fisheries_data.gpkg")
    parser.add_argument("--out", help="Output JSON path")
    args = parser.parse_args()

    config = get_config()
    raw_path = Path(args.raw) if args.raw else config.synopsis_raw_data_path
    graph_path = Path(args.graph) if args.graph else config.fwa_graph_path
    gpkg_path = Path(args.gpkg) if args.gpkg else config.fwa_data_gpkg

    with open(config.project_root / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    out_path = (
        Path(args.out)
        if args.out
        else config.project_root / cfg["output"]["pipeline"]["match_table"]
    )

    print(f"Loading synopsis rows from: {raw_path}")
    rows = load_synopsis_rows(raw_path)
    print(f"  {len(rows)} rows")

    builder = BaseEntryBuilder(graph_path, gpkg_path)

    print("Linking (natural search only)...")
    entries = builder.build_table(rows)

    _print_summary(entries, rows)
    save_table(entries, out_path)


if __name__ == "__main__":
    main()
