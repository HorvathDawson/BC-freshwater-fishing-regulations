"""
FreshWaterAtlas — regulation-free spatial index of every BC water feature.

Reads the FWA graph pickle and the BC fisheries GeoPackage.
Produces stable, indexed feature records suitable for immutable PMTiles.

No regulation data touches this class.  Downstream consumers (tile exporter,
future regulation resolver) read from this atlas — never from raw geometry
files directly.

Usage
-----
    # Build from scratch:
    atlas = FreshWaterAtlas(graph_path, gpkg_path)

    # Cache for later:
    atlas.save(Path("output/pipeline/atlas/atlas.pkl"))

    # Reload without rebuilding:
    atlas = FreshWaterAtlas.load(Path("output/pipeline/atlas/atlas.pkl"))
"""

from __future__ import annotations

import hashlib
import logging
import pickle
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
from shapely.geometry import LineString
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.prepared import prep
from tqdm import tqdm

from data.data_extractor import FWADataAccessor
from .geometry_utils import merge_overlapping_polygons

from .models import AdminRecord, PolygonRecord, StreamRecord, trim_wsc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Minzoom constants (mirrored from canonical_store — single source of truth
# for the V2 pipeline; canonical_store owns V1).
# ---------------------------------------------------------------------------

WEIGHTS = {
    "order": 0.0,
    "magnitude": 1.0,
    "length_km": 0.0,
    "has_name": 0.0,
    "side_channel_penalty": 0.0,
}

PERCENTILES = {5: 100.0, 6: 99.99, 7: 99.97, 8: 99.0, 10: 95.0, 11: 0.0}

LAKE_ZOOM_THRESHOLDS: List[Tuple[float, int]] = sorted(
    [
        (100_000_000, 5),
        (25_000_000, 6),
        (1_000_000, 7),
        (1_000_000, 8),
        (10_000, 9),
        (10_000, 10),
        (5_000, 11),
        (0, 12),
    ],
    reverse=True,
)

ADMIN_ZOOM_THRESHOLDS: List[Tuple[float, int]] = sorted(
    [
        (500_000_000, 5),
        (50_000_000, 6),
        (5_000_000, 7),
        (1_000_000, 8),
        (100_000, 9),
        (10_000, 10),
        (5_000, 11),
        (0, 12),
    ],
    reverse=True,
)

ADMIN_ZOOM_THRESHOLDS_AGGRESSIVE: List[Tuple[float, int]] = sorted(
    [
        (1_500_000_000, 5),
        (200_000_000, 6),
        (20_000_000, 7),
        (5_000_000, 8),
        (500_000, 9),
        (50_000, 10),
        (10_000, 11),
        (0, 12),
    ],
    reverse=True,
)

MAIN_FLOW_CODES = {1000, 1050, 1200, 1250, 1410, 1450}

# Atlas pickle version — bump when the schema changes.
_ATLAS_VERSION = 8


# ---------------------------------------------------------------------------
# FreshWaterAtlas
# ---------------------------------------------------------------------------


class FreshWaterAtlas:
    """Immutable spatial index of BC water features.

    Reads graph pickle + GPKG.  Produces stable, regulation-free
    feature records with permanent fid's for each atomic FWA segment.

    Build pipeline (runs once in __init__):
        1. Load tidal boundary polygon
        2. Load polygon layers (lakes, wetlands, manmade)
        3. Load admin layers (parks_nat, eco_reserves from parks_bc)
        4. Collect lake + manmade waterbody_keys
        5. Load streams from graph → classify under-lake → exclude tidal
        6. Assign minzooms (BLK/magnitude for streams, area for polygons)
    """

    def __init__(self, graph_path: Path, gpkg_path: Path) -> None:
        self.graph_path = Path(graph_path)
        self.gpkg_path = Path(gpkg_path)

        # Public collections — populated by _build()
        self.streams: Dict[str, StreamRecord] = {}
        self.under_lake_streams: Dict[str, StreamRecord] = {}
        self.lakes: Dict[str, PolygonRecord] = {}
        self.wetlands: Dict[str, PolygonRecord] = {}
        self.manmade: Dict[str, PolygonRecord] = {}
        self.parks_nat: Dict[str, AdminRecord] = {}
        self.eco_reserves: Dict[str, AdminRecord] = {}
        self.wma: Dict[str, AdminRecord] = {}
        self.historic_sites: Dict[str, AdminRecord] = {}
        self.watersheds: Dict[str, AdminRecord] = {}
        self.wmu: Dict[str, AdminRecord] = {}
        self.osm_admin: Dict[str, AdminRecord] = {}
        self.aboriginal_lands: Dict[str, AdminRecord] = {}
        self.tidal_boundary: Optional[BaseGeometry] = None
        self.poly_id_to_wbk: Dict[str, str] = {}

        self._build()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: Path) -> None:
        """Serialize the built atlas to disk."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": _ATLAS_VERSION,
            "graph_path": str(self.graph_path),
            "gpkg_path": str(self.gpkg_path),
            "streams": self.streams,
            "under_lake_streams": self.under_lake_streams,
            "lakes": self.lakes,
            "wetlands": self.wetlands,
            "manmade": self.manmade,
            "parks_nat": self.parks_nat,
            "eco_reserves": self.eco_reserves,
            "wma": self.wma,
            "historic_sites": self.historic_sites,
            "watersheds": self.watersheds,
            "wmu": self.wmu,
            "osm_admin": self.osm_admin,
            "aboriginal_lands": self.aboriginal_lands,
            "tidal_boundary": self.tidal_boundary,
            "poly_id_to_wbk": self.poly_id_to_wbk,
        }
        with open(path, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
        total = (
            len(self.streams)
            + len(self.under_lake_streams)
            + len(self.lakes)
            + len(self.wetlands)
            + len(self.manmade)
            + len(self.parks_nat)
            + len(self.eco_reserves)
            + len(self.wma)
            + len(self.historic_sites)
            + len(self.watersheds)
            + len(self.wmu)
            + len(self.osm_admin)
            + len(self.aboriginal_lands)
        )
        logger.info(f"Atlas saved → {path}  ({total:,} features)")

    @classmethod
    def load(cls, path: Path) -> "FreshWaterAtlas":
        """Deserialize a cached atlas from disk.

        Raises ValueError if the pickle was written by an incompatible version.
        """
        with open(path, "rb") as f:
            payload = pickle.load(f)

        version = payload.get("version")
        if version != _ATLAS_VERSION:
            raise ValueError(
                f"Atlas pickle version mismatch: expected {_ATLAS_VERSION}, "
                f"got {version}.  Rebuild with FreshWaterAtlas(graph, gpkg)."
            )

        obj = cls.__new__(cls)
        obj.graph_path = Path(payload["graph_path"])
        obj.gpkg_path = Path(payload["gpkg_path"])
        obj.streams = payload["streams"]
        obj.under_lake_streams = payload["under_lake_streams"]
        obj.lakes = payload["lakes"]
        obj.wetlands = payload["wetlands"]
        obj.manmade = payload["manmade"]
        obj.parks_nat = payload["parks_nat"]
        obj.eco_reserves = payload["eco_reserves"]
        obj.wma = payload.get("wma", {})
        obj.historic_sites = payload.get("historic_sites", {})
        obj.watersheds = payload.get("watersheds", {})
        obj.wmu = payload.get("wmu", {})
        obj.osm_admin = payload.get("osm_admin", {})
        obj.aboriginal_lands = payload.get("aboriginal_lands", {})
        obj.tidal_boundary = payload["tidal_boundary"]
        obj.poly_id_to_wbk = payload.get("poly_id_to_wbk", {})

        total = (
            len(obj.streams)
            + len(obj.under_lake_streams)
            + len(obj.lakes)
            + len(obj.wetlands)
            + len(obj.manmade)
            + len(obj.parks_nat)
            + len(obj.eco_reserves)
            + len(obj.wma)
            + len(obj.historic_sites)
            + len(obj.watersheds)
            + len(obj.wmu)
            + len(obj.osm_admin)
            + len(obj.aboriginal_lands)
        )
        logger.info(f"Atlas loaded ← {path}  ({total:,} features)")
        return obj

    # ------------------------------------------------------------------
    # Build pipeline
    # ------------------------------------------------------------------

    def _build(self) -> None:
        """Run the full build pipeline."""
        accessor = FWADataAccessor(self.gpkg_path)
        available = set(accessor.list_layers())

        self._load_tidal_boundary(accessor, available)
        self._load_polygons(accessor, available)
        self._load_admin_layers(accessor, available)

        lake_manmade_wbkeys = self._collect_lake_manmade_wbkeys()
        self._load_streams(accessor, lake_manmade_wbkeys)
        self._assign_stream_minzooms()

        total = (
            len(self.streams)
            + len(self.under_lake_streams)
            + len(self.lakes)
            + len(self.wetlands)
            + len(self.manmade)
            + len(self.parks_nat)
            + len(self.eco_reserves)
            + len(self.wma)
            + len(self.historic_sites)
            + len(self.watersheds)
            + len(self.wmu)
            + len(self.osm_admin)
            + len(self.aboriginal_lands)
        )
        logger.info(f"Atlas built: {total:,} total features")

    # ------------------------------------------------------------------
    # Step 1: Tidal boundary
    # ------------------------------------------------------------------

    def _load_tidal_boundary(
        self, accessor: FWADataAccessor, available: Set[str]
    ) -> None:
        if "tidal_boundary" not in available:
            raise FileNotFoundError(
                "'tidal_boundary' layer not found in GPKG. "
                "Run data fetch first: python -m data.fetch_data --layers tidal_boundary"
            )
        gdf = accessor.get_layer("tidal_boundary")
        if hasattr(gdf, "to_crs"):
            gdf = gdf.to_crs(epsg=3005)
        if gdf.empty:
            return
        self.tidal_boundary = unary_union(gdf.geometry.tolist())
        logger.info(f"Tidal boundary loaded ({len(gdf)} polygon(s))")

    # ------------------------------------------------------------------
    # Step 2: Polygon layers (lakes, wetlands, manmade)
    # ------------------------------------------------------------------

    def _load_polygons(self, accessor: FWADataAccessor, available: Set[str]) -> None:
        layer_map = {
            "lakes": "lakes",
            "wetlands": "wetlands",
            "manmade_water": "manmade",
        }
        for gpkg_layer, attr_name in layer_map.items():
            if gpkg_layer not in available:
                raise FileNotFoundError(
                    f"Layer '{gpkg_layer}' not found in GPKG. "
                    f"Run data fetch first: python -m data.fetch_data --layers {gpkg_layer}"
                )
            gdf = accessor.get_layer(gpkg_layer)
            if gdf.empty:
                continue

            records: Dict[str, PolygonRecord] = {}
            # Group by waterbody_key, union multi-part geometries
            groups: Dict[str, dict] = {}
            for _, row in tqdm(
                gdf.iterrows(), total=len(gdf), desc=f"  {attr_name}", leave=False
            ):
                wbk = str(row.get("WATERBODY_KEY") or "")
                if not wbk:
                    raise ValueError(
                        f"Row in '{gpkg_layer}' has no WATERBODY_KEY: "
                        f"{dict(row.drop('geometry', errors='ignore'))}"
                    )
                # Build poly_id → waterbody_key index
                poly_id = str(row.get("WATERBODY_POLY_ID") or "")
                if poly_id:
                    self.poly_id_to_wbk[poly_id] = wbk
                grp = groups.setdefault(wbk, {"name": "", "gnis_id": "", "geoms": []})
                if not grp["name"]:
                    grp["name"] = row.get("GNIS_NAME_1") or row.get("GNIS_NAME_2") or ""
                if not grp["gnis_id"]:
                    grp["gnis_id"] = str(row.get("GNIS_ID_1") or "")
                grp["geoms"].append(row.geometry)

            for wbk, grp in groups.items():
                geom = unary_union(grp["geoms"])
                area = geom.area
                minzoom = _area_minzoom(area, LAKE_ZOOM_THRESHOLDS)
                records[wbk] = PolygonRecord(
                    waterbody_key=wbk,
                    geometry=geom,
                    display_name=grp["name"],
                    area=area,
                    gnis_id=grp["gnis_id"],
                    minzoom=minzoom,
                )

            setattr(self, attr_name, records)
            logger.info(f"Loaded {len(records):,} {attr_name} polygons")

        logger.info(f"Built poly_id_to_wbk index: {len(self.poly_id_to_wbk):,} entries")

    # ------------------------------------------------------------------
    # Step 3: Admin layers (parks_nat, eco_reserves)
    # ------------------------------------------------------------------

    def _load_admin_layers(
        self, accessor: FWADataAccessor, available: Set[str]
    ) -> None:
        # National parks
        if "parks_nat" in available:
            gdf = accessor.get_layer("parks_nat")
            for _, row in tqdm(
                gdf.iterrows(), total=len(gdf), desc="  parks_nat", leave=False
            ):
                aid = str(row.get("NATIONAL_PARK_ID") or "")
                if not aid:
                    raise ValueError(
                        f"Row in 'parks_nat' has no NATIONAL_PARK_ID: "
                        f"{dict(row.drop('geometry', errors='ignore'))}"
                    )
                geom = row.geometry
                area = geom.area
                self.parks_nat[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("ENGLISH_NAME") or "",
                    admin_type="parks_nat",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.parks_nat):,} national parks")
        else:
            raise FileNotFoundError(
                "'parks_nat' layer not found in GPKG. "
                "Run data fetch first: python -m data.fetch_data --layers parks_nat"
            )

        # Provincial parks & eco reserves — classified by PROTECTED_LANDS_CODE.
        # OI = Ecological Reserve, PP = Provincial Park,
        # PA = Protected Area, RC = Recreation Area.
        # All go into self.eco_reserves (single tile layer) but with distinct
        # admin_type so styles can colour them differently.
        _PARKS_BC_CODE_MAP = {
            "OI": "ECOLOGICAL_RESERVE",
            "PA": "PROTECTED_AREA",
            "PP": "PROVINCIAL_PARK",
            "RC": "RECREATION_AREA",
        }
        if "parks_bc" in available:
            gdf = accessor.get_layer("parks_bc")
            for _, row in tqdm(
                gdf.iterrows(),
                total=len(gdf),
                desc="  parks_bc",
                leave=False,
            ):
                aid = str(row.get("ADMIN_AREA_SID") or "")
                if not aid:
                    raise ValueError(
                        f"Row in 'parks_bc' has no ADMIN_AREA_SID: "
                        f"{dict(row.drop('geometry', errors='ignore'))}"
                    )
                code = str(row.get("PROTECTED_LANDS_CODE") or "")
                admin_type = _PARKS_BC_CODE_MAP.get(code, "PROVINCIAL_PARK")
                geom = row.geometry
                area = geom.area
                self.eco_reserves[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("PROTECTED_LANDS_NAME") or "",
                    admin_type=admin_type,
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS_AGGRESSIVE),
                )
            eco_count = sum(
                1
                for r in self.eco_reserves.values()
                if r.admin_type == "ECOLOGICAL_RESERVE"
            )
            logger.info(
                f"Loaded {len(self.eco_reserves):,} parks_bc "
                f"({eco_count} eco reserves, {len(self.eco_reserves) - eco_count} other parks)"
            )
        else:
            raise FileNotFoundError(
                "'parks_bc' layer not found in GPKG. "
                "Run data fetch first: python -m data.fetch_data --layers parks_bc"
            )

        # Wildlife Management Areas
        if "wma" in available:
            gdf = accessor.get_layer("wma")
            for _, row in tqdm(
                gdf.iterrows(), total=len(gdf), desc="  wma", leave=False
            ):
                aid = str(row.get("ADMIN_AREA_SID") or "")
                if not aid:
                    raise ValueError(
                        f"Row in 'wma' has no ADMIN_AREA_SID: "
                        f"{dict(row.drop('geometry', errors='ignore'))}"
                    )
                geom = row.geometry
                area = geom.area
                self.wma[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("WILDLIFE_MANAGEMENT_AREA_NAME") or "",
                    admin_type="wma",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.wma):,} wildlife management areas")
        else:
            logger.warning("'wma' layer not found in GPKG — skipping")

        # Historic sites
        if "historic_sites" in available:
            gdf = accessor.get_layer("historic_sites")
            for _, row in tqdm(
                gdf.iterrows(), total=len(gdf), desc="  historic_sites", leave=False
            ):
                aid = str(row.get("SITE_ID") or "")
                if not aid:
                    continue  # many historic sites lack polygon relevance
                geom = row.geometry
                area = geom.area
                self.historic_sites[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("COMMON_SITE_NAME") or "",
                    admin_type="historic_sites",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.historic_sites):,} historic sites")
        else:
            logger.warning("'historic_sites' layer not found in GPKG — skipping")

        # Named watersheds
        if "watersheds" in available:
            gdf = accessor.get_layer("watersheds")
            for _, row in tqdm(
                gdf.iterrows(), total=len(gdf), desc="  watersheds", leave=False
            ):
                aid = str(row.get("NAMED_WATERSHED_ID") or "")
                if not aid:
                    continue
                geom = row.geometry
                area = geom.area
                self.watersheds[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("GNIS_NAME") or "",
                    admin_type="watersheds",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.watersheds):,} named watersheds")
        else:
            logger.warning("'watersheds' layer not found in GPKG — skipping")

        # Wildlife Management Units (zones/MUs)
        if "wmu" in available:
            gdf = accessor.get_layer("wmu")
            for _, row in tqdm(
                gdf.iterrows(), total=len(gdf), desc="  wmu", leave=False
            ):
                aid = str(row.get("WILDLIFE_MGMT_UNIT_ID") or "")
                if not aid:
                    continue
                geom = row.geometry
                area = geom.area
                self.wmu[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("GAME_MANAGEMENT_ZONE_NAME") or aid,
                    admin_type="wmu",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.wmu):,} wildlife management units")
        else:
            logger.warning("'wmu' layer not found in GPKG — skipping")

        # Cache BC boundary (WMU union) for stream filtering and admin clipping
        if self.wmu:
            self._bc_boundary = unary_union([r.geometry for r in self.wmu.values()])
            if not self._bc_boundary.is_valid:
                self._bc_boundary = self._bc_boundary.buffer(0)
            logger.info("Cached BC boundary from WMU union")
        else:
            self._bc_boundary = None

        # OSM admin boundaries (research forests, protected areas, etc.)
        if "osm_admin_boundaries" in available:
            gdf = accessor.get_layer("osm_admin_boundaries")
            for _, row in tqdm(
                gdf.iterrows(),
                total=len(gdf),
                desc="  osm_admin",
                leave=False,
            ):
                aid = str(row.get("osm_id") or "")
                if not aid:
                    continue
                geom = row.geometry
                area = geom.area
                self.osm_admin[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("name") or "",
                    admin_type="osm_admin",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.osm_admin):,} OSM admin boundaries")
        else:
            logger.warning("'osm_admin_boundaries' layer not found in GPKG — skipping")

        # Aboriginal lands (Indigenous territories from OSM)
        if "aboriginal_lands" in available:
            gdf = accessor.get_layer("aboriginal_lands")
            gdf = merge_overlapping_polygons(gdf, "osm_id", "name")

            # Clip to BC boundary (cached WMU union) to remove portions outside BC
            bc_boundary = self._bc_boundary
            if bc_boundary is not None:
                logger.info("  Clipping aboriginal lands to BC boundary")

            for _, row in tqdm(
                gdf.iterrows(),
                total=len(gdf),
                desc="  aboriginal_lands",
                leave=False,
            ):
                aid = str(row.get("osm_id") or "")
                if not aid:
                    continue
                geom = row.geometry
                if bc_boundary is not None:
                    geom = geom.intersection(bc_boundary)
                    if geom.is_empty:
                        continue
                area = geom.area
                self.aboriginal_lands[aid] = AdminRecord(
                    admin_id=aid,
                    geometry=geom,
                    display_name=row.get("name") or "",
                    admin_type="aboriginal_lands",
                    area=area,
                    minzoom=_area_minzoom(area, ADMIN_ZOOM_THRESHOLDS),
                )
            logger.info(f"Loaded {len(self.aboriginal_lands):,} aboriginal lands")
        else:
            logger.warning("'aboriginal_lands' layer not found in GPKG — skipping")

    # ------------------------------------------------------------------
    # Step 4: Collect lake + manmade waterbody keys
    # ------------------------------------------------------------------

    def _collect_lake_manmade_wbkeys(self) -> Set[str]:
        keys: Set[str] = set()
        keys.update(self.lakes.keys())
        keys.update(self.manmade.keys())
        logger.info(f"Lake + manmade waterbody keys: {len(keys):,}")
        return keys

    # ------------------------------------------------------------------
    # Step 5: Load streams from graph pickle
    # ------------------------------------------------------------------

    def _load_streams(
        self, accessor: "FWADataAccessor", lake_manmade_wbkeys: Set[str]
    ) -> None:
        """Load every edge from the graph pickle as a StreamRecord.

        Geometry is sourced from the GPKG "streams" layer (full polylines)
        rather than the graph pickle's 2-point node coordinates.

        Classification:
            - waterbody_key in lake_manmade_wbkeys → under_lake_streams
            - geometry fully within tidal boundary → excluded
            - everything else → streams
        """
        with open(self.graph_path, "rb") as f:
            dump = pickle.load(f)

        edge_attrs = dump["edge_attrs"]
        node_coords = dump["node_coords"]
        logger.info(f"Graph loaded: {len(edge_attrs):,} edges")

        # Load actual polyline geometries from GPKG, keyed by LINEAR_FEATURE_ID
        logger.info("Loading stream geometries from GPKG …")
        streams_gdf = accessor.get_layer(
            "streams", columns=["LINEAR_FEATURE_ID", "geometry"]
        )
        gpkg_geom: Dict[str, BaseGeometry] = {}
        for row in streams_gdf.itertuples():
            lid = str(row.LINEAR_FEATURE_ID)
            if lid and row.geometry is not None and not row.geometry.is_empty:
                gpkg_geom[lid] = row.geometry
        logger.info(f"GPKG stream geometries indexed: {len(gpkg_geom):,}")
        del streams_gdf  # free memory

        tidal_prep = prep(self.tidal_boundary) if self.tidal_boundary else None
        bc_prep = prep(self._bc_boundary) if self._bc_boundary else None

        skipped = 0
        tidal_excluded = 0
        bc_excluded = 0
        geom_fallback = 0
        for key, attrs in tqdm(edge_attrs.items(), desc="  streams", leave=False):
            fid = str(attrs.get("linear_feature_id") or key)

            # Prefer full polyline from GPKG; fall back to 2-point line
            geom = gpkg_geom.get(fid)
            if geom is None:
                try:
                    u = node_coords[attrs["source"]]
                    v = node_coords[attrs["target"]]
                except KeyError:
                    skipped += 1
                    continue
                geom = LineString([u, v])
                geom_fallback += 1

            # Exclude streams fully inside tidal boundary
            if tidal_prep is not None and tidal_prep.contains(geom):
                tidal_excluded += 1
                continue

            # Exclude streams fully outside BC (e.g. cross-border into US)
            if bc_prep is not None and not bc_prep.intersects(geom):
                bc_excluded += 1
                continue

            wbk = str(attrs.get("waterbody_key") or "")
            display_name = attrs.get("gnis_name") or ""
            # Inherit name from graph's upstream BFS when gnis_name is empty
            if not display_name:
                inherited = attrs.get("inherited_gnis_names")
                if inherited and len(inherited) == 1:
                    display_name = inherited[0].get("gnis_name", "")
            blk = str(attrs.get("blue_line_key") or "")
            stream_order = attrs.get("stream_order")
            stream_magnitude = attrs.get("stream_magnitude")
            fwa_wsc = trim_wsc(str(attrs.get("fwa_watershed_code") or ""))

            record = StreamRecord(
                fid=fid,
                geometry=geom,
                display_name=display_name,
                blk=blk,
                stream_order=stream_order,
                stream_magnitude=stream_magnitude,
                waterbody_key=wbk,
                fwa_watershed_code=fwa_wsc,
            )

            if wbk and wbk in lake_manmade_wbkeys:
                self.under_lake_streams[fid] = record
            else:
                self.streams[fid] = record

        del gpkg_geom  # free memory
        logger.info(
            f"Streams: {len(self.streams):,} main, "
            f"{len(self.under_lake_streams):,} under-lake, "
            f"{tidal_excluded:,} tidal-excluded, "
            f"{bc_excluded:,} outside-BC-excluded, "
            f"{skipped:,} skipped (missing coords), "
            f"{geom_fallback:,} geometry fallbacks (2-point)"
        )

    # ------------------------------------------------------------------
    # Step 6: Assign stream minzooms (BLK grouping + magnitude percentiles)
    # ------------------------------------------------------------------

    def _assign_stream_minzooms(self) -> None:
        """Compute BLK-grouped magnitude percentiles, then stamp each stream."""
        # Gather BLK stats across ALL streams (main + under-lake)
        all_streams = {**self.streams, **self.under_lake_streams}
        blk_stats = _compute_blk_stats(all_streams)

        if not blk_stats:
            logger.warning("No BLK stats — all streams get default minzoom 12")
            return

        # Compute percentile thresholds
        scores = np.array([s["max_magnitude"] for s in blk_stats.values()])
        thresholds: List[Tuple[float, int]] = []
        for zoom in sorted(PERCENTILES.keys()):
            pct = PERCENTILES[zoom]
            thresholds.append((np.percentile(scores, pct), zoom))

        # Build BLK → minzoom lookup
        blk_zooms: Dict[str, int] = {}
        for blk, stats in blk_stats.items():
            blk_zooms[blk] = _threshold_minzoom(stats["max_magnitude"], thresholds)

        # Stamp streams (frozen dataclass → rebuild with new minzoom)
        def _stamp(records: Dict[str, StreamRecord]) -> Dict[str, StreamRecord]:
            stamped: Dict[str, StreamRecord] = {}
            for fid, rec in records.items():
                mz = blk_zooms.get(rec.blk, 12)
                stamped[fid] = StreamRecord(
                    fid=rec.fid,
                    geometry=rec.geometry,
                    display_name=rec.display_name,
                    blk=rec.blk,
                    stream_order=rec.stream_order,
                    stream_magnitude=rec.stream_magnitude,
                    waterbody_key=rec.waterbody_key,
                    fwa_watershed_code=rec.fwa_watershed_code,
                    minzoom=mz,
                )
            return stamped

        self.streams = _stamp(self.streams)
        self.under_lake_streams = _stamp(self.under_lake_streams)
        logger.info(
            f"Minzooms assigned: {len(blk_zooms):,} BLK groups, "
            f"zoom range {min(blk_zooms.values())}–{max(blk_zooms.values())}"
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _compute_blk_stats(streams: Dict[str, StreamRecord]) -> Dict[str, dict]:
    """Group streams by BLK, compute max magnitude per group."""
    stats: Dict[str, dict] = defaultdict(lambda: {"max_magnitude": 0, "max_order": 0})
    for rec in streams.values():
        if not rec.blk:
            continue
        s = stats[rec.blk]
        s["max_magnitude"] = max(s["max_magnitude"], rec.stream_magnitude or 0)
        s["max_order"] = max(s["max_order"], rec.stream_order or 0)
    return dict(stats)


def _threshold_minzoom(
    value: float, thresholds: List[Tuple[float, int]], default: int = 12
) -> int:
    """Return the minimum zoom level for a value using threshold lookup."""
    return next(
        (zoom for threshold, zoom in thresholds if value >= threshold),
        default,
    )


def _area_minzoom(area: float, thresholds: List[Tuple[float, int]]) -> int:
    """Return minzoom for a polygon based on its area."""
    return next(
        (zoom for min_area, zoom in thresholds if area >= min_area),
        12,
    )
