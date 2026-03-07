"""
GeoArtifactGenerator — writes GPKG (fat) and PMTiles (lean) from the
CanonicalDataStore.

This module handles **only IO**: GeoJSONSeq serialisation, ogr2ogr
invocation, and tippecanoe invocation.  All geometry computation lives
in ``canonical_store.py``; all search-index logic lives in
``search_exporter.py``.

No Fallbacks:
    If ``ogr2ogr`` or ``tippecanoe`` is missing, a ``FileNotFoundError``
    is raised immediately — we never silently degrade.

Performance:
    Uses ``orjson`` for GeoJSONSeq serialisation (2-4× faster than
    ``json.dumps`` for coordinate-heavy features).
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from fwa_pipeline.metadata_builder import ADMIN_LAYER_CONFIG
from fwa_pipeline.metadata_gazetteer import FeatureType

from .canonical_store import (
    CanonicalDataStore,
    _ADMIN_ZOOM_LOOKUP,
)
from .geometry_utils import round_coords
from .logger_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# orjson — required dependency
# ---------------------------------------------------------------------------
try:
    import orjson
except ImportError as exc:
    raise ImportError(
        "orjson is required for GeoArtifactGenerator.  "
        "Install it with:  pip install orjson"
    ) from exc


# ---------------------------------------------------------------------------
# PMTiles column allow-lists (lean tiles — visuals only)
# ---------------------------------------------------------------------------

_PMTILES_COLUMNS: Dict[str, set] = {
    "streams": {
        "group_id",
        "frontend_group_id",
        "display_name",
        "waterbody_key",
        "blue_line_key",
        "stream_order",
        "tippecanoe:minzoom",
    },
    "lakes": {
        "group_id",
        "frontend_group_id",
        "display_name",
        "waterbody_key",
        "area_sqm",
        "tippecanoe:minzoom",
    },
    "wetlands": {
        "group_id",
        "frontend_group_id",
        "display_name",
        "waterbody_key",
        "area_sqm",
        "tippecanoe:minzoom",
    },
    "manmade": {
        "group_id",
        "frontend_group_id",
        "display_name",
        "waterbody_key",
        "area_sqm",
        "tippecanoe:minzoom",
    },
    "ungazetted": {
        "ungazetted_id",
        "group_id",
        "frontend_group_id",
        "display_name",
        "tippecanoe:minzoom",
    },
}


# ---------------------------------------------------------------------------
# GeoArtifactGenerator
# ---------------------------------------------------------------------------


class GeoArtifactGenerator:
    """Writes GPKG (debug/fat) and PMTiles (frontend/lean) files.

    Reads features exclusively from a ``CanonicalDataStore`` instance.
    No geometry helpers or scoring logic live here — only IO.
    """

    def __init__(self, store: CanonicalDataStore) -> None:
        self.store = store
        self._layer_cache: Dict[Any, Optional[gpd.GeoDataFrame]] = {}

    # ------------------------------------------------------------------
    # Canonical-feature layer helpers (GeoDataFrame wrappers)
    # ------------------------------------------------------------------

    def _get_cached_layer(
        self,
        cache_key: Any,
        filter_fn: Callable[[dict], bool],
    ) -> Optional[gpd.GeoDataFrame]:
        """Filter canonical features → GeoDataFrame, with caching."""
        if cache_key in self._layer_cache:
            return self._layer_cache[cache_key]

        features = [f for f in self.store.get_canonical_features() if filter_fn(f)]
        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None
        self._layer_cache[cache_key] = result
        return result

    def _create_streams_layer(
        self,
        exclude_lake_streams: bool = False,
    ) -> Optional[gpd.GeoDataFrame]:
        lake_wbkeys = (
            self.store.get_lake_manmade_wbkeys() if exclude_lake_streams else set()
        )
        return self._get_cached_layer(
            ("streams", exclude_lake_streams),
            lambda f: (
                f["feature_type"] == FeatureType.STREAM.value
                and not (
                    exclude_lake_streams
                    and f["waterbody_key"]
                    and f["waterbody_key"] in lake_wbkeys
                )
            ),
        )

    def _create_polygon_layer(
        self, ftype_enum: FeatureType
    ) -> Optional[gpd.GeoDataFrame]:
        return self._get_cached_layer(
            f"poly_{ftype_enum.value}",
            lambda f: f["feature_type"] == ftype_enum.value,
        )

    def _create_ungazetted_layer(self) -> Optional[gpd.GeoDataFrame]:
        return self._get_cached_layer(
            "ungazetted",
            lambda f: f["feature_type"] == FeatureType.UNGAZETTED.value,
        )

    # ------------------------------------------------------------------
    # Non-canonical layers (admin, regions, WMU, mask)
    # ------------------------------------------------------------------

    def _create_admin_layer(self, layer_key: str) -> Optional[gpd.GeoDataFrame]:
        """Create a layer for an admin boundary type (matched features only)."""
        cfg = ADMIN_LAYER_CONFIG.get(layer_key)
        if not cfg:
            return None

        matched_ids_map = self.store.admin_area_reg_map.get(layer_key, {})
        if not matched_ids_map:
            logger.debug(f"  No matched admin features for '{layer_key}', skipping")
            return None

        id_field = cfg["id_field"]
        name_field = cfg.get("name_field")
        code_field = cfg.get("code_field")
        code_map = cfg.get("code_map", {})

        gdf = self.store.get_admin_gdf(layer_key)
        if gdf is None:
            logger.warning(f"  Admin layer '{layer_key}' not available, skipping")
            return None
        matched_ids = set(matched_ids_map.keys())
        gdf = gdf[gdf[id_field].isin(matched_ids)].copy()
        if gdf.empty:
            logger.debug(f"  No geometry matches for '{layer_key}' after ID filter")
            return None

        gdf["regulation_ids"] = gdf[id_field].apply(
            lambda fid: ",".join(
                sorted(self.store.expand_admin_reg_ids(matched_ids_map.get(fid, set())))
            )
        )

        if name_field and name_field in gdf.columns:
            gdf["name"] = gdf[name_field]
        else:
            gdf["name"] = ""

        gdf["admin_id"] = gdf[id_field]
        out_cols = ["admin_id", "name", "regulation_ids"]

        if code_field and code_field in gdf.columns:
            gdf["admin_type"] = gdf[code_field].map(code_map).fillna(gdf[code_field])
        else:
            gdf["admin_type"] = layer_key
        out_cols.append("admin_type")

        gdf["tippecanoe:minzoom"] = gdf.geometry.area.apply(
            lambda a: CanonicalDataStore._calculate_minzoom(a, _ADMIN_ZOOM_LOOKUP)
        )
        out_cols.extend(["tippecanoe:minzoom", "geometry"])

        logger.info(f"  Admin layer '{layer_key}': {len(gdf)} features")
        return gdf[out_cols]

    def _create_regions_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build region boundary layer from the 'wmu' GPKG layer."""
        if "wmu" not in self.store.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping regions")
            return None
        zones_gdf = self.store.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        zones_gdf["zone"] = zones_gdf["REGION_RESPONSIBLE_ID"]
        zones_gdf["region_name"] = zones_gdf["REGION_RESPONSIBLE_NAME"]
        regions_gdf = zones_gdf.dissolve(by="zone", as_index=False, aggfunc="first")
        regions_gdf["geometry"] = regions_gdf["geometry"].boundary
        regions_gdf["stroke_color"] = "#555555"
        regions_gdf["stroke_width"] = 2.5
        regions_gdf["stroke_dasharray"] = "3,3"
        regions_gdf["tippecanoe:minzoom"] = 0
        return regions_gdf[
            [
                "zone",
                "region_name",
                "stroke_color",
                "stroke_width",
                "stroke_dasharray",
                "tippecanoe:minzoom",
                "geometry",
            ]
        ]

    def _create_management_units_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build management-unit boundary layer."""
        if "wmu" not in self.store.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping management_units")
            return None
        mu_gdf = self.store.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        mu_gdf["mu_code"] = mu_gdf["WILDLIFE_MGMT_UNIT_ID"]
        mu_gdf["region_name"] = mu_gdf["REGION_RESPONSIBLE_NAME"]
        mu_gdf["zone"] = mu_gdf["REGION_RESPONSIBLE_ID"]
        mu_gdf["geometry"] = mu_gdf["geometry"].boundary
        mu_gdf["stroke_color"] = "#888888"
        mu_gdf["stroke_width"] = 1.0
        mu_gdf["tippecanoe:minzoom"] = 4
        return mu_gdf[
            [
                "mu_code",
                "zone",
                "region_name",
                "stroke_color",
                "stroke_width",
                "tippecanoe:minzoom",
                "geometry",
            ]
        ]

    def _create_management_units_fill_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build lightweight polygon fill layer for centroid labels."""
        if "wmu" not in self.store.data_accessor.list_layers():
            logger.warning(
                "'wmu' layer not found in GPKG — skipping management_units_fill"
            )
            return None
        mu_gdf = self.store.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        mu_gdf["mu_code"] = mu_gdf["WILDLIFE_MGMT_UNIT_ID"]
        mu_gdf["tippecanoe:minzoom"] = 4
        return mu_gdf[["mu_code", "tippecanoe:minzoom", "geometry"]]

    def _create_bc_mask_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Create an outside-BC grey mask polygon."""
        if "wmu" not in self.store.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping bc_mask")
            return None
        zones_gdf = self.store.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        bc_union = zones_gdf.geometry.union_all()
        if bc_union.is_empty:
            logger.warning("BC zone union is empty — skipping bc_mask")
            return None

        minx, miny, maxx, maxy = bc_union.bounds
        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2
        half_size = max(maxx - minx, maxy - miny) / 2 + 1_000_000
        outer_box = box(
            center_x - half_size,
            center_y - half_size,
            center_x + half_size,
            center_y + half_size,
        )
        mask_polygon = outer_box.difference(bc_union)
        mask_polygon = mask_polygon.simplify(100, preserve_topology=True)

        mask_gdf = gpd.GeoDataFrame(
            {
                "fill_color": ["#374151"],
                "fill_opacity": [0.65],
                "tippecanoe:minzoom": [0],
                "geometry": [mask_polygon],
            },
            crs="EPSG:3005",
        )
        logger.debug(
            f"  BC mask layer: 1 feature covering area outside {len(zones_gdf)} zones"
        )
        return mask_gdf

    # ------------------------------------------------------------------
    # Layer config
    # ------------------------------------------------------------------

    def _get_layer_configs(
        self,
        exclude_lake_streams: bool = False,
        include_regions: bool = True,
    ) -> List[Tuple[str, Callable]]:
        """Return (layer_name, creator_fn) pairs for GPKG/PMTiles exports."""
        layers: List[Tuple[str, Callable]] = [
            ("lakes", lambda: self._create_polygon_layer(FeatureType.LAKE)),
            ("wetlands", lambda: self._create_polygon_layer(FeatureType.WETLAND)),
            ("manmade", lambda: self._create_polygon_layer(FeatureType.MANMADE)),
            (
                "streams",
                lambda: self._create_streams_layer(
                    exclude_lake_streams=exclude_lake_streams
                ),
            ),
        ]
        if include_regions:
            layers.extend(
                [
                    ("regions", lambda: self._create_regions_layer()),
                    ("management_units", lambda: self._create_management_units_layer()),
                    (
                        "management_units_fill",
                        lambda: self._create_management_units_fill_layer(),
                    ),
                    ("bc_mask", lambda: self._create_bc_mask_layer()),
                ]
            )

        layers.append(("ungazetted", lambda: self._create_ungazetted_layer()))

        for layer_key in ADMIN_LAYER_CONFIG:
            layers.append(
                (
                    f"admin_{layer_key}",
                    lambda lk=layer_key: self._create_admin_layer(lk),
                )
            )
        return layers

    # ------------------------------------------------------------------
    # File-lock check
    # ------------------------------------------------------------------

    @staticmethod
    def _is_file_locked(filepath: Path) -> bool:
        if not filepath.exists():
            return False
        try:
            with open(filepath, "a"):
                pass
            return False
        except PermissionError:
            logger.error(f"File {filepath.name} is locked. Skipping export.")
            return True

    # ------------------------------------------------------------------
    # GPKG export (FAT — all columns)
    # ------------------------------------------------------------------

    def export_gpkg(self, output_path: Path) -> Optional[Path]:
        """Export all layers to GPKG with full metadata for debugging."""
        if self._is_file_locked(output_path):
            return None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()

        layer_count = 0
        for name, create_fn in self._get_layer_configs(exclude_lake_streams=False):
            if (gdf := create_fn()) is not None and not gdf.empty:
                gdf.to_file(output_path, layer=name, driver="GPKG")
                layer_count += 1

        if not layer_count:
            return None
        logger.info(
            f"Created GPKG {output_path} "
            f"({output_path.stat().st_size / 1048576:.1f} MB)"
        )
        return output_path

    # ------------------------------------------------------------------
    # PMTiles export (LEAN — stripped columns, orjson serialisation)
    # ------------------------------------------------------------------

    def export_pmtiles(
        self,
        output_path: Path,
        work_dir: Optional[Path] = None,
    ) -> Optional[Path]:
        """Export lean PMTiles for frontend map rendering.

        Raises ``FileNotFoundError`` if ``tippecanoe`` is not on ``$PATH``.
        """
        # Fail-fast: check tippecanoe availability
        if shutil.which("tippecanoe") is None:
            raise FileNotFoundError(
                "tippecanoe not found on $PATH.  "
                "Install it: https://github.com/felt/tippecanoe"
            )

        if self._is_file_locked(output_path):
            return None
        work_dir = work_dir or output_path.parent / "temp"
        work_dir.mkdir(parents=True, exist_ok=True)

        layer_files: list = []
        for name, create_fn in self._get_layer_configs(exclude_lake_streams=True):
            if (gdf := create_fn()) is not None and not gdf.empty:
                # Strip non-visual columns for lean tiles
                keep = _PMTILES_COLUMNS.get(name)
                if keep:
                    drop_cols = [
                        c for c in gdf.columns if c not in keep and c != "geometry"
                    ]
                    gdf = gdf.drop(columns=drop_cols, errors="ignore")

                layer_path = work_dir / f"{name}.geojsonseq"
                with open(layer_path, "wb") as f:
                    for _, row in gdf.to_crs("EPSG:4326").iterrows():
                        props = {
                            k: v for k, v in row.drop("geometry").items() if pd.notna(v)
                        }
                        record = {
                            "type": "Feature",
                            "properties": props,
                            "geometry": round_coords(row["geometry"].__geo_interface__),
                            "tippecanoe": {
                                "layer": name,
                                "minzoom": int(row["tippecanoe:minzoom"]),
                            },
                        }
                        f.write(orjson.dumps(record) + b"\n")
                layer_files.append(layer_path)

        if not layer_files:
            return None

        cmd = [
            "tippecanoe",
            "-o",
            str(output_path),
            "--force",
            "--hilbert",
            "--minimum-zoom=4",
            "--maximum-zoom=12",
            "--simplification=8",
            "--simplification-at-maximum-zoom=1",
            "--read-parallel",
            "--detect-shared-borders",
            "--generate-ids",
            "--buffer=10",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--no-simplification-of-shared-nodes",
            "--maximum-tile-bytes=2500000",
        ] + [arg for lp in layer_files for arg in ("-L", f"{lp.stem}:{lp}")]

        result = subprocess.run(cmd, text=True)
        if result.returncode == 0 and output_path.exists():
            logger.info(
                f"Created PMTiles {output_path} "
                f"({output_path.stat().st_size / 1048576:.1f} MB)"
            )
            return output_path

        raise RuntimeError(
            f"tippecanoe failed (rc={result.returncode}).  " f"Command: {' '.join(cmd)}"
        )
