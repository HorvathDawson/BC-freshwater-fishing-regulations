"""
TileExporter — FreshWaterAtlas → GeoJSONSeq → tippecanoe → .pmtiles.

Pure IO module.  No geographic logic.  All geometry decisions have
already been made by the FreshWaterAtlas; this just serializes and
invokes tippecanoe.

No Fallbacks:
    If ``tippecanoe`` is missing, a ``FileNotFoundError`` is raised
    immediately — we never silently degrade.

Performance:
    Uses ``orjson`` for GeoJSONSeq serialisation (2-4× faster than
    ``json.dumps`` for coordinate-heavy features).
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from ..atlas.freshwater_atlas import FreshWaterAtlas
from ..atlas.models import AdminRecord, PolygonRecord, StreamRecord
from ..matching.display_name_resolver import DisplayNameResolver

logger = logging.getLogger(__name__)

try:
    import orjson
except ImportError as exc:
    raise ImportError(
        "orjson is required for TileExporter.  " "Install it with:  pip install orjson"
    ) from exc

try:
    from pyproj import Transformer
except ImportError as exc:
    raise ImportError(
        "pyproj is required for TileExporter.  " "Install it with:  pip install pyproj"
    ) from exc

# EPSG:3005 → WGS 84 transformer (cached once)
_TO_WGS84 = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------


def _round_coords(geom_dict: dict, precision: int = 7) -> dict:
    """Round all coordinates in a __geo_interface__ dict."""

    def _round(coords):
        if isinstance(coords, (float, int)):
            return round(coords, precision)
        return [_round(c) for c in coords]

    return {**geom_dict, "coordinates": _round(geom_dict["coordinates"])}


def _to_wgs84(geom: BaseGeometry) -> BaseGeometry:
    """Reproject a shapely geometry from EPSG:3005 → EPSG:4326."""
    from shapely.ops import transform

    return transform(_TO_WGS84.transform, geom)


# ---------------------------------------------------------------------------
# TileExporter
# ---------------------------------------------------------------------------


class TileExporter:
    """Write an immutable PMTiles file from a FreshWaterAtlas.

    The output contains zero regulation data — only permanent
    geographic features with stable fid's.
    """

    def __init__(
        self,
        atlas: FreshWaterAtlas,
        resolver: Optional[DisplayNameResolver] = None,
    ) -> None:
        self.atlas = atlas
        self._resolver = resolver

    def export(
        self,
        output_path: Path,
        work_dir: Optional[Path] = None,
    ) -> Path:
        """Build PMTiles and return the output path.

        Raises FileNotFoundError if tippecanoe is not on $PATH.
        Raises RuntimeError if tippecanoe fails.
        """
        if shutil.which("tippecanoe") is None:
            raise FileNotFoundError(
                "tippecanoe not found on $PATH.  "
                "Install it: https://github.com/felt/tippecanoe"
            )

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        work_dir = work_dir or output_path.parent / "_tile_temp"
        work_dir.mkdir(parents=True, exist_ok=True)

        layer_files = []
        for layer_name, writer_fn in self._layer_writers():
            layer_path = work_dir / f"{layer_name}.geojsonseq"
            count = writer_fn(layer_path)
            if count > 0:
                layer_files.append((layer_name, layer_path))
                logger.info(f"  {layer_name}: {count:,} features")

        if not layer_files:
            raise RuntimeError("No features to export — all layers empty")

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
        ]
        for name, path in layer_files:
            cmd.extend(["-L", f"{name}:{path}"])

        logger.info(f"Running tippecanoe ({len(layer_files)} layers)...")
        result = subprocess.run(cmd, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"tippecanoe failed (rc={result.returncode}).  "
                f"Command: {' '.join(cmd)}"
            )

        logger.info(
            f"PMTiles created → {output_path}  "
            f"({output_path.stat().st_size / 1048576:.1f} MB)"
        )

        # Write layer manifest for frontend rendering config
        self._write_layer_manifest(output_path.parent / "layer_manifest.json")

        return output_path

    # ------------------------------------------------------------------
    # Layer manifest
    # ------------------------------------------------------------------

    # Admin layers default to not visible — the frontend enables them
    # when instructed (e.g. user toggles a layer, or a regulation
    # references an admin_id and the UI highlights it).
    _LAYER_MANIFEST = {
        "streams": {"visible": True, "label": "Streams", "type": "line"},
        "under_lake_streams": {
            "visible": True,
            "label": "Under-Lake Streams",
            "type": "line",
        },
        "lakes": {"visible": True, "label": "Lakes", "type": "polygon"},
        "wetlands": {"visible": True, "label": "Wetlands", "type": "polygon"},
        "manmade": {"visible": True, "label": "Manmade Water", "type": "polygon"},
        "tidal_boundary": {
            "visible": True,
            "label": "Tidal Boundary",
            "type": "polygon",
        },
        "regions": {
            "visible": True,
            "label": "Region Boundaries",
            "type": "line",
        },
        "regions_fill": {
            "visible": True,
            "label": "Regions",
            "type": "polygon",
        },
        "wmu": {
            "visible": True,
            "label": "Management Units",
            "type": "polygon",
        },
        "wmu_boundary": {
            "visible": True,
            "label": "Management Unit Boundaries",
            "type": "line",
        },
        "parks_nat": {
            "visible": True,
            "label": "National Parks",
            "type": "polygon",
        },
        "eco_reserves": {
            "visible": True,
            "label": "Parks & Eco Reserves",
            "type": "polygon",
        },
        "wma": {
            "visible": True,
            "label": "Wildlife Management Areas",
            "type": "polygon",
        },
        "historic_sites": {
            "visible": True,
            "label": "Historic Sites",
            "type": "polygon",
        },
        "watersheds": {
            "visible": True,
            "label": "Named Watersheds",
            "type": "polygon",
        },
        "osm_admin": {
            "visible": True,
            "label": "Restricted Areas",
            "type": "polygon",
        },
        "aboriginal_lands": {
            "visible": True,
            "label": "Indigenous Lands",
            "type": "polygon",
        },
        "bc_mask": {
            "visible": True,
            "label": "Outside BC",
            "type": "polygon",
        },
    }

    def _write_layer_manifest(self, path: Path) -> None:
        """Write layer_manifest.json for frontend layer visibility config."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            f.write(orjson.dumps(self._LAYER_MANIFEST, option=orjson.OPT_INDENT_2))
        logger.info(f"Layer manifest → {path}")

    # ------------------------------------------------------------------
    # Layer writers
    # ------------------------------------------------------------------

    def _layer_writers(self):
        """Return (layer_name, writer_fn) pairs.

        Each writer_fn(path) writes a .geojsonseq file and returns
        the number of features written.
        """
        return [
            ("streams", lambda p: self._write_streams(p, self.atlas.streams)),
            (
                "under_lake_streams",
                lambda p: self._write_streams(p, self.atlas.under_lake_streams),
            ),
            ("lakes", lambda p: self._write_polygons(p, self.atlas.lakes, "lakes")),
            (
                "wetlands",
                lambda p: self._write_polygons(p, self.atlas.wetlands, "wetlands"),
            ),
            (
                "manmade",
                lambda p: self._write_polygons(p, self.atlas.manmade, "manmade"),
            ),
            ("tidal_boundary", lambda p: self._write_tidal(p)),
            (
                "parks_nat",
                lambda p: self._write_admin(p, self.atlas.parks_nat, "parks_nat"),
            ),
            (
                "eco_reserves",
                lambda p: self._write_admin(p, self.atlas.eco_reserves, "eco_reserves"),
            ),
            (
                "wma",
                lambda p: self._write_admin(p, self.atlas.wma, "wma"),
            ),
            (
                "historic_sites",
                lambda p: self._write_admin(
                    p, self.atlas.historic_sites, "historic_sites"
                ),
            ),
            (
                "watersheds",
                lambda p: self._write_admin(p, self.atlas.watersheds, "watersheds"),
            ),
            (
                "osm_admin",
                lambda p: self._write_admin(p, self.atlas.osm_admin, "osm_admin"),
            ),
            (
                "aboriginal_lands",
                lambda p: self._write_admin(
                    p, self.atlas.aboriginal_lands, "aboriginal_lands"
                ),
            ),
            (
                "wmu",
                lambda p: self._write_admin(p, self.atlas.wmu, "wmu"),
            ),
            ("wmu_boundary", lambda p: self._write_wmu_boundary(p)),
            ("regions", lambda p: self._write_regions(p)),
            ("regions_fill", lambda p: self._write_regions_fill(p)),
            ("bc_mask", lambda p: self._write_bc_mask(p)),
        ]

    def _write_streams(self, path: Path, records: Dict[str, StreamRecord]) -> int:
        """Write stream records as GeoJSONSeq."""
        count = 0
        with open(path, "wb") as f:
            for rec in records.values():
                geom = _to_wgs84(rec.geometry)
                dn = (
                    self._resolver.resolve_stream(
                        rec.blk, rec.display_name, fid=rec.fid
                    )
                    if self._resolver
                    else rec.display_name
                )
                feature = {
                    "type": "Feature",
                    "properties": {
                        "fid": rec.fid,
                        "display_name": dn,
                        "blk": rec.blk,
                        "stream_order": rec.stream_order,
                        "fwa_watershed_code": rec.fwa_watershed_code,
                    },
                    "geometry": _round_coords(geom.__geo_interface__),
                    "tippecanoe": {
                        "layer": path.stem,
                        "minzoom": rec.minzoom,
                    },
                }
                f.write(orjson.dumps(feature) + b"\n")
                count += 1
        return count

    def _write_polygons(
        self, path: Path, records: Dict[str, PolygonRecord], layer_name: str
    ) -> int:
        """Write polygon records as GeoJSONSeq."""
        count = 0
        with open(path, "wb") as f:
            for rec in records.values():
                geom = _to_wgs84(rec.geometry)
                dn = (
                    self._resolver.resolve_polygon(rec.waterbody_key, rec.display_name)
                    if self._resolver
                    else rec.display_name
                )
                feature = {
                    "type": "Feature",
                    "properties": {
                        "waterbody_key": rec.waterbody_key,
                        "display_name": dn,
                        "area": rec.area,
                    },
                    "geometry": _round_coords(geom.__geo_interface__),
                    "tippecanoe": {
                        "layer": layer_name,
                        "minzoom": rec.minzoom,
                    },
                }
                f.write(orjson.dumps(feature) + b"\n")
                count += 1
        return count

    def _write_admin(
        self,
        path: Path,
        records: Dict[str, AdminRecord],
        layer_name: str,
    ) -> int:
        """Write admin boundary records as GeoJSONSeq."""
        count = 0
        with open(path, "wb") as f:
            for rec in records.values():
                geom = _to_wgs84(rec.geometry)
                feature = {
                    "type": "Feature",
                    "properties": {
                        "admin_id": rec.admin_id,
                        "name": rec.display_name,
                        "display_name": rec.display_name,
                        "admin_type": rec.admin_type,
                        "area": rec.area,
                    },
                    "geometry": _round_coords(geom.__geo_interface__),
                    "tippecanoe": {
                        "layer": layer_name,
                        "minzoom": rec.minzoom,
                    },
                }
                f.write(orjson.dumps(feature) + b"\n")
                count += 1
        return count

    def _write_wmu_boundary(self, path: Path) -> int:
        """Write WMU polygon boundaries as line features."""
        count = 0
        with open(path, "wb") as f:
            for rec in self.atlas.wmu.values():
                boundary = rec.geometry.boundary
                geom = _to_wgs84(boundary)
                feature = {
                    "type": "Feature",
                    "properties": {
                        "admin_id": rec.admin_id,
                        "display_name": rec.display_name,
                        "stroke_color": "#888888",
                        "stroke_width": 1.0,
                    },
                    "geometry": _round_coords(geom.__geo_interface__),
                    "tippecanoe": {
                        "layer": "wmu_boundary",
                        "minzoom": 4,
                    },
                }
                f.write(orjson.dumps(feature) + b"\n")
                count += 1
        return count

    def _write_regions(self, path: Path) -> int:
        """Write dissolved region boundary lines (one per zone)."""
        zones = self._dissolve_zones()
        count = 0
        with open(path, "wb") as f:
            for zone_id, name, polygon in zones:
                boundary = polygon.boundary
                geom = _to_wgs84(boundary)
                feature = {
                    "type": "Feature",
                    "properties": {
                        "zone": zone_id,
                        "region_name": name,
                        "stroke_color": "#555555",
                        "stroke_width": 2.5,
                        "stroke_dasharray": "3,3",
                    },
                    "geometry": _round_coords(geom.__geo_interface__),
                    "tippecanoe": {
                        "layer": "regions",
                        "minzoom": 0,
                    },
                }
                f.write(orjson.dumps(feature) + b"\n")
                count += 1
        return count

    def _write_regions_fill(self, path: Path) -> int:
        """Write dissolved region fill polygons (for labels at low zoom)."""
        zones = self._dissolve_zones()
        count = 0
        with open(path, "wb") as f:
            for zone_id, name, polygon in zones:
                geom = _to_wgs84(polygon)
                feature = {
                    "type": "Feature",
                    "properties": {
                        "zone": zone_id,
                        "region_name": name,
                    },
                    "geometry": _round_coords(geom.__geo_interface__),
                    "tippecanoe": {
                        "layer": "regions_fill",
                        "minzoom": 0,
                    },
                }
                f.write(orjson.dumps(feature) + b"\n")
                count += 1
        return count

    def _dissolve_zones(
        self,
    ) -> list[tuple[str, str, BaseGeometry]]:
        """Dissolve WMU polygons into zone-level polygons.

        Returns [(zone_id, region_name, dissolved_polygon), ...].
        Cached on first call.
        """
        if hasattr(self, "_zone_cache"):
            return self._zone_cache

        from collections import defaultdict

        # WMU admin_ids contain the zone prefix (e.g., "1-1" → zone "1")
        # But AdminRecord only has admin_id and display_name.
        # We need to load the raw WMU GDF to get REGION_RESPONSIBLE_ID.
        # Instead, derive zone from admin_id: first segment before '-'
        zone_polys: dict[str, list] = defaultdict(list)
        zone_names: dict[str, str] = {}
        for rec in self.atlas.wmu.values():
            zone_id = (
                rec.admin_id.split("-")[0] if "-" in rec.admin_id else rec.admin_id
            )
            zone_polys[zone_id].append(rec.geometry)
            if zone_id not in zone_names:
                zone_names[zone_id] = rec.display_name

        result = []
        for zone_id, polys in sorted(zone_polys.items()):
            dissolved = unary_union(polys)
            if not dissolved.is_valid:
                dissolved = dissolved.buffer(0)
            result.append((zone_id, zone_names.get(zone_id, zone_id), dissolved))

        self._zone_cache = result
        return result

    def _write_tidal(self, path: Path) -> int:
        """Write the tidal boundary clipped to BC extent.

        Subtracts the BC mask so tidal and mask don't overlap outside BC.
        """
        if self.atlas.tidal_boundary is None:
            return 0
        tidal = self.atlas.tidal_boundary
        bc_union = self._get_bc_union()
        if bc_union is not None:
            tidal = tidal.intersection(bc_union)
            if tidal.is_empty:
                logger.warning("Tidal boundary fully outside BC union — skipping")
                return 0
        geom = _to_wgs84(tidal)
        feature = {
            "type": "Feature",
            "properties": {
                "name": "Tidal Waters",
            },
            "geometry": _round_coords(geom.__geo_interface__),
            "tippecanoe": {
                "layer": "tidal_boundary",
                "minzoom": 0,
            },
        }
        with open(path, "wb") as f:
            f.write(orjson.dumps(feature) + b"\n")
        return 1

    def _write_bc_mask(self, path: Path) -> int:
        """Write an outside-BC grey mask polygon.

        Creates a large bounding box and subtracts the dissolved BC
        (WMU union), producing a polygon that covers everything
        outside the province.
        """
        bc_union = self._get_bc_union()
        if bc_union is None:
            logger.warning("No WMU polygons — skipping bc_mask")
            return 0

        minx, miny, maxx, maxy = bc_union.bounds
        cx, cy = (minx + maxx) / 2, (miny + maxy) / 2
        half = max(maxx - minx, maxy - miny) / 2 + 1_000_000
        outer = box(cx - half, cy - half, cx + half, cy + half)
        mask = outer.difference(bc_union)
        mask = mask.simplify(100, preserve_topology=True)

        geom = _to_wgs84(mask)
        feature = {
            "type": "Feature",
            "properties": {
                "fill_color": "#374151",
                "fill_opacity": 0.65,
            },
            "geometry": _round_coords(geom.__geo_interface__),
            "tippecanoe": {
                "layer": "bc_mask",
                "minzoom": 0,
            },
        }
        with open(path, "wb") as f:
            f.write(orjson.dumps(feature) + b"\n")
        return 1

    def _get_bc_union(self) -> BaseGeometry | None:
        """Dissolve all WMU polygons into a single BC outline. Cached."""
        if hasattr(self, "_bc_union_cache"):
            return self._bc_union_cache
        if not self.atlas.wmu:
            self._bc_union_cache = None
            return None
        polys = [rec.geometry for rec in self.atlas.wmu.values()]
        bc = unary_union(polys)
        if not bc.is_valid:
            bc = bc.buffer(0)
        self._bc_union_cache = bc
        return bc
