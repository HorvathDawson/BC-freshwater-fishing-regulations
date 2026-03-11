"""
One-time script to extract the BC provincial boundary from the WMU layer
and save it as a simplified GeoJSON for use as a MapLibre `within` filter.
"""

import geopandas as gpd
import json
from pathlib import Path

GPKG_PATH = Path(__file__).resolve().parent.parent / "data" / "bc_fisheries_data.gpkg"
OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "webapp"
    / "public"
    / "data"
    / "bc_boundary.geojson"
)


def main() -> None:
    print(f"Reading WMU layer from {GPKG_PATH}…")
    gdf = gpd.read_file(GPKG_PATH, layer="wmu")
    gdf = gdf.to_crs("EPSG:3005")

    print(f"  {len(gdf)} WMU features loaded")
    bc_union = gdf.geometry.union_all()

    # Simplify in projected CRS (metres) for accuracy, then reproject
    bc_simple = bc_union.simplify(500, preserve_topology=True)
    print(f"  Simplified: {bc_union.geom_type} → {bc_simple.geom_type}")

    # Reproject to WGS84 (MapLibre requires EPSG:4326)
    gdf_out = gpd.GeoDataFrame(geometry=[bc_simple], crs="EPSG:3005").to_crs(
        "EPSG:4326"
    )
    geom = gdf_out.geometry.iloc[0]

    # Build a minimal GeoJSON Feature (MapLibre `within` needs a GeoJSON geometry)
    feature = {
        "type": "Feature",
        "properties": {},
        "geometry": json.loads(gpd.GeoSeries([geom]).to_json())["features"][0][
            "geometry"
        ],
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(feature, f)

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  Wrote {OUTPUT_PATH} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
