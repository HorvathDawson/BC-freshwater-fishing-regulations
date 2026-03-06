import geopandas as gpd
import pandas as pd
import pyogrio
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(x, **kwargs):
        return x


class FWADataAccessor:
    """
    High-performance accessor for the BC Freshwater GeoPackage (FWA).
    Uses 'pyogrio' and 'arrow' for fast spatial and attribute queries.
    """

    def __init__(self, gpkg_path: Union[str, Path]) -> None:
        self.gpkg_path = Path(gpkg_path)
        if not self.gpkg_path.exists():
            raise FileNotFoundError(f"GeoPackage not found at: {self.gpkg_path}")

        # Cache the available layers on initialization
        # pyogrio.list_layers returns a list of tuples: (layer_name, geometry_type)
        self.layer_info = pyogrio.list_layers(self.gpkg_path)
        self.layer_names = [info[0] for info in self.layer_info]

    def list_layers(
        self, with_details: bool = False
    ) -> Union[List[str], Dict[str, str]]:
        """
        Returns available layers.

        Args:
            with_details: If True, returns dict with layer names as keys and
                         geometry types as values. If False, returns list of layer names.

        Returns:
            List of layer names or dict of {layer_name: geometry_type}
        """
        if with_details:
            return {name: geom_type for name, geom_type in self.layer_info}
        return self.layer_names

    # ── Column normalization rules ──────────────────────────────────────
    # Applied uniformly by _normalize_columns() so EVERY access path
    # (get_layer, get_attributes, get_features_by_attribute) returns
    # consistent Python types that match pickle metadata keys.

    NUMERIC_ID_COLUMNS = [
        "GNIS_ID",
        "GNIS_ID_1",
        "GNIS_ID_2",
        "WATERBODY_KEY",
        "WATERBODY_POLY_ID",
        "LINEAR_FEATURE_ID",
        "BLUE_LINE_KEY",
        # Admin layer ID fields
        "NATIONAL_PARK_ID",
        "ADMIN_AREA_SID",
        "NAMED_WATERSHED_ID",
        "osm_id",
    ]

    STRING_COLUMNS = [
        "FWA_WATERSHED_CODE",
        "FEATURE_CODE",
        "EDGE_TYPE",
        # Admin layer fields
        "SITE_ID",  # UUID for historic sites
        "PROTECTED_LANDS_CODE",  # Classification code for parks_bc
    ]

    INT_COLUMNS = ["STREAM_ORDER", "STREAM_MAGNITUDE"]

    def _normalize_columns(
        self, df: Union[pd.DataFrame, gpd.GeoDataFrame]
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Apply consistent type normalization to all known columns present in *df*.

        - Numeric ID columns → str  (float64 169001887.0 → "169001887", 0/null → "")
        - String columns     → str  (null → "")
        - Integer columns    → int | None
        """
        for col in self.NUMERIC_ID_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: (
                        "" if pd.isnull(x) or x == 0 or x == "" else str(int(float(x)))
                    )
                )

        for col in self.STRING_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: "" if pd.isnull(x) else str(x))

        for col in self.INT_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: None if pd.isnull(x) or x == "" else int(float(x))
                )
        return df

    # ── Public access methods ─────────────────────────────────────────

    def get_layer(
        self,
        layer_name: str,
        columns: Optional[List[str]] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
    ) -> gpd.GeoDataFrame:
        """
        Loads a full layer or a spatial subset, with a progress bar.
        All columns are normalized to consistent Python types.

        :param columns: List of specific columns to load (saves memory).
        :param bbox: Tuple of (minx, miny, maxx, maxy) to filter spatially.
        """
        self._check_layer(layer_name)
        gdf = gpd.read_file(
            self.gpkg_path,
            layer=layer_name,
            engine="pyogrio",
            use_arrow=True,
            columns=columns,
            bbox=bbox,
        )
        gdf = self._normalize_columns(gdf)

        # Show progress bar as we iterate through the rows (forces load)
        _ = [
            row
            for row in tqdm(
                gdf.itertuples(),
                total=len(gdf),
                desc=f"Loading '{layer_name}'",
                unit="row",
            )
        ]
        return gdf

    def get_features_by_attribute(
        self,
        layer_name: str,
        column: str,
        values: Union[List[str], str, int],
        ignore_geom: bool = False,
    ) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
        """
        Pushes a SQL 'WHERE' clause directly to the GeoPackage for fast lookup.
        All columns are normalized to consistent Python types.

        :param column: The attribute column (e.g., 'GNIS_NAME' or 'LINEAR_FEATURE_ID')
        :param values: A single value or a list of values to search for.
        """
        self._check_layer(layer_name)

        # Format the SQL WHERE clause
        if isinstance(values, (list, tuple, set)):
            if len(values) == 0:
                return gpd.GeoDataFrame() if not ignore_geom else pd.DataFrame()

            is_str = isinstance(list(values)[0], str)
            val_str = ", ".join(f"'{v}'" if is_str else str(v) for v in values)
            where_clause = f"{column} IN ({val_str})"
        else:
            is_str = isinstance(values, str)
            where_clause = (
                f"{column} = '{values}'" if is_str else f"{column} = {values}"
            )

        result = gpd.read_file(
            self.gpkg_path,
            layer=layer_name,
            engine="pyogrio",
            use_arrow=True,
            where=where_clause,
            ignore_geometry=ignore_geom,
        )
        return self._normalize_columns(result)

    def _check_layer(self, layer_name: str) -> None:
        if layer_name not in self.layer_names:
            raise ValueError(
                f"Layer '{layer_name}' not found. Available: {self.layer_names}"
            )
