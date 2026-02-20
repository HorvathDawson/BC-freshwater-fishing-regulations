import geopandas as gpd
import pandas as pd
import pyogrio
from pathlib import Path

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

    def __init__(self, gpkg_path: str | Path):
        self.gpkg_path = Path(gpkg_path)
        if not self.gpkg_path.exists():
            raise FileNotFoundError(f"GeoPackage not found at: {self.gpkg_path}")

        # Cache the available layers on initialization
        # pyogrio.list_layers returns a list of tuples: (layer_name, geometry_type)
        self.layer_info = pyogrio.list_layers(self.gpkg_path)
        self.layer_names = [info[0] for info in self.layer_info]

    def list_layers(self, with_details: bool = False) -> list | dict:
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

    def get_layer(
        self, layer_name: str, columns: list = None, bbox: tuple = None
    ) -> gpd.GeoDataFrame:
        """
        Loads a full layer or a spatial subset, with a progress bar.
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
        # Clean and convert relevant columns to appropriate types
        # Numeric ID columns: convert to string, treat 0 as empty (no value)
        numeric_id_columns = [
            "GNIS_ID",
            "GNIS_ID_1",
            "GNIS_ID_2",
            "WATERBODY_KEY",
            "WATERBODY_POLY_ID",
            "LINEAR_FEATURE_ID",
            "BLUE_LINE_KEY",
        ]
        # String code columns: just convert to string, preserve values
        string_columns = [
            "FWA_WATERSHED_CODE",
            "FEATURE_CODE",
            "EDGE_TYPE",
        ]
        # Integer columns: keep as int, null stays as None
        int_columns = ["STREAM_ORDER", "STREAM_MAGNITUDE"]

        for col in numeric_id_columns:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(
                    lambda x: (
                        "" if pd.isnull(x) or x == 0 or x == "" else str(int(float(x)))
                    )
                )

        for col in string_columns:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(lambda x: "" if pd.isnull(x) else str(x))

        for col in int_columns:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(
                    lambda x: None if pd.isnull(x) or x == "" else int(float(x))
                )
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

    def get_attributes(self, layer_name: str, columns: list = None) -> pd.DataFrame:
        """
        Loads ONLY the tabular data (ignores shapes).
        Extremely fast for attribute analysis or cross-referencing IDs.
        """
        self._check_layer(layer_name)
        return gpd.read_file(
            self.gpkg_path,
            layer=layer_name,
            engine="pyogrio",
            use_arrow=True,
            columns=columns,
            ignore_geometry=True,  # This returns a standard, lightweight Pandas DataFrame
        )

    def get_features_by_attribute(
        self,
        layer_name: str,
        column: str,
        values: list | str | int,
        ignore_geom: bool = False,
    ) -> gpd.GeoDataFrame | pd.DataFrame:
        """
        The magic method. Pushes a SQL 'WHERE' clause directly to the GeoPackage.
        Finds specific features instantly without loading the whole layer.

        :param column: The attribute column (e.g., 'GNIS_NAME' or 'LINEAR_FEATURE_ID')
        :param values: A single value or a list of values to search for.
        """
        self._check_layer(layer_name)

        # Format the SQL WHERE clause
        if isinstance(values, (list, tuple, set)):
            # Handle lists: format as SQL IN ('A', 'B') or IN (1, 2)
            if len(values) == 0:
                return gpd.GeoDataFrame() if not ignore_geom else pd.DataFrame()

            is_str = isinstance(list(values)[0], str)
            val_str = ", ".join(f"'{v}'" if is_str else str(v) for v in values)
            where_clause = f"{column} IN ({val_str})"
        else:
            # Handle single values
            is_str = isinstance(values, str)
            where_clause = (
                f"{column} = '{values}'" if is_str else f"{column} = {values}"
            )

        return gpd.read_file(
            self.gpkg_path,
            layer=layer_name,
            engine="pyogrio",
            use_arrow=True,
            where=where_clause,
            ignore_geometry=ignore_geom,
        )

    def get_geometry_dict(
        self, layer_name: str, id_column: str, target_ids: list = None
    ) -> dict:
        """
        Returns a dictionary mapping ID -> Geometry.
        Highly useful for network/graph logic (like mapping valid streams).
        """
        # If target IDs are provided, only load those via SQL. Otherwise, load all.
        if target_ids:
            gdf = self.get_features_by_attribute(layer_name, id_column, target_ids)
        else:
            gdf = self.get_layer(layer_name, columns=[id_column])

        if gdf.empty:
            return {}

        # Ensure the active geometry column is used
        geom_col = gdf.active_geometry_name

        # Convert to dictionary mapping
        return pd.Series(gdf[geom_col].values, index=gdf[id_column]).to_dict()

    def _check_layer(self, layer_name: str):
        if layer_name not in self.layer_names:
            raise ValueError(
                f"Layer '{layer_name}' not found. Available: {self.layer_names}"
            )
