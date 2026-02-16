"""
TributaryEnricher - Enriches feature sets with tributaries/upstream features

Traverses FWA stream network graph to find upstream tributaries.
Uses caching to avoid duplicate graph traversals.
"""

from typing import List, Dict, Set, Union, Optional
from pathlib import Path
import pickle
from collections import deque

from .metadata_gazetteer import FWAFeature
from .logger_config import get_logger

logger = get_logger(__name__)


def _get_attr(obj, attr_name, default=None):
    """Get attribute from FWAFeature object or dict."""
    if hasattr(obj, attr_name):
        return getattr(obj, attr_name, default)
    elif isinstance(obj, dict):
        return obj.get(attr_name, default)
    return default


class TributaryEnricher:
    """
    Pure graph traversal utility for finding upstream tributaries.

    ARCHITECTURE:
    - Accepts FWAFeature objects and handles seed lookup internally
    - Uses metadata to determine feature type and find appropriate seeds
    - Streams: Use linear_feature_id, exclude mainstem by default
    - Polygons: Use waterbody_key to find connected streams, include all upstream

    KEY METHOD:
    - enrich_with_tributaries(features, exclude_mainstem=True)
      Returns ONLY tributaries (excludes input features)
    """

    def __init__(
        self, graph_source: Union[str, Path, Dict, None] = None, metadata_gazetteer=None
    ):
        """
        Initialize TributaryEnricher.

        Args:
            graph_source: One of:
                - Path to .gpickle file (str or Path)
                - Loaded graph dict with keys: 'graph', 'node_id_to_index', etc.
                - None (enrichment disabled)
            metadata_gazetteer: MetadataGazetteer instance for looking up zone assignments
        """
        self.graph = None
        self.graph_data = None
        self.metadata_gazetteer = metadata_gazetteer

        # Load graph if provided
        if graph_source is not None:
            if isinstance(graph_source, (str, Path)):
                # Load from pickle file
                pickle_path = Path(graph_source)
                if pickle_path.exists():
                    logger.info(f"Loading graph from {pickle_path}...")
                    with open(pickle_path, "rb") as f:
                        self.graph_data = pickle.load(f)
                    self.graph = self.graph_data["graph"]
                    logger.info(
                        f"Graph loaded: {self.graph.vcount():,} nodes, {self.graph.ecount():,} edges"
                    )
                else:
                    logger.warning(f"Graph file not found: {pickle_path}")
            elif isinstance(graph_source, dict):
                # Use pre-loaded graph
                self.graph_data = graph_source
                self.graph = graph_source.get("graph")
                if self.graph:
                    logger.info(
                        f"Using pre-loaded graph: {self.graph.vcount():,} nodes, {self.graph.ecount():,} edges"
                    )

        # Build lookup indices for O(1) access
        self.linear_feature_id_to_edge_idx = {}
        self.waterbody_key_to_edge_indices = {}

        if self.graph:
            logger.info("Building lookup indices...")
            for edge in self.graph.es:
                linear_id = edge["linear_feature_id"]
                if linear_id:
                    self.linear_feature_id_to_edge_idx[linear_id] = edge.index

                wb_key = edge["waterbody_key"]
                if wb_key:
                    if wb_key not in self.waterbody_key_to_edge_indices:
                        self.waterbody_key_to_edge_indices[wb_key] = []
                    self.waterbody_key_to_edge_indices[wb_key].append(edge.index)

            logger.info(
                f"  Indexed {len(self.linear_feature_id_to_edge_idx):,} linear features"
            )
            logger.info(
                f"  Indexed {len(self.waterbody_key_to_edge_indices):,} waterbody keys"
            )

        # Cache: frozenset of base feature IDs → list of tributary features
        self.enrichment_cache = {}

        # Statistics
        self.enrichment_requests = 0
        self.cache_hits = 0
        self.total_tributaries_found = 0
        self.total_base_features = 0
        self.total_stream_seeds = 0
        self.total_lake_seeds = 0
        self.warnings_logged = set()  # Track which warnings we've shown

    def enrich_with_tributaries(
        self,
        linear_feature_ids: List[str],
        excluded_watershed_codes: Set[str] = None,
        parent_features: List = None,
    ) -> List:
        """
        Find upstream tributaries for given stream linear_feature_ids.

        Pure graph utility - accepts stream seeds only.
        Caller is responsible for:
        - Determining feature types
        - Converting polygons to stream seeds via waterbody_key
        - Determining which watershed codes to exclude
        - Validating inputs

        Args:
            linear_feature_ids: List of stream linear_feature_ids to use as seeds
            excluded_watershed_codes: Set of watershed codes to exclude during traversal.
                                     Defaults to empty set (include all upstream).
            parent_features: Optional parent features for zone inheritance fallback

        Returns:
            List of FWAFeature objects representing tributaries (excludes seed features)
        """
        self.enrichment_requests += 1

        if not linear_feature_ids:
            return []

        # Check if graph is available
        if not self.graph:
            logger.debug("No graph loaded - tributary enrichment disabled")
            return []

        # Find seed edges
        seed_edges = []
        missing_seeds = []
        for linear_id in linear_feature_ids:
            edge_idx = self.linear_feature_id_to_edge_idx.get(str(linear_id))
            if edge_idx is not None:
                seed_edges.append(edge_idx)
            else:
                missing_seeds.append(linear_id)

        if missing_seeds:
            logger.warning(
                f"Could not find {len(missing_seeds)}/{len(linear_feature_ids)} seed edges in graph: "
                f"{missing_seeds[:10]}{'...' if len(missing_seeds) > 10 else ''}"
            )

        if not seed_edges:
            logger.debug("No valid seed edges found for linear_feature_ids")
            return []

        # Use provided exclusions or empty set
        excluded_codes = (
            excluded_watershed_codes if excluded_watershed_codes is not None else set()
        )

        # Check cache
        cache_key = (frozenset(seed_edges), frozenset(excluded_codes))
        if cache_key in self.enrichment_cache:
            self.cache_hits += 1
            return self.enrichment_cache[cache_key]

        # BFS upstream traversal
        tributaries = self._traverse_upstream(seed_edges, excluded_codes)

        # Convert to FWAFeature objects
        tributary_features = self._edges_to_features(tributaries, parent_features or [])

        # Cache results
        self.enrichment_cache[cache_key] = tributary_features

        # Update statistics
        self.total_tributaries_found += len(tributary_features)
        self.total_base_features += len(linear_feature_ids)

        logger.info(
            f"Tributary enrichment: {len(linear_feature_ids)} seeds → "
            f"{len(tributary_features)} tributaries "
            f"[{len(seed_edges)} edges, {len(excluded_codes)} excluded watershed codes]"
        )

        return tributary_features

    def _traverse_upstream(
        self,
        seed_edges: List[int],
        excluded_watershed_codes: Set[str],
    ) -> Set[int]:
        """
        BFS traversal upstream from seed edges.

        Args:
            seed_edges: Edge indices to start from
            excluded_watershed_codes: Watershed codes to exclude (stop traversal)
                                     Empty set = include everything upstream

        Returns:
            Set of tributary edge indices (excludes seed edges)
        """
        tributaries = set()
        visited = set()
        queue = deque()

        # Initialize with seed edges (don't add seeds to tributaries)
        for edge_idx in seed_edges:
            queue.append(edge_idx)
            visited.add(edge_idx)

        while queue:
            edge_idx = queue.popleft()
            edge = self.graph.es[edge_idx]

            # Continue upstream from this edge's source node
            source_node = edge.source
            for upstream_edge in self.graph.es.select(_target=source_node):
                upstream_idx = upstream_edge.index
                if upstream_idx in visited:
                    continue

                visited.add(upstream_idx)

                # Check if this upstream edge should be excluded
                upstream_watershed = upstream_edge["fwa_watershed_code"]
                if (
                    upstream_watershed
                    and upstream_watershed in excluded_watershed_codes
                ):
                    # This is part of the excluded watershed (mainstem/side channel)
                    # Don't include it in tributaries, but DO continue traversing upstream
                    # to find real tributaries beyond this excluded segment
                    queue.append(upstream_idx)
                    continue

                # This is a tributary - add it to results
                tributaries.add(upstream_idx)

                # Continue traversing from this tributary
                queue.append(upstream_idx)

        return tributaries

    def _edges_to_features(self, edge_indices: Set[int], parent_features: List) -> List:
        """
        Convert edge indices to FWAFeature objects.

        Args:
            edge_indices: Set of edge indices to convert
            parent_features: Parent features for zone inheritance fallback

        Returns:
            List of FWAFeature objects
        """
        tributary_features = []
        for edge_idx in edge_indices:
            edge = self.graph.es[edge_idx]
            linear_feature_id = str(edge["linear_feature_id"])

            # Look up zones from metadata (preferred method)
            zones = None
            mgmt_units = None

            if self.metadata_gazetteer:
                # Try to get metadata for this stream segment
                metadata = self.metadata_gazetteer.get_stream_metadata(
                    linear_feature_id
                )
                if metadata:
                    zones = metadata.get("zones", [])
                    mgmt_units = metadata.get("mgmt_units", [])

            # Fallback 1: Inherit zones from parent feature if metadata lookup failed
            if not zones and parent_features:
                zones = _get_attr(parent_features[0], "zones", [])
                if zones:
                    logger.debug(
                        f"Tributary {linear_feature_id} inheriting zones {zones} from parent "
                        f"(metadata lookup failed)"
                    )

            # Fallback 2: Use "Unknown" as last resort (should rarely happen)
            if not zones:
                zones = ["Unknown"]
                logger.warning(
                    f"Tributary {linear_feature_id} has no zone metadata - using 'Unknown'. "
                    f"This indicates missing data in stream_metadata.pickle"
                )

            # Create FWAFeature object
            tributary_features.append(
                FWAFeature(
                    fwa_id=linear_feature_id,
                    name=edge["gnis_name"] or "unnamed",
                    geometry_type="multilinestring",
                    zones=zones,
                    feature_type="stream",
                    gnis_name=edge["gnis_name"],
                    gnis_id=edge["gnis_id"],
                    fwa_watershed_code=edge["fwa_watershed_code"],
                    waterbody_key=edge["waterbody_key"],
                    mgmt_units=mgmt_units,
                    matched_via="tributary_enrichment",
                )
            )

        return tributary_features

    def clear_cache(self):
        """
        Clear enrichment cache.

        Should be called between regulation batches to free memory.
        """
        self.enrichment_cache.clear()
        logger.debug("Cleared tributary enrichment cache")

    def get_stats(self) -> Dict:
        """
        Return statistics about tributary enrichment.

        Returns:
            Dict with enrichment requests, cache hits, etc.
        """
        cache_hit_rate = 0.0
        if self.enrichment_requests > 0:
            cache_hit_rate = self.cache_hits / self.enrichment_requests

        return {
            "enrichment_requests": self.enrichment_requests,
            "cache_hits": self.cache_hits,
            "cache_hit_rate": cache_hit_rate,
            "cache_size": len(self.enrichment_cache),
            "total_tributaries_found": self.total_tributaries_found,
            "total_base_features": self.total_base_features,
            "total_stream_seeds": self.total_stream_seeds,
            "total_lake_seeds": self.total_lake_seeds,
        }

    def reset_stats(self):
        """Reset statistics counters."""
        self.enrichment_requests = 0
        self.cache_hits = 0
        self.total_tributaries_found = 0
        self.total_base_features = 0
        self.total_stream_seeds = 0
        self.total_lake_seeds = 0
