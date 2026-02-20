"""
TributaryEnricher - Enriches feature sets with tributaries/upstream features

Traverses FWA stream network graph to find upstream tributaries.
Uses caching to avoid duplicate graph traversals.
"""

from typing import List, Dict, Set, Union, Optional
from pathlib import Path
import pickle
from collections import deque

from fwa_pipeline.metadata_gazetteer import FWAFeature, FeatureType
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

    # Edge types to include but stop traversal beyond
    # When traversing upstream and encountering these edge types:
    # - Include the edge with this type in results
    # - Continue upstream through more edges of this type
    # - Stop when reaching a different edge type (don't include non-matching edges)
    EXCLUDED_EDGE_TYPES = {"2300"}  # Add more as needed

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
        excluded_waterbody_keys: Set[str] = None,
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
            excluded_waterbody_keys: Set of waterbody keys to exclude (if using lake seeds)

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

        excluded_keys = (
            excluded_waterbody_keys if excluded_waterbody_keys is not None else set()
        )

        # Check cache
        cache_key = (
            frozenset(seed_edges),
            frozenset(excluded_codes),
            frozenset(excluded_keys),
        )
        if cache_key in self.enrichment_cache:
            self.cache_hits += 1
            return self.enrichment_cache[cache_key]

        # BFS upstream traversal
        tributaries = self._traverse_upstream(seed_edges, excluded_codes, excluded_keys)

        # Convert to FWAFeature objects
        tributary_features = self._edges_to_features(tributaries)

        # Cache results
        self.enrichment_cache[cache_key] = tributary_features

        # Update statistics
        self.total_tributaries_found += len(tributary_features)
        self.total_base_features += len(linear_feature_ids)

        logger.info(
            f"Tributary enrichment: {len(linear_feature_ids)} seeds → "
            f"{len(tributary_features)} tributaries "
            f"[{len(seed_edges)} edges, {len(excluded_codes)} excluded watershed codes and excluded waterbody keys: {len(excluded_keys)}]"
        )

        return tributary_features

    def _traverse_upstream(
        self,
        seed_edges: List[int],
        excluded_watershed_codes: Set[str],
        excluded_waterbody_keys: Set[str] = None,
    ) -> Set[int]:
        """
        BFS traversal upstream from seed edges.

        Args:
            seed_edges: Edge indices to start from
            excluded_watershed_codes: Watershed codes to exclude (stop traversal)
                                      Empty set = include everything upstream
            excluded_waterbody_keys: Set of waterbody keys to exclude (if using lake seeds)

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

            linear_id = str(edge["linear_feature_id"])

            # Check if CURRENT edge has an excluded type
            try:
                current_edge_type = edge["edge_type"]
            except (KeyError, AttributeError):
                current_edge_type = ""
            # Convert to string for comparison to handle both int and str types
            current_edge_type_str = str(current_edge_type) if current_edge_type else ""
            current_is_excluded_type = current_edge_type_str in self.EXCLUDED_EDGE_TYPES

            # Continue upstream from this edge's source node
            source_node = edge.source
            for upstream_edge in self.graph.es.select(_target=source_node):
                upstream_idx = upstream_edge.index
                if upstream_idx in visited:
                    continue

                visited.add(upstream_idx)

                # Check if this upstream edge should be excluded by waterbody key
                upstream_wb_key = upstream_edge["waterbody_key"]
                # if "329093898" == str(upstream_wb_key):
                #     logger.warning(
                #         f"Encountered excluded waterbody_key 329093898 during tributary enrichment. This edge will be skipped ({upstream_wb_key in excluded_waterbody_keys}). came from edge with linear_feature_id {linear_id}"
                #     )
                #     logger.warning(
                #         f"\nUpstream edge details: {upstream_edge.attributes()}"
                #     )
                #     logger.warning(f"\nSeed edge details: {edge.attributes()}")
                #     logger.warning(
                #         f"\nExcluded waterbody keys: {excluded_waterbody_keys}"
                #     )
                #     exit(0)

                if upstream_wb_key and upstream_wb_key in excluded_waterbody_keys:
                    # This edge has an excluded waterbody key (e.g., lake)
                    # Skip it entirely - don't include in results, don't traverse upstream from it
                    continue

                # Check if this upstream edge should be excluded by watershed code
                upstream_watershed = upstream_edge["fwa_watershed_code"]
                if (
                    upstream_watershed
                    and upstream_watershed in excluded_watershed_codes
                ):
                    # # This is part of the excluded watershed (mainstem/side channel)
                    # # Don't include it in tributaries, but DO continue traversing upstream
                    # # to find real tributaries beyond this excluded segment
                    # queue.append(upstream_idx)

                    # TODO: figure this out. it will cause issues for lakes where tributaries have a segment in the lake...
                    # might be able to seed lake blueline key and watershedcode... also lakes we want all items so no excluded?
                    # but this might not be a huge issue in practice since most tributaries will be connected by stream segments,
                    # which means it doesnt have any items of tributaries as seeds, just the mainstem/side channel segment which will exclude itself but not the tributaries beyond it.

                    # This edge has an excluded watershed code (mainstem/side channel)
                    # Skip it entirely - don't include in results, don't traverse upstream from it
                    continue

                # Get upstream edge type
                try:
                    upstream_edge_type = upstream_edge["edge_type"]
                except (KeyError, AttributeError):
                    upstream_edge_type = ""
                # Convert to string for comparison to handle both int and str types
                upstream_edge_type_str = (
                    str(upstream_edge_type) if upstream_edge_type else ""
                )
                upstream_is_excluded_type = (
                    upstream_edge_type_str in self.EXCLUDED_EDGE_TYPES
                )
                # Handle excluded edge types (e.g., 2300)
                # If we're on an excluded edge type and upstream is NOT excluded, stop
                if current_is_excluded_type and not upstream_is_excluded_type:
                    # We're on a 2300 edge and upstream is different (e.g., 1450)
                    # Don't include this edge, and stop traversal on this path
                    continue

                # This is a tributary - add it to results
                tributaries.add(upstream_idx)

                # Continue traversing from this tributary
                queue.append(upstream_idx)

        return tributaries

    def _edges_to_features(self, edge_indices: Set[int]) -> List:
        """
        Convert edge indices to FWAFeature objects STRICTLY using the MetadataGazetteer.

        Args:
            edge_indices: Set of edge indices to convert

        Returns:
            List of FWAFeature objects
        """
        tributary_features = []

        if not self.metadata_gazetteer:
            logger.error(
                "MetadataGazetteer is required but missing. Cannot fetch features."
            )
            return tributary_features

        for edge_idx in edge_indices:
            edge = self.graph.es[edge_idx]
            linear_feature_id = str(edge["linear_feature_id"])

            # Ask the gazetteer for the canonical feature. Period.
            feature = self.metadata_gazetteer.get_stream_by_id(linear_feature_id)

            if feature:
                # We successfully pulled the centralized feature
                # If it's missing zone metadata, try to inherit from the parent feature

                # Tag how this feature was matched into the set
                feature.matched_via = "tributary_enrichment"
                tributary_features.append(feature)

            else:
                # The feature is in the graph but missing from the metadata gazetteer index.
                # We DO NOT build a fake one here anymore. We trust the gazetteer.
                logger.warning(
                    f"Tributary {linear_feature_id} found in graph but missing from metadata gazetteer. Skipping."
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
