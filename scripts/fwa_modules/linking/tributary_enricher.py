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
    Enriches feature set with tributaries/upstream features.

    IMPORTANT: enrich_with_tributaries() returns ONLY tributaries (excludes base features).
    Caller must combine if needed.

    Algorithm:
    1. Collect watershed codes from ALL base features (excluded_watershed_codes)
    2. For stream features: Start upstream traversal from those edges
    3. For lake features: Find stream edges with matching waterbody_key, use as seeds
    4. During traversal: Stop when we hit any segment with excluded_watershed_code
    5. Return: All upstream tributary features (EXCLUDING seeds and base features)

    Caching: Results cached by frozenset of base feature IDs for performance.
    """

    def __init__(self, graph_source: Union[str, Path, Dict, None] = None):
        """
        Initialize TributaryEnricher.

        Args:
            graph_source: One of:
                - Path to .gpickle file (str or Path)
                - Loaded graph dict with keys: 'graph', 'node_id_to_index', etc.
                - None (enrichment disabled)
        """
        self.graph = None
        self.graph_data = None

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
        features: List,
        scope: Dict,
        global_scope: Dict,
        include_mainstem: bool = False,
    ) -> List:
        """
        Get upstream tributary features (EXCLUDES base features).

        IMPORTANT: Returns ONLY tributary features, NOT including the base features.
        Caller must combine if needed.

        Args:
            features: Base feature set whose tributaries to find
            scope: Current scope (may have tributary flags)
            global_scope: Global scope (for fallback)
            include_mainstem: If True, include upstream segments with same watershed code.
                            Useful for lakes where you want ALL upstream water including main river.
                            If False (default), only include actual tributaries (different watershed codes).

        Returns:
            List of tributary FWAFeature objects ONLY (excludes input features)

        Algorithm:
        1. Collect watershed codes from ALL base features → excluded_watershed_codes
        2. For streams: Start traversal from those edges
        3. For lakes: Find stream edges with matching waterbody_key → use as seeds
        4. BFS upstream: Stop when we hit excluded_watershed_codes (unless include_mainstem=True)
        5. Return tributaries (exclude seeds and base features)
        """
        self.enrichment_requests += 1

        if not features:
            return []

        # Check if graph is available
        if not self.graph:
            logger.debug("No graph loaded - tributary enrichment disabled")
            return []

        # Check cache first
        cache_key = frozenset(
            _get_attr(f, "linear_feature_id") or _get_attr(f, "fwa_id")
            for f in features
            if _get_attr(f, "linear_feature_id") or _get_attr(f, "fwa_id")
        )
        if cache_key in self.enrichment_cache:
            self.cache_hits += 1
            return self.enrichment_cache[cache_key]

        # Step 1: Collect watershed codes from ALL base features
        excluded_watershed_codes = set()
        for feature in features:
            watershed_code = _get_attr(feature, "fwa_watershed_code")
            if watershed_code:
                excluded_watershed_codes.add(watershed_code)
            elif "fwa_watershed_code" not in self.warnings_logged:
                logger.warning(
                    f"Feature {_get_attr(feature, 'linear_feature_id') or _get_attr(feature, 'fwa_id')} missing fwa_watershed_code - skipping"
                )
                self.warnings_logged.add("fwa_watershed_code")

        if not excluded_watershed_codes:
            feature_types = [type(f).__name__ for f in features[:5]]
            feature_ids = [
                _get_attr(f, "linear_feature_id") or _get_attr(f, "fwa_id") or str(f)
                for f in features[:5]
            ]
            logger.warning(
                f"No watershed codes found in base features - cannot determine tributaries. "
                f"Base features: {len(features)}, Types: {feature_types}, IDs: {feature_ids}"
            )
            return []

        logger.debug(
            f"Enriching {len(features)} features - excluding {len(excluded_watershed_codes)} watershed codes"
        )

        # Step 2: Separate streams and lakes, find starting edges using O(1) lookups
        stream_seeds = []  # Starting edges for streams
        lake_seeds = []  # Starting edges for lakes (will be excluded from results)

        for feature in features:
            feature_id = _get_attr(feature, "linear_feature_id") or _get_attr(
                feature, "fwa_id"
            )
            waterbody_key = _get_attr(feature, "waterbody_key")
            geometry_type = _get_attr(feature, "geometry_type")

            # Determine if this is a lake feature
            # Lakes have geometry_type='polygon' and waterbody_key
            is_lake = geometry_type == "polygon" and waterbody_key is not None

            if is_lake:
                # Lake: Find all stream edges with matching waterbody_key (O(1) lookup)
                edge_indices = self.waterbody_key_to_edge_indices.get(waterbody_key, [])
                if edge_indices:
                    lake_seeds.extend(edge_indices)
                    logger.debug(
                        f"Lake feature {feature_id} ({_get_attr(feature, 'gnis_name', 'unnamed')}) "
                        f"→ {len(edge_indices)} stream seeds (waterbody_key={waterbody_key})"
                    )
                else:
                    logger.debug(
                        f"Lake feature {feature_id} ({_get_attr(feature, 'gnis_name', 'unnamed')}) "
                        f"has no connected stream edges (waterbody_key={waterbody_key})"
                    )
            else:
                # Stream: Find edge by linear_feature_id (O(1) lookup)
                edge_idx = self.linear_feature_id_to_edge_idx.get(feature_id)
                if edge_idx is not None:
                    stream_seeds.append(edge_idx)

        if not stream_seeds and not lake_seeds:
            logger.debug(f"No seed edges found for {len(features)} features")
            return []

        # Step 3: BFS upstream traversal
        # We traverse FROM the seeds, adding upstream edges that DON'T have excluded watershed codes
        tributaries = set()  # Edge indices of tributaries
        visited = set()  # Edge indices we've visited
        queue = deque()

        # Initialize queue with seed edges
        # Seeds are the BASE features - we don't add them to tributaries
        for edge_idx in stream_seeds + lake_seeds:
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

                # Check if this upstream edge has an excluded watershed code
                upstream_watershed = upstream_edge["fwa_watershed_code"]
                if (
                    not include_mainstem
                    and upstream_watershed
                    and upstream_watershed in excluded_watershed_codes
                ):
                    # This is part of the mainstem - don't include or continue (unless include_mainstem=True)
                    continue

                # This is a tributary - add it to results
                tributaries.add(upstream_idx)

                # Continue traversing from this tributary
                queue.append(upstream_idx)

        # Step 4: Convert edge indices to FWAFeature objects
        tributary_features = []
        for edge_idx in tributaries:
            edge = self.graph.es[edge_idx]

            # Determine region from first base feature
            region = "Region 1"  # Default
            if features:
                region = _get_attr(features[0], "region", "Region 1")

            # Create FWAFeature object
            tributary_features.append(
                FWAFeature(
                    fwa_id=str(edge["linear_feature_id"]),
                    name=edge["gnis_name"] or edge["lake_name"] or "unnamed",
                    geometry_type="multilinestring",
                    region=region,
                    gnis_name=edge["gnis_name"],
                    gnis_id=edge["gnis_id"],
                    fwa_watershed_code=edge["fwa_watershed_code"],
                    waterbody_key=edge["waterbody_key"],
                    mgmt_units=None,  # Could extract from zones if needed
                    matched_via="tributary_enrichment",
                )
            )

        # Cache results
        self.enrichment_cache[cache_key] = tributary_features

        # Update statistics
        self.total_tributaries_found += len(tributary_features)
        self.total_base_features += len(features)
        self.total_stream_seeds += len(stream_seeds)
        self.total_lake_seeds += len(lake_seeds)

        # Log enrichment details
        base_names = [
            _get_attr(f, "gnis_name") or _get_attr(f, "name") or "unnamed"
            for f in features[:3]
        ]
        if len(features) > 3:
            base_names_str = f"{', '.join(base_names)}, ... ({len(features)} total)"
        else:
            base_names_str = ", ".join(base_names)

        mainstem_mode = (
            "including mainstem" if include_mainstem else "excluding mainstem"
        )
        logger.info(
            f"Tributary enrichment: {len(features)} base features ({base_names_str}) → "
            f"{len(tributary_features)} tributaries "
            f"[{len(stream_seeds)} stream seeds + {len(lake_seeds)} lake seeds, "
            f"{mainstem_mode}, {len(excluded_watershed_codes)} watershed codes]"
        )

        return tributary_features
        #     upstream = self._get_upstream_tributaries(feature)
        #     tributaries.update(upstream)
        # return list(tributaries)  # NOTE: Excludes base features

        # MVP: Return empty list (tributaries only)
        return []

    def _get_upstream_features(self, feature) -> Set:
        """
        Get all upstream features for a given feature.

        NOT YET IMPLEMENTED - Returns empty set.

        Future implementation will:
        1. Check cache
        2. Traverse graph upstream
        3. Collect all reachable features
        4. Cache results
        5. Return set of upstream features
        """
        feature_id = self._get_feature_id(feature)

        # Check cache
        if feature_id in self.enrichment_cache:
            self.cache_hits += 1
            return self.enrichment_cache[feature_id]

        # MVP: No graph traversal
        upstream = set()

        # Cache the result
        self.enrichment_cache[feature_id] = upstream

        return upstream

    def _get_feature_id(self, feature) -> str:
        """Extract unique identifier from feature."""
        if hasattr(feature, "waterbody_poly_id"):
            return str(feature.waterbody_poly_id)
        if isinstance(feature, dict):
            return str(feature.get("waterbody_poly_id", id(feature)))
        return str(id(feature))

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
