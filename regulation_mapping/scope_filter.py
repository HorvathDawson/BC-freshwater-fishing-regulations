"""
ScopeFilter - Applies spatial scope constraints to filter features

For MVP, all scope types except WHOLE_SYSTEM fall back to returning all features.
This provides safe defaults while landmark/polygon data is being developed.

Future implementation will add:
- DIRECTIONAL: upstream/downstream of landmarks
- SEGMENT: between two landmarks
- BUFFER: within distance of landmark
- NAMED_PART: specific named regions/zones
- VAGUE: ambiguous spatial references
"""

from typing import List, Dict, Optional

from .logger_config import get_logger

logger = get_logger(__name__)


class ScopeFilter:
    """
    Applies spatial scope constraints to filter features.

    Current implementation (MVP):
    - WHOLE_SYSTEM: Returns all features (implemented)
    - TRIBUTARIES_ONLY: Not handled here (RegulationMapper handles it)
    - All other types: Fall back to WHOLE_SYSTEM (not yet implemented)

    Note: TRIBUTARIES_ONLY is handled by RegulationMapper, not ScopeFilter.
    It's a relationship filter (get tributaries) not a spatial filter.
    This allows the system to run and produce valid (though overly broad) results
    while landmark/polygon data is being collected.
    """

    def __init__(self, graph=None, landmarks=None):
        """
        Initialize ScopeFilter.

        Args:
            graph: FWA stream network graph (not used in MVP)
            landmarks: Landmark lookup table (not used in MVP)
        """
        self.graph = graph
        self.landmarks = landmarks or {}

        # Track scope types encountered for reporting
        self.scope_types_seen = set()
        self.fallback_count = 0

    def apply_scope(self, features: List, scope: Dict) -> List:
        """
        Main entry point - filters features by spatial constraint.

        MVP Implementation: Only WHOLE_SYSTEM is implemented.
        TRIBUTARIES_ONLY is handled by RegulationMapper, not here.
        All other scope types fall back to returning all features.

        Args:
            features: List of FWA features to filter
            scope: Scope object (dict with 'type', 'location_verbatim', etc.)

        Returns:
            Filtered feature list (MVP: usually returns all features)
        """
        if not features:
            return []

        scope_type = scope.get("type", "WHOLE_SYSTEM")
        self.scope_types_seen.add(scope_type)

        # WHOLE_SYSTEM: Return all features (implemented)
        if scope_type == "WHOLE_SYSTEM":
            logger.debug(f"WHOLE_SYSTEM scope - returning all {len(features)} features")
            return features

        # All other scope types: Fall back to WHOLE_SYSTEM
        self.fallback_count += 1
        logger.debug(
            f"Scope type '{scope_type}' not yet implemented - "
            f"falling back to WHOLE_SYSTEM ({len(features)} features)"
        )

        return features

    def filter_directional(self, features: List, scope: Dict) -> List:
        """
        Filter by upstream/downstream of landmark.

        NOT YET IMPLEMENTED - Falls back to WHOLE_SYSTEM.

        Future implementation will:
        1. Resolve landmark from scope.landmark_verbatim
        2. Filter features by downstream_measure
        3. Handle UPSTREAM vs DOWNSTREAM direction
        """
        logger.debug("DIRECTIONAL scope not yet implemented - returning all features")
        return features

    def filter_segment(self, features: List, scope: Dict) -> List:
        """
        Filter to segment between two landmarks.

        NOT YET IMPLEMENTED - Falls back to WHOLE_SYSTEM.

        Future implementation will:
        1. Resolve start and end landmarks
        2. Filter features between landmark positions
        3. Handle BETWEEN direction
        """
        logger.debug("SEGMENT scope not yet implemented - returning all features")
        return features

    def filter_buffer(self, features: List, scope: Dict) -> List:
        """
        Filter by distance buffer around landmark.

        NOT YET IMPLEMENTED - Falls back to WHOLE_SYSTEM.

        Future implementation will:
        1. Resolve landmark location
        2. Create buffer polygon
        3. Filter features within buffer
        """
        logger.debug("BUFFER scope not yet implemented - returning all features")
        return features

    def filter_named_part(self, features: List, scope: Dict) -> List:
        """
        Filter to specific named regions/zones.

        NOT YET IMPLEMENTED - Falls back to WHOLE_SYSTEM.

        Future implementation will:
        1. Look up named region by ID
        2. Filter to features in region
        3. Handle spatial polygon definitions
        """
        logger.debug("NAMED_PART scope not yet implemented - returning all features")
        return features

    def filter_vague(self, features: List, scope: Dict) -> List:
        """
        Handle ambiguous spatial references.

        NOT YET IMPLEMENTED - Falls back to WHOLE_SYSTEM.

        Future implementation will:
        1. Parse vague location description
        2. Apply best-effort spatial filtering
        3. Flag for manual review
        """
        logger.debug("VAGUE scope not yet implemented - returning all features")
        return features

    def get_stats(self) -> Dict:
        """
        Return statistics about scope filtering.

        Returns:
            Dict with scope types seen and fallback counts
        """
        return {
            "scope_types_seen": sorted(list(self.scope_types_seen)),
            "fallback_count": self.fallback_count,
        }

    def reset_stats(self):
        """Reset statistics counters."""
        self.scope_types_seen = set()
        self.fallback_count = 0
