"""
Waterbody Linker - Business logic for linking regulation names to FWA features.

Handles the linking workflow:
1. Check DirectMatch (highest priority)
2. Check NameVariation
3. Natural gazetteer search

Tracks statistics and provides clean interface for test coverage script.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict
from enum import Enum
from collections import Counter

from .metadata_gazetteer import MetadataGazetteer, FWAFeature
from .name_variations import ManualCorrections, NameVariation, DirectMatch


class LinkStatus(Enum):
    """Status of a linking attempt."""

    SUCCESS = "success"  # Single feature matched
    AMBIGUOUS = "ambiguous"  # Multiple features found (ambiguous)
    NOT_FOUND = "not_found"  # No features found
    NOT_IN_DATA = "not_in_data"  # Searched but doesn't exist in FWA data
    IGNORED = "ignored"  # Manually marked to ignore
    ERROR = "error"  # Linking process error


@dataclass
class LinkingResult:
    """Result of attempting to link a waterbody name to FWA feature(s)."""

    waterbody_key: str
    status: LinkStatus
    matched_feature: Optional[FWAFeature] = None  # Single match
    matched_features: List[FWAFeature] = (
        None  # Multiple successful matches (e.g., split entries)
    )
    candidate_features: List[FWAFeature] = None  # Ambiguous candidates
    error_message: Optional[str] = None
    link_method: Optional[str] = (
        None  # "direct_match", "name_variation", "natural_search"
    )
    matched_name: Optional[str] = (
        None  # The actual name that matched (for name variations)
    )

    def __post_init__(self):
        if self.matched_features is None:
            self.matched_features = []
        if self.candidate_features is None:
            self.candidate_features = []


class WaterbodyLinker:
    """
    Links regulation waterbody names to FWA features.

    Workflow:
    1. Check DirectMatch (manual ID mappings)
    2. Check NameVariation (name corrections)
    3. Natural gazetteer search

    Tracks statistics for reporting.
    """

    def __init__(
        self,
        gazetteer: MetadataGazetteer,
        manual_corrections: ManualCorrections,
    ):
        self.gazetteer = gazetteer
        self.corrections = manual_corrections
        self.stats = Counter()

    def link_waterbody(
        self,
        waterbody_key: str,
        region: Optional[str] = None,
        mgmt_units: Optional[List[str]] = None,
        name_verbatim: Optional[str] = None,
    ) -> LinkingResult:
        """
        Link a waterbody name to FWA feature(s).

        Args:
            waterbody_key: Waterbody name from regulation (primary key)
            region: Region identifier (e.g., "Region 1")
            mgmt_units: Management units from regulation
            name_verbatim: Exact verbatim name from regulation text

        Returns:
            LinkingResult with status and matched feature(s)
        """
        lookup_name = name_verbatim if name_verbatim else waterbody_key

        # STEP 1: Check DirectMatch (highest priority - manual ID mapping)
        if region:
            direct_match = self.corrections.get_direct_match(region, lookup_name)
            if direct_match:
                result = self._apply_direct_match(waterbody_key, direct_match)
                if result:
                    result.link_method = "direct_match"
                    self.stats[result.status] += 1
                    return result

        # STEP 2: Check NameVariation (manual name corrections)
        if region:
            name_var = self.corrections.get_name_variation(region, lookup_name)
            if name_var:
                if name_var.ignored:
                    result = LinkingResult(
                        waterbody_key=waterbody_key,
                        status=LinkStatus.IGNORED,
                        error_message=f"Ignored: {name_var.note}",
                        link_method="name_variation",
                    )
                    self.stats[result.status] += 1
                    return result
                # Check if this is marked as not found (searched but no data)
                if name_var.not_found:
                    result = LinkingResult(
                        waterbody_key=waterbody_key,
                        status=LinkStatus.NOT_IN_DATA,
                        error_message=f"Not found in FWA data: {name_var.note}",
                        link_method="name_variation",
                    )
                    self.stats[result.status] += 1
                    return result
                # Use the corrected name(s) to search
                result = self._link_with_name_variation(
                    waterbody_key, name_var, region, mgmt_units
                )
                result.link_method = "name_variation"
                self.stats[result.status] += 1
                return result

        # STEP 3: Natural gazetteer search (no manual corrections)
        # Try waterbody_key first (cleaned name), then fallback to name_verbatim if no results
        result = self._natural_search(waterbody_key, region, mgmt_units)

        # If no matches found and we have a different verbatim name, try that as fallback
        if (
            result.status == LinkStatus.NOT_FOUND
            and name_verbatim
            and name_verbatim.lower() != waterbody_key.lower()
        ):
            result = self._natural_search(name_verbatim, region, mgmt_units)

        result.link_method = "natural_search"
        self.stats[result.status] += 1
        return result

    def _apply_direct_match(
        self,
        waterbody_key: str,
        direct_match: DirectMatch,
    ) -> Optional[LinkingResult]:
        """Apply a DirectMatch (pure ID lookup)."""
        if direct_match.ignored:
            return LinkingResult(
                waterbody_key=waterbody_key,
                status=LinkStatus.IGNORED,
                error_message=f"Ignored (DirectMatch): {direct_match.note}",
            )

        if direct_match.not_found:
            return LinkingResult(
                waterbody_key=waterbody_key,
                status=LinkStatus.NOT_IN_DATA,
                error_message=f"Not found in FWA data (DirectMatch): {direct_match.note}",
            )

        # Lookup features by the specified ID type(s)
        # Can combine multiple ID types for mixed matches (e.g., polygon + streams)
        features = []

        if direct_match.gnis_id:
            # Search by GNIS ID - should return all polygons with this GNIS
            features.extend(self.gazetteer.search_by_gnis_id(direct_match.gnis_id))

        if direct_match.fwa_watershed_code:
            # Search by watershed code - should return all stream segments
            features.extend(
                self.gazetteer.search_by_watershed_code(direct_match.fwa_watershed_code)
            )

        if direct_match.waterbody_key:
            # Lookup specific polygon by waterbody_key
            feature = self.gazetteer.get_waterbody_by_key(direct_match.waterbody_key)
            if feature:
                features.append(feature)

        if direct_match.waterbody_keys:
            # Lookup multiple specific polygons
            for key in direct_match.waterbody_keys:
                feature = self.gazetteer.get_waterbody_by_key(key)
                if feature:
                    features.append(feature)

        if direct_match.linear_feature_ids:
            # Lookup specific stream segments
            for lfid in direct_match.linear_feature_ids:
                feature = self.gazetteer.get_stream_by_id(lfid)
                if feature:
                    features.append(feature)

        if len(features) == 0:
            return LinkingResult(
                waterbody_key=waterbody_key,
                status=LinkStatus.NOT_FOUND,
                error_message=f"DirectMatch ID not found in gazetteer: {direct_match.note}",
            )
        elif len(features) == 1:
            return LinkingResult(
                waterbody_key=waterbody_key,
                status=LinkStatus.SUCCESS,
                matched_feature=features[0],
            )
        else:
            # Multiple features (e.g., lake with multiple polygons, or stream with many segments)
            # This is SUCCESS - DirectMatch explicitly maps to all these features
            return LinkingResult(
                waterbody_key=waterbody_key,
                status=LinkStatus.SUCCESS,
                matched_features=features,
            )

    def _link_with_name_variation(
        self,
        waterbody_key: str,
        name_var: NameVariation,
        region: Optional[str],
        mgmt_units: Optional[List[str]],
    ) -> LinkingResult:
        """Link using corrected name(s) from NameVariation."""
        # Handle multi-waterbody case (e.g., "RIVER A AND RIVER B" -> ["river a", "river b"])
        if len(name_var.target_names) > 1:
            # Search for each target and combine results
            all_features = []
            for target in name_var.target_names:
                features = self.gazetteer.search(target, region=region)
                all_features.extend(features)

            if len(all_features) == len(name_var.target_names):
                # Found all expected waterbodies - this is SUCCESS
                return LinkingResult(
                    waterbody_key=waterbody_key,
                    status=LinkStatus.SUCCESS,
                    matched_features=all_features,
                    matched_name=", ".join(
                        name_var.target_names
                    ),  # Track which names were used
                )
            else:
                return LinkingResult(
                    waterbody_key=waterbody_key,
                    status=LinkStatus.NOT_FOUND,
                    error_message=f"Found {len(all_features)}/{len(name_var.target_names)} expected waterbodies",
                )

        # Single target name
        target_name = name_var.target_names[0]
        result = self._natural_search(
            target_name, region, mgmt_units, is_variation=True
        )
        # Set matched_name if this was a variation
        if result.status == LinkStatus.SUCCESS:
            result.matched_name = target_name
        return result

    def _natural_search(
        self,
        name: str,
        region: Optional[str],
        mgmt_units: Optional[List[str]],
        is_variation: bool = False,
    ) -> LinkingResult:
        """Search gazetteer naturally by name."""
        # Try searching with region filter first
        matches = self.gazetteer.search(name, region=region)

        # If no matches and region was specified, try searching all regions
        # (allows cross-region matches for boundary issues)
        if not matches and region:
            matches = self.gazetteer.search(name, region=None)

        # Deduplicate by waterbody identity
        # - For streams: group by fwa_watershed_code (multiple segments = one stream)
        # - For lakes: group by gnis_id (multiple polygons = one lake)
        # - For polygons with waterbody_key: group by waterbody_key (point + polygon = one waterbody)
        # - For KML points and others: use fwa_id
        # Keep ALL matches (not just representatives) for comprehensive MU data
        unique_matches = []
        seen_identities = set()
        identity_groups = {}  # Track all matches per identity

        for match in matches:
            # Determine unique identity for this waterbody
            if match.geometry_type == "multilinestring" and match.fwa_watershed_code:
                # Stream: use watershed code
                identity = ("stream", match.fwa_watershed_code)
            elif match.gnis_id:
                # Lake/wetland/manmade with GNIS: use GNIS ID
                # This groups all polygons of the same lake (multiple polygons with same GNIS = 1 lake)
                identity = ("gnis", match.gnis_id)
            elif match.waterbody_key:
                # Polygon or point with waterbody_key but no GNIS: group by waterbody_key
                # This ensures KML points (which don't have GNIS) can be matched
                identity = ("waterbody_key", match.waterbody_key)
            else:
                # No GNIS/waterbody_key: use fwa_id
                identity = ("fwa_id", match.fwa_id)

            # Store all matches for this identity (for comprehensive MU data in candidates)
            if identity not in identity_groups:
                identity_groups[identity] = []
            identity_groups[identity].append(match)

            # Track unique identities for counting distinct waterbodies
            if identity not in seen_identities:
                seen_identities.add(identity)
                unique_matches.append(match)  # Keep first representative

        # Filter by MU overlap if provided (skip for KML points - trust human labels)
        all_kml_points = all(m.geometry_type == "point" for m in unique_matches)
        if mgmt_units and not all_kml_points:
            # For each identity group, check if ANY segment/polygon has overlapping MUs
            # (important for streams where first segment might be in different MU than others)
            mu_filtered = []
            for representative in unique_matches:
                # Get identity for this representative (must match deduplication logic)
                if (
                    representative.geometry_type == "multilinestring"
                    and representative.fwa_watershed_code
                ):
                    identity = ("stream", representative.fwa_watershed_code)
                elif representative.gnis_id:
                    identity = ("gnis", representative.gnis_id)
                elif representative.waterbody_key:
                    identity = ("waterbody_key", representative.waterbody_key)
                else:
                    identity = ("fwa_id", representative.fwa_id)

                # Check if ANY feature in this identity group has overlapping MUs
                group_matches = identity_groups.get(identity, [])
                has_overlap = any(
                    f.mgmt_units and any(mu in f.mgmt_units for mu in mgmt_units)
                    for f in group_matches
                )

                if has_overlap:
                    mu_filtered.append(representative)

            if mu_filtered:
                unique_matches = mu_filtered
            elif unique_matches:
                # Name matched but wrong MU area - include all matches for comprehensive data
                all_candidates = []
                for identity_matches in identity_groups.values():
                    all_candidates.extend(identity_matches)

                return LinkingResult(
                    waterbody_key=name,
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_candidates,
                    error_message=f"Name matched but MUs don't overlap. Regulation MUs: {mgmt_units}",
                )

        # Return result based on match count
        if len(unique_matches) == 0:
            return LinkingResult(
                waterbody_key=name,
                status=LinkStatus.NOT_FOUND,
                error_message="No matches found in gazetteer",
            )
        elif len(unique_matches) == 1:
            return LinkingResult(
                waterbody_key=name,
                status=LinkStatus.SUCCESS,
                matched_feature=unique_matches[0],
                matched_name=(
                    name if is_variation else None
                ),  # Track if matched via variation
            )
        else:
            # Multiple matches - AMBIGUOUS
            # Include ALL matches (not just representatives) for comprehensive MU data
            all_candidates = []
            for identity_matches in identity_groups.values():
                all_candidates.extend(identity_matches)

            return LinkingResult(
                waterbody_key=name,
                status=LinkStatus.AMBIGUOUS,
                candidate_features=all_candidates,
                error_message=f"Found {len(unique_matches)} candidates",
            )

    def get_stats(self) -> Dict[str, int]:
        """Get linking statistics."""
        return dict(self.stats)

    def reset_stats(self):
        """Reset statistics counter."""
        self.stats.clear()
