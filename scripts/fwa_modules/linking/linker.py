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
from .name_variations import ManualCorrections, NameVariation, DirectMatch, SkipEntry
from .logger_config import get_logger

logger = get_logger(__name__)


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
        self.manual_corrections = manual_corrections  # Add alias for consistency
        self.stats = Counter()
        # Track which regulation names map to which FWA features
        # Key: FWA feature ID, Value: list of (region, waterbody_key, name_verbatim) tuples
        self.feature_to_regulations: Dict[str, List[tuple]] = {}

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

        # STEP 0: Check SkipEntry first (highest priority - don't even try to link)
        if region:
            skip_entry = self.corrections.get_skip_entry(region, lookup_name)
            if skip_entry:
                if skip_entry.ignored:
                    result = LinkingResult(
                        waterbody_key=waterbody_key,
                        status=LinkStatus.IGNORED,
                        error_message=f"Ignored: {skip_entry.note}",
                        link_method="skip_entry",
                    )
                    self.stats[result.status] += 1
                    return result
                if skip_entry.not_found:
                    result = LinkingResult(
                        waterbody_key=waterbody_key,
                        status=LinkStatus.NOT_IN_DATA,
                        error_message=f"Not found in FWA data: {skip_entry.note}",
                        link_method="skip_entry",
                    )
                    self.stats[result.status] += 1
                    return result

        # STEP 1: Check DirectMatch (manual ID mapping)
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
                # Use the corrected name(s) to search
                result = self._link_with_name_variation(
                    waterbody_key, name_var, region, mgmt_units
                )
                result.link_method = "name_variation"
                self.stats[result.status] += 1
                return result

        # STEP 3: Natural gazetteer search (no manual corrections)
        # Use verbatim name only (not the parsed waterbody_key)
        # Try 3 variations in order:
        # 1. Verbatim name as-is
        # 2. Verbatim with outermost brackets removed (e.g., "LAKE (Region 5)" -> "LAKE")
        # 3. Verbatim with brackets and quotes removed

        search_name = name_verbatim if name_verbatim else waterbody_key
        search_variations = [search_name]  # Always try original first

        # Generate bracket-removed variation
        if "(" in search_name and ")" in search_name:
            # Find outermost matching brackets and remove them + contents
            # e.g., "LAKE (Region 5)" -> "LAKE "
            # e.g., "LEWIS (\"Cameron\") SLOUGH" -> "LEWIS  SLOUGH"
            bracket_removed = search_name
            # Remove all (...) patterns
            import re

            bracket_removed = re.sub(r"\s*\([^)]*\)\s*", " ", bracket_removed).strip()
            # Normalize multiple spaces to single space
            bracket_removed = re.sub(r"\s+", " ", bracket_removed)
            if bracket_removed != search_name:
                search_variations.append(bracket_removed)

        # Generate brackets + quotes removed variation
        if "(" in search_name or '"' in search_name or "'" in search_name:
            # Remove brackets, then quotes
            import re

            cleaned = re.sub(r"\s*\([^)]*\)\s*", " ", search_name).strip()
            cleaned = cleaned.replace('"', "").replace("'", "")
            # Normalize multiple spaces
            cleaned = re.sub(r"\s+", " ", cleaned)
            if cleaned not in search_variations:
                search_variations.append(cleaned)

        # Try each variation in sequence
        result = None
        for variation in search_variations:
            result = self._natural_search(variation, region, mgmt_units)
            if result.status != LinkStatus.NOT_FOUND:
                break  # Found a match or ambiguous, stop searching

        result.link_method = "natural_search"
        self.stats[result.status] += 1
        # Track successful matches
        if result.status == LinkStatus.SUCCESS:
            self._record_match(result, region, waterbody_key, name_verbatim)
        return result

    def _record_match(
        self,
        result: LinkingResult,
        region: Optional[str],
        waterbody_key: str,
        name_verbatim: Optional[str],
    ):
        """Record which regulation name mapped to which FWA feature(s)."""
        # Get all matched features
        features = []
        if result.matched_feature:
            features = [result.matched_feature]
        elif result.matched_features:
            features = result.matched_features

        # Record mapping for each unique waterbody identity
        # Group streams by name AND watershed code, keep individual polygons separate
        seen_identities = set()
        for feature in features:
            # Use same identity logic as natural search deduplication, but don't group polygons by GNIS
            if (
                feature.geometry_type == "multilinestring"
                and feature.fwa_watershed_code
            ):
                # Stream: group by name AND watershed code (prevents grouping different waterbodies with same WSC)
                waterbody_name = (
                    (feature.gnis_name or "unnamed").lower().replace(" ", "_")
                )
                identity_key = f"stream_{feature.fwa_watershed_code}_{waterbody_name}"
            elif feature.waterbody_key:
                # Polygon: use waterbody_key (keeps each polygon separate)
                identity_key = f"waterbody_{feature.waterbody_key}"
            else:
                # Fallback: use fwa_id
                identity_key = f"fwa_{feature.fwa_id}"

            # Only record once per identity
            if identity_key in seen_identities:
                continue
            seen_identities.add(identity_key)

            if identity_key not in self.feature_to_regulations:
                self.feature_to_regulations[identity_key] = {
                    "feature": feature,  # Store a representative feature for display
                    "regulations": [],
                }

            # Store tuple of (region, waterbody_key, name_verbatim)
            regulation_info = (region, waterbody_key, name_verbatim or waterbody_key)
            if (
                regulation_info
                not in self.feature_to_regulations[identity_key]["regulations"]
            ):
                self.feature_to_regulations[identity_key]["regulations"].append(
                    regulation_info
                )

    def get_duplicate_mappings(self) -> Dict[str, Dict]:
        """
        Get FWA features that have multiple regulation names mapped to them.

        Returns:
            Dict mapping identity key to dict with 'feature' and 'regulations' list
            Only includes features with 2+ regulation mappings.
        """
        return {
            identity_key: data
            for identity_key, data in self.feature_to_regulations.items()
            if len(data["regulations"]) > 1
        }

    def _validate_region_mu_match(
        self, region: Optional[str], features: List[FWAFeature]
    ) -> bool:
        """
        Validate that the regulation's region matches the FWA feature's MU region.

        Args:
            region: Regulation region (e.g., "Region 5")
            features: List of FWA features to validate

        Returns:
            True if region matches MU region for ALL features, False otherwise
        """
        if not region or not features:
            return True  # Can't validate without region or features

        # Extract region number from regulation (e.g., "Region 5" -> "5")
        try:
            reg_region_num = region.split()[-1]  # Get last part after space
        except (IndexError, AttributeError):
            return True  # Can't parse region, skip validation

        # Check all features' MUs
        for feature in features:
            if not feature.mgmt_units:
                continue  # No MU data, skip this feature

            # Check if ANY of this feature's MUs match the regulation region
            # MU format: "6-1", "5-2", etc. (first part is region number)
            feature_has_matching_mu = False
            for mu in feature.mgmt_units:
                try:
                    mu_region_num = mu.split("-")[0]  # Get first part before hyphen
                    if mu_region_num == reg_region_num:
                        feature_has_matching_mu = True
                        break
                except (IndexError, AttributeError):
                    continue  # Can't parse MU, skip

            # If this feature has no MUs matching the regulation region, validation fails
            if not feature_has_matching_mu:
                return False

        return True  # All features have at least one MU matching regulation region

    def _apply_direct_match(
        self,
        waterbody_key: str,
        direct_match: DirectMatch,
    ) -> Optional[LinkingResult]:
        """Apply a DirectMatch (pure ID lookup)."""
        # Lookup features by the specified ID type(s)
        # Can combine multiple ID types for mixed matches (e.g., polygon + streams)
        features = []

        if direct_match.gnis_id:
            # Search by GNIS ID - should return all polygons with this GNIS
            found = self.gazetteer.search_by_gnis_id(direct_match.gnis_id)
            logger.debug(
                f"Direct match by GNIS {direct_match.gnis_id} for '{waterbody_key}' found {len(found)} features"
            )
            features.extend(found)

        if direct_match.gnis_ids:
            # Search by multiple GNIS IDs - for regulations covering multiple features
            for gnis_id in direct_match.gnis_ids:
                found = self.gazetteer.search_by_gnis_id(gnis_id)
                logger.debug(
                    f"Direct match by GNIS {gnis_id} for '{waterbody_key}' found {len(found)} features"
                )
                features.extend(found)

        if direct_match.fwa_watershed_code:
            # Search by watershed code - should return all stream segments
            features.extend(
                self.gazetteer.search_by_watershed_code(direct_match.fwa_watershed_code)
            )

        if direct_match.fwa_watershed_codes:
            # Search by multiple watershed codes - for ambiguous creek candidates
            for watershed_code in direct_match.fwa_watershed_codes:
                features.extend(self.gazetteer.search_by_watershed_code(watershed_code))

        if direct_match.waterbody_poly_id:
            # Lookup specific polygon by WATERBODY_POLY_ID (most precise)
            feature = self.gazetteer.get_polygon_by_id(direct_match.waterbody_poly_id)
            if feature:
                features.append(feature)

        if direct_match.waterbody_poly_ids:
            # Lookup multiple specific polygons by WATERBODY_POLY_ID
            for poly_id in direct_match.waterbody_poly_ids:
                feature = self.gazetteer.get_polygon_by_id(poly_id)
                if feature:
                    features.append(feature)

        if direct_match.waterbody_key:
            # Lookup all polygons with this waterbody_key (may be multiple)
            found = self.gazetteer.get_waterbody_by_key(direct_match.waterbody_key)
            features.extend(found)

        if direct_match.waterbody_keys:
            # Lookup multiple waterbody_keys
            for key in direct_match.waterbody_keys:
                found = self.gazetteer.get_waterbody_by_key(key)
                features.extend(found)

        if direct_match.linear_feature_ids:
            # Lookup specific stream segments
            for lfid in direct_match.linear_feature_ids:
                feature = self.gazetteer.get_stream_by_id(lfid)
                if feature:
                    features.append(feature)

        if direct_match.unmarked_waterbody_id:
            # Lookup unmarked waterbody from manual corrections
            unmarked_waterbody = self.manual_corrections.get_unmarked_waterbody(
                direct_match.unmarked_waterbody_id
            )
            if unmarked_waterbody:
                # Convert unmarked waterbody to a feature-like object
                # Create appropriate Shapely geometry based on type
                from shapely.geometry import Point, LineString, Polygon

                if unmarked_waterbody.geometry_type == "point":
                    geometry = Point(unmarked_waterbody.coordinates)
                elif unmarked_waterbody.geometry_type == "linestring":
                    geometry = LineString(unmarked_waterbody.coordinates)
                elif unmarked_waterbody.geometry_type == "polygon":
                    # coordinates is a list of rings, first is exterior
                    geometry = Polygon(
                        unmarked_waterbody.coordinates[0],
                        (
                            unmarked_waterbody.coordinates[1:]
                            if len(unmarked_waterbody.coordinates) > 1
                            else None
                        ),
                    )
                else:
                    raise ValueError(
                        f"Unsupported unmarked waterbody geometry type: {unmarked_waterbody.geometry_type}"
                    )

                feature = type(
                    "obj",
                    (object,),
                    {
                        "fwa_id": unmarked_waterbody.unmarked_waterbody_id,
                        "name": unmarked_waterbody.name,
                        "gnis_name": unmarked_waterbody.name,
                        "gnis_id": None,
                        "gnis_name_2": None,
                        "gnis_id_2": None,
                        "geometry": geometry,
                        "geometry_type": unmarked_waterbody.geometry_type,
                        "region": unmarked_waterbody.region,
                        "mgmt_units": unmarked_waterbody.mgmt_units,
                        "waterbody_key": None,
                        "fwa_watershed_code": None,
                        "matched_via": None,
                        "is_unmarked_waterbody": True,
                        "unmarked_waterbody_note": unmarked_waterbody.note,
                        "unmarked_waterbody_source_url": unmarked_waterbody.source_url,
                    },
                )()
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
            logger.debug(
                f"Direct match for '{waterbody_key}' returning {len(features)} features as SUCCESS"
            )
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
        if "williston" in name.lower():
            logger.debug(
                f"Natural search for '{name}' (region={region}) found {len(matches)} initial matches"
            )

        # If no matches and region was specified, try searching all regions
        # (allows cross-region matches for boundary issues)
        if not matches and region:
            matches = self.gazetteer.search(name, region=None)

        # Filter out KML points - not needed for linking
        matches = [m for m in matches if m.geometry_type != "point"]

        # Deduplicate by waterbody identity
        # - For streams: group by fwa_watershed_code (multiple segments = one stream)
        # - For lakes: group by gnis_id (multiple polygons = one lake)
        # - For polygons with waterbody_key: group by waterbody_key
        # - For others: use fwa_id
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
                # Polygon with waterbody_key but no GNIS: group by waterbody_key
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

        # Filter by MU overlap if provided
        if mgmt_units:
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
            # Single unique identity found
            # Check if this identity has multiple physical features (e.g., multiple polygons)
            identity_key = list(identity_groups.keys())[0]
            identity_type = identity_key[0]
            all_features_in_group = identity_groups[identity_key]

            # Validate that regulation region matches FWA feature's MU region
            if not self._validate_region_mu_match(region, all_features_in_group):
                # Region/MU mismatch - mark as AMBIGUOUS
                return LinkingResult(
                    waterbody_key=name,
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_features_in_group,
                    error_message=f"Regulation region {region} doesn't match feature MU region",
                )

            # For streams: multiple segments with same watershed code = SUCCESS (it's one stream)
            # For GNIS polygons: multiple polygons with same GNIS = AMBIGUOUS (need direct match to confirm)
            # For waterbody_key/fwa_id: single feature = SUCCESS
            if identity_type == "gnis" and len(all_features_in_group) > 1:
                # Multiple polygons with same GNIS ID without direct match = AMBIGUOUS
                # Requires manual disambiguation via DIRECT_MATCHES
                logger.debug(
                    f"Natural search found {len(all_features_in_group)} polygons with GNIS {identity_key[1]} for '{name}'"
                )
                return LinkingResult(
                    waterbody_key=name,
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_features_in_group,
                    error_message=f"Found {len(all_features_in_group)} polygons with same GNIS ID (needs direct match)",
                )
            elif identity_type == "stream":
                # Multiple segments of same stream = SUCCESS with all segments
                return LinkingResult(
                    waterbody_key=name,
                    status=LinkStatus.SUCCESS,
                    matched_features=all_features_in_group,
                    matched_name=(name if is_variation else None),
                )
            else:
                # Single feature (or single waterbody_key/fwa_id group) = SUCCESS
                return LinkingResult(
                    waterbody_key=name,
                    status=LinkStatus.SUCCESS,
                    matched_feature=unique_matches[0],
                    matched_name=(name if is_variation else None),
                )
        else:
            # Multiple matches - check region/MU consistency first
            all_candidates = []
            for identity_matches in identity_groups.values():
                all_candidates.extend(identity_matches)

            # Validate that regulation region matches FWA feature's MU region
            if not self._validate_region_mu_match(region, all_candidates):
                # Region/MU mismatch - mark as AMBIGUOUS with specific error
                return LinkingResult(
                    waterbody_key=name,
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_candidates,
                    error_message=f"Found {len(unique_matches)} candidates, but regulation region {region} doesn't match feature MU region",
                )

            # Multiple matches - AMBIGUOUS
            # Include ALL matches (not just representatives) for comprehensive MU data
            return LinkingResult(
                waterbody_key=name,
                status=LinkStatus.AMBIGUOUS,
                candidate_features=all_candidates,
                error_message=f"Found {len(unique_matches)} candidates",
            )

    def apply_polygon_match(
        self,
        name_verbatim: str,
        identity_type: str,
    ) -> LinkingResult:
        """
        Apply polygon-based matching for administrative areas.

        NOT YET IMPLEMENTED - Future feature for handling:
        - Provincial parks (e.g., "KIKOMUN CREEK PARK")
        - Watersheds (e.g., "MISSION CREEK WATERSHED")
        - Conservation areas

        Would work by:
        1. Load polygon boundary for the park/watershed
        2. Spatial query to find all waterbodies within boundary
        3. Apply optional inclusion/exclusion lists
        4. Return all matching features

        Args:
            name_verbatim: Exact verbatim name from regulation text (e.g., "KIKOMUN CREEK PARK")
            identity_type: "ADMINISTRATIVE_AREA" or similar

        Returns:
            LinkingResult with matched features

        Raises:
            NotImplementedError: This feature is not yet implemented
        """
        raise NotImplementedError(
            f"Polygon matching for administrative areas not yet implemented. "
            f"Attempted to match: {name_verbatim} (type: {identity_type}). "
            f"Current workaround: Add explicit feature lists to DirectMatch in name_variations.py"
        )

    def get_stats(self) -> Dict[str, int]:
        """Get linking statistics."""
        return dict(self.stats)

    def reset_stats(self):
        """Reset statistics counter."""
        self.stats.clear()
