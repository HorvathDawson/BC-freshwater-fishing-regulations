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

from fwa_pipeline.metadata_gazetteer import MetadataGazetteer, FWAFeature
from .linking_corrections import (
    ManualCorrections,
    NameVariation,
    DirectMatch,
    AdminDirectMatch,
    SkipEntry,
)
from .logger_config import get_logger

logger = get_logger(__name__)


class LinkStatus(Enum):
    """Status of a linking attempt."""

    SUCCESS = "success"  # Feature(s) matched
    ADMIN_MATCH = "admin_match"  # Admin boundary match (features resolved via spatial intersection in mapper)
    AMBIGUOUS = "ambiguous"  # Multiple features found (ambiguous)
    NOT_FOUND = "not_found"  # No features found
    NOT_IN_DATA = "not_in_data"  # Searched but doesn't exist in FWA data
    IGNORED = "ignored"  # Manually marked to ignore
    ERROR = "error"  # Linking process error


@dataclass
class LinkingResult:
    """Result of attempting to link a waterbody name to FWA feature(s)."""

    status: LinkStatus
    matched_features: List[FWAFeature] = (
        None  # Multiple successful matches (e.g., split entries)
    )
    candidate_features: List[FWAFeature] = None  # Ambiguous candidates
    link_method: Optional[str] = (
        None  # "direct_match", "name_variation", "natural_search", "admin_direct_match"
    )
    matched_name: Optional[str] = (
        None  # The actual name that matched (for name variations)
    )
    error_message: Optional[str] = None
    admin_match: Optional[AdminDirectMatch] = (
        None  # Set when link_method == "admin_direct_match"
    )
    additional_info: Optional[str] = (
        None  # Extra note text from linking corrections, injected as a "Note" rule
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
        # Track which regulation names map to which FWA features
        # Key: FWA feature ID, Value: list of (region, name_verbatim) tuples
        self.feature_to_regulations: Dict[str, List[tuple]] = {}

    def link_waterbody(
        self,
        region: Optional[str] = None,
        mgmt_units: Optional[List[str]] = None,
        name_verbatim: Optional[str] = None,
    ) -> LinkingResult:
        """
        Link a waterbody name to FWA feature(s).

        Args:
            region: Region identifier (e.g., "Region 1" or "1")
            mgmt_units: Management units from regulation
            name_verbatim: Exact verbatim name from regulation text

        Returns:
            LinkingResult with status and matched feature(s)
        """
        lookup_name = name_verbatim

        # Extract zone number from region (e.g., "Region 4" -> "4", or "4" -> "4")
        zone_number = None
        if region:
            if region.lower().startswith("region "):
                zone_number = region.split()[-1]  # "Region 4" -> "4"
            else:
                zone_number = region  # Already a zone number

        # STEP 0: Check SkipEntry first (highest priority - don't even try to link)
        if region:
            skip_entry = self.corrections.get_skip_entry(region, lookup_name)
            if skip_entry:
                if skip_entry.ignored:
                    result = LinkingResult(
                        status=LinkStatus.IGNORED,
                        error_message=f"Ignored: {skip_entry.note}",
                        link_method="skip_entry",
                    )
                    self.stats[result.status] += 1
                    return result
                if skip_entry.not_found:
                    result = LinkingResult(
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
                result = self._apply_direct_match(lookup_name, direct_match)
                if result:
                    result.link_method = "direct_match"
                    result.additional_info = direct_match.additional_info
                    self.stats[result.status] += 1
                    return result

        # STEP 1b: Check AdminDirectMatch (admin boundary polygon matching)
        if region:
            admin_match = self.corrections.get_admin_direct_match(region, lookup_name)
            if admin_match:
                # Validate that the admin match has search criteria
                if (
                    admin_match.feature_ids is None
                    and admin_match.feature_names is None
                ):
                    result = LinkingResult(
                        status=LinkStatus.ERROR,
                        matched_features=[],
                        link_method="admin_direct_match",
                        error_message=f"AdminDirectMatch configured but feature_ids=None (need to fill in admin IDs)",
                    )
                    result.admin_match = admin_match
                    self.stats[result.status] += 1
                    return result

                result = LinkingResult(
                    status=LinkStatus.ADMIN_MATCH,
                    matched_features=[],
                    link_method="admin_direct_match",
                    error_message=None,
                    additional_info=admin_match.additional_info,
                )
                result.admin_match = admin_match
                self.stats[result.status] += 1
                return result

        # STEP 2: Check NameVariation (manual name corrections)
        if region:
            name_var = self.corrections.get_name_variation(region, lookup_name)
            if name_var:
                # Use the corrected name(s) to search
                result = self._link_with_name_variation(
                    name_var, zone_number, mgmt_units
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

        search_name = name_verbatim
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
            result = self._natural_search(variation, zone_number, mgmt_units)
            if result.status != LinkStatus.NOT_FOUND:
                break  # Found a match or ambiguous, stop searching

        result.link_method = "natural_search"
        self.stats[result.status] += 1
        # Track successful matches
        if result.status == LinkStatus.SUCCESS:
            self._record_match(result, region, name_verbatim)
        return result

    def _record_match(
        self,
        result: LinkingResult,
        region: Optional[str],
        name_verbatim: Optional[str],
    ):
        """Record which regulation name mapped to which FWA feature(s)."""
        # Get all matched features
        features = result.matched_features or []

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

            # Store tuple of (region, name_verbatim)
            regulation_info = (region, name_verbatim)
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

        # Extract region number from regulation (e.g., "Region 5" -> "5", "Region 7A" -> "7A")
        try:
            reg_region_num = region.split()[-1]  # Get last part after space
        except (IndexError, AttributeError):
            return True  # Can't parse region, skip validation

        # For MU comparison, use only the numeric portion of the region
        # (e.g., "7A" -> "7") since MU IDs always use the numeric prefix ("7-55")
        reg_region_num_digits = "".join(c for c in reg_region_num if c.isdigit())

        # Check all features' MUs
        for feature in features:
            if not feature.mgmt_units:
                continue  # No MU data, skip this feature

            # Check if ANY of this feature's MUs match the regulation region
            # MU format: "6-1", "5-2", "7-58", etc. (first part is region number)
            feature_has_matching_mu = False
            for mu in feature.mgmt_units:
                try:
                    mu_region_num = mu.split("-")[0]  # Get first part before hyphen
                    if mu_region_num == reg_region_num_digits:
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
        name_verbatim: str,
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
                f"Direct match by GNIS {direct_match.gnis_id} for '{name_verbatim}' found {len(found)} features"
            )
            features.extend(found)

        if direct_match.gnis_ids:
            # Search by multiple GNIS IDs - for regulations covering multiple features
            for gnis_id in direct_match.gnis_ids:
                found = self.gazetteer.search_by_gnis_id(gnis_id)
                logger.debug(
                    f"Direct match by GNIS {gnis_id} for '{name_verbatim}' found {len(found)} features"
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

        if direct_match.blue_line_key:
            # Search by blue line key
            features.extend(
                self.gazetteer.search_by_blue_line_key(direct_match.blue_line_key)
            )

        if direct_match.blue_line_keys:
            # Search by multiple blue line keys
            for blk in direct_match.blue_line_keys:
                features.extend(self.gazetteer.search_by_blue_line_key(blk))

        if direct_match.unmarked_waterbody_id:
            # Lookup unmarked waterbody from manual corrections
            unmarked_waterbody = self.corrections.get_unmarked_waterbody(
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
                        "feature_type": "unmarked",  # Mark as unmarked waterbody type
                        "zones": unmarked_waterbody.zones,
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
                status=LinkStatus.NOT_FOUND,
                error_message=f"DirectMatch ID not found in gazetteer: {direct_match.note}",
            )
        else:
            # Multiple features (e.g., lake with multiple polygons, or stream with many segments)
            # This is SUCCESS - DirectMatch explicitly maps to all these features
            logger.debug(
                f"Direct match for '{name_verbatim}' returning {len(features)} features as SUCCESS"
            )
            # update features matched_via for tracking
            for feature in features:
                feature.matched_via = f"direct_match ({direct_match.note})"
            return LinkingResult(
                status=LinkStatus.SUCCESS,
                matched_features=features,
            )

    def _link_with_name_variation(
        self,
        name_var: NameVariation,
        zone_number: Optional[str],
        mgmt_units: Optional[List[str]],
    ) -> LinkingResult:
        """Link using corrected name(s) from NameVariation."""

        # Handle multi-waterbody case (e.g., "RIVER A AND RIVER B" -> ["river a", "river b"])
        if len(name_var.target_names) > 1:
            all_features = []
            successful_targets = 0

            # Use _natural_search for every target name to benefit from dedup/MU filtering
            for target in name_var.target_names:
                res = self._natural_search(
                    target, zone_number, mgmt_units, is_variation=True
                )
                if res.status == LinkStatus.SUCCESS:
                    successful_targets += 1
                    all_features.extend(res.matched_features)

            # We count successful targets instead of raw features so multiple stream segments
            # won't falsely fail the length validation check.
            if successful_targets == len(name_var.target_names):
                # Enforce consistent attribution on matched features
                for feature in all_features:
                    feature.matched_via = f"name_variation ({name_var.note})"

                return LinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=all_features,
                    matched_name=", ".join(
                        name_var.target_names
                    ),  # Track which names were used
                )
            else:
                return LinkingResult(
                    status=LinkStatus.NOT_FOUND,
                    error_message=f"Found {successful_targets}/{len(name_var.target_names)} expected waterbodies",
                )

        # Single target name
        target_name = name_var.target_names[0]
        result = self._natural_search(
            target_name, zone_number, mgmt_units, is_variation=True
        )

        # Set matched_name and explicitly format matched_via if this was a variation
        if result.status == LinkStatus.SUCCESS:
            result.matched_name = target_name
            for feature in result.matched_features:
                feature.matched_via = f"name_variation ({name_var.note})"

        return result

    def _natural_search(
        self,
        name: str,
        zone_number: Optional[str],
        mgmt_units: Optional[List[str]],
        is_variation: bool = False,
    ) -> LinkingResult:
        """Search gazetteer naturally by name."""
        # Try searching with zone_number filter first
        matches = self.gazetteer.search(name, zone_number=zone_number)
        if "williston" in name.lower():
            logger.debug(
                f"Natural search for '{name}' (zone_number={zone_number}) found {len(matches)} initial matches"
            )

        # If no matches and zone_number was specified, try searching all zone_numbers
        # (allows cross-zone_number matches for boundary issues)
        if not matches and zone_number:
            matches = self.gazetteer.search(name, zone_number=None)

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
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_candidates,
                    error_message=f"Name matched but MUs don't overlap. Regulation MUs: {mgmt_units}",
                )

        # Return result based on match count
        if len(unique_matches) == 0:
            return LinkingResult(
                status=LinkStatus.NOT_FOUND,
                error_message="No matches found in gazetteer",
            )
        elif len(unique_matches) == 1:
            # Single unique identity found
            # Construct the correct identity_key from the SURVIVING unique match
            # (Do not use list(identity_groups.keys())[0] as it bypasses the MU filter)
            rep = unique_matches[0]
            if rep.geometry_type == "multilinestring" and rep.fwa_watershed_code:
                identity_key = ("stream", rep.fwa_watershed_code)
            elif rep.gnis_id:
                identity_key = ("gnis", rep.gnis_id)
            elif rep.waterbody_key:
                identity_key = ("waterbody_key", rep.waterbody_key)
            else:
                identity_key = ("fwa_id", rep.fwa_id)

            identity_type = identity_key[0]
            all_features_in_group = identity_groups[identity_key]

            for feature in all_features_in_group:
                feature.matched_via = (
                    f"natural_search (variation)" if is_variation else "natural_search"
                )

            # Validate that regulation region matches FWA feature's MU region
            if not self._validate_region_mu_match(zone_number, all_features_in_group):
                # Region/MU mismatch - mark as AMBIGUOUS
                return LinkingResult(
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_features_in_group,
                    error_message=f"Regulation region {zone_number} doesn't match feature MU region",
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
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_features_in_group,
                    error_message=f"Found {len(all_features_in_group)} polygons with same GNIS ID (needs direct match)",
                )
            elif identity_type == "stream":
                # Fetch the rest of the stream segments using the watershed code
                watershed_code = identity_key[1]
                if watershed_code:
                    full_stream_segments = self.gazetteer.search_by_watershed_code(
                        watershed_code
                    )

                    target_gnis_id = rep.gnis_id
                    target_name_lower = (rep.gnis_name or "").lower()

                    expanded_features = []
                    for seg in full_stream_segments:
                        # Ensure it matches GNIS ID or name to prevent collecting named side channels

                        # if "similkameen" in name.lower():
                        #     logger.warning(
                        #         f"Expanding stream segments for '{name}' with watershed code {watershed_code}"
                        #     )
                        #     logger.warning(
                        #         f"items in full stream segmentsa but not in unique matches: {[seg.fwa_id for seg in self.gazetteer.search_by_watershed_code(watershed_code) if seg not in all_features_in_group]}"
                        #     )
                        #     logger.warning(
                        #         f"unique gnis_ids in both sets: {set(seg.gnis_id for seg in full_stream_segments)} vs {set(seg.gnis_id for seg in all_features_in_group)} and gnis names: {set(seg.gnis_name for seg in full_stream_segments)} vs {set(seg.gnis_name for seg in all_features_in_group)}  "
                        #     )
                        #     exit(0)

                        if target_gnis_id and seg.gnis_id == target_gnis_id:
                            expanded_features.append(seg)
                        elif (
                            target_name_lower
                            and (seg.gnis_name or "").lower() == target_name_lower
                        ):
                            expanded_features.append(seg)
                        elif (
                            not target_gnis_id
                            and not target_name_lower
                            and not seg.gnis_name
                        ):
                            # Fallback just in case the entire stream is unnamed
                            expanded_features.append(seg)

                    if expanded_features:
                        for feature in expanded_features:
                            feature.matched_via = (
                                f"natural_search (variation)"
                                if is_variation
                                else "natural_search"
                            )
                        all_features_in_group = expanded_features

                # Multiple segments of same stream = SUCCESS with all segments
                return LinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=all_features_in_group,
                    matched_name=name,
                )
            else:
                # Single feature (or single waterbody_key/fwa_id group) = SUCCESS
                return LinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=unique_matches,
                    matched_name=name,
                )
        else:
            # Multiple matches - check region/MU consistency first
            all_candidates = []
            for identity_matches in identity_groups.values():
                all_candidates.extend(identity_matches)

            # Validate that regulation region matches FWA feature's MU region
            if not self._validate_region_mu_match(zone_number, all_candidates):
                # Region/MU mismatch - mark as AMBIGUOUS with specific error
                return LinkingResult(
                    status=LinkStatus.AMBIGUOUS,
                    candidate_features=all_candidates,
                    error_message=f"Found {len(unique_matches)} candidates, but regulation region {zone_number} doesn't match feature MU region",
                )

            # Multiple matches - AMBIGUOUS
            # Include ALL matches (not just representatives) for comprehensive MU data
            return LinkingResult(
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
        Match a waterbody by administrative area polygon intersection.

        Note: Admin area matching is now handled by AdminDirectMatch entries
        in linking_corrections.py, processed via the 'admin_direct_match' path
        in link_waterbody(). This method is retained for backward compatibility
        but should not normally be called directly.

        Args:
            name_verbatim: Exact verbatim name from regulation text
            identity_type: "ADMINISTRATIVE_AREA" or similar

        Returns:
            LinkingResult indicating admin match should be used
        """
        logger.warning(
            f"apply_polygon_match called directly for '{name_verbatim}'. "
            f"Consider adding an AdminDirectMatch entry in linking_corrections.py instead."
        )
        return LinkingResult(
            status=LinkStatus.NOT_FOUND,
            error_message=(
                f"Admin polygon matching for '{name_verbatim}' requires an "
                f"AdminDirectMatch entry in linking_corrections.py"
            ),
            link_method="polygon_match",
        )

    def get_stats(self) -> Dict[str, int]:
        """Get linking statistics."""
        return dict(self.stats)

    def reset_stats(self):
        """Reset statistics counter."""
        self.stats.clear()


# ============================================================================
# CLI Coverage Test  (python -m regulation_mapping.linker)
# ============================================================================


def _run_coverage_test():
    """
    Run waterbody linking coverage test.

    Loads parsed regulations, links each against the FWA gazetteer, and
    prints a full diagnostic report (status breakdown, region table, unused
    configs, samples, etc.).

    Optional flags:
        --export-not-found  PATH   Export NOT_FOUND entries to JSON
        --export-ambiguous  PATH   Export AMBIGUOUS entries to JSON
    """
    import argparse
    import json
    import shutil
    from pathlib import Path
    from collections import defaultdict, Counter as _Counter

    from .linking_corrections import (
        NAME_VARIATIONS,
        DIRECT_MATCHES,
        SKIP_ENTRIES,
        UNMARKED_WATERBODIES,
        ADMIN_DIRECT_MATCHES,
        ManualCorrections,
    )
    from project_config import get_config

    # --- Terminal formatting helpers ---

    RED = "\033[91m"
    YELLOW = "\033[93m"
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

    def tw(default=80):
        try:
            return shutil.get_terminal_size((default, 20)).columns
        except Exception:
            return default

    def divider(char="="):
        print(char * tw())

    def header(text):
        print()
        divider("=")
        print(text)
        divider("=")
        print()

    def sub_header(text):
        print(f"\n{text}")
        print("-" * tw())

    def extract_region(region_str):
        if not region_str or not region_str.startswith("REGION"):
            return None
        num = "".join(c for c in region_str.split("-")[0] if c.isdigit())
        return f"Region {num}" if num else None

    # --- Export helpers ---

    def _instructions_block(mode):
        if mode == "NOT_FOUND":
            return {
                "description": "NOT_FOUND waterbodies - need to add name variations",
                "how_to_fix": [
                    "1. Check spelling/formatting/renaming -> Add to NAME_VARIATIONS",
                    "2. Not in gazetteer? -> Add to DIRECT_MATCHES",
                    "3. Check 'search_terms_used' and 'location_descriptor'",
                    "4. Use management_units to narrow down",
                ],
                "example_name_variation": {
                    "TOQUART LAKE": {
                        "target_names": ["toquaht lake"],
                        "note": "Spelling",
                    }
                },
                "example_direct_match": {
                    "LONG LAKE (Nanaimo)": {"gnis_id": "17501", "note": "Disambiguate"}
                },
            }
        return {
            "description": "AMBIGUOUS waterbodies - multiple candidates found",
            "how_to_fix": [
                "1. Review candidates",
                "2. Compare MUs",
                "3. Add DIRECT_MATCH with specific ID",
            ],
            "example_direct_match": {
                "RAINBOW LAKE": {"gnis_id": "28692", "note": "Disambiguate MU"}
            },
        }

    def _export_data(items, path, lookup, mode):
        entries = []
        for item in items:
            reg, name = item["region"], item["name_verbatim"]
            full = lookup.get((reg, name), {})
            ex_var = NAME_VARIATIONS.get(reg, {}).get(name)
            ex_match = DIRECT_MATCHES.get(reg, {}).get(name)

            entry = {
                "name_verbatim": name,
                "waterbody_key": item["waterbody_key"],
                "region": reg,
                "management_units": (
                    item["mu"]
                    if mode == "NOT_FOUND"
                    else item.get("regulation_management_units")
                ),
                "identity_type": item.get("identity_type"),
                "location_descriptor": item.get("location_descriptor"),
                "alternate_names": item.get("alternate_names", []),
                "page": full.get("page"),
                "regulations_summary": full.get("regs_verbatim"),
                "full_identity": full.get("identity"),
                "existing_variation": (
                    {"target_names": ex_var.target_names, "note": ex_var.note}
                    if ex_var
                    else None
                ),
                "existing_direct_match": (
                    {"gnis_id": ex_match.gnis_id, "note": ex_match.note}
                    if ex_match
                    else None
                ),
            }

            if mode == "NOT_FOUND":
                entry["search_terms_used"] = item.get(
                    "search_terms", [item["waterbody_key"].lower()]
                )
                entry["suggested_action"] = "Add to NAME_VARIATIONS or DIRECT_MATCHES"
            else:
                entry["candidate_count"] = len(item.get("candidates", []))
                entry["candidate_waterbodies"] = item.get("candidate_details", [])
                entry["suggested_action"] = "Add to DIRECT_MATCHES with correct ID"

            entries.append(entry)

        out = {
            "_instructions": _instructions_block(mode),
            "count": len(entries),
            "entries": entries,
        }
        with open(path, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nExported {len(entries)} {mode} entries to {path}")

    def _process_ambiguous_candidates(candidates, reg_mus):
        processed = []
        for c in candidates:
            fwa_mus = c.get("management_units", [])
            match = any(mu in fwa_mus for mu in reg_mus)
            processed.append(
                {
                    "fwa_name": c.get("name"),
                    "gnis_id": c.get("gnis_id"),
                    "fwa_watershed_code": c.get("fwa_watershed_code"),
                    "feature_type": c.get("feature_type"),
                    "region": c.get("region"),
                    "fwa_mus": fwa_mus,
                    "mu_match": match,
                }
            )
        return processed

    # --- Stats holder ---

    class _Stats:
        def __init__(self):
            self.results = defaultdict(list)
            self.by_region = defaultdict(_Counter)
            self.success_methods = _Counter()
            self.failed_parse = []
            self.trib_non_stream = []
            self.mu_mismatches = []
            self.used_vars = set()
            self.used_direct = set()
            self.used_skips = set()
            self.reg_names_seen = set()

    # --- Main entry ---

    parser = argparse.ArgumentParser(description="Waterbody Linking Coverage Test")
    parser.add_argument("--export-not-found", help="Export NOT_FOUND entries to JSON")
    parser.add_argument("--export-ambiguous", help="Export AMBIGUOUS entries to JSON")
    args = parser.parse_args()

    config = get_config()
    header("WATERBODY LINKING COVERAGE TEST")

    # Load data
    print("Loading parsed regulations...")
    with open(config.synopsis_parsed_results_path) as f:
        parsed_data = json.load(f)
    print(f"Loaded {len(parsed_data)} waterbodies")

    print("Loading FWA metadata...")
    gazetteer = MetadataGazetteer(config.fwa_metadata_path)

    from fwa_pipeline.metadata_builder import FeatureType as _FT

    print(
        f"Loaded {len(gazetteer.metadata.get(_FT.STREAM, {})):,} streams, "
        f"{len(gazetteer.metadata.get(_FT.LAKE, {})):,} lakes"
    )

    print("Initializing linker...")
    linker = WaterbodyLinker(
        gazetteer,
        ManualCorrections(
            NAME_VARIATIONS,
            DIRECT_MATCHES,
            SKIP_ENTRIES,
            UNMARKED_WATERBODIES,
            ADMIN_DIRECT_MATCHES,
        ),
    )
    print(f"Loaded configuration across {len(NAME_VARIATIONS)} regions")

    header("TESTING LINKING")
    stats = _Stats()
    parsed_lookup = {}

    for i, wb in enumerate(parsed_data):
        ident = wb["identity"]
        key, name = ident["waterbody_key"], ident["name_verbatim"]
        region = extract_region(wb.get("region", ""))
        mus = wb.get("mu", [])

        if key == "FAILED":
            stats.failed_parse.append(wb)
            continue
        if not region:
            continue

        parsed_lookup[(region, name)] = wb
        if name:
            stats.reg_names_seen.add((region, name))
        stats.reg_names_seen.add((region, key))

        if region in SKIP_ENTRIES and name in SKIP_ENTRIES[region]:
            stats.used_skips.add((region, name))

        # Link
        res = linker.link_waterbody(region=region, mgmt_units=mus, name_verbatim=name)

        # Statistics & validation
        if res.status in (LinkStatus.SUCCESS, LinkStatus.ADMIN_MATCH):
            stats.success_methods[res.link_method] += 1

            if res.link_method == "name_variation":
                if name in NAME_VARIATIONS.get(region, {}):
                    stats.used_vars.add((region, name))
                elif key in NAME_VARIATIONS.get(region, {}):
                    stats.used_vars.add((region, key))
            elif res.link_method == "direct_match":
                matched = name if name in DIRECT_MATCHES.get(region, {}) else key
                if matched:
                    stats.used_direct.add((region, matched))

            # MU mismatch check for direct matches
            if res.link_method == "direct_match":
                feats = res.matched_features
                fwa_mus = set().union(*(f.mgmt_units for f in feats if f.mgmt_units))
                reg_set = set(mus)
                mismatch = False
                if reg_set and fwa_mus and not reg_set.issubset(fwa_mus):
                    mismatch = True
                elif reg_set and not fwa_mus:
                    mismatch = True
                if mismatch:
                    reg_num = region.split()[-1]
                    is_cross = bool(fwa_mus) and not any(
                        m.startswith(f"{reg_num}-") for m in fwa_mus
                    )
                    stats.mu_mismatches.append(
                        {
                            "name_verbatim": name,
                            "region": region,
                            "reg_mus": sorted(reg_set),
                            "fwa_mus": sorted(fwa_mus),
                            "is_cross": is_cross,
                            "page": wb.get("page"),
                        }
                    )

            # Tributaries matched to non-stream
            if ident.get("identity_type") == "TRIBUTARIES":
                feats = res.matched_features
                if feats and all(f.geometry_type != "multilinestring" for f in feats):
                    stats.trib_non_stream.append(
                        {"key": key, "region": region, "matched_to": feats[0].gnis_name}
                    )

        # Candidate details
        cands_list = []
        if res.candidate_features:
            for f in res.candidate_features:
                cands_list.append(
                    {
                        "name": f.gnis_name,
                        "gnis_id": f.gnis_id,
                        "fwa_watershed_code": f.fwa_watershed_code,
                        "feature_type": getattr(f, "geometry_type", "Unknown"),
                        "zones": f.zones,
                        "management_units": f.mgmt_units,
                    }
                )

        item_data = {
            "waterbody_key": key,
            "name_verbatim": name,
            "location_descriptor": ident.get("location_descriptor"),
            "alternate_names": ident.get("alternate_names", []),
            "identity_type": ident.get("identity_type"),
            "region": region,
            "mu": mus,
            "result": res,
            "regulation_management_units": mus,
            "candidates": cands_list,
            "candidate_details": (
                _process_ambiguous_candidates(cands_list, mus)
                if res.status == LinkStatus.AMBIGUOUS
                else []
            ),
            "error_message": res.error_message,
        }
        stats.results[res.status].append(item_data)
        stats.by_region[region][res.status] += 1

        if (i + 1) % 100 == 0:
            print(f"Processed {i+1}/{len(parsed_data)}...", end="\r")

    print(f"Processed {len(parsed_data)}/{len(parsed_data)} waterbodies    ")

    # --- REPORTING ---

    header("SUMMARY STATISTICS")
    if stats.failed_parse:
        print(f"  FAILED PARSE ENTRIES: {len(stats.failed_parse)} (excluded)")

    total = len(parsed_data) - len(stats.failed_parse)

    for st in LinkStatus:
        count = len(stats.results[st])
        pct = (count / total * 100) if total else 0
        color = (
            GREEN
            if st == LinkStatus.SUCCESS
            else (RED if st == LinkStatus.NOT_FOUND else RESET)
        )
        print(f"{color}{st.value.upper():<20} : {count:5d} ({pct:5.1f}%){RESET}")

    print("\n   --- Success Breakdown ---")
    method_map = {
        "direct_match": "Direct Match (Config)",
        "name_variation": "Name Variation (Config)",
        "natural_search": "Natural Search (Fuzzy)",
        "exact_match": "Exact Name Match",
    }
    for method, label in method_map.items():
        count = stats.success_methods[method]
        if count > 0:
            print(f"   {label:<25} : {count:5d}")

    if stats.trib_non_stream:
        print(
            f"\n   {YELLOW}  Tributaries -> Non-Stream : {len(stats.trib_non_stream):5d} (Check these){RESET}"
        )

    not_in_data = len(stats.results[LinkStatus.NOT_IN_DATA])
    ignored = len(stats.results[LinkStatus.IGNORED])
    if not_in_data + ignored > 0:
        print("\n   --- Excluded/Known Missing ---")
        if not_in_data:
            print(f"   Not In FWA Data           : {not_in_data:5d}")
        if ignored:
            print(f"   Manually Ignored          : {ignored:5d}")

    # Region breakdown
    header("RESULTS BY REGION")
    width = tw()
    col_w = max(10, int((width - 20) / 4))

    hdr_row = f"{'Region':<15} | {'SUCCESS':<{col_w}} | {'AMBIG':<{col_w}} | {'NOT_FOUND':<{col_w}}"
    print(hdr_row)
    print("-" * len(hdr_row))
    for reg in sorted(stats.by_region.keys()):
        s = stats.by_region[reg]
        print(
            f"{reg:<15} | {s[LinkStatus.SUCCESS]:<{col_w}} | "
            f"{s[LinkStatus.AMBIGUOUS]:<{col_w}} | {s[LinkStatus.NOT_FOUND]:<{col_w}}"
        )

    # Direct match MU validation
    header("DIRECT MATCH MU VALIDATION")
    if stats.mu_mismatches:
        print(f"Found {len(stats.mu_mismatches)} mismatches:")
        for m in stats.mu_mismatches:
            tag = (
                f" {RED}[CROSS-REGION]{RESET}"
                if m["is_cross"]
                else f" {YELLOW}[MU MISMATCH]{RESET}"
            )
            print(f"\n{tag} {m['name_verbatim']}")
            print(f"   Regulation Region: {m['region']}")
            print(f"   Regulation MUs:    {', '.join(m['reg_mus']) or '(none)'}")
            print(f"   FWA MUs:           {', '.join(m['fwa_mus']) or '(none)'}")
            if m.get("page"):
                print(f"   Page:              {m['page']}")
    else:
        print("All direct matches verified.")

    # Duplicate mappings
    header("DUPLICATE REGULATION MAPPINGS")
    dupes = linker.get_duplicate_mappings()
    if dupes:
        print(
            f"Found {len(dupes)} FWA waterbodies with multiple regulation names mapped to them:"
        )
        sorted_dupes = sorted(
            dupes.items(), key=lambda x: len(x[1]["regulations"]), reverse=True
        )
        for identity_key, data in sorted_dupes[:50]:
            feat = data["feature"]
            print(f"\n  {identity_key}")
            if feat:
                info = f"{feat.gnis_name} ({feat.geometry_type})"
                if feat.gnis_id:
                    info += f" [GNIS {feat.gnis_id}]"
                print(f"  Feature: {info}")
                if feat.mgmt_units:
                    print(f"  FWA MUs: {', '.join(sorted(feat.mgmt_units))}")
            print(f"  Mapped by {len(data['regulations'])} regulation(s):")
            for r, n in data["regulations"]:
                print(f"    * {r:10s} | {n}")
        if len(dupes) > 50:
            print(f"\n  ... and {len(dupes) - 50} more")
    else:
        print("No duplicate mappings found.")

    # Samples
    def _print_sample(status, limit=5):
        items = stats.results[status]
        if not items:
            return
        header(
            f"{status.value.upper()} SAMPLES (Showing {min(limit, len(items))} of {len(items)})"
        )
        by_type = defaultdict(list)
        for it in items:
            by_type[it.get("identity_type", "UNKNOWN")].append(it)
        for itype, type_items in sorted(by_type.items()):
            sub_header(itype)
            for item in type_items[:limit]:
                mus_str = ",".join(item["mu"]) or "-"
                print(f" * {item['name_verbatim']} ({item['region']}) | MUs: {mus_str}")
                if item.get("location_descriptor"):
                    print(f"   Loc: {item['location_descriptor']}")
                if item.get("error_message") and status == LinkStatus.ERROR:
                    print(f"   Error: {item['error_message']}")
                if status == LinkStatus.AMBIGUOUS:
                    cand_features = item["candidates"]
                    unique_candidates = defaultdict(list)
                    for c in cand_features:
                        cid = c["fwa_watershed_code"] or c["gnis_id"]
                        unique_candidates[cid].append(c)
                    print(f"   Candidates ({len(unique_candidates)}):")
                    for cid, feats in list(unique_candidates.items())[:5]:
                        ft = feats[0]
                        f_mus = (
                            ", ".join(
                                sorted(
                                    set().union(
                                        *(
                                            x["management_units"]
                                            for x in feats
                                            if x["management_units"]
                                        )
                                    )
                                )
                            )
                            or "None"
                        )
                        zones_str = (
                            ",".join(ft["zones"]) if ft.get("zones") else "Unknown"
                        )
                        print(
                            f"     - {ft['name']} [{cid}] (Zones: {zones_str}) | MUs: {f_mus}"
                        )

    _print_sample(LinkStatus.NOT_FOUND, limit=5)
    _print_sample(LinkStatus.AMBIGUOUS, limit=5)
    _print_sample(LinkStatus.ERROR, limit=5)

    # Unused configs
    header("UNUSED CONFIGURATION")

    unused_vars = []
    for r, v in NAME_VARIATIONS.items():
        for k in v:
            if not SKIP_ENTRIES.get(r, {}).get(k) and (r, k) not in stats.used_vars:
                unused_vars.append(f"{r} | {k}")

    unused_direct = []
    for r, m in DIRECT_MATCHES.items():
        for k in m:
            if (r, k) in stats.reg_names_seen and (r, k) not in stats.used_direct:
                unused_direct.append(f"{r} | {k}")

    if unused_vars:
        print(f"  {len(unused_vars)} Unused Name Variations (first 10):")
        for u in unused_vars[:10]:
            print(f"   - {u}")
    else:
        print("Name variations clean.")

    if unused_direct:
        print(f"\n  {len(unused_direct)} Unused Direct Matches (first 10):")
        for u in unused_direct[:10]:
            print(f"   - {u}")
    else:
        print("\nDirect matches clean.")

    # Final tally
    header("FINAL TALLY")
    print(f"Total Processed:    {total}")
    linked_total = len(stats.results[LinkStatus.SUCCESS]) + len(
        stats.results[LinkStatus.ADMIN_MATCH]
    )
    print(f"{GREEN}Linked (Total):     {linked_total}{RESET}")
    print(f"  - Natural Search: {stats.success_methods['natural_search']}")
    print(f"  - Direct Match:   {stats.success_methods['direct_match']}")
    print(f"  - Name Variation: {stats.success_methods['name_variation']}")
    print(f"  - Exact Match:    {stats.success_methods['exact_match']}")
    print(f"  - Admin Match:    {stats.success_methods.get('admin_direct_match', 0)}")
    print(f"{RED}Not Found:          {len(stats.results[LinkStatus.NOT_FOUND])}{RESET}")
    print(
        f"{YELLOW}Ambiguous:          {len(stats.results[LinkStatus.AMBIGUOUS])}{RESET}"
    )
    print(
        f"{BLUE}Not In Data:        {len(stats.results[LinkStatus.NOT_IN_DATA])}{RESET}"
    )
    print(f"{RED}Error:              {len(stats.results[LinkStatus.ERROR])}{RESET}")
    print(f"Ignored:            {len(stats.results[LinkStatus.IGNORED])}")
    print()

    # Exports
    if args.export_not_found:
        _export_data(
            stats.results[LinkStatus.NOT_FOUND],
            Path(args.export_not_found),
            parsed_lookup,
            "NOT_FOUND",
        )
    if args.export_ambiguous:
        _export_data(
            stats.results[LinkStatus.AMBIGUOUS],
            Path(args.export_ambiguous),
            parsed_lookup,
            "AMBIGUOUS",
        )


if __name__ == "__main__":
    import warnings

    warnings.filterwarnings(
        "ignore", message=".*found in sys.modules.*", category=RuntimeWarning
    )
    _run_coverage_test()
