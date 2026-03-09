"""
Waterbody Linker - Business logic for linking regulation names to FWA features.

Handles the linking workflow:
1. Check DirectMatch (highest priority)
2. Natural gazetteer search

Tracks statistics and provides clean interface for test coverage script.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from enum import Enum
from collections import Counter
import re

from fwa_pipeline.metadata_gazetteer import MetadataGazetteer, FWAFeature, FeatureType
from .linking_corrections import (
    ManualCorrections,
    DirectMatch,
    AdminDirectMatch,
    NameVariationLink,
    SkipEntry,
)
from .logger_config import get_logger

logger = get_logger(__name__)

# Pre-compiled regex patterns for search name cleanup
_RE_BRACKETS = re.compile(r"\s*\([^)]*\)\s*")
_RE_MULTI_SPACE = re.compile(r"\s+")


class DirectMatchError(Exception):
    """Raised when a DirectMatch configuration fails to resolve all its IDs.

    This is a *configuration error* — every ID listed in a DirectMatch must
    resolve to at least one feature in the gazetteer.  Partial matches are
    never acceptable because they silently drop regulation coverage.
    """


class LinkStatus(Enum):
    """Status of a linking attempt."""

    SUCCESS = "success"  # Feature(s) matched
    ADMIN_MATCH = "admin_match"  # Admin boundary match (features resolved via spatial intersection in mapper)
    NAME_VARIATION = "name_variation"  # Alternate name for an already-linked waterbody (skip link, pass alias downstream)
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
        None  # "direct_match", "natural_search", "admin_direct_match", "name_variation_link"
    )
    matched_name: Optional[str] = (
        None  # The actual name that matched (for name variations)
    )
    error_message: Optional[str] = None
    admin_match: Optional[AdminDirectMatch] = (
        None  # Set when link_method == "admin_direct_match"
    )
    name_variation_link: Optional[NameVariationLink] = (
        None  # Set when link_method == "name_variation_link"
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
    2. Natural gazetteer search

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

    @staticmethod
    def _feature_identity(feature: FWAFeature) -> Tuple[str, str]:
        """Derive the unique identity key for a feature.

        Streams are identified by watershed code, lakes/wetlands by GNIS ID,
        other polygons by waterbody_key, and all others by fwa_id.

        Args:
            feature: The FWA feature to identify.

        Returns:
            A (type_label, id_value) tuple uniquely identifying the waterbody.
        """
        if feature.geometry_type == "multilinestring" and feature.fwa_watershed_code:
            return ("stream", feature.fwa_watershed_code)
        elif feature.gnis_id:
            return ("gnis", feature.gnis_id)
        elif feature.waterbody_key:
            return ("waterbody_key", feature.waterbody_key)
        else:
            return ("fwa_id", feature.fwa_id)

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

        # STEP 0b: Check NameVariationLink (alternate name for already-linked waterbody)
        if region:
            nv_link = self.corrections.get_name_variation_link(region, lookup_name)
            if nv_link:
                logger.debug(
                    f"Name variation '{name_verbatim}' → primary '{nv_link.primary_name}'"
                )
                result = LinkingResult(
                    status=LinkStatus.NAME_VARIATION,
                    matched_name=nv_link.primary_name,
                    error_message=f"Name variation: {nv_link.note}",
                    link_method="name_variation_link",
                    name_variation_link=nv_link,
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
                # Validate that the admin match has target polygons
                if not admin_match.admin_targets:
                    result = LinkingResult(
                        status=LinkStatus.ERROR,
                        matched_features=[],
                        link_method="admin_direct_match",
                        error_message=f"AdminDirectMatch configured but admin_targets is empty (need to fill in admin targets)",
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

        # STEP 2: Natural gazetteer search (no manual corrections)
        # Use verbatim name only (not the parsed lookup_name)
        # Try 3 variations in order:
        # 1. Verbatim name as-is
        # 2. Verbatim with outermost brackets removed (e.g., "LAKE (Region 5)" -> "LAKE")
        # 3. Verbatim with brackets and quotes removed

        search_name = name_verbatim
        search_variations = [search_name]  # Always try original first

        # Generate bracket-removed variation
        if "(" in search_name and ")" in search_name:
            bracket_removed = _RE_BRACKETS.sub(" ", search_name).strip()
            bracket_removed = _RE_MULTI_SPACE.sub(" ", bracket_removed)
            if bracket_removed != search_name:
                search_variations.append(bracket_removed)

        # Generate brackets + quotes removed variation
        if "(" in search_name or '"' in search_name or "'" in search_name:
            cleaned = _RE_BRACKETS.sub(" ", search_name).strip()
            cleaned = cleaned.replace('"', "").replace("'", "")
            cleaned = _RE_MULTI_SPACE.sub(" ", cleaned)
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
        return result

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
            logger.warning(f"Could not parse region string: {region!r}")
            return False  # Can't parse region, fail validation

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
                    logger.warning(
                        f"Could not parse MU string: {mu!r} for feature {feature}"
                    )
                    continue

            # If this feature has no MUs matching the regulation region, validation fails
            if not feature_has_matching_mu:
                return False

        return True  # All features have at least one MU matching regulation region

    def _validate_direct_match_resolution(
        self,
        name_verbatim: str,
        direct_match: DirectMatch,
    ) -> None:
        """Validate that every ID in a DirectMatch resolves to ≥1 feature.

        Raises :class:`DirectMatchError` if **any** configured ID fails to
        resolve.  This prevents silent data loss from misconfigured entries
        in ``linking_corrections.DIRECT_MATCHES``.
        """
        missing: List[str] = []

        if direct_match.gnis_ids:
            for gnis_id in direct_match.gnis_ids:
                if not self.gazetteer.search_by_gnis_id(str(gnis_id)):
                    missing.append(f"gnis_id={gnis_id}")

        if direct_match.fwa_watershed_codes:
            for wsc in direct_match.fwa_watershed_codes:
                if not self.gazetteer.search_by_watershed_code(wsc):
                    missing.append(f"fwa_watershed_code={wsc}")

        if direct_match.waterbody_poly_ids:
            for poly_id in direct_match.waterbody_poly_ids:
                if not self.gazetteer.get_polygon_by_id(str(poly_id)):
                    missing.append(f"waterbody_poly_id={poly_id}")

        if direct_match.waterbody_keys:
            for wbk in direct_match.waterbody_keys:
                if not self.gazetteer.get_waterbody_by_key(str(wbk)):
                    missing.append(f"waterbody_key={wbk}")

        if direct_match.linear_feature_ids:
            for lf_id in direct_match.linear_feature_ids:
                if not self.gazetteer.get_stream_by_id(str(lf_id)):
                    missing.append(f"linear_feature_id={lf_id}")

        if direct_match.blue_line_keys:
            for blk in direct_match.blue_line_keys:
                if not self.gazetteer.search_by_blue_line_key(blk):
                    missing.append(f"blue_line_key={blk}")

        if direct_match.sub_polygon_ids:
            for sp_id in direct_match.sub_polygon_ids:
                if not self.gazetteer.get_polygon_by_id(sp_id):
                    missing.append(f"sub_polygon_id={sp_id}")

        if direct_match.ungazetted_waterbody_id:
            if not self.corrections.get_ungazetted_waterbody(
                direct_match.ungazetted_waterbody_id
            ):
                missing.append(
                    f"ungazetted_waterbody_id={direct_match.ungazetted_waterbody_id}"
                )

        if missing:
            raise DirectMatchError(
                f"DirectMatch for '{name_verbatim}' has {len(missing)} unresolved ID(s): "
                f"{', '.join(missing)}. "
                f"Fix the DirectMatch in linking_corrections.py or update the gazetteer data. "
                f"(note: {direct_match.note})"
            )

    def _apply_direct_match(
        self,
        name_verbatim: str,
        direct_match: DirectMatch,
    ) -> LinkingResult:
        """Apply a DirectMatch (pure ID lookup).

        Uses :func:`resolve_direct_match_features` for the standard gazetteer
        ID fields, then adds linker-specific ungazetted-corrections handling.

        Raises:
            DirectMatchError: If any configured ID fails to resolve.  A
                DirectMatch is an explicit configuration — every ID must
                exist in the gazetteer.  Partial matches are never acceptable.
        """
        from .regulation_resolvers import resolve_direct_match_features

        # Validate every configured ID resolves — raises on any miss
        self._validate_direct_match_resolution(name_verbatim, direct_match)

        # Standard gazetteer lookup (shared with zone/mapper pipeline)
        features = resolve_direct_match_features(self.gazetteer, direct_match)

        # Linker-specific: ungazetted waterbody from manual corrections
        # This path uses corrections data (geometry coords) rather than the
        # gazetteer's injected ungazetted features, so it must remain here.
        # Validation above already confirmed the ID exists in corrections.
        if direct_match.ungazetted_waterbody_id:
            ungazetted_wb = self.corrections.get_ungazetted_waterbody(
                direct_match.ungazetted_waterbody_id
            )
            from shapely.geometry import Point, LineString, Polygon

            if ungazetted_wb.geometry_type == "point":
                geometry = Point(ungazetted_wb.coordinates)
            elif ungazetted_wb.geometry_type == "linestring":
                geometry = LineString(ungazetted_wb.coordinates)
            elif ungazetted_wb.geometry_type == "polygon":
                geometry = Polygon(
                    ungazetted_wb.coordinates[0],
                    (
                        ungazetted_wb.coordinates[1:]
                        if len(ungazetted_wb.coordinates) > 1
                        else None
                    ),
                )
            else:
                raise ValueError(
                    f"Unsupported ungazetted waterbody geometry type: {ungazetted_wb.geometry_type}"
                )

            feature = type(
                "obj",
                (object,),
                {
                    "fwa_id": ungazetted_wb.ungazetted_id,
                    "name": ungazetted_wb.name,
                    "gnis_name": ungazetted_wb.name,
                    "gnis_id": None,
                    "gnis_name_2": None,
                    "gnis_id_2": None,
                    "geometry": geometry,
                    "geometry_type": ungazetted_wb.geometry_type,
                    "feature_type": FeatureType.UNGAZETTED,
                    "zones": ungazetted_wb.zones,
                    "mgmt_units": ungazetted_wb.mgmt_units,
                    "waterbody_key": None,
                    "fwa_watershed_code": None,
                    "matched_via": None,
                    "is_ungazetted_waterbody": True,
                    "ungazetted_note": ungazetted_wb.note,
                    "ungazetted_source_url": ungazetted_wb.source_url,
                },
            )()
            features.append(feature)

        # Validation passed — features must not be empty
        assert features, (
            f"DirectMatch for '{name_verbatim}' passed validation but produced "
            f"0 features. This is an internal error."
        )

        logger.debug(
            f"Direct match for '{name_verbatim}' returning {len(features)} features as SUCCESS"
        )
        for feature in features:
            feature.matched_via = f"direct_match ({direct_match.note})"
        return LinkingResult(
            status=LinkStatus.SUCCESS,
            matched_features=features,
        )

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
        # (allows cross-zone_number matches for boundary features)
        cross_zone_fallback = False
        if not matches and zone_number:
            matches = self.gazetteer.search(name, zone_number=None)
            if matches:
                cross_zone_fallback = True
                cross_zones = set()
                for m in matches:
                    cross_zones.update(m.zones or [])
                logger.warning(
                    f"Zone mismatch: '{name}' not found in zone {zone_number} "
                    f"but found in zone(s) {sorted(cross_zones)}. "
                    f"Regulation region may be wrong or feature zone assignment needs review."
                )
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
            identity = self._feature_identity(match)

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
                identity = self._feature_identity(representative)

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
            identity_key = self._feature_identity(rep)

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

                        if target_gnis_id and seg.gnis_id == target_gnis_id:
                            expanded_features.append(seg)
                        elif (
                            target_name_lower
                            and (seg.gnis_name or "").lower() == target_name_lower
                        ):
                            expanded_features.append(seg)
                        elif not seg.gnis_name and seg.inherited_gnis_names:
                            # Unnamed segment — check inherited names from graph context
                            for inh in seg.inherited_gnis_names:
                                inh_id = inh.get("gnis_id", "")
                                inh_name = (inh.get("gnis_name") or "").lower()
                                if (target_gnis_id and inh_id == target_gnis_id) or (
                                    target_name_lower and inh_name == target_name_lower
                                ):
                                    expanded_features.append(seg)
                                    break
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

    def get_stats(self) -> Dict[str, int]:
        """Get linking statistics."""
        return dict(self.stats)


# ============================================================================
# CLI Coverage Test  (python -m regulation_mapping.linker)
# ============================================================================


def _run_coverage_test() -> None:
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
    from pathlib import Path
    from collections import defaultdict, Counter as _Counter

    from .linking_corrections import (
        DIRECT_MATCHES,
        SKIP_ENTRIES,
        UNGAZETTED_WATERBODIES,
        ADMIN_DIRECT_MATCHES,
        NAME_VARIATION_LINKS,
        ManualCorrections,
    )
    from project_config import get_config
    from .cli_helpers import (
        RED,
        YELLOW,
        GREEN,
        BLUE,
        RESET,
        header,
        sub_header,
        divider,
        tw,
    )
    from .regulation_resolvers import parse_region as extract_region

    # --- Export helpers ---

    def _instructions_block(mode: str) -> Dict[str, object]:
        if mode == "NOT_FOUND":
            return {
                "description": "NOT_FOUND waterbodies - need manual corrections",
                "how_to_fix": [
                    "1. Check spelling/formatting/renaming -> Add to DIRECT_MATCHES with GNIS ID",
                    "2. Not in gazetteer? -> Add to DIRECT_MATCHES with waterbody_key",
                    "3. Check 'search_terms_used' and 'location_descriptor'",
                    "4. Use management_units to narrow down",
                ],
                "example_direct_match": {
                    "LONG LAKE (Nanaimo)": {
                        "gnis_ids": ["17501"],
                        "note": "Disambiguate",
                    }
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

    def _export_data(items: List[Dict], path: str, lookup: Dict, mode: str) -> None:
        entries = []
        for item in items:
            reg, name = item["region"], item["name_verbatim"]
            full = lookup.get((reg, name), {})
            ex_match = DIRECT_MATCHES.get(reg, {}).get(name)

            entry = {
                "name_verbatim": name,
                "lookup_name": item["lookup_name"],
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
                "existing_direct_match": (
                    {"gnis_ids": ex_match.gnis_ids, "note": ex_match.note}
                    if ex_match
                    else None
                ),
            }

            if mode == "NOT_FOUND":
                entry["search_terms_used"] = item.get(
                    "search_terms", [item["lookup_name"].lower()]
                )
                entry["suggested_action"] = "Add to DIRECT_MATCHES with GNIS ID"
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

    def _process_ambiguous_candidates(
        candidates: List[Dict], reg_mus: List[str]
    ) -> List[Dict]:
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
            DIRECT_MATCHES,
            SKIP_ENTRIES,
            UNGAZETTED_WATERBODIES,
            ADMIN_DIRECT_MATCHES,
            NAME_VARIATION_LINKS,
        ),
    )
    print(f"Loaded configuration")

    header("TESTING LINKING")
    stats = _Stats()
    parsed_lookup = {}

    for i, wb in enumerate(parsed_data):
        ident = wb["identity"]
        key, name = (
            ident.get("lookup_name") or ident.get("waterbody_key", ""),
            ident["name_verbatim"],
        )
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
        if region in NAME_VARIATION_LINKS and name in NAME_VARIATION_LINKS[region]:
            stats.used_skips.add((region, name))

        # Link
        try:
            res = linker.link_waterbody(
                region=region, mgmt_units=mus, name_verbatim=name
            )
        except DirectMatchError as exc:
            print(f"{RED}  ERROR: {exc}{RESET}")
            stats.results["DIRECT_MATCH_ERROR"].append(
                {"region": region, "name_verbatim": name, "error": str(exc)}
            )
            continue

        # Statistics & validation
        if res.status in (LinkStatus.SUCCESS, LinkStatus.ADMIN_MATCH):
            stats.success_methods[res.link_method] += 1

            if res.link_method == "direct_match":
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
                    # Strip letter suffix for MU prefix comparison
                    # "Region 7A" -> "7A" -> "7" (MUs are "7-10" not "7A-10")
                    reg_num_raw = region.split()[-1]
                    reg_num_digits = "".join(c for c in reg_num_raw if c.isdigit())
                    is_cross = bool(fwa_mus) and not any(
                        m.startswith(f"{reg_num_digits}-") for m in fwa_mus
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
            "lookup_name": key,
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
        "natural_search": "Natural Search (Fuzzy)",
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
    name_variations = len(stats.results[LinkStatus.NAME_VARIATION])
    if not_in_data + ignored + name_variations > 0:
        print("\n   --- Excluded/Known Missing ---")
        if not_in_data:
            print(f"   Not In FWA Data           : {not_in_data:5d}")
        if ignored:
            print(f"   Manually Ignored          : {ignored:5d}")
        if name_variations:
            print(f"   Name Variation (alias)    : {name_variations:5d}")

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

    # Duplicate mappings — reconstructed from stats.results[SUCCESS]
    header("DUPLICATE REGULATION MAPPINGS")
    # Build feature_to_regulations locally (same identity logic that was removed from linker)
    _feat_to_regs: Dict[str, Dict] = {}
    for item_data in stats.results[LinkStatus.SUCCESS]:
        res = item_data["result"]
        region = item_data["region"]
        name = item_data["name_verbatim"]
        for feature in res.matched_features or []:
            if (
                feature.geometry_type == "multilinestring"
                and feature.fwa_watershed_code
            ):
                wname = (feature.gnis_name or "unnamed").lower().replace(" ", "_")
                ik = f"stream_{feature.fwa_watershed_code}_{wname}"
            elif feature.waterbody_key:
                ik = f"waterbody_{feature.waterbody_key}"
            else:
                ik = f"fwa_{feature.fwa_id}"
            entry = _feat_to_regs.setdefault(
                ik, {"feature": feature, "regulations": []}
            )
            reg_info = (region, name)
            if reg_info not in entry["regulations"]:
                entry["regulations"].append(reg_info)
    dupes = {k: v for k, v in _feat_to_regs.items() if len(v["regulations"]) > 1}
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

    # Multi-name watersheds — linked streams whose watershed code also
    # carries segments with a different GNIS name.  These are worth
    # reviewing because the linker matched one name but the same
    # watershed physically contains features under other names.
    header("MULTI-NAME WATERSHEDS (linked streams)")

    # 1. Build wc → {(gnis_name, gnis_id)} and per-name zone sets from raw stream metadata
    _stream_meta = gazetteer.metadata.get(_FT.STREAM, {})
    _wc_names: Dict[str, set] = {}
    # Track zones each (name, gnis_id) appears in across all watershed codes
    _name_zones: Dict[tuple, set] = {}  # (gnis_name, gnis_id) → {zone_ids}
    for _sm in _stream_meta.values():
        wc = _sm.get("fwa_watershed_code")
        gname = _sm.get("gnis_name")
        if wc and gname:
            gid = _sm.get("gnis_id", "")
            _wc_names.setdefault(wc, set()).add((gname, gid))
            zones = _sm.get("zones") or []
            _name_zones.setdefault((gname, gid), set()).update(zones)

    # Keep only watershed codes with ≥2 distinct names
    _multi_wc = {wc: names for wc, names in _wc_names.items() if len(names) > 1}

    # 2. Walk linked stream features, collect those hitting multi-name WCs
    _multi_name_hits: Dict[str, Dict] = {}  # wc → {reg_name, matched_gnis, all_names}
    for item_data in stats.results[LinkStatus.SUCCESS]:
        res = item_data["result"]
        reg_name = item_data["name_verbatim"]
        region = item_data["region"]
        for feat in res.matched_features or []:
            if feat.geometry_type != "multilinestring":
                continue
            wc = feat.fwa_watershed_code
            if wc and wc in _multi_wc:
                if wc not in _multi_name_hits:
                    _multi_name_hits[wc] = {
                        "regulations": [],
                        "matched_gnis": feat.gnis_name,
                        "all_names": _multi_wc[wc],
                    }
                entry = (region, reg_name, feat.gnis_name)
                if entry not in _multi_name_hits[wc]["regulations"]:
                    _multi_name_hits[wc]["regulations"].append(entry)

    if _multi_name_hits:
        # Sort by number of distinct names descending
        sorted_hits = sorted(
            _multi_name_hits.items(),
            key=lambda x: len(x[1]["all_names"]),
            reverse=True,
        )
        print(
            f"Found {len(sorted_hits)} linked watershed codes with multiple GNIS names:\n"
        )
        for wc, data in sorted_hits[:50]:
            names_list = sorted(data["all_names"])
            name_strs = []
            for n, gid in names_list:
                zones = sorted(_name_zones.get((n, gid), set()))
                zone_str = (
                    f", Zone{'s' if len(zones) > 1 else ''} {'/'.join(zones)}"
                    if zones
                    else ""
                )
                if gid:
                    name_strs.append(f"{n} [GNIS {gid}{zone_str}]")
                else:
                    name_strs.append(
                        f"{n}{f' [{zone_str.lstrip(', ')}]' if zone_str else ''}"
                    )
            print(f"  WC: {wc}")
            print(f"  Names on watershed ({len(names_list)}): {', '.join(name_strs)}")
            for reg, rname, matched in data["regulations"]:
                tag = f" (matched: {matched})" if matched else ""
                print(f"    * {reg:10s} | {rname}{tag}")
            print()
        if len(sorted_hits) > 50:
            print(f"  ... and {len(sorted_hits) - 50} more")
    else:
        print("No linked streams with multi-name watershed codes found.")

    # Samples
    def _print_sample(status: LinkStatus, limit: int = 5) -> None:
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

    unused_direct = []
    for r, m in DIRECT_MATCHES.items():
        for k in m:
            if (r, k) in stats.reg_names_seen and (r, k) not in stats.used_direct:
                unused_direct.append(f"{r} | {k}")

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
    print(f"  - Admin Match:    {stats.success_methods.get('admin_direct_match', 0)}")
    print(f"{RED}Not Found:          {len(stats.results[LinkStatus.NOT_FOUND])}{RESET}")
    print(
        f"{YELLOW}Ambiguous:          {len(stats.results[LinkStatus.AMBIGUOUS])}{RESET}"
    )
    print(
        f"{BLUE}Not In Data:        {len(stats.results[LinkStatus.NOT_IN_DATA])}{RESET}"
    )
    print(f"{RED}Error:              {len(stats.results[LinkStatus.ERROR])}{RESET}")
    dm_errors = len(stats.results["DIRECT_MATCH_ERROR"])
    if dm_errors:
        print(f"{RED}DirectMatch Errors: {dm_errors}{RESET}")
    print(f"Ignored:            {len(stats.results[LinkStatus.IGNORED])}")
    print(f"Name Variations:    {len(stats.results[LinkStatus.NAME_VARIATION])}")
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
