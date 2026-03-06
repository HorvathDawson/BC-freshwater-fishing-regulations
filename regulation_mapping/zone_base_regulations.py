"""
Zone Base Regulations
======================

Defines zone-level default fishing regulations that apply to all waterbodies
(or all of a specific type) within a zone, unless overridden by the
waterbody-specific tables in the synopsis.

Overview
--------
Zone regulations fill the gap between provincial regulations (province-wide)
and waterbody-specific regulations (from synopsis tables). They capture
blanket rules from the synopsis preamble text, such as:

- *"All streams in Region 1 are closed Nov 1–June 30 unless noted"*
- *"Daily quota: 5 trout (all species combined) in Region 4 streams"*
- *"Set lines permitted in lakes of Region 6"*

Two resolution modes (auto-detected from fields):

1. **Zone-wide mode** (default) — When all direct-match ID fields are None,
   the regulation applies to every FWA feature matching ``feature_types``
   within the specified ``zone_ids``. Uses pre-computed ``zones`` metadata
   on each feature — no spatial join required.

2. **Direct-match mode** — When any ID field is populated (``gnis_ids``,
   ``waterbody_poly_ids``, etc.), the regulation targets ONLY those specific
   features. ``feature_types`` and zone-wide scanning are skipped. Use this
   for waterbody-specific regulations that appear in the zone preamble
   instead of the waterbody tables.

Regulation IDs & the ``zone_`` Prefix
--------------------------------------
Every ``ZoneRegulation.regulation_id`` **must** start with ``zone_``.
This prefix is used downstream to distinguish zone rules from
synopsis-derived and provincial rules:

* **RegulationMapper** (``regulation_mapper.py``) processes zone
  regulations via ``_process_zone_regulations()`` and assigns the
  resulting ``regulation_id`` values to matching FWA features.
* **GeoExporter** (``geo_exporter.py``) uses the ``zone_`` prefix in
  ``_get_reg_names()`` to **exclude** zone regulations from the
  human-readable regulation names in search index and map tiles —
  these are regional defaults, not waterbody-specific names.
* The ``restriction`` dict for each regulation is exported alongside
  synopsis-derived and provincial rules in ``regulations.json``, with
  ``source: "zone"`` to distinguish them.

Architecture Decision
---------------------
Zone regulations are **manually defined** as static data (same pattern as
``provincial_base_regulations.py``). The synopsis preamble is free-form text
not currently parsed; zone defaults change infrequently (2-year publication
cycle); and manual entry avoids LLM hallucination risk on nuanced text.
If a preamble parser is added later, it can populate ``ZoneRegulation``
entries automatically — the downstream pipeline is identical.

Adding New Regulations
----------------------
1. Create a new ``ZoneRegulation`` entry in ``ZONE_BASE_REGULATIONS``.
2. Use the ``zone_`` prefix for ``regulation_id``.
3. Set ``zone_ids`` to the target zone(s).
4. For zone-wide scope: set ``feature_types`` (or leave None for all types).
5. For waterbody-specific scope: populate one or more direct-match ID fields.
6. Populate ``restriction`` with at least ``type`` and ``details`` keys.
7. Run the CLI test: ``python -m regulation_mapping.zone_base_regulations``
   to verify counts.
8. Re-run the regulation pipeline to propagate.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional

from fwa_pipeline.metadata_builder import FeatureType
from .admin_target import AdminTarget


@dataclass
class ZoneRegulation:
    """
    A zone-level default regulation that applies to all waterbodies of
    specified types within one or more zones, OR to specific waterbodies
    identified by direct-match ID fields.

    **Zone-wide mode** (default): When all direct-match ID fields are None,
    the regulation applies to every FWA feature matching ``feature_types``
    within the specified ``zone_ids``. Uses the pre-computed ``zones`` field
    on each feature — no spatial join required.

    **Direct-match mode**: When any ID field is populated (gnis_ids,
    waterbody_poly_ids, etc.), the regulation targets ONLY those specific
    features. ``feature_types`` and zone-wide scanning are skipped. Use this
    for waterbody-specific regulations that appear in the zone preamble
    instead of the waterbody tables.

    **Admin-match mode**: When ``admin_targets`` is set, the regulation targets
    all FWA features that spatially intersect the specified admin polygons
    (same mechanism as provincial regulations). Each target is an
    ``AdminTarget(layer, feature_id)`` pair, supporting multi-layer matching.
    Use for area closures tied to named polygons (e.g., research forests,
    landslide hazard areas).

    Attributes:
        regulation_id: Unique identifier. MUST start with "zone_".
        zone_ids: List of zone IDs this regulation applies to (e.g., ["1"],
                  ["7A", "7B"]). In direct-match mode, this indicates
                  provenance (which preamble section the regulation comes
                  from) rather than feature targeting.
        rule_text: Human-readable regulation text (from synopsis preamble).
        restriction: Regulation details dict with ``type`` and ``details``
                     keys, matching the synopsis format.
        notes: Source references (synopsis page numbers, edition, etc.).
        dates: Optional list of date strings (e.g. ["Jul 15 – Aug 31"]).
               Matches the synopsis ``dates`` format — simple string list.
        scope_location: Optional human-readable location string. Exported
                        as ``scope_location`` in regulations.json, displayed
                        by the frontend alongside the regulation text.

        feature_types: Which FWA feature types this regulation applies to.
                       Uses FeatureType enum values. If None, applies to ALL
                       feature types. Ignored in direct-match mode.
        mu_ids: Optional list of specific MU codes (e.g., ["6-1", "6-2"]).
                Only features whose mgmt_units overlap this list are affected.
                If None, all features in the zone are affected.
                Ignored in direct-match mode.
        exclude_mu_ids: Optional list of MU codes to remove from the match.
                Features in these MUs are dropped after zone collection.
                Use to carve out sub-areas (e.g., zone 1 except MU 1-5).
        include_mu_ids: Optional list of MU codes to add to the match.
                Features in these MUs are added regardless of zone membership.
                Use to pull in adjacent MUs (e.g., zone 1 + MU 6-20).

        # Direct-match fields (any populated → direct-match mode)
        gnis_ids: GNIS identifiers — matches all features with these GNIS IDs.
        waterbody_poly_ids: Specific polygon IDs (most precise).
        fwa_watershed_codes: Watershed codes — matches all segments from each.
        waterbody_keys: Matches all polygons sharing a WATERBODY_KEY.
        linear_feature_ids: Specific stream segment IDs.
        blue_line_keys: Matches all features from each BLK.
        sub_polygon_ids: Links to synthetic sub-polygon features.
        ungazetted_waterbody_id: Links to an UngazettedWaterbody entry.

        # Admin-match fields (admin_targets set → admin-match mode)
        admin_targets: List of AdminTarget(layer, feature_id) pairs for
            spatial intersection. Supports multi-layer matching.
    """

    regulation_id: str
    zone_ids: List[str]
    rule_text: str
    restriction: Dict[str, Any]  # {type, details} — matches synopsis format
    notes: str

    # Scope (zone-wide mode)
    feature_types: Optional[List[FeatureType]] = None  # None = all types
    dates: Optional[List[str]] = None  # Date strings, e.g. ["Jul 15 – Aug 31"]
    scope_location: Optional[str] = None  # Location context (e.g. "Shuswap Lake")

    # Set True to skip this regulation during processing
    _disabled: bool = False
    mu_ids: Optional[List[str]] = None  # Only include these MUs (None = all in zone)
    exclude_mu_ids: Optional[List[str]] = None  # MUs to exclude from zone match
    include_mu_ids: Optional[List[str]] = None  # Extra MUs to add (can be outside zone)

    # Direct-match fields (any populated → targets specific waterbodies)
    gnis_ids: Optional[List[str]] = None
    waterbody_poly_ids: Optional[List[str]] = None
    fwa_watershed_codes: Optional[List[str]] = None
    waterbody_keys: Optional[List[str]] = None
    linear_feature_ids: Optional[List[str]] = None
    blue_line_keys: Optional[List[str]] = None
    sub_polygon_ids: Optional[List[str]] = None
    ungazetted_waterbody_id: Optional[str] = None

    # Admin-match fields (admin_targets set → spatial intersection with admin polygons)
    admin_targets: Optional[List[AdminTarget]] = None

    @property
    def scope_type(self) -> str:
        """Return the resolution mode based on field state."""
        if self.admin_targets:
            return "admin_match"
        if self.has_direct_target():
            return "direct_match"
        return "zone_wide"

    def has_direct_target(self) -> bool:
        """Return True if any direct-match ID field is populated."""
        return any(
            [
                self.gnis_ids,
                self.waterbody_poly_ids,
                self.fwa_watershed_codes,
                self.waterbody_keys,
                self.linear_feature_ids,
                self.blue_line_keys,
                self.sub_polygon_ids,
                self.ungazetted_waterbody_id,
            ]
        )


# ============================================================================
# Zone Base Regulations Data
# ============================================================================

ZONE_BASE_REGULATIONS: List[ZoneRegulation] = [
    # ========================================================================
    # REGION 1 — Vancouver Island (excluding Haida Gwaii) — Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r1_trout_char_daily_quota",
        zone_ids=["1"],
        rule_text="Trout: 4 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 4 trout (all species combined).",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_trout_char_over_50cm_limit",
        zone_ids=["1"],
        rule_text="Trout: not more than 1 over 50 cm (2 hatchery steelhead over 50 cm allowed).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": (
                "Max 1 trout over 50 cm in daily quota "
                "(2 hatchery steelhead over 50 cm allowed)."
            ),
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_trout_stream_limit",
        zone_ids=["1"],
        rule_text="Trout from streams: max 2 (must be hatchery origin).",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout from streams. Must be hatchery origin.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_steelhead_release",
        zone_ids=["1"],
        rule_text="Release all wild steelhead.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Release all wild steelhead.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_char_release",
        zone_ids=["1"],
        rule_text="Release all char (includes Dolly Varden).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Release all char including Dolly Varden.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_bass_unlimited",
        zone_ids=["1"],
        rule_text="Bass: unlimited.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Unlimited daily quota for bass.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_crayfish_quota",
        zone_ids=["1"],
        rule_text="Crayfish: 25.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 25 crayfish.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_kokanee_quota",
        zone_ids=["1"],
        rule_text="Kokanee: 5 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 kokanee.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_kokanee_closed_streams",
        zone_ids=["1"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_sturgeon_catch_release",
        zone_ids=["1"],
        rule_text="White Sturgeon: catch and release only.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Catch and release only for white sturgeon.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_yellow_perch_unlimited",
        zone_ids=["1"],
        rule_text="Yellow perch: unlimited.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Unlimited daily quota for yellow perch.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 1 — Wild Trout Release (all streams)
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r1_wild_trout_release_streams",
        zone_ids=["1"],
        rule_text=(
            "Release of all wild origin trout in streams required. "
            "Only hatchery origin trout (identified by healed adipose fin "
            "scar) may be harvested from streams. Does not apply to lakes."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": (
                "All wild trout must be released in streams. Only hatchery "
                "trout (adipose fin clipped) may be kept."
            ),
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 1 + HAIDA GWAII — General Stream Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r1_summer_stream_closure",
        zone_ids=["1"],
        mu_ids=["1-1", "1-2", "1-3", "1-4", "1-5", "1-6"],
        rule_text=(
            "No fishing in any stream in Management Units 1-1 to 1-6 "
            "from July 15 to August 31."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Jul 15 – Aug 31"],
        restriction={
            "type": "Closed",
            "details": "Summer stream closure Jul 15–Aug 31. MUs 1-1 to 1-6.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_single_barbless_hook_streams",
        zone_ids=["1"],
        include_mu_ids=["6-12", "6-13"],
        rule_text=(
            "Single barbless hook must be used in all streams of Region 1 "
            "and Haida Gwaii (MUs 6-12, 6-13), all year."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hook required in all streams, all year.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_bait_ban_streams",
        zone_ids=["1"],
        rule_text=(
            "Bait ban applies to all streams of Region 1, all year, "
            "with some exceptions noted in the tables."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Bait Restriction",
            "details": "Bait banned in all streams, all year. See tables for exceptions.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_bait_ban_streams",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text=(
            "Bait ban applies to all streams in Management Units 6-12 "
            "and 6-13 (Haida Gwaii), November 1 to April 30."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Nov 1 – Apr 30"],
        restriction={
            "type": "Bait Restriction",
            "details": "Bait banned in streams Nov 1–Apr 30.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # HAIDA GWAII (MUs 6-12, 6-13) — Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_hg_trout_char_daily_quota",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii trout/char: 5 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_trout_char_over_50cm_limit",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii trout/char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_dolly_varden_limit",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii trout/char: not more than 3 Dolly Varden.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 3 Dolly Varden in daily quota.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_trout_char_stream_limit",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii trout/char from streams: max 2.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout/char from streams.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_trout_char_stream_release",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii: release trout/char under 30 cm from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "Release trout/char under 30 cm from streams.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_steelhead_release",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii: release all wild steelhead.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Release all wild steelhead.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_kokanee_quota",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii kokanee: 10 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 10 kokanee.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_hg_kokanee_closed_streams",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text="Haida Gwaii kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 1 preamble (Haida Gwaii), 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 1 — Possession, Annual Quotas & Salmon Notice
    # NOTE: These appear in Region 1's preamble but are province-wide rules.
    # They are entered here per-region as written in the synopsis. If all
    # regions share identical text, consider promoting to provincial_base_regulations.
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r1_possession_quota",
        zone_ids=["1"],
        include_mu_ids=["6-12", "6-13"],
        rule_text="Possession quotas = 2 daily quotas.",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_steelhead_annual_quota",
        zone_ids=["1"],
        include_mu_ids=["6-12", "6-13"],
        rule_text=(
            "Annual catch quota for all B.C.: 10 steelhead per licence year "
            "(only hatchery steelhead may be retained)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Annual Quota",
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_salmon_notice",
        zone_ids=["1"],
        include_mu_ids=["6-12", "6-13"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations at "
            "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/salmon-saumon-eng.html. "
            "When fresh waters are closed to fishing or have gear restrictions "
            "in this Synopsis, those regulations apply to salmon as well."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Stream closures and "
                "gear restrictions in this Synopsis also apply to salmon."
            ),
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    # ========================================================================
    # REGION 1 — Mercury Advisory (Vancouver Island & Gulf Islands)
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r1_mercury_advisory_bass",
        zone_ids=["1"],
        rule_text=(
            "Mercury advisory: Mercury levels in larger Smallmouth Bass in "
            "lakes on Vancouver Island and the Gulf Islands may be above "
            "national guidelines. The general public, especially children "
            "and women of child bearing age, are recommended to limit "
            "consumption of Smallmouth Bass."
        ),
        feature_types=[FeatureType.LAKE],
        scope_location="Vancouver Island & Gulf Islands lakes",
        restriction={
            "type": "Advisory",
            "details": (
                "Mercury advisory for Smallmouth Bass in lakes. "
                "Levels increase with fish size. Limit consumption, "
                "especially children and women of child bearing age."
            ),
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    #
    # ========================================================================
    # REGION 2 — Lower Mainland — General Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r2_single_barbless_hook_streams",
        zone_ids=["2"],
        rule_text=(
            "Single barbless hook must be used in all streams of Region 2, " "all year."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hook required in all streams, all year.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # TODO: NEEDS DIRECT-MATCH IDS — currently zone-wide on all R2 streams
    #   but should only target Fraser River, Lower Pitt River (CPR Bridge to
    #   Pitt Lake), Lower Harrison River (Fraser R to Harrison Lake).
    #   Need: blue_line_keys for each river segment.
    ZoneRegulation(
        regulation_id="zone_r2_dead_finfish_bait_sturgeon",
        zone_ids=["2"],
        _disabled=True,
        rule_text=(
            "Dead fin fish as bait only permitted in Region 2 when sport "
            "fishing for sturgeon in the Fraser River, Lower Pitt River "
            "(CPR Bridge upstream to Pitt Lake), Lower Harrison River "
            "(Fraser River upstream to Harrison Lake)."
        ),
        feature_types=[FeatureType.STREAM],
        scope_location="Fraser River, Lower Pitt River, Lower Harrison River",
        restriction={
            "type": "Bait Exception",
            "details": ("Dead fin fish bait permitted only for sturgeon fishing."),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
        # blue_line_keys=[],  # TODO: Fraser River BLK, Lower Pitt River BLK, Lower Harrison River BLK
    ),
    ZoneRegulation(
        regulation_id="zone_r2_steelhead_surcharge",
        zone_ids=["2"],
        rule_text=(
            "Steelhead fishing in the Lower Mainland Region: Your basic "
            "licence must be validated with a Conservation Surcharge Stamp "
            "if you fish for steelhead anywhere in B.C."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Licence Requirement",
            "details": "Conservation Surcharge Stamp required for steelhead fishing.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis. Province-wide requirement.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_steelhead_stop_fishing",
        zone_ids=["2"],
        rule_text=(
            "When you have caught and retained your daily quota of hatchery "
            "steelhead from any water, you must stop fishing that water for "
            "the remainder of that day."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota Enforcement",
            "details": (
                "Must stop fishing a water for the day once daily hatchery "
                "steelhead quota is reached."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_protected_species",
        zone_ids=["2"],
        rule_text=(
            "Protected species in Region 2: Nooksack dace, Salish sucker, "
            "Green sturgeon, Cultus Lake sculpin. It is illegal to fish for "
            "or catch and retain protected species."
        ),
        restriction={
            "type": "Protected Species",
            "details": (
                "Illegal to fish for or retain: Nooksack dace, Salish sucker, "
                "Green sturgeon, Cultus Lake sculpin."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 2 — Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r2_trout_char_daily_quota",
        zone_ids=["2"],
        rule_text="Trout/char: 4 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 4 trout/char (all species combined).",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_trout_char_over_50cm_limit",
        zone_ids=["2"],
        rule_text="Trout/char: not more than 1 over 50 cm (2 hatchery steelhead over 50 cm allowed).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": (
                "Max 1 trout/char over 50 cm in daily quota "
                "(2 hatchery steelhead over 50 cm allowed)."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_trout_char_stream_limit",
        zone_ids=["2"],
        rule_text="Trout/char from streams: max 2 (must be hatchery origin).",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout/char from streams. Must be hatchery origin.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_char_limit",
        zone_ids=["2"],
        rule_text="Max 1 char (bull trout, Dolly Varden, or lake trout). None under 60 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 char (bull trout, Dolly Varden, lake trout). None under 60 cm.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_wild_trout_char_stream_release",
        zone_ids=["2"],
        rule_text=(
            "Release all wild trout/char from streams. "
            "Release hatchery trout/char under 30 cm from streams."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": (
                "Release all wild trout/char from streams. "
                "Release hatchery trout/char under 30 cm from streams."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_steelhead_release",
        zone_ids=["2"],
        rule_text="Release all wild steelhead.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Release all wild steelhead.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_trout_lake_size_notice",
        zone_ids=["2"],
        rule_text="No general minimum size for trout in lakes.",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": "No general minimum size for trout in lakes.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # TODO: EXCEPTION NOT ENCODED — Mill Lake has a different bass quota
    #   but is not excluded from this zone-wide rule. Need gnis_ids for
    #   Mill Lake to either exclude it or create a separate override reg.
    ZoneRegulation(
        regulation_id="zone_r2_bass_quota",
        zone_ids=["2"],
        rule_text="Bass: 20 (excluding Mill Lake — see page 24 for quota).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 20 bass. Mill Lake exception — see water-specific tables.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
        # gnis_ids=[],  # TODO: Mill Lake GNIS ID to exclude or create override
    ),
    ZoneRegulation(
        regulation_id="zone_r2_crappie_quota",
        zone_ids=["2"],
        rule_text="Crappie: 20.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 20 crappie.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_crayfish_quota",
        zone_ids=["2"],
        rule_text="Crayfish: 25.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 25 crayfish.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_kokanee_quota",
        zone_ids=["2"],
        rule_text="Kokanee: 5 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 kokanee. None from streams.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_whitefish_quota",
        zone_ids=["2"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_sturgeon_catch_release",
        zone_ids=["2"],
        rule_text="White Sturgeon: catch and release only.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Catch and release only for white sturgeon.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # TODO: NEEDS DIRECT-MATCH IDS [HIGH PRIORITY] — closure currently applies
    #   to ALL R2 streams but should only target 3 specific side channels:
    #   - Jesperson's Side Channel
    #   - Herrling Island Side Channel
    #   - Seabird Island north Side Channel
    #   Need: blue_line_keys or linear_feature_ids for each side channel.
    ZoneRegulation(
        regulation_id="zone_r2_fraser_sturgeon_seasonal_closure",
        zone_ids=["2"],
        _disabled=True,
        rule_text=(
            "Fraser River: closed to all fishing in the Fraser areas of "
            "Jesperson's Side Channel, Herrling Island Side Channel, and "
            "Seabird Island north Side Channel, May 15 – July 31."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["May 15 – Jul 31"],
        scope_location="Fraser River side channels",
        restriction={
            "type": "Closed",
            "details": (
                "Jesperson's Side Channel, Herrling Island Side Channel, and "
                "Seabird Island north Side Channel closed to all fishing."
            ),
        },
        notes=(
            "Source: Region 2 preamble (under White Sturgeon), 2025-2027 Synopsis. "
            "See page 26 for map of closed area."
        ),
        # blue_line_keys=[],  # TODO: Jesperson's Side Channel BLK, Herrling Island Side Channel BLK, Seabird Island north Side Channel BLK
    ),
    # ========================================================================
    # REGION 2 — Possession, Annual Quotas & Salmon Notice
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r2_possession_quota",
        zone_ids=["2"],
        rule_text="Possession quotas = 2 daily quotas.",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_steelhead_annual_quota",
        zone_ids=["2"],
        rule_text=(
            "Annual catch quota for all B.C.: 10 steelhead per licence year "
            "(only hatchery steelhead may be retained)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Annual Quota",
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_salmon_notice",
        zone_ids=["2"],
        rule_text=(
            "Daily and annual quotas for salmon: refer to the notice on "
            "page 77 for salmon regulations."
        ),
        restriction={
            "type": "Notice",
            "details": ("Salmon regulations managed by DFO. See page 77 for details."),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 2 — Night Fishing Prohibition
    # ========================================================================
    # TODO: NEEDS DIRECT-MATCH IDS — currently zone-wide on all R2 streams
    #   but only applies to portions of Fraser, Harrison, and Pitt Rivers.
    #   Need: blue_line_keys for affected river portions.
    ZoneRegulation(
        regulation_id="zone_r2_night_fishing_prohibition",
        zone_ids=["2"],
        _disabled=True,
        rule_text=(
            "From one hour after sunset to one hour before sunrise, fishing "
            "is prohibited on portions of the Fraser, Harrison, and Pitt Rivers. "
            "See water-specific tables for details."
        ),
        feature_types=[FeatureType.STREAM],
        scope_location="Fraser, Harrison, and Pitt Rivers",
        restriction={
            "type": "Time Restriction",
            "details": (
                "Night fishing prohibited (1 hr after sunset to 1 hr before "
                "sunrise). See water-specific tables for details."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
        # blue_line_keys=[],  # TODO: Fraser River BLK(s), Harrison River BLK(s), Pitt River BLK(s)
    ),
    # ========================================================================
    # REGION 3 — Thompson-Nicola — General Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r3_spring_stream_closure",
        zone_ids=["3"],
        rule_text=(
            "No fishing in any stream in Region 3 from Jan 1 to June 30 "
            "(see tables for exceptions)."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Jan 1 – Jun 30"],
        restriction={
            "type": "Closed",
            "details": "Spring stream closure Jan 1–Jun 30. See tables for exceptions.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_single_barbless_hook_streams",
        zone_ids=["3"],
        rule_text=(
            "Single barbless hook must be used in all streams of Region 3, " "all year."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hook required in all streams, all year.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_steelhead_surcharge",
        zone_ids=["3"],
        rule_text=(
            "Your basic licence must be validated with a Steelhead "
            "Conservation Surcharge Stamp if you fish for steelhead "
            "anywhere in B.C. A Steelhead Stamp is mandatory when "
            "fishing most Classified Waters regardless of species."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Steelhead Conservation Surcharge Stamp required to fish "
                "for steelhead. Mandatory on most Classified Waters."
            ),
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis. Province-wide requirement.",
    ),
    # ========================================================================
    # REGION 3 — Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r3_trout_char_daily_quota",
        zone_ids=["3"],
        rule_text="Trout/char: 5 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_trout_char_over_50cm_limit",
        zone_ids=["3"],
        rule_text="Trout/char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_trout_char_stream_limit",
        zone_ids=["3"],
        rule_text="Trout/char from streams: max 4.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 4 trout/char from streams.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_char_limit",
        zone_ids=["3"],
        rule_text="Max 1 bull trout (Dolly Varden) or lake trout. None under 60 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 bull trout (Dolly Varden) or lake trout. None under 60 cm.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_steelhead_release",
        zone_ids=["3"],
        rule_text="Release ALL steelhead.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Release ALL steelhead.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_bull_trout_seasonal_release",
        zone_ids=["3"],
        rule_text="Release bull trout (Dolly Varden) from streams Aug 1–Oct 31.",
        feature_types=[FeatureType.STREAM],
        dates=["Aug 1 – Oct 31"],
        restriction={
            "type": "Catch and Release",
            "details": "Release bull trout (Dolly Varden) from streams Aug 1–Oct 31.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_lake_trout_seasonal_release",
        zone_ids=["3"],
        rule_text="Release lake trout Oct 15–Jan 31.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        dates=["Oct 15 – Jan 31"],
        restriction={
            "type": "Catch and Release",
            "details": "Release lake trout Oct 15–Jan 31.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_bass_closed",
        zone_ids=["3"],
        rule_text="Bass: 0 quota, closed to fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Bass: closed to fishing (0 quota).",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_burbot_quota",
        zone_ids=["3"],
        rule_text="Burbot: 2.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 2 burbot.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_crayfish_quota",
        zone_ids=["3"],
        rule_text="Crayfish: 25.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 25 crayfish.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_kokanee_quota",
        zone_ids=["3"],
        rule_text="Kokanee: 5 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 kokanee.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_kokanee_closed_streams",
        zone_ids=["3"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_whitefish_quota",
        zone_ids=["3"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_sturgeon_catch_release",
        zone_ids=["3"],
        rule_text="White Sturgeon: catch and release only.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Catch and release only for white sturgeon.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_yellow_perch_closed",
        zone_ids=["3"],
        rule_text="Yellow Perch: 0 quota, closed to fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Yellow perch: closed to fishing (0 quota).",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 3 — Possession, Annual Quotas & Notices
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r3_possession_quota",
        zone_ids=["3"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    # TODO: NEEDS DIRECT-MATCH IDS [HIGH PRIORITY] — Shuswap Lake annual
    #   rainbow quota currently applied to ALL R3 lakes. Need:
    #   gnis_ids or waterbody_keys for Shuswap Lake.
    ZoneRegulation(
        regulation_id="zone_r3_shuswap_annual_rainbow",
        zone_ids=["3"],
        _disabled=True,
        rule_text=(
            "Annual catch quota for Shuswap Lake: rainbow trout — "
            "5 over 50 cm per licence year."
        ),
        feature_types=[FeatureType.LAKE],
        scope_location="Shuswap Lake",
        restriction={
            "type": "Annual Quota",
            "details": "Annual quota: 5 rainbow trout over 50 cm.",
        },
        notes=(
            "Source: Region 3 preamble, 2025-2027 Synopsis. "
            "TODO: Convert to direct-match with Shuswap Lake GNIS/waterbody_key."
        ),
    ),
    # TODO: NEEDS DIRECT-MATCH IDS [HIGH PRIORITY] — Shuswap Lake annual
    #   char quota currently applied to ALL R3 lakes. Need:
    #   gnis_ids or waterbody_keys for Shuswap Lake.
    ZoneRegulation(
        regulation_id="zone_r3_shuswap_annual_char",
        zone_ids=["3"],
        _disabled=True,
        rule_text=(
            "Annual catch quota for Shuswap Lake: char — lake trout and "
            "bull trout (Dolly Varden) — 5 over 60 cm per licence year."
        ),
        feature_types=[FeatureType.LAKE],
        scope_location="Shuswap Lake",
        restriction={
            "type": "Annual Quota",
            "details": "Annual quota: 5 char (lake trout/bull trout) over 60 cm.",
        },
        notes=(
            "Source: Region 3 preamble, 2025-2027 Synopsis. "
            "TODO: Convert to direct-match with Shuswap Lake GNIS/waterbody_key."
        ),
    ),
    ZoneRegulation(
        regulation_id="zone_r3_steelhead_annual_quota",
        zone_ids=["3"],
        rule_text=(
            "Annual catch quota for all B.C.: 10 steelhead per licence year "
            "(only hatchery steelhead may be retained)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Annual Quota",
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_salmon_notice",
        zone_ids=["3"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations. When fresh waters are closed to "
            "fishing or have gear restrictions in this Synopsis, those "
            "regulations apply to salmon as well."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Stream closures and "
                "gear restrictions in this Synopsis also apply to salmon."
            ),
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_report_tagged_fish",
        zone_ids=["3"],
        rule_text=(
            "Please report tagged fish to the Fish and Wildlife Regional "
            "Office in Kamloops at 1-800-388-1606. Include tag number and "
            "colour, fish length and weight, and location of capture."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Report tagged fish to Kamloops regional office: " "1-800-388-1606."
            ),
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r3_bass_perch_illegal_notice",
        zone_ids=["3"],
        rule_text=(
            "It is illegal to fish for bass or perch in the Thompson-Nicola "
            "Region. This measure is part of B.C.'s management approach to "
            "illegal fish introductions."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Illegal to fish for bass or perch in Thompson-Nicola. "
                "Part of B.C.'s illegal fish introduction management."
            ),
        },
        notes="Source: Region 3 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 3 — Steelhead Management Closures (direct-match — need feature IDs)
    # ========================================================================
    #
    # TODO: These regulations target specific river segments and lakes.
    # They need direct-match IDs (blue_line_keys, linear_feature_ids, or
    # gnis_ids) to be looked up in the FWA data. Uncomment and populate
    # the ID fields when available.
    #
    # ZoneRegulation(
    #     regulation_id="zone_r3_thompson_steelhead_closure",
    #     zone_ids=["3"],
    #     rule_text=(
    #         "Thompson River: downstream of signs at Kamloops Lake outlet "
    #         "to the confluence with Fraser River, Oct 1-May 31 "
    #         "(see tables for exceptions)."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["steelhead"],
    #         "details": (
    #             "Thompson River closed to steelhead fishing Oct 1–May 31. "
    #             "Kamloops Lake outlet to Fraser River confluence."
    #         ),
    #         "dates": {"period": "Oct 1 – May 31", "type": "closure"},
    #     },
    #     notes="Source: Region 3 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     blue_line_keys=[],  # TODO: Thompson River BLK(s)
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r3_fraser_steelhead_closure_lillooet",
    #     zone_ids=["3"],
    #     rule_text=(
    #         "Fraser River: from Hwy 99 bridge at Lillooet to BC Hydro "
    #         "tail race outflow channel, Oct 1-May 31."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["steelhead"],
    #         "details": (
    #             "Fraser River closed to steelhead fishing Oct 1–May 31. "
    #             "Hwy 99 bridge at Lillooet to BC Hydro tail race outflow."
    #         ),
    #         "dates": {"period": "Oct 1 – May 31", "type": "closure"},
    #     },
    #     notes="Source: Region 3 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     blue_line_keys=[],  # TODO: Fraser River BLK(s) for this segment
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r3_fraser_steelhead_closure_thompson",
    #     zone_ids=["3"],
    #     rule_text=(
    #         "Fraser River: from the confluence with Thompson River to CNR "
    #         "Bridge approximately 1 km downstream, Oct 1-May 31."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["steelhead"],
    #         "details": (
    #             "Fraser River closed to steelhead fishing Oct 1–May 31. "
    #             "Thompson River confluence to CNR Bridge ~1 km downstream."
    #         ),
    #         "dates": {"period": "Oct 1 – May 31", "type": "closure"},
    #     },
    #     notes="Source: Region 3 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     blue_line_keys=[],  # TODO: Fraser River BLK(s) for this segment
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r3_nahatlatch_stein_closure",
    #     zone_ids=["3"],
    #     rule_text=(
    #         "Nahatlatch River downstream of Nahatlatch Lake and Stein River: "
    #         "closed Jan 1-May 31."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["steelhead"],
    #         "details": (
    #             "Nahatlatch River (below Nahatlatch Lake) and Stein River "
    #             "closed Jan 1–May 31."
    #         ),
    #         "dates": {"period": "Jan 1 – May 31", "type": "closure"},
    #     },
    #     notes="Source: Region 3 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     blue_line_keys=[],  # TODO: Nahatlatch River + Stein River BLKs
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r3_frances_hannah_closure",
    #     zone_ids=["3"],
    #     rule_text="Frances and Hannah lakes: closed Jan 1-May 31.",
    #     feature_types=[FeatureType.LAKE],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["steelhead"],
    #         "details": "Frances and Hannah lakes closed Jan 1–May 31.",
    #         "dates": {"period": "Jan 1 – May 31", "type": "closure"},
    #     },
    #     notes="Source: Region 3 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     gnis_ids=[],  # TODO: GNIS IDs for Frances Lake and Hannah Lake
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r3_seton_closure",
    #     zone_ids=["3"],
    #     rule_text=(
    #         "Seton River downstream of Seton Lake: closed Apr 1-May 31."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["steelhead"],
    #         "details": "Seton River (below Seton Lake) closed Apr 1–May 31.",
    #         "dates": {"period": "Apr 1 – May 31", "type": "closure"},
    #     },
    #     notes="Source: Region 3 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     blue_line_keys=[],  # TODO: Seton River BLK
    # ),
    #
    # ========================================================================
    # REGION 2 — Admin-match regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r2_knapp_forest_closure",
        zone_ids=["2"],
        rule_text=(
            "No fishing in any lake in the UBC Malcolm Knapp Research "
            "Forest near Maple Ridge."
        ),
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": "All lakes closed in UBC Malcolm Knapp Research Forest.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
        admin_targets=[AdminTarget("osm_admin_boundaries", "1166294466")],
    ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r2_rubble_creek_advisory",
    #     zone_ids=["2"],
    #     rule_text=(
    #         "Notice: The Rubble Creek Landslide Hazard Area is a high risk "
    #         "slide area. People who fish in this area do so at their own risk."
    #     ),
    #     restriction={
    #         "type": "Advisory",
    #         "details": (
    #             "Rubble Creek Landslide Hazard Area — high risk slide zone. "
    #             "Fish at own risk."
    #         ),
    #     },
    #     notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    #     admin_targets=[],  # TODO: polygon data not yet available
    # ),
    # ========================================================================
    # REGION 4 — Kootenay: General Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r4_spring_stream_closure",
        zone_ids=["4"],
        rule_text=(
            "Unless otherwise noted in the tables that follow, streams in "
            "Region 4 are closed to fishing Apr 1–Jun 14."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Apr 1 – Jun 14"],
        restriction={
            "type": "Closed",
            "details": "Streams closed Apr 1–Jun 14 unless noted in tables.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_trout_char_stream_release",
        zone_ids=["4"],
        rule_text=(
            "Nov 1–Mar 31 all trout and char caught in streams must be "
            "released immediately (unless otherwise noted)."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Nov 1 – Mar 31"],
        restriction={
            "type": "Catch and Release",
            "details": (
                "All trout and char caught in streams must be released " "Nov 1–Mar 31."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_single_barbless_hook_streams",
        zone_ids=["4"],
        rule_text="Only single barbless hooks may be used in all streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hooks only in all streams.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_classified_waters_notice",
        zone_ids=["4"],
        rule_text=(
            "Classified Waters require a Classified Waters licence in "
            "addition to a basic angling licence. See waterbody tables."
        ),
        restriction={
            "type": "Licence Requirement",
            "details": (
                "Classified Waters require a Classified Waters licence "
                "in addition to a basic angling licence."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 4 — Kootenay: Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r4_trout_char_daily_quota",
        zone_ids=["4"],
        rule_text=(
            "Trout and Char: 5 (all species combined), including not more "
            "than 1 over 50 cm any species."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_trout_char_stream_limit",
        zone_ids=["4"],
        rule_text="Trout and Char: not more than 2 from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout/char may be kept from streams.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_rainbow_cutthroat_size_limit",
        zone_ids=["4"],
        rule_text=(
            "Trout and Char: not more than 1 rainbow or cutthroat over "
            "50 cm of any species."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": ("Max 1 rainbow or cutthroat trout over 50 cm in daily quota."),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_bull_trout_limit",
        zone_ids=["4"],
        rule_text="Trout and Char: not more than 1 bull trout (Dolly Varden) of any size.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 bull trout (Dolly Varden) of any size in daily quota.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_bass_closed",
        zone_ids=["4"],
        rule_text="Bass: 0 quota, closed to fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Bass: closed to fishing (0 quota).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_burbot_quota",
        zone_ids=["4"],
        rule_text="Burbot: 2.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 2 burbot.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_crayfish_quota",
        zone_ids=["4"],
        rule_text="Crayfish: 25.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 25 crayfish.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_kokanee_quota",
        zone_ids=["4"],
        rule_text="Kokanee: 15, of which not more than 5 may be over 30 cm.",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 kokanee (max 5 over 30 cm).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_kokanee_closed_streams",
        zone_ids=["4"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_northern_pike_closed",
        zone_ids=["4"],
        rule_text="Northern Pike: 0 quota, closed to fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Northern pike: closed to fishing (0 quota).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_walleye_closed",
        zone_ids=["4"],
        rule_text="Walleye: 0 quota, closed to fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Walleye: closed to fishing (0 quota).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_sturgeon_closed",
        zone_ids=["4"],
        rule_text="White Sturgeon: no fishing (no exceptions).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": (
                "White sturgeon: no fishing permitted (no exceptions). "
                "Endangered under SARA."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_whitefish_quota",
        zone_ids=["4"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_yellow_perch_closed",
        zone_ids=["4"],
        rule_text="Yellow Perch: 0 quota, closed to fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Yellow perch: closed to fishing (0 quota).",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 4 — Kootenay: Possession, Annual Quotas & Notices
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r4_possession_quota",
        zone_ids=["4"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    # TODO: NEEDS DIRECT-MATCH IDS [HIGH PRIORITY] — Kootenay Lake annual
    #   rainbow quota currently applied to ALL R4 lakes. Need:
    #   gnis_ids or waterbody_keys for Kootenay Lake.
    ZoneRegulation(
        regulation_id="zone_r4_kootenay_lake_rainbow_annual",
        zone_ids=["4"],
        _disabled=True,
        rule_text=(
            "Annual catch quota for Kootenay Lake: rainbow trout — "
            "20 over 50 cm per licence year."
        ),
        feature_types=[FeatureType.LAKE],
        scope_location="Kootenay Lake",
        restriction={
            "type": "Annual Quota",
            "details": "Annual quota: 20 rainbow trout over 50 cm.",
        },
        notes=(
            "Source: Region 4 preamble, 2025-2027 Synopsis. "
            "TODO: Convert to direct-match with Kootenay Lake GNIS/waterbody_key."
        ),
    ),
    ZoneRegulation(
        regulation_id="zone_r4_steelhead_annual_quota",
        zone_ids=["4"],
        rule_text=(
            "Annual catch quota for all B.C.: 10 steelhead per licence year "
            "(only hatchery steelhead may be retained)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Annual Quota",
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    # TODO: EXCEPTION NOT ENCODED — Koocanusa Reservoir is excluded from
    #   the lake trout reporting request but is not carved out. Need:
    #   gnis_ids or waterbody_keys for Koocanusa Reservoir to exclude.
    ZoneRegulation(
        regulation_id="zone_r4_report_lake_trout",
        zone_ids=["4"],
        rule_text=(
            "Please report all lake trout caught from Region 4 waters "
            "(except Koocanusa Reservoir) to the Kootenay regional office "
            "at (250) 489-8540."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Report all lake trout caught in Region 4 (except "
                "Koocanusa Reservoir) to (250) 489-8540."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_fish_consumption_notice",
        zone_ids=["4"],
        rule_text=(
            "Fish Consumption Notice: for Region 4 waters, see the "
            "Interior Region fish consumption tables."
        ),
        restriction={
            "type": "Advisory",
            "details": (
                "Fish consumption advisory: see Interior Region fish "
                "consumption tables for Region 4 waters."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_creston_valley_wma_permit",
        zone_ids=["4"],
        rule_text=(
            "A permit is required for fishing on all waters within the "
            "Creston Valley Wildlife Management Area, including Six Mile, "
            "Leach, Kootenay River and Canal and Duck Lake."
        ),
        admin_targets=[AdminTarget("wma", "5364")],
        restriction={
            "type": "Licence Requirement",
            "details": (
                "A permit is required for fishing in the Creston Valley "
                "Wildlife Management Area. Visit www.crestonwildlife.ca "
                "or call 250-402-6900."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_bass_perch_pike_walleye_illegal_notice",
        zone_ids=["4"],
        rule_text=(
            "It is illegal to fish for bass, yellow perch, northern pike "
            "or walleye in the Kootenay Region. This measure is part of "
            "B.C.'s management approach to illegal fish introductions."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Illegal to fish for bass, yellow perch, northern pike, "
                "or walleye in Kootenay. Part of B.C.'s illegal fish "
                "introduction management."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    # TODO: NEEDS DIRECT-MATCH IDS — Kootenay Lake recovery notice
    #   currently applied to ALL R4 waterbodies. Need:
    #   gnis_ids or waterbody_keys for Kootenay Lake.
    ZoneRegulation(
        regulation_id="zone_r4_kootenay_lake_recovery_notice",
        zone_ids=["4"],
        _disabled=True,
        rule_text=(
            "Note: Kootenay Lake fish populations are in a state of recovery. "
            "All conservation measures and fishing regulations should be "
            "carefully followed."
        ),
        scope_location="Kootenay Lake",
        restriction={
            "type": "Notice",
            "details": (
                "Kootenay Lake fish populations are in a state of recovery. "
                "Follow all conservation measures carefully."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r4_salmon_notice",
        zone_ids=["4"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations. When fresh waters are closed to "
            "fishing or have gear restrictions in this Synopsis, those "
            "regulations apply to salmon as well."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Stream closures and "
                "gear restrictions in this Synopsis also apply to salmon."
            ),
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    # ========================================================================
    # REGION 5 — Cariboo: General Regulations
    # ========================================================================
    # TODO: EXCEPTION NOT ENCODED — Fraser River mainstem and listed streams
    #   are exempt from this closure but are not carved out. Need:
    #   blue_line_keys for Fraser River mainstem to exclude.
    ZoneRegulation(
        regulation_id="zone_r5_spring_stream_closure",
        zone_ids=["5"],
        rule_text=(
            "No fishing in any stream in Fraser River Watershed of Region 5 "
            "(including the Thompson River Watershed) from Apr 1–Jun 30, "
            "EXCEPT the mainstem of the Fraser River and other streams listed "
            "in the tables."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Apr 1 – Jun 30"],
        restriction={
            "type": "Closed",
            "details": (
                "Streams closed Apr 1–Jun 30 (Fraser River Watershed of "
                "Region 5) unless noted in tables."
            ),
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_single_barbless_hook_streams",
        zone_ids=["5"],
        rule_text="Only single barbless hooks may be used in all streams, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hooks only in all streams, all year.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_no_minimum_size_lakes",
        zone_ids=["5"],
        rule_text="There is no minimum size limit in lakes (see tables for exceptions).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": "No minimum size limit in lakes (see tables for exceptions).",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_classified_waters_notice",
        zone_ids=["5"],
        rule_text=(
            "Classified Waters require a Classified Waters licence in "
            "addition to a basic angling licence. See waterbody tables."
        ),
        restriction={
            "type": "Licence Requirement",
            "details": (
                "Classified Waters require a Classified Waters licence "
                "in addition to a basic angling licence."
            ),
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_steelhead_stamp_notice",
        zone_ids=["5"],
        rule_text=(
            "Your basic licence must be validated with a Steelhead "
            "Conservation Surcharge Stamp if you fish for steelhead "
            "anywhere in B.C."
        ),
        restriction={
            "type": "Licence Requirement",
            "details": (
                "Steelhead Conservation Surcharge Stamp required to fish "
                "for steelhead. Steelhead Stamp mandatory for most "
                "Classified Waters."
            ),
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    # ========================================================================
    # REGION 5 — Cariboo: Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r5_trout_char_daily_quota",
        zone_ids=["5"],
        rule_text=(
            "Trout and Char: 5 (all species combined), including not more "
            "than 1 over 50 cm."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_trout_char_over_50cm_limit",
        zone_ids=["5"],
        rule_text="Trout and Char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_trout_char_stream_limit",
        zone_ids=["5"],
        rule_text="Trout and Char: not more than 2 from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout/char may be kept from streams.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_bull_trout_limit",
        zone_ids=["5"],
        rule_text="Trout and Char: not more than 1 Dolly Varden/bull trout.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 bull trout (Dolly Varden) of any size in daily quota.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_lake_trout_limit",
        zone_ids=["5"],
        rule_text="Trout and Char: not more than 2 lake trout.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 2 lake trout in daily quota.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_steelhead_release",
        zone_ids=["5"],
        rule_text="All steelhead must be released.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "All steelhead must be released.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_lake_trout_seasonal_release",
        zone_ids=["5"],
        rule_text="Release lake trout Oct 1–Nov 30.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        dates=["Oct 1 – Nov 30"],
        restriction={
            "type": "Catch and Release",
            "details": "Release lake trout Oct 1–Nov 30.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_bull_trout_stream_release",
        zone_ids=["5"],
        rule_text="Release bull trout (Dolly Varden) from streams Aug 1–Oct 31.",
        feature_types=[FeatureType.STREAM],
        dates=["Aug 1 – Oct 31"],
        restriction={
            "type": "Catch and Release",
            "details": "Release bull trout (Dolly Varden) from streams Aug 1–Oct 31.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_bass_closed",
        zone_ids=["5"],
        rule_text="Bass: 0 quota, closed to all fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Bass: closed to fishing (0 quota).",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_burbot_quota",
        zone_ids=["5"],
        rule_text="Burbot: 5.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 burbot.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_kokanee_quota",
        zone_ids=["5"],
        rule_text="Kokanee: 5 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 kokanee.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_kokanee_closed_streams",
        zone_ids=["5"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_whitefish_quota",
        zone_ids=["5"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 5 — Cariboo: Possession, Notices & Warnings
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r5_possession_quota",
        zone_ids=["5"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_salmon_notice",
        zone_ids=["5"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Stream closures and "
                "gear restrictions in this Synopsis also apply to salmon."
            ),
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_bass_illegal_notice",
        zone_ids=["5"],
        rule_text=(
            "It is illegal to fish for bass in the Cariboo Region. This "
            "measure is part of B.C.'s management approach to illegal fish "
            "introductions."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Illegal to fish for bass in Cariboo. Part of B.C.'s "
                "illegal fish introduction management."
            ),
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r5_ice_fishing_huts_notice",
        zone_ids=["5"],
        rule_text=(
            "Ice Fishing Huts: Failure to remove ice fishing huts from lakes "
            "before spring breakup is an offence under the Environmental "
            "Management Act."
        ),
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Ice fishing huts must be removed before spring breakup "
                "(Environmental Management Act)."
            ),
        },
        notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 5 — Waterbody-specific (commented out — need linking IDs)
    # ========================================================================
    #
    # --- White Sturgeon (Fraser River split at Williams Lake River) ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_sturgeon_closed_upstream",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "White Sturgeon: closed to all fishing in the Fraser River "
    #         "Watershed upstream of Williams Lake River."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "White sturgeon: closed to all fishing in Fraser River "
    #             "Watershed upstream of Williams Lake River."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs blue_line_keys or fwa_watershed_codes for Fraser River
    #     #       Watershed upstream of Williams Lake River
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_sturgeon_catch_release_downstream",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "White Sturgeon: catch and release in the Fraser River "
    #         "Watershed downstream of and including Williams Lake River."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     restriction={
    #         "type": "Catch and Release",
    #         "details": (
    #             "Catch and release only for white sturgeon in Fraser River "
    #             "Watershed downstream of Williams Lake River."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs blue_line_keys or fwa_watershed_codes for Fraser River
    #     #       Watershed downstream of and including Williams Lake River
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_sturgeon_seasonal_closure_fraser",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "White Sturgeon: closed to all fishing in the Fraser River "
    #         "downstream of and including Williams Lake River, "
    #         "Sept 15–Jul 15."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     dates=["Sept 15 – Jul 15"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "White sturgeon: closed Sept 15–Jul 15 in Fraser River "
    #             "downstream of Williams Lake River."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs blue_line_keys for Fraser River mainstem downstream
    #     #       of Williams Lake River confluence
    # ),
    #
    # --- Chilcotin River steelhead closure ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_chilcotin_steelhead_closure",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "Chilcotin River downstream of Chilko River: closed to all "
    #         "fishing Oct 1–Jun 10. Sport fishing openings announced "
    #         "in-season if abundance is adequate."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     dates=["Oct 1 – Jun 10"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "Chilcotin River downstream of Chilko River closed "
    #             "Oct 1–Jun 10. In-season openings may be announced."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble (Notice to Anglers), 2025-2027 Synopsis.",
    #     # TODO: needs blue_line_keys for Chilcotin River downstream of
    #     #       Chilko River confluence
    # ),
    #
    # --- Dean River Classified Waters ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_dean_river_classified",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "Dean River: all anglers must buy a Classified Waters Licence "
    #         "to fish classified portions. Non-Resident Aliens limited to "
    #         "one licence, one classified section, max 8 consecutive days "
    #         "per year. See tables for areas and dates."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Licence Requirement",
    #         "details": (
    #             "Dean River: Classified Waters Licence required. "
    #             "Non-Resident Aliens limited to 1 licence, 1 section, "
    #             "max 8 consecutive days/year."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble (Dean River), 2025-2027 Synopsis.",
    #     # TODO: needs blue_line_keys for Dean River classified sections
    # ),
    #
    # --- Thin ice warning (specific lakes) ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_thin_ice_warning",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "WARNING: Due to aeration projects, dangerous thin ice and "
    #         "open water may exist on Dewar, Higgins, Irish, Simon and "
    #         "Skulow Lakes."
    #     ),
    #     feature_types=[FeatureType.LAKE],
    #     restriction={
    #         "type": "Advisory",
    #         "details": (
    #             "Dangerous thin ice and open water due to aeration on "
    #             "Dewar, Higgins, Irish, Simon and Skulow Lakes."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs gnis_ids for Dewar, Higgins, Irish, Simon, Skulow Lakes
    # ),
    #
    # --- Steelhead management (Chilcotin Watershed) ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r5_steelhead_chilcotin_watershed_notice",
    #     zone_ids=["5"],
    #     rule_text=(
    #         "In response to declining abundance of Fraser Basin steelhead, "
    #         "steelhead fisheries within the Chilcotin River Watershed may "
    #         "be closed."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Notice",
    #         "details": (
    #             "Steelhead fisheries in Chilcotin River Watershed may be "
    #             "closed due to declining Fraser Basin steelhead abundance."
    #         ),
    #     },
    #     notes="Source: Region 5 preamble (Steelhead Management), 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Chilcotin River Watershed
    # ),
    # ========================================================================
    # REGION 6 — Skeena: General Regulations
    # ========================================================================
    # TODO: EXCEPTION NOT ENCODED — Skeena, Nass, Iskut, Stikine, Taku
    #   River mainstems are exempt from this closure but are not carved out.
    #   Need: blue_line_keys for each mainstem to exclude.
    ZoneRegulation(
        regulation_id="zone_r6_steelhead_stream_closure",
        zone_ids=["6"],
        rule_text=(
            "No fishing in all rivers and streams for steelhead, "
            "May 15–Jun 15. Exemptions include mainstem portions of the "
            "Skeena, Nass, Iskut, Stikine, and Taku Rivers not currently "
            "closed under existing winter/spring closure."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["May 15 – Jun 15"],
        restriction={
            "type": "Closed",
            "details": (
                "Streams closed to steelhead fishing May 15–Jun 15. "
                "Exemptions: Skeena, Nass, Iskut, Stikine, Taku mainstems."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_single_barbless_hook_streams",
        zone_ids=["6"],
        rule_text="Only single barbless hooks may be used in all streams, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hooks only in all streams, all year.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_set_lining_lakes",
        zone_ids=["6"],
        rule_text=(
            "Set lining is only permitted in the lakes of Region 6. "
            "Set lines restricted to one line with a single hook (gap ≥3 cm), "
            "marked with angler's name, address and phone. Any game fish "
            "caught other than burbot must be released."
        ),
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Gear Restriction",
            "details": (
                "Set lines permitted in lakes only. One line, single hook "
                "(≥3 cm gap), labelled with name/address/phone. Only burbot "
                "may be retained; all other game fish must be released."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 6 — Skeena: Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r6_trout_char_daily_quota",
        zone_ids=["6"],
        rule_text=(
            "Trout and Char: 5 (all species combined), including not more "
            "than 1 over 50 cm (quota includes hatchery steelhead)."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": (
                "Daily quota: 5 trout/char (all species combined, "
                "includes hatchery steelhead)."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_trout_char_over_50cm_limit",
        zone_ids=["6"],
        rule_text="Trout and Char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_bull_lake_trout_limit",
        zone_ids=["6"],
        rule_text=(
            "Trout and Char: not more than 3 Dolly Varden/bull trout "
            "and/or lake trout combined."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": (
                "Max 3 bull trout (Dolly Varden) and/or lake trout "
                "combined in daily quota."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_trout_stream_limit",
        zone_ids=["6"],
        rule_text="Trout: not more than 1 from streams, Jul 1–Oct 31.",
        feature_types=[FeatureType.STREAM],
        dates=["Jul 1 – Oct 31"],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout may be kept from streams Jul 1–Oct 31.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_bull_trout_stream_release",
        zone_ids=["6"],
        rule_text=("Release all Dolly Varden/bull trout from streams, all year."),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "All bull trout (Dolly Varden) must be released from streams.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_trout_under_30cm_stream_release",
        zone_ids=["6"],
        rule_text="Release trout under 30 cm from any stream.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "Trout under 30 cm must be released from streams.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_trout_stream_seasonal_release",
        zone_ids=["6"],
        rule_text="Release trout of any size from streams Nov 1–Jun 30.",
        feature_types=[FeatureType.STREAM],
        dates=["Nov 1 – Jun 30"],
        restriction={
            "type": "Catch and Release",
            "details": "All trout must be released from streams Nov 1–Jun 30.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_steelhead_release",
        zone_ids=["6"],
        rule_text="All wild steelhead must be released.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "All wild steelhead must be released.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_arctic_grayling_quota",
        zone_ids=["6"],
        rule_text="Arctic Grayling: 3.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 3 Arctic grayling.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_burbot_quota",
        zone_ids=["6"],
        rule_text="Burbot: 5.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 burbot.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_inconnu_quota",
        zone_ids=["6"],
        rule_text="Inconnu: 1.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 1 inconnu.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_kokanee_quota",
        zone_ids=["6"],
        rule_text="Kokanee: 10 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 10 kokanee.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_kokanee_closed_streams",
        zone_ids=["6"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_northern_pike_quota",
        zone_ids=["6"],
        rule_text="Northern Pike: 5.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 northern pike.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_whitefish_quota",
        zone_ids=["6"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_sturgeon_closed",
        zone_ids=["6"],
        rule_text="White Sturgeon: closed to all fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "White sturgeon: closed to all fishing.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 6 — Skeena: Possession, Annual Quotas & Notices
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r6_possession_quota",
        zone_ids=["6"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_steelhead_annual_quota",
        zone_ids=["6"],
        rule_text=(
            "Annual catch quota for all B.C.: 10 hatchery steelhead per "
            "licence year."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Annual Quota",
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_steelhead_stamp_notice",
        zone_ids=["6"],
        rule_text=(
            "Your basic licence must be validated with a Steelhead "
            "Conservation Surcharge Stamp if you fish for steelhead "
            "anywhere in B.C."
        ),
        restriction={
            "type": "Licence Requirement",
            "details": (
                "Steelhead Conservation Surcharge Stamp required to fish "
                "for steelhead. Steelhead Stamp mandatory for most "
                "Classified Waters."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_steelhead_daily_stop",
        zone_ids=["6"],
        rule_text=(
            "When you have caught and retained your daily quota of hatchery "
            "steelhead from any water, you must stop fishing that water for "
            "the remainder of that day."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota Enforcement",
            "details": (
                "Stop fishing the water for the day once daily quota of "
                "hatchery steelhead is retained."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_salmon_notice",
        zone_ids=["6"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Stream closures and "
                "gear restrictions in this Synopsis also apply to salmon."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_skeena_quality_waters",
        zone_ids=["6"],
        rule_text=(
            "Skeena Quality Waters Strategy: for more information visit "
            "http://www.env.gov.bc.ca/skeena/qws/"
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Skeena Quality Waters Strategy in effect. "
                "See gov.bc.ca/skeena/qws for details."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_tagging_program_notice",
        zone_ids=["6"],
        rule_text=(
            "HIGH REWARD ($100) TAGGING PROGRAMS: The Skeena Fisheries team "
            "has multiple ongoing $100 reward tagging programs. Report "
            "captures by call or text to 250-643-7290, or return tags in "
            "person to 3726 Alfred Ave, Smithers."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "$100 reward tagging programs in Skeena Region. "
                "Report: call/text 250-643-7290 or visit 3726 Alfred Ave, "
                "Smithers."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r6_first_nations_notice",
        zone_ids=["6"],
        rule_text=(
            "Notice to Anglers: Accurate and up-to-date information regarding "
            "First Nations notices can be found under Angler Alerts at "
            "www.gov.bc.ca/FishingRegulations."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "First Nations notices available under Angler Alerts at "
                "gov.bc.ca/FishingRegulations."
            ),
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 6 — Waterbody-specific (commented out — need linking IDs)
    # ========================================================================
    #
    # --- Bait ban: Skeena River + tribs and Nass River + tribs ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_bait_ban_skeena",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "Bait ban in the Skeena River including tributaries, year-round."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Bait Restriction",
    #         "details": "Bait banned in Skeena River and all tributaries, year-round.",
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Skeena River watershed
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_bait_ban_nass",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "Bait ban in the Nass River including tributaries, year-round."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Bait Restriction",
    #         "details": "Bait banned in Nass River and all tributaries, year-round.",
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Nass River watershed
    # ),
    #
    # --- Stream closures: Skeena upstream of Cedarvale / Nass upstream of Kitsault ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_skeena_upstream_cedarvale_closure",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "No fishing in any stream in the watershed of Skeena River "
    #         "upstream of Cedarvale, Jan 1–Jun 15. NOTE: Skeena River "
    #         "mainstem upstream of Cedarvale is only closed Jan 1–May 31."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     dates=["Jan 1 – Jun 15"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "Streams in Skeena watershed upstream of Cedarvale "
    #             "closed Jan 1–Jun 15. Skeena mainstem closed Jan 1–May 31."
    #         ),
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Skeena River watershed
    #     #       upstream of Cedarvale
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_nass_upstream_kitsault_closure",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "No fishing in any stream in the watershed of Nass River "
    #         "upstream of Kitsault Bridge, Jan 1–Jun 15. NOTE: Nass River "
    #         "mainstem is EXEMPT."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     dates=["Jan 1 – Jun 15"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "Streams in Nass watershed upstream of Kitsault Bridge "
    #             "closed Jan 1–Jun 15. Nass mainstem exempt."
    #         ),
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Nass River watershed
    #     #       upstream of Kitsault Bridge
    # ),
    #
    # --- Iskut River watershed closure ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_iskut_watershed_closure",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "No fishing in any stream in the Iskut River watershed "
    #         "(upstream of Forest Kerr Canyon), Apr 1–Jun 30."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     dates=["Apr 1 – Jun 30"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "Streams in Iskut River watershed (upstream of Forest "
    #             "Kerr Canyon) closed Apr 1–Jun 30."
    #         ),
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Iskut River watershed
    #     #       upstream of Forest Kerr Canyon
    # ),
    #
    # --- Fraser River watershed in Region 6 closure ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_fraser_watershed_closure",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "No fishing in any stream in the Fraser River watershed in "
    #         "Region 6, Apr 1–Jun 30."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     dates=["Apr 1 – Jun 30"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "Streams in Fraser River watershed (Region 6 portion) "
    #             "closed Apr 1–Jun 30."
    #         ),
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Fraser River watershed
    #     #       within Region 6
    # ),
    #
    # --- Lake trout release: Fraser and Skeena Watersheds ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r6_lake_trout_fraser_skeena_release",
    #     zone_ids=["6"],
    #     rule_text=(
    #         "Release lake trout from Fraser and Skeena Watersheds, "
    #         "Sept 15–Nov 30."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     dates=["Sept 15 – Nov 30"],
    #     restriction={
    #         "type": "Catch and Release",
    #         "details": (
    #             "Release lake trout from Fraser and Skeena Watersheds "
    #             "Sept 15–Nov 30."
    #         ),
    #     },
    #     notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Fraser and Skeena Watersheds
    #     #       within Region 6
    # ),
    # ========================================================================
    # REGION 7A — Omineca (Zone A): General Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r7a_spring_stream_closure",
        zone_ids=["7A"],
        rule_text=(
            "No fishing in any stream of Zone A, Apr 1–Jun 30. "
            "See tables for exceptions."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Apr 1 – Jun 30"],
        restriction={
            "type": "Closed",
            "details": "Streams closed Apr 1–Jun 30 unless noted in tables.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_single_barbless_hook_streams",
        zone_ids=["7A"],
        rule_text="Only single barbless hooks may be used in all streams, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hooks only in all streams, all year.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_bait_ban_streams",
        zone_ids=["7A"],
        rule_text=(
            "Bait ban applies to all streams of Zone A, all year. "
            "See tables for exceptions."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Bait Restriction",
            "details": "Bait banned in all streams, all year.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_set_lining_lakes",
        zone_ids=["7A"],
        rule_text=(
            "Set lining is only permitted in the lakes of Zone A. "
            "Set lines restricted to one line with a single hook (gap ≥3 cm), "
            "marked with angler's name, address and phone. Any game fish "
            "caught other than burbot must be released."
        ),
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Gear Restriction",
            "details": (
                "Set lines permitted in lakes only. One line, single hook "
                "(≥3 cm gap), labelled with name/address/phone. Only burbot "
                "may be retained; all other game fish must be released."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 7A — Omineca (Zone A): Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r7a_trout_char_daily_quota",
        zone_ids=["7A"],
        rule_text=(
            "Trout and Char: 5 (all species combined), including not more "
            "than 1 over 50 cm."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_trout_char_over_50cm_limit",
        zone_ids=["7A"],
        rule_text="Trout and Char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_trout_char_stream_limit",
        zone_ids=["7A"],
        rule_text="Trout and Char: not more than 2 from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout/char may be kept from streams.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_lake_trout_limit",
        zone_ids=["7A"],
        rule_text="Trout and Char: not more than 3 lake trout.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 3 lake trout in daily quota.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_bull_trout_limit",
        zone_ids=["7A"],
        rule_text=(
            "Trout and Char: not more than 1 bull trout (Dolly Varden). "
            "May only be retained Oct 16–Aug 14, from lakes only, "
            "30–50 cm in length."
        ),
        feature_types=[FeatureType.LAKE],
        dates=["Oct 16 – Aug 14"],
        restriction={
            "type": "Quota",
            "details": (
                "Max 1 bull trout (Dolly Varden). Retention Oct 16–Aug 14 "
                "only, from lakes only, 30–50 cm in length."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_bull_trout_stream_release",
        zone_ids=["7A"],
        rule_text="Release bull trout (Dolly Varden) from streams, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "All bull trout (Dolly Varden) must be released from streams.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_bull_trout_lake_seasonal_release",
        zone_ids=["7A"],
        rule_text="Release bull trout (Dolly Varden) from lakes Aug 15–Oct 15.",
        feature_types=[FeatureType.LAKE],
        dates=["Aug 15 – Oct 15"],
        restriction={
            "type": "Catch and Release",
            "details": "Release bull trout (Dolly Varden) from lakes Aug 15–Oct 15.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_lake_trout_seasonal_release",
        zone_ids=["7A"],
        rule_text="Release lake trout of any size Sept 15–Oct 31.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        dates=["Sept 15 – Oct 31"],
        restriction={
            "type": "Catch and Release",
            "details": "Release lake trout of any size Sept 15–Oct 31.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_lake_trout_under_30cm_release",
        zone_ids=["7A"],
        rule_text="Release lake trout under 30 cm, all year.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Lake trout under 30 cm must be released, all year.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_arctic_grayling_release",
        zone_ids=["7A"],
        rule_text="Arctic Grayling: catch and release only.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Catch and release only for Arctic grayling.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_burbot_quota",
        zone_ids=["7A"],
        rule_text="Burbot: 5.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 burbot.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_kokanee_quota",
        zone_ids=["7A"],
        rule_text="Kokanee: 10 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 10 kokanee.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_kokanee_closed_streams",
        zone_ids=["7A"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_sturgeon_closed",
        zone_ids=["7A"],
        rule_text="White Sturgeon: closed to all fishing.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": (
                "White sturgeon: closed to all fishing. "
                "Endangered under SARA (Upper Fraser/Nechako populations)."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_whitefish_quota",
        zone_ids=["7A"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 7A — Omineca (Zone A): Possession, Notices & Warnings
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r7a_possession_quota",
        zone_ids=["7A"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_lake_trout_possession",
        zone_ids=["7A"],
        rule_text="Lake trout: possession quota = 1 daily quota.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Possession Quota",
            "details": "Lake trout possession limit equals 1 daily quota.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_bull_trout_possession",
        zone_ids=["7A"],
        rule_text="Bull trout (Dolly Varden): possession quota = 1 daily quota.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Possession Quota",
            "details": "Bull trout (Dolly Varden) possession limit equals 1 daily quota.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_salmon_notice",
        zone_ids=["7A"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Stream closures and "
                "gear restrictions in this Synopsis also apply to salmon."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_tagging_program_notice",
        zone_ids=["7A"],
        rule_text=(
            "The Ministry and partners are conducting tagging studies on "
            "various species throughout the Omineca region. Some tags offer "
            "cash rewards. Report tag number, colour, date, time and "
            "location to Prince George office: (250) 614-7400."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Tagging programs in Omineca Region — some offer cash "
                "rewards. Report captures to (250) 614-7400."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_sturgeon_report_notice",
        zone_ids=["7A"],
        rule_text=(
            "White Sturgeon in Nechako and Upper Fraser are SARA-listed "
            "endangered. Report all sightings or incidental captures to "
            "250-614-7400. See nechakowhitesturgeon.org."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "White sturgeon (Upper Fraser/Nechako) are SARA endangered. "
                "Report sightings to 250-614-7400. "
                "Info: nechakowhitesturgeon.org."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7a_ice_fishing_huts_notice",
        zone_ids=["7A"],
        rule_text=(
            "Ice Fishing Huts: Failure to remove ice fishing huts from lakes "
            "before spring breakup is an offence under the Environmental "
            "Management Act."
        ),
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Ice fishing huts must be removed before spring breakup "
                "(Environmental Management Act)."
            ),
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 7B — Peace (Zone B): General Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r7b_single_barbless_hook_streams",
        zone_ids=["7B"],
        rule_text="Only single barbless hooks may be used in all streams, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hooks only in all streams, all year.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_bait_ban_streams",
        zone_ids=["7B"],
        rule_text="Bait ban applies to all streams of Zone B, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Bait Restriction",
            "details": "Bait banned in all streams, all year.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_finfish_bait_ban",
        zone_ids=["7B"],
        rule_text="Fin fish may not be used as bait in any waters of Zone B.",
        restriction={
            "type": "Bait Restriction",
            "details": "Fin fish may not be used as bait in any waters.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_set_lining_prohibited",
        zone_ids=["7B"],
        rule_text="Set lining is not permitted in Zone B.",
        restriction={
            "type": "Gear Restriction",
            "details": "Set lining is not permitted.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_ice_fishing_huts_notice",
        zone_ids=["7B"],
        rule_text=(
            "Ice fishing huts should have the owner's contact information "
            "displayed when left unoccupied. Failure to remove before "
            "spring breakup is an offence under the Environmental "
            "Management Act."
        ),
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Ice fishing huts must display owner info and be removed "
                "before spring breakup (Environmental Management Act)."
            ),
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 7B — Peace (Zone B): Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r7b_trout_char_daily_quota",
        zone_ids=["7B"],
        rule_text=(
            "Trout and Char: 5 (all species combined), including not more "
            "than 1 over 50 cm."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_trout_char_over_50cm_limit",
        zone_ids=["7B"],
        rule_text="Trout and Char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_trout_char_stream_limit",
        zone_ids=["7B"],
        rule_text="Trout and Char: not more than 2 from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 2 trout/char may be kept from streams.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_lake_trout_limit",
        zone_ids=["7B"],
        rule_text="Trout and Char: not more than 2 lake trout.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 2 lake trout in daily quota.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_lake_trout_under_30cm_release",
        zone_ids=["7B"],
        rule_text="Release lake trout under 30 cm, all year.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Catch and Release",
            "details": "Lake trout under 30 cm must be released, all year.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_lake_trout_seasonal_release",
        zone_ids=["7B"],
        rule_text="Release lake trout of any size Sept 15–Oct 31.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        dates=["Sept 15 – Oct 31"],
        restriction={
            "type": "Catch and Release",
            "details": "Release lake trout of any size Sept 15–Oct 31.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_rainbow_stream_seasonal_release",
        zone_ids=["7B"],
        rule_text="Release rainbow trout of any size from streams May 1–Jun 15.",
        feature_types=[FeatureType.STREAM],
        dates=["May 1 – Jun 15"],
        restriction={
            "type": "Catch and Release",
            "details": "Release rainbow trout from streams May 1–Jun 15.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_arctic_grayling_quota",
        zone_ids=["7B"],
        rule_text=("Arctic Grayling: 2 (none under 30 cm and only 1 over 45 cm)."),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": (
                "Daily quota: 2 Arctic grayling (none under 30 cm, "
                "max 1 over 45 cm)."
            ),
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_arctic_grayling_seasonal_release",
        zone_ids=["7B"],
        rule_text="Release Arctic grayling of any size May 1–Jun 15.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        dates=["May 1 – Jun 15"],
        restriction={
            "type": "Catch and Release",
            "details": "Release Arctic grayling May 1–Jun 15.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_burbot_quota",
        zone_ids=["7B"],
        rule_text="Burbot: 5.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 burbot.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_goldeye_quota",
        zone_ids=["7B"],
        rule_text="Goldeye: 10.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 10 goldeye.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_inconnu_quota",
        zone_ids=["7B"],
        rule_text="Inconnu: 1.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 1 inconnu.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    # TODO: EXCEPTION NOT ENCODED — Peace River is exempt from the
    #   "none from streams" kokanee rule but is not carved out. Need:
    #   blue_line_keys for Peace River to exclude.
    ZoneRegulation(
        regulation_id="zone_r7b_kokanee_quota",
        zone_ids=["7B"],
        rule_text="Kokanee: 10 (none from streams, except Peace River).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 10 kokanee (none from streams, except Peace River).",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    # TODO: EXCEPTION NOT ENCODED — Peace River is exempt from this
    #   kokanee stream closure but is not carved out. Need:
    #   blue_line_keys for Peace River to exclude.
    ZoneRegulation(
        regulation_id="zone_r7b_kokanee_closed_streams",
        zone_ids=["7B"],
        rule_text="Kokanee: none from streams (except Peace River).",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams (except Peace River).",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_northern_pike_quota",
        zone_ids=["7B"],
        rule_text="Northern Pike: 3 (only 1 over 90 cm).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 3 northern pike (max 1 over 90 cm).",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_walleye_quota",
        zone_ids=["7B"],
        rule_text="Walleye: 3 (only 1 over 70 cm).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 3 walleye (max 1 over 70 cm).",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_walleye_stream_seasonal_release",
        zone_ids=["7B"],
        rule_text="Release all walleye from streams Apr 1–May 15.",
        feature_types=[FeatureType.STREAM],
        dates=["Apr 1 – May 15"],
        restriction={
            "type": "Catch and Release",
            "details": "Release all walleye from streams Apr 1–May 15.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_whitefish_quota",
        zone_ids=["7B"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_yellow_perch_quota",
        zone_ids=["7B"],
        rule_text="Yellow Perch: 5.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 yellow perch.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 7B — Peace (Zone B): Possession & Notices
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r7b_possession_quota",
        zone_ids=["7B"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_arctic_grayling_possession",
        zone_ids=["7B"],
        rule_text="Arctic grayling: possession quota = 1 daily quota.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Possession Quota",
            "details": "Arctic grayling possession limit equals 1 daily quota.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_bull_trout_possession",
        zone_ids=["7B"],
        rule_text="Bull trout: possession quota = 1 daily quota.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Possession Quota",
            "details": "Bull trout possession limit equals 1 daily quota.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_lake_trout_possession",
        zone_ids=["7B"],
        rule_text="Lake trout: possession quota = 1 daily quota.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Possession Quota",
            "details": "Lake trout possession limit equals 1 daily quota.",
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_bull_trout_dolly_varden_notice",
        zone_ids=["7B"],
        rule_text=(
            "NOTE: Bull trout and Dolly Varden are two distinct species. "
            "Only bull trout are found in the Peace Region."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Only bull trout (not Dolly Varden) are found in the " "Peace Region."
            ),
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r7b_site_c_tagging_notice",
        zone_ids=["7B"],
        rule_text=(
            "BC Hydro is tracking fish movement throughout the Site C "
            "Reservoir, Peace River and tributaries using telemetry. "
            "Return tags by emailing tagreturns@golder.com or calling "
            "(250) 785-9281. For other areas call (250) 787-3415."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "BC Hydro fish tagging study — return tags: "
                "tagreturns@golder.com or (250) 785-9281. "
                "Other areas: (250) 787-3415."
            ),
        },
        notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 7B — Waterbody-specific (commented out — need linking IDs)
    # ========================================================================
    #
    # --- Bull trout retention: Liard River watershed only ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r7b_bull_trout_limit",
    #     zone_ids=["7B"],
    #     rule_text=(
    #         "Trout and Char: not more than 1 bull trout. May only be "
    #         "retained Oct 16–Aug 14, from Liard River watershed (or "
    #         "other specified waters) only, 30–50 cm in length."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     dates=["Oct 16 – Aug 14"],
    #     restriction={
    #         "type": "Quota",
    #         "details": (
    #             "Max 1 bull trout. Retention Oct 16–Aug 14 only, "
    #             "Liard River watershed only, 30–50 cm in length."
    #         ),
    #     },
    #     notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Liard River watershed
    # ),
    #
    # --- Bull trout release: Liard vs Peace watershed ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r7b_bull_trout_liard_seasonal_release",
    #     zone_ids=["7B"],
    #     rule_text=(
    #         "Release bull trout from Liard River watershed Aug 15–Oct 15."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     dates=["Aug 15 – Oct 15"],
    #     restriction={
    #         "type": "Catch and Release",
    #         "details": (
    #             "Release bull trout from Liard River watershed "
    #             "Aug 15–Oct 15."
    #         ),
    #     },
    #     notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Liard River watershed
    # ),
    #
    # ZoneRegulation(
    #     regulation_id="zone_r7b_bull_trout_peace_release",
    #     zone_ids=["7B"],
    #     rule_text=(
    #         "Release bull trout from Peace River watershed, all year "
    #         "(see tables for exceptions)."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     restriction={
    #         "type": "Catch and Release",
    #         "details": (
    #             "Release all bull trout from Peace River watershed, "
    #             "all year (see tables for exceptions)."
    #         ),
    #     },
    #     notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    #     # TODO: needs fwa_watershed_codes for Peace River watershed
    # ),
    #
    # --- Arctic grayling: Williston Lake and tribs release ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r7b_arctic_grayling_williston_release",
    #     zone_ids=["7B"],
    #     rule_text=(
    #         "Release all Arctic grayling from Williston Lake and its "
    #         "tributaries."
    #     ),
    #     feature_types=[FeatureType.STREAM, FeatureType.LAKE],
    #     restriction={
    #         "type": "Catch and Release",
    #         "details": (
    #             "Release all Arctic grayling from Williston Lake "
    #             "and tributaries."
    #         ),
    #     },
    #     notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    #     # TODO: needs gnis_ids/waterbody_keys for Williston Lake
    #     #       and fwa_watershed_codes for its tributaries
    # ),
    #
    # --- Peace River navigation warning (Site C) ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r7b_peace_river_site_c_warning",
    #     zone_ids=["7B"],
    #     rule_text=(
    #         "WARNING: Peace River is no longer navigable past the Site C "
    #         "construction site. Avoid boat travel between 2 km upstream "
    #         "of the dam site and the downstream construction bridge."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Advisory",
    #         "details": (
    #             "Peace River not navigable past Site C construction. "
    #             "Avoid boat travel near dam site. "
    #             "See sitecproject.com/boating."
    #         ),
    #     },
    #     notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    #     # TODO: needs blue_line_keys for Peace River near Site C
    # ),
    #
    # --- Thin ice warning: Inga and Sundance Lakes ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r7b_thin_ice_warning",
    #     zone_ids=["7B"],
    #     rule_text=(
    #         "WARNING: Due to aeration projects, dangerous thin ice and "
    #         "open water may exist on Inga and Sundance Lakes."
    #     ),
    #     feature_types=[FeatureType.LAKE],
    #     restriction={
    #         "type": "Advisory",
    #         "details": (
    #             "Dangerous thin ice and open water due to aeration on "
    #             "Inga and Sundance Lakes. Do not enter fenced areas."
    #         ),
    #     },
    #     notes="Source: Region 7B preamble, 2025-2027 Synopsis.",
    #     # TODO: needs gnis_ids for Inga Lake and Sundance Lake
    # ),
    # ========================================================================
    # REGION 8 — Okanagan: General Regulations
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r8_spring_stream_closure",
        zone_ids=["8"],
        rule_text=(
            "No fishing in any stream in Region 8 from Apr 1–Jun 30. "
            "See tables for exceptions."
        ),
        feature_types=[FeatureType.STREAM],
        dates=["Apr 1 – Jun 30"],
        restriction={
            "type": "Closed",
            "details": "Streams closed Apr 1–Jun 30 unless noted in tables.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_single_barbless_hook_streams",
        zone_ids=["8"],
        rule_text="Only single barbless hooks may be used in all streams, all year.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hooks only in all streams, all year.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 8 — Okanagan: Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r8_trout_char_daily_quota",
        zone_ids=["8"],
        rule_text=(
            "Trout and Char: 5 (all species combined), including not more "
            "than 1 over 50 cm."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 trout/char (all species combined).",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_trout_char_over_50cm_limit",
        zone_ids=["8"],
        rule_text="Trout and Char: not more than 1 over 50 cm.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Max 1 trout/char over 50 cm in daily quota.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_trout_char_stream_limit",
        zone_ids=["8"],
        rule_text=(
            "Trout and Char: not more than 4 from streams " "(only 2 over 30 cm)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "Max 4 trout/char from streams (only 2 over 30 cm).",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_brook_trout_stream_bonus",
        zone_ids=["8"],
        rule_text="You may retain 20 brook trout from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Quota",
            "details": "May retain 20 brook trout from streams (in addition to daily quota).",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_bull_trout_stream_release",
        zone_ids=["8"],
        rule_text="Release bull trout (Dolly Varden) from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Catch and Release",
            "details": "All bull trout (Dolly Varden) must be released from streams.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_bass_closed",
        zone_ids=["8"],
        rule_text="Bass: 0 quota, closed to fishing (see tables for exceptions).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Bass: closed to fishing (0 quota). See tables for exceptions.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_burbot_quota",
        zone_ids=["8"],
        rule_text="Burbot: 2.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 2 burbot.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_crappie_quota",
        zone_ids=["8"],
        rule_text="Crappie: 20.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 20 crappie.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_crayfish_quota",
        zone_ids=["8"],
        rule_text="Crayfish: 25.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 25 crayfish.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_kokanee_quota",
        zone_ids=["8"],
        rule_text="Kokanee: 5 (none from streams).",
        feature_types=[FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 5 kokanee.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_kokanee_closed_streams",
        zone_ids=["8"],
        rule_text="Kokanee: none from streams.",
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "details": "No kokanee may be kept from streams.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_walleye_quota",
        zone_ids=["8"],
        rule_text="Walleye: 8.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 8 walleye.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_whitefish_quota",
        zone_ids=["8"],
        rule_text="Whitefish: 15 (all species combined).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "details": "Daily quota: 15 whitefish (all species combined).",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_yellow_perch_closed",
        zone_ids=["8"],
        rule_text="Yellow Perch: 0 quota, closed to fishing (see tables for exceptions).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Closed",
            "details": "Yellow perch: closed to fishing (0 quota). See tables for exceptions.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 8 — Okanagan: Possession & Notices
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r8_possession_quota",
        zone_ids=["8"],
        rule_text="Possession quotas = 2 daily quotas (see tables for exceptions).",
        restriction={
            "type": "Possession Quota",
            "details": "Possession limit equals 2 times the daily quota.",
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_salmon_notice",
        zone_ids=["8"],
        rule_text=(
            "Non-tidal salmon fishing regulations are not included in this "
            "Synopsis. See DFO regulations (including Osoyoos Lake sockeye "
            "fishery enquiries)."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Salmon regulations managed by DFO. Includes Osoyoos Lake "
                "sockeye fishery enquiries."
            ),
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis. Province-wide notice.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_crayfish_turtle_notice",
        zone_ids=["8"],
        rule_text=(
            "Crayfish trapping: use traps with minimally-sized circular "
            "openings to reduce chance of capturing Western Painted Turtles."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Notice",
            "details": (
                "Use crayfish traps with minimal circular openings to "
                "protect Western Painted Turtles."
            ),
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r8_syilx_nation_notice",
        zone_ids=["8"],
        rule_text=(
            "The Okanagan is the traditional territory of the Syilx people. "
            "The Province collaborates with ONA on key initiatives to "
            "restore and manage fish stocks."
        ),
        restriction={
            "type": "Notice",
            "details": (
                "Okanagan is Syilx traditional territory. Province "
                "collaborates with Okanagan Nation Alliance on fish "
                "stock restoration."
            ),
        },
        notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 8 — Waterbody-specific (commented out — need linking IDs)
    # ========================================================================
    #
    # --- Garnet Lake angling closure ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r8_garnet_lake_closure",
    #     zone_ids=["8"],
    #     rule_text=(
    #         "Garnet Lake has been closed to all angling due to illegal "
    #         "introduction of largemouth bass. Garnet Valley Reservoir "
    #         "will be used as a research lake."
    #     ),
    #     feature_types=[FeatureType.LAKE],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "Garnet Lake closed to all angling (illegal bass "
    #             "introduction). Now a research lake."
    #         ),
    #     },
    #     notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs gnis_ids for Garnet Lake (Garnet Valley Reservoir)
    # ),
    #
    # --- Okanagan Lake Dam fish passage ---
    #
    # ZoneRegulation(
    #     regulation_id="zone_r8_okanagan_dam_tagging_notice",
    #     zone_ids=["8"],
    #     rule_text=(
    #         "Okanagan Lake Dam Fish Passage Initiative: controlled testing "
    #         "underway. Report tagged fish to Okanagan Fish & Wildlife "
    #         "office in Penticton at 250-490-8200."
    #     ),
    #     feature_types=[FeatureType.LAKE],
    #     restriction={
    #         "type": "Notice",
    #         "details": (
    #             "Okanagan Lake Dam fish passage testing underway. "
    #             "Report tagged fish to 250-490-8200 (Penticton)."
    #         ),
    #     },
    #     notes="Source: Region 8 preamble, 2025-2027 Synopsis.",
    #     # TODO: needs gnis_ids/waterbody_keys for Okanagan Lake
    # ),
]


# ============================================================================
# CLI Zone Regulation Test
#
# Run with:  python -m regulation_mapping.zone_base_regulations
#
# Loads the FWA metadata, builds the zone feature index, then resolves each
# active regulation against FWA features. Prints a summary table showing
# how many features were matched. Useful for verifying zone IDs, feature
# types, and MU filters.
# ============================================================================


def _run_zone_test():
    """
    Test zone base regulations against the FWA metadata.

    For each active regulation, either builds the zone index and scans for
    matching features (zone-wide mode), or resolves direct-match IDs
    (direct-match mode). Prints counts and sample feature names.
    """
    from pathlib import Path

    from fwa_pipeline.metadata_gazetteer import MetadataGazetteer
    from project_config import get_config
    from .cli_helpers import RED, YELLOW, GREEN, CYAN, RESET, header

    # --- Setup ---

    cfg = get_config()
    pickle_path = cfg.fwa_metadata_path

    header("ZONE REGULATION TEST")
    print(f"  Pickle:  {pickle_path}")

    if not pickle_path.exists():
        print(f"\n  {RED}ERROR: Pickle not found at {pickle_path}{RESET}")
        print("  Run the FWA pipeline first to generate metadata.")
        return

    print(f"\n  Loading metadata...")
    gazetteer = MetadataGazetteer(pickle_path)

    active_regs = [r for r in ZONE_BASE_REGULATIONS if not r._disabled]
    print(f"  Active zone regulations: {len(active_regs)}")

    if not active_regs:
        print(f"  {YELLOW}No active regulations to test.{RESET}")
        return

    # Build zone + MU feature index (same function the mapper uses)
    print(f"\n  Building zone feature index...")
    from .regulation_resolvers import (
        build_feature_index,
        resolve_direct_match_ids,
        resolve_zone_wide_ids,
        collect_features_from_index,
        ALL_FWA_TYPES,
    )

    zone_index, mu_index, _, _ = build_feature_index(gazetteer)

    total_indexed = sum(
        len(features) for zones in zone_index.values() for features in zones.values()
    )
    print(
        f"  Index: {len(zone_index)} zones, {len(mu_index)} MUs, {total_indexed:,} entries"
    )

    results = []
    total_features = 0

    for zone_reg in active_regs:
        header(f"REGULATION: {zone_reg.regulation_id}")
        print(f"  Rule:          {zone_reg.rule_text[:100]}...")
        print(f"  Zone IDs:      {zone_reg.zone_ids}")
        print(f"  Mode:          {zone_reg.scope_type}")
        print(
            f"  Feature types: "
            f"{[ft.value for ft in zone_reg.feature_types] if zone_reg.feature_types else 'ALL'}"
        )

        if zone_reg.admin_targets:
            # Admin-match mode — skip in CLI test (requires GPKG)
            print(
                f"\n  {YELLOW}Admin-match mode — skipped (requires GPKG spatial ops){RESET}"
            )
            results.append((zone_reg.regulation_id, "ADMIN", 0))

        elif zone_reg.has_direct_target():
            # Direct-match mode — use shared resolution function
            matched_ids = resolve_direct_match_ids(gazetteer, zone_reg)

            n_features = len(matched_ids)
            total_features += n_features
            print(f"\n  {GREEN}Direct-match features: {n_features}{RESET}")
            results.append((zone_reg.regulation_id, "DIRECT", n_features))

        else:
            # Zone-wide mode — use shared resolution function
            matched_ids = resolve_zone_wide_ids(zone_reg, zone_index, mu_index)

            n_features = len(matched_ids)
            total_features += n_features

            # Breakdown by feature type
            target_types = zone_reg.feature_types or ALL_FWA_TYPES
            type_counts: Dict[str, int] = {}
            for zone_id in zone_reg.zone_ids:
                zone_features = zone_index.get(zone_id, {})
                for ftype in target_types:
                    count = len(zone_features.get(ftype, {}))
                    type_counts[ftype.value] = type_counts.get(ftype.value, 0) + count

            print(f"\n  {GREEN}Zone-wide features: {n_features:,}{RESET}")
            for ft_name, count in sorted(type_counts.items()):
                print(f"    {ft_name}: {count:,}")

            results.append((zone_reg.regulation_id, "ZONE-WIDE", n_features))

    # Summary table
    header("SUMMARY")
    col1, col2, col3 = 40, 12, 12
    hdr_row = f"{'Regulation':<{col1}} {'Mode':<{col2}} {'Features':>{col3}}"
    print(hdr_row)
    print("-" * len(hdr_row))
    for reg_id, mode, feats in results:
        color = GREEN if feats > 0 else RED
        print(f"{color}{reg_id:<{col1}} {mode:<{col2}} {feats:>{col3},}{RESET}")

    print(f"\nTotal FWA features affected by zone regulations: {total_features:,}")
    print()


if __name__ == "__main__":
    _run_zone_test()
