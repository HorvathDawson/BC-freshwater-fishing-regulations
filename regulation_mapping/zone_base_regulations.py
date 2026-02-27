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
        restriction: Regulation details dict — same schema as synopsis-derived
                     restrictions (type, species, details, dates, etc.).
        notes: Source references (synopsis page numbers, edition, etc.).

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
    restriction: Dict[str, Any]
    notes: str

    # Scope (zone-wide mode)
    feature_types: Optional[List[FeatureType]] = None  # None = all types

    # Set True to skip this regulation during processing
    _disabled: bool = False
    mu_ids: Optional[List[str]] = None        # Only include these MUs (None = all in zone)
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
        return any([
            self.gnis_ids,
            self.waterbody_poly_ids,
            self.fwa_watershed_codes,
            self.waterbody_keys,
            self.linear_feature_ids,
            self.blue_line_keys,
            self.sub_polygon_ids,
            self.ungazetted_waterbody_id,
        ])


# ============================================================================
# Zone Base Regulations Data
# ============================================================================

ZONE_BASE_REGULATIONS: List[ZoneRegulation] = [
    # ========================================================================
    # REGION 1 — Vancouver Island (excluding Haida Gwaii) — Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r1_trout_quota",
        zone_ids=["1"],
        rule_text=(
            "Trout: 4, but not more than 1 over 50 cm (2 hatchery steelhead "
            "over 50 cm allowed), 2 from streams (must be hatchery). "
            "Release all wild steelhead, all wild trout from streams, "
            "and all char (includes Dolly Varden)."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "species": ["trout"],
            "details": (
                "Daily quota: 4 trout. Max 1 over 50 cm "
                "(2 hatchery steelhead over 50 cm allowed). "
                "Max 2 from streams (must be hatchery). "
                "Release all wild steelhead, wild trout from streams, "
                "and all char (Dolly Varden)."
            ),
            "daily_quota": 4,
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
            "species": ["bass"],
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
            "species": ["crayfish"],
            "details": "Daily quota: 25 crayfish.",
            "daily_quota": 25,
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
            "species": ["kokanee"],
            "details": "Daily quota: 5 kokanee. None from streams.",
            "daily_quota": 5,
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
            "species": ["white sturgeon"],
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
            "species": ["yellow perch"],
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
            "species": ["trout"],
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
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": "Summer stream closure Jul 15–Aug 31. MUs 1-1 to 1-6.",
            "dates": {"period": "Jul 15 – Aug 31", "type": "closure"},
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
        restriction={
            "type": "Bait Restriction",
            "details": "Bait banned in streams Nov 1–Apr 30.",
            "dates": {"period": "Nov 1 – Apr 30", "type": "restriction"},
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # HAIDA GWAII (MUs 6-12, 6-13) — Daily Quotas
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_hg_trout_char_quota",
        zone_ids=["6"],
        mu_ids=["6-12", "6-13"],
        rule_text=(
            "Haida Gwaii trout/char: 5, but not more than 1 over 50 cm, "
            "3 Dolly Varden, 2 from streams. Release trout/char under "
            "30 cm from streams and all wild steelhead."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "species": ["trout", "char", "dolly varden"],
            "details": (
                "Daily quota: 5 trout/char. Max 1 over 50 cm. "
                "Max 3 Dolly Varden. Max 2 from streams. "
                "Release trout/char under 30 cm from streams. "
                "Release all wild steelhead."
            ),
            "daily_quota": 5,
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
            "species": ["kokanee"],
            "details": "Daily quota: 10 kokanee. None from streams.",
            "daily_quota": 10,
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
        rule_text=(
            "Annual catch quota for all B.C.: 10 steelhead per licence year "
            "(only hatchery steelhead may be retained)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Annual Quota",
            "species": ["steelhead"],
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
            "annual_quota": 10,
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis. Province-wide rule.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_salmon_notice",
        zone_ids=["1"],
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
        restriction={
            "type": "Advisory",
            "species": ["smallmouth bass"],
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
            "Single barbless hook must be used in all streams of Region 2, "
            "all year."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Gear Restriction",
            "details": "Single barbless hook required in all streams, all year.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_dead_finfish_bait_sturgeon",
        zone_ids=["2"],
        rule_text=(
            "Dead fin fish as bait only permitted in Region 2 when sport "
            "fishing for sturgeon in the Fraser River, Lower Pitt River "
            "(CPR Bridge upstream to Pitt Lake), Lower Harrison River "
            "(Fraser River upstream to Harrison Lake)."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Bait Exception",
            "species": ["sturgeon"],
            "details": (
                "Dead fin fish bait permitted only for sturgeon fishing "
                "in Fraser River, Lower Pitt River (CPR Bridge to Pitt Lake), "
                "Lower Harrison River (Fraser R to Harrison Lake)."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
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
            "species": ["steelhead"],
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
            "species": ["steelhead"],
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
            "species": [
                "nooksack dace", "salish sucker",
                "green sturgeon", "cultus lake sculpin",
            ],
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
        regulation_id="zone_r2_trout_char_quota",
        zone_ids=["2"],
        rule_text=(
            "Trout/char: 4, but not more than 1 over 50 cm (2 hatchery "
            "steelhead over 50 cm allowed), 2 from streams (must be "
            "hatchery), 1 char (bull trout, Dolly Varden, or lake trout) "
            "none under 60 cm. Release wild trout/char from streams, all "
            "wild steelhead, hatchery trout/char under 30 cm from streams. "
            "No general minimum size for trout in lakes."
        ),
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "species": ["trout", "char", "bull trout", "dolly varden", "lake trout", "steelhead"],
            "details": (
                "Daily quota: 4 trout/char. Max 1 over 50 cm "
                "(2 hatchery steelhead over 50 cm allowed). "
                "Max 2 from streams (must be hatchery). "
                "Max 1 char (bull trout, Dolly Varden, lake trout), none under 60 cm. "
                "Release all wild trout/char from streams, all wild steelhead, "
                "hatchery trout/char under 30 cm from streams. "
                "No general minimum size for trout in lakes."
            ),
            "daily_quota": 4,
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_bass_quota",
        zone_ids=["2"],
        rule_text="Bass: 20 (excluding Mill Lake — see page 24 for quota).",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "species": ["bass"],
            "details": "Daily quota: 20 bass. Mill Lake exception — see water-specific tables.",
            "daily_quota": 20,
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_crappie_quota",
        zone_ids=["2"],
        rule_text="Crappie: 20.",
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        restriction={
            "type": "Quota",
            "species": ["crappie"],
            "details": "Daily quota: 20 crappie.",
            "daily_quota": 20,
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
            "species": ["crayfish"],
            "details": "Daily quota: 25 crayfish.",
            "daily_quota": 25,
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
            "species": ["kokanee"],
            "details": "Daily quota: 5 kokanee. None from streams.",
            "daily_quota": 5,
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
            "species": ["whitefish"],
            "details": "Daily quota: 15 whitefish (all species combined).",
            "daily_quota": 15,
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
            "species": ["white sturgeon"],
            "details": "Catch and release only for white sturgeon.",
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r2_fraser_sturgeon_seasonal_closure",
        zone_ids=["2"],
        rule_text=(
            "Fraser River: closed to all fishing in the Fraser areas of "
            "Jesperson's Side Channel, Herrling Island Side Channel, and "
            "Seabird Island north Side Channel, May 15 – July 31."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": (
                "Fraser River seasonal closure: Jesperson's Side Channel, "
                "Herrling Island Side Channel, and Seabird Island north Side Channel "
                "closed to all fishing May 15 – Jul 31."
            ),
            "dates": {"period": "May 15 – Jul 31", "type": "closure"},
        },
        notes=(
            "Source: Region 2 preamble (under White Sturgeon), 2025-2027 Synopsis. "
            "See page 26 for map of closed area."
        ),
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
            "species": ["steelhead"],
            "details": (
                "Annual quota: 10 hatchery steelhead province-wide. "
                "All wild steelhead must be released."
            ),
            "annual_quota": 10,
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
            "details": (
                "Salmon regulations managed by DFO. See page 77 for details."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 2 — Night Fishing Prohibition
    # ========================================================================
    ZoneRegulation(
        regulation_id="zone_r2_night_fishing_prohibition",
        zone_ids=["2"],
        rule_text=(
            "From one hour after sunset to one hour before sunrise, fishing "
            "is prohibited on portions of the Fraser, Harrison, and Pitt Rivers. "
            "See water-specific tables for details."
        ),
        feature_types=[FeatureType.STREAM],
        restriction={
            "type": "Time Restriction",
            "details": (
                "Night fishing prohibited (1 hr after sunset to 1 hr before "
                "sunrise) on portions of Fraser, Harrison, and Pitt Rivers. "
                "See water-specific tables for details."
            ),
        },
        notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    ),
    # ========================================================================
    # REGION 2 — Admin-match regulations (commented out — need polygon data)
    # ========================================================================
    #
    # TODO: These areas are not yet in the data layers. When polygon data
    # becomes available, uncomment and set the admin_targets accordingly.
    #
    # ZoneRegulation(
    #     regulation_id="zone_r2_knapp_forest_closure",
    #     zone_ids=["2"],
    #     rule_text=(
    #         "No fishing in any lake in the UBC Malcolm Knapp Research "
    #         "Forest near Maple Ridge."
    #     ),
    #     feature_types=[FeatureType.LAKE],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["all"],
    #         "details": "All lakes closed in UBC Malcolm Knapp Research Forest.",
    #     },
    #     notes="Source: Region 2 preamble, 2025-2027 Synopsis.",
    #     admin_targets=[],  # TODO: polygon data not yet available
    # ),
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
    #
    # ========================================================================
    # OTHER REGIONS — Placeholder (commented out)
    # ========================================================================
    #
    # ZoneRegulation(
    #     regulation_id="zone_r4_stream_closure_default",
    #     zone_ids=["4"],
    #     rule_text=(
    #         "Unless otherwise noted, all streams in Region 4 are closed "
    #         "to fishing from November 1 to June 30."
    #     ),
    #     feature_types=[FeatureType.STREAM],
    #     restriction={
    #         "type": "Closed",
    #         "species": ["all"],
    #         "details": "Streams closed Nov 1–Jun 30 unless noted in tables.",
    #         "dates": {"period": "Nov 1 – Jun 30", "type": "closure"},
    #     },
    #     notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
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
    from .regulation_mapper import (
        build_feature_index,
        resolve_direct_match_ids,
        resolve_zone_wide_ids,
        collect_features_from_index,
        ALL_FWA_TYPES,
    )

    zone_index, mu_index = build_feature_index(gazetteer)

    total_indexed = sum(
        len(features)
        for zones in zone_index.values()
        for features in zones.values()
    )
    print(f"  Index: {len(zone_index)} zones, {len(mu_index)} MUs, {total_indexed:,} entries")

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
            print(f"\n  {YELLOW}Admin-match mode — skipped (requires GPKG spatial ops){RESET}")
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
