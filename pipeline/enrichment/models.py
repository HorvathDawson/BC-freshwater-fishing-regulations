"""Intermediate dataclasses for the enrichment pipeline.

Import order (no circular deps):
    models → loader → feature_resolver → tributary_enricher
          → base_reg_assigner → reach_builder → builder
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Set, TypedDict, Union

from pipeline.matching.match_table import (
    AnyEntry,
    BaseEntry,
    OverrideEntry,
)


# ---------------------------------------------------------------------------
# Metadata lookup types (gnis_id → feature info from atlas build)
# ---------------------------------------------------------------------------


class StreamMetaEntry(TypedDict):
    """Stream feature metadata grouped by gnis_id."""

    gnis_id: str
    gnis_name: str
    edge_ids: List[str]  # linear_feature_ids (fids)
    fwa_watershed_codes: List[str]
    blue_line_keys: List[str]
    zones: List[str]
    mgmt_units: List[str]


class PolyMetaEntry(TypedDict):
    """Polygon feature metadata grouped by waterbody_key."""

    waterbody_key: str
    gnis_name: str
    gnis_id: str
    gnis_name_2: str
    gnis_id_2: str
    poly_ids: List[str]
    zones: List[str]
    mgmt_units: List[str]


class AtlasMetadata(TypedDict):
    """Full metadata dict structure returned by the atlas build step."""

    streams: Dict[str, StreamMetaEntry]  # gnis_id → stream info
    lakes: Dict[str, PolyMetaEntry]  # waterbody_key → poly info
    wetlands: Dict[str, PolyMetaEntry]
    manmade: Dict[str, PolyMetaEntry]
    # Reverse-lookup tables built by build_metadata_from_graph /
    # enrich_metadata_with_polygons — always present after Phase 2 init.
    _gnis_to_wbk: Dict[str, Set[str]]  # gnis_id → {waterbody_key, …}
    _wbk_to_fids: Dict[str, List[str]]  # waterbody_key → [linear_feature_id, …]


# ---------------------------------------------------------------------------
# Phase 1 output: RegulationRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RegulationRecord:
    """One synopsis regulation, fully merged from raw + match + parsed.

    Created in Phase 1 (loader).  Frozen — never mutated after creation.
    """

    index: int  # positional index (0–1394)
    reg_id: str  # "R2_ALICE_LAKE_2-7"
    water: str  # "ALICE LAKE"
    region: str  # "Region 2"
    mu: tuple  # ("2-7",) — tuple for hashability
    raw_regs: str
    symbols: tuple  # ("Stocked", "Incl. Tribs")
    page: int
    image: str
    source: Literal["synopsis"] = "synopsis"

    # From MatchTable.lookup() — BaseEntry or OverrideEntry
    match_entry: AnyEntry = field(repr=False, default=None)  # type: ignore[assignment]

    # From session results
    parsed: Optional[Dict[str, Any]] = field(repr=False, default=None)
    parse_status: Literal["success", "failed"] = "failed"


# ---------------------------------------------------------------------------
# Phase 2 output: ResolvedRegulation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedRegulation:
    """A regulation resolved to concrete atlas features.

    Created in Phase 2 (feature_resolver).  Frozen — seeds and direct
    matches are finalized before tributary enrichment begins.
    """

    record: RegulationRecord

    # Directly matched features
    matched_stream_fids: frozenset = field(default_factory=frozenset)
    matched_waterbody_keys: frozenset = field(default_factory=frozenset)

    # Tributary enrichment config
    includes_tributaries: bool = False
    tributary_only: bool = False

    # Seeds for Phase 3 BFS
    tributary_stream_seeds: tuple = field(default_factory=tuple)  # fids
    lake_outlet_fids: tuple = field(default_factory=tuple)  # (wbk, [fid, ...]) pairs


# ---------------------------------------------------------------------------
# Shared mutable accumulator (phases 2–4)
# ---------------------------------------------------------------------------


@dataclass
class FeatureAssignment:
    """Running accumulator: which reg_ids apply to which features.

    Mutated by phases 2, 3, and 4 in strict sequence.  Each phase logs
    what it added so diffs can be inspected.

    Phase 2: direct matches (synopsis regs → fids/wbks)
    Phase 3: tributary expansion (BFS-discovered fids)
    Phase 4: base regulations (zone-wide + admin polygon)
    """

    fid_to_reg_ids: Dict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    wbk_to_reg_ids: Dict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    # Phase 3 tributary tracking: fid → {reg_ids assigned via BFS}
    # Only records assignments where Phase 3 was first (Phase 2 didn't
    # already claim it).  Used by reach_builder to compute the ``tr`` field.
    fid_to_trib_reg_ids: Dict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    # Phase 4 admin tracking: fid/wbk → {reg_ids assigned via admin polygon}
    # Used by reach_builder to tag name_variants with source="admin".
    fid_to_admin_reg_ids: Dict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    wbk_to_admin_reg_ids: Dict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    # Per-phase audit counters
    phase2_assignments: int = 0
    phase3_tributary_additions: int = 0
    phase4_base_additions: int = 0

    def assign_fid(
        self,
        fid: str,
        reg_id: str,
        *,
        phase: int,
        is_admin: bool = False,
    ) -> None:
        """Add a reg_id to a stream fid.  Track which phase did it."""
        is_new = reg_id not in self.fid_to_reg_ids[fid]
        self.fid_to_reg_ids[fid].add(reg_id)
        if is_new:
            if phase == 2:
                self.phase2_assignments += 1
            elif phase == 3:
                self.phase3_tributary_additions += 1
                self.fid_to_trib_reg_ids[fid].add(reg_id)
            elif phase == 4:
                self.phase4_base_additions += 1
        if is_admin:
            self.fid_to_admin_reg_ids[fid].add(reg_id)

    def assign_wbk(
        self,
        wbk: str,
        reg_id: str,
        *,
        phase: int,
        is_admin: bool = False,
    ) -> None:
        """Add a reg_id to a polygon waterbody_key.  Track which phase did it."""
        is_new = reg_id not in self.wbk_to_reg_ids[wbk]
        self.wbk_to_reg_ids[wbk].add(reg_id)
        if is_new:
            if phase == 2:
                self.phase2_assignments += 1
            elif phase == 3:
                self.phase3_tributary_additions += 1
            elif phase == 4:
                self.phase4_base_additions += 1
        if is_admin:
            self.wbk_to_admin_reg_ids[wbk].add(reg_id)

    def assign_fids_bulk(
        self,
        fids: Set[str],
        reg_id: str,
        *,
        phase: int,
        is_admin: bool = False,
    ) -> int:
        """Bulk-assign a reg_id to many stream fids. Returns new-assignment count."""
        ftri = self.fid_to_reg_ids
        trib = self.fid_to_trib_reg_ids
        admin = self.fid_to_admin_reg_ids
        new = 0
        for fid in fids:
            s = ftri[fid]
            if reg_id not in s:
                s.add(reg_id)
                new += 1
                if phase == 3:
                    trib[fid].add(reg_id)
            if is_admin:
                admin[fid].add(reg_id)
        if phase == 4:
            self.phase4_base_additions += new
        elif phase == 3:
            self.phase3_tributary_additions += new
        elif phase == 2:
            self.phase2_assignments += new
        return new

    def assign_wbks_bulk(
        self,
        wbks: Set[str],
        reg_id: str,
        *,
        phase: int,
        is_admin: bool = False,
    ) -> int:
        """Bulk-assign a reg_id to many waterbody keys. Returns new-assignment count."""
        wtri = self.wbk_to_reg_ids
        admin = self.wbk_to_admin_reg_ids
        new = 0
        for wbk in wbks:
            s = wtri[wbk]
            if reg_id not in s:
                s.add(reg_id)
                new += 1
            if is_admin:
                admin[wbk].add(reg_id)
        if phase == 4:
            self.phase4_base_additions += new
        elif phase == 3:
            self.phase3_tributary_additions += new
        elif phase == 2:
            self.phase2_assignments += new
        return new

    def summary(self) -> Dict[str, int]:
        """Return assignment counts for logging."""
        return {
            "stream_fids_with_regs": len(self.fid_to_reg_ids),
            "polygon_wbks_with_regs": len(self.wbk_to_reg_ids),
            "phase2_direct": self.phase2_assignments,
            "phase3_tributaries": self.phase3_tributary_additions,
            "phase4_base": self.phase4_base_additions,
        }


# ---------------------------------------------------------------------------
# Base regulation definition (loaded from JSON)
# ---------------------------------------------------------------------------


class AdminTargetDef(TypedDict, total=False):
    """Admin polygon target for base regulation spatial matching."""

    layer: str  # "parks_nat", "parks_bc", etc.
    feature_id: str  # specific polygon ID (optional)
    code_filter: str  # e.g. "OI" for eco reserves (optional)


@dataclass(frozen=True)
class BaseRegulationDef:
    """Zone or provincial base regulation definition, loaded from JSON."""

    reg_id: str  # "ZONE_2_BASE" or "PROV_NAT_PARKS_CLOSED"
    source: Literal["zone", "provincial"]
    rule_text: str
    restriction: Dict[str, Any]  # {"type": "Quota", "details": "..."}
    zone_ids: tuple = field(default_factory=tuple)
    feature_types: Optional[tuple] = None  # ("stream", "lake") or None=all
    mu_ids: Optional[tuple] = None
    exclude_mu_ids: Optional[tuple] = None
    include_mu_ids: Optional[tuple] = None  # extra MUs outside zone to add
    admin_targets: Optional[tuple] = None  # tuple of AdminTargetDef dicts
    buffer_m: float = 500.0

    # Passthrough fields — exported to regulation_index.json, no matching logic
    dates: Optional[tuple] = None  # ("Jul 15 – Aug 31",)
    scope_location: Optional[str] = None  # "Shuswap Lake"
    notes: str = ""

    # Direct-match fields — resolve by ID instead of polygon intersection
    gnis_ids: Optional[tuple] = None
    blue_line_keys: Optional[tuple] = None
    fwa_watershed_codes: Optional[tuple] = None
    waterbody_keys: Optional[tuple] = None
    linear_feature_ids: Optional[tuple] = None

    # Reach-level regs don't split reaches — if any fid in a reach has
    # the reg, the whole reach gets it.
    reach_level: bool = False

    # Disabled regs are loaded but skipped at runtime
    disabled: bool = False

    @property
    def has_direct_target(self) -> bool:
        """True if any direct-match ID field is populated."""
        return any(
            [
                self.gnis_ids,
                self.blue_line_keys,
                self.fwa_watershed_codes,
                self.waterbody_keys,
                self.linear_feature_ids,
            ]
        )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BaseRegulationDef":
        return cls(
            reg_id=d["reg_id"],
            source=d["source"],
            rule_text=d["rule_text"],
            restriction=d["restriction"],
            zone_ids=tuple(d.get("zone_ids", [])),
            feature_types=tuple(d["feature_types"]) if d.get("feature_types") else None,
            mu_ids=tuple(d["mu_ids"]) if d.get("mu_ids") else None,
            exclude_mu_ids=(
                tuple(d["exclude_mu_ids"]) if d.get("exclude_mu_ids") else None
            ),
            include_mu_ids=(
                tuple(d["include_mu_ids"]) if d.get("include_mu_ids") else None
            ),
            admin_targets=tuple(d["admin_targets"]) if d.get("admin_targets") else None,
            buffer_m=d.get("buffer_m", 500.0),
            dates=tuple(d["dates"]) if d.get("dates") else None,
            scope_location=d.get("scope_location"),
            notes=d.get("notes", ""),
            gnis_ids=tuple(d["gnis_ids"]) if d.get("gnis_ids") else None,
            blue_line_keys=(
                tuple(d["blue_line_keys"]) if d.get("blue_line_keys") else None
            ),
            fwa_watershed_codes=(
                tuple(d["fwa_watershed_codes"])
                if d.get("fwa_watershed_codes")
                else None
            ),
            waterbody_keys=(
                tuple(d["waterbody_keys"]) if d.get("waterbody_keys") else None
            ),
            linear_feature_ids=(
                tuple(d["linear_feature_ids"]) if d.get("linear_feature_ids") else None
            ),
            disabled=d.get("disabled", False),
            reach_level=d.get("reach_level", False),
        )
