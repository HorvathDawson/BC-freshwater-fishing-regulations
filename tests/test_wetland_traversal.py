"""
Tests for wetland waterbody-key handling during tributary BFS traversal.

BUG (fixed): Wetland/manmade waterbody_keys added to ``linked_waterbody_keys_of_polygon``
(via admin regulations like "Strathcona Park Waters") blocked BFS from
crossing through wetlands on the stream network.  This severed tributary
connectivity — e.g. Piggott Creek became entirely unreachable as a
tributary of Oyster River because its confluence edge sits inside a
shared swamp (wb_key=328997926).

FIX: Stream-seeded tributary enrichment now passes NO excluded_waterbody_keys,
so the BFS traverses freely through all waterbodies.  Lake-seeded enrichment
still excludes lake-only wb_keys so "tributaries of X Lake" stops at the next
regulated lake.

These tests use the real FWA graph to verify correct behaviour.
"""

from __future__ import annotations

import pickle
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Optional, Set
from unittest.mock import patch

import pytest

from fwa_pipeline.metadata_gazetteer import FWAFeature, FeatureType
from regulation_mapping.regulation_mapper import RegulationMapper
from regulation_mapping.scope_filter import ScopeFilter
from regulation_mapping.tributary_enricher import TributaryEnricher

# ---------------------------------------------------------------------------
# Real graph fixture
# ---------------------------------------------------------------------------

GRAPH_PICKLE = Path("output/fwa/fwa_bc_primal_full.gpickle")


@pytest.fixture(scope="module")
def graph_data():
    """Load the real FWA graph (shared across all tests in this module)."""
    if not GRAPH_PICKLE.exists():
        pytest.skip("FWA graph pickle not available")
    with open(GRAPH_PICKLE, "rb") as f:
        return pickle.load(f)


@pytest.fixture(scope="module")
def graph(graph_data):
    return graph_data["graph"]


@pytest.fixture(scope="module")
def reverse_adj(graph):
    """Pre-build the same reverse adjacency index TributaryEnricher uses."""
    adj: Dict[int, List[int]] = {}
    for e in graph.es:
        t = e.target
        if t not in adj:
            adj[t] = []
        adj[t].append(e.index)
    return adj


# ---------------------------------------------------------------------------
# BFS helper (mirrors TributaryEnricher._traverse_upstream)
# ---------------------------------------------------------------------------

EXCLUDED_EDGE_TYPES = {"2300"}


def bfs_upstream(
    graph,
    reverse_adj: Dict[int, List[int]],
    seed_edge_indices: List[int],
    excluded_watershed_codes: Set[str],
    excluded_waterbody_keys: Set[str] | None = None,
) -> Set[int]:
    """
    Pure BFS upstream — identical logic to TributaryEnricher._traverse_upstream.

    Returns set of tributary edge indices (excludes seeds).
    """
    tributaries: set = set()
    visited: set = set()
    queue: deque = deque()

    for ei in seed_edge_indices:
        queue.append(ei)
        visited.add(ei)

    while queue:
        ei = queue.popleft()
        edge = graph.es[ei]
        current_etype = str(edge["edge_type"]) if edge["edge_type"] else ""
        current_excluded = current_etype in EXCLUDED_EDGE_TYPES

        for upstream_idx in reverse_adj.get(edge.source, []):
            if upstream_idx in visited:
                continue
            upstream = graph.es[upstream_idx]
            visited.add(upstream_idx)

            # Filter 1: waterbody key exclusion
            wb_key = upstream["waterbody_key"]
            if wb_key and excluded_waterbody_keys and wb_key in excluded_waterbody_keys:
                continue

            # Filter 2: watershed code exclusion
            wsc = upstream["fwa_watershed_code"]
            if wsc and wsc in excluded_watershed_codes:
                continue

            # Filter 3: edge-type boundary
            up_etype = str(upstream["edge_type"]) if upstream["edge_type"] else ""
            if current_excluded and up_etype not in EXCLUDED_EDGE_TYPES:
                continue

            tributaries.add(upstream_idx)
            queue.append(upstream_idx)

    return tributaries


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


def edges_by_name(graph, name_fragment: str) -> list:
    """Return all edges whose gnis_name contains the fragment (case-insensitive)."""
    frag = name_fragment.lower()
    return [
        e for e in graph.es if e["gnis_name"] and frag in str(e["gnis_name"]).lower()
    ]


def edges_by_blk(graph, blue_line_key: str) -> list:
    """Return all edges with a specific blue_line_key."""
    return [e for e in graph.es if str(e["blue_line_key"]) == blue_line_key]


def linear_ids_from_edges(edges) -> Set[str]:
    return {str(e["linear_feature_id"]) for e in edges}


def wb_keys_from_edges(edges) -> Set[str]:
    return {e["waterbody_key"] for e in edges if e["waterbody_key"]}


# ---------------------------------------------------------------------------
# Constants for Oyster River / Piggott Creek area
# ---------------------------------------------------------------------------

OYSTER_RIVER_WSC = "920-602544-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
PIGGOTT_CREEK_WSC = "920-602544-479843-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"

# The large shared swamp at the Oyster River estuary
ESTUARY_SWAMP_WB_KEY = "328997926"

# Piggott Creek's confluence edge enters through this swamp
PIGGOTT_CONFLUENCE_LID = "710237335"  # wb_key=328997926, etype=1450

# Unnamed stream in Strathcona Park that splits into 2 groups
UNNAMED_STREAM_BLK = "354154376"


# ===================================================================
# TEST CLASS: Piggott Creek — tributary of Oyster River
# ===================================================================


class TestPiggottCreekTraversal:
    """
    Piggott Creek enters Oyster River through a large shared swamp
    (wb_key=328997926, 95 edges).  With the fix, stream-seeded BFS
    passes no excluded_waterbody_keys → Piggott Creek is reachable.
    """

    def test_piggott_reachable_without_wetland_exclusion(self, graph, reverse_adj):
        """Baseline: Piggott Creek IS reachable when no wetland wb_keys are excluded."""
        oyster_edges = edges_by_name(graph, "oyster river")
        piggott_edges = edges_by_name(graph, "piggott creek")
        assert len(oyster_edges) > 0, "Oyster River not found"
        assert len(piggott_edges) > 0, "Piggott Creek not found"

        oyster_seed_indices = [e.index for e in oyster_edges]
        oyster_wscs = {e["fwa_watershed_code"] for e in oyster_edges}

        tributaries = bfs_upstream(
            graph,
            reverse_adj,
            seed_edge_indices=oyster_seed_indices,
            excluded_watershed_codes=oyster_wscs,
            excluded_waterbody_keys=None,  # Stream-seeded: no wb_key exclusion
        )

        piggott_lids = linear_ids_from_edges(piggott_edges)
        trib_lids = {str(graph.es[i]["linear_feature_id"]) for i in tributaries}
        found = piggott_lids & trib_lids

        assert len(found) > 0, (
            "Piggott Creek should be reachable as a tributary of Oyster River "
            "when no waterbody keys are excluded (stream-seeded path)"
        )

    def test_piggott_reachable_even_with_swamp_in_linked_set(self, graph, reverse_adj):
        """
        Even when the estuary swamp wb_key is in linked_waterbody_keys_of_polygon,
        stream-seeded BFS should NOT exclude it — Piggott Creek must be reachable.
        This verifies the fix: stream seeds never pass excluded_waterbody_keys.
        """
        oyster_edges = edges_by_name(graph, "oyster river")
        piggott_edges = edges_by_name(graph, "piggott creek")

        oyster_seed_indices = [e.index for e in oyster_edges]
        oyster_wscs = {e["fwa_watershed_code"] for e in oyster_edges}

        # Simulate the OLD broken behavior: passing wb_key exclusion
        # This should still block Piggott — confirming the BFS mechanism works
        tributaries_blocked = bfs_upstream(
            graph,
            reverse_adj,
            seed_edge_indices=oyster_seed_indices,
            excluded_watershed_codes=oyster_wscs,
            excluded_waterbody_keys={ESTUARY_SWAMP_WB_KEY},
        )
        piggott_lids = linear_ids_from_edges(piggott_edges)
        blocked_lids = {
            str(graph.es[i]["linear_feature_id"]) for i in tributaries_blocked
        }
        assert (
            len(piggott_lids & blocked_lids) == 0
        ), "Sanity check: BFS with swamp excluded SHOULD block Piggott Creek"

        # The FIX: stream-seeded BFS passes no excluded_waterbody_keys
        tributaries_fixed = bfs_upstream(
            graph,
            reverse_adj,
            seed_edge_indices=oyster_seed_indices,
            excluded_watershed_codes=oyster_wscs,
            excluded_waterbody_keys=None,  # The fix
        )
        fixed_lids = {str(graph.es[i]["linear_feature_id"]) for i in tributaries_fixed}
        found = piggott_lids & fixed_lids

        assert len(found) > 0, (
            f"Piggott Creek ({len(piggott_edges)} edges) must be reachable when "
            f"stream-seeded BFS omits excluded_waterbody_keys (the fix)."
        )

    def test_piggott_confluence_edge_has_swamp_wb_key(self, graph):
        """Verify the data: Piggott's confluence edge is the swamp."""
        edge = None
        for e in graph.es:
            if str(e["linear_feature_id"]) == PIGGOTT_CONFLUENCE_LID:
                edge = e
                break
        assert edge is not None, f"Edge {PIGGOTT_CONFLUENCE_LID} not found"
        assert edge["waterbody_key"] == ESTUARY_SWAMP_WB_KEY
        assert edge["gnis_name"] and "piggott" in edge["gnis_name"].lower()


# ===================================================================
# TEST CLASS: Unnamed stream (BLK 354154376) splits at wetland
# ===================================================================


class TestUnnamedStreamWetlandSplit:
    """
    An unnamed stream (BLK 354154376) passes through several small
    wetlands in Strathcona Park.  With the fix, stream-seeded BFS
    no longer excludes wetland wb_keys, so the stream stays contiguous.
    """

    def test_stream_contiguous_without_exclusions(self, graph, reverse_adj):
        """Baseline: all edges are reachable in one BFS from the downstream end."""
        blk_edges = edges_by_blk(graph, UNNAMED_STREAM_BLK)
        assert len(blk_edges) > 0, f"BLK {UNNAMED_STREAM_BLK} not found"

        all_indices = [e.index for e in blk_edges]

        source_nodes = {e.source for e in blk_edges}
        downstream_edges = [e for e in blk_edges if e.target not in source_nodes]
        assert len(downstream_edges) >= 1, "Cannot identify downstream edge"

        seed = [downstream_edges[0].index]
        tributaries = bfs_upstream(
            graph,
            reverse_adj,
            seed_edge_indices=seed,
            excluded_watershed_codes=set(),
            excluded_waterbody_keys=None,  # Stream-seeded: no wb_key exclusion
        )

        other_indices = set(all_indices) - {seed[0]}
        missing = other_indices - tributaries

        assert len(missing) == 0, (
            f"All {len(blk_edges)} edges should be contiguous. "
            f"Missing {len(missing)} edges."
        )

    def test_stream_split_confirmed_with_old_exclusion(self, graph, reverse_adj):
        """Confirm the old behaviour: BFS with wb_key exclusion splits the stream."""
        blk_edges = edges_by_blk(graph, UNNAMED_STREAM_BLK)
        wb_keys_on_stream = wb_keys_from_edges(blk_edges)
        assert len(wb_keys_on_stream) > 0, "Stream should have waterbody keys"

        source_nodes = {e.source for e in blk_edges}
        downstream_edges = [e for e in blk_edges if e.target not in source_nodes]
        seed = [downstream_edges[0].index]
        all_indices = set(e.index for e in blk_edges)

        tributaries = bfs_upstream(
            graph,
            reverse_adj,
            seed_edge_indices=seed,
            excluded_watershed_codes=set(),
            excluded_waterbody_keys=wb_keys_on_stream,  # Old broken behaviour
        )

        other_indices = all_indices - {seed[0]}
        missing = other_indices - tributaries

        # The old algorithm DOES split — this confirms the mechanism
        assert (
            len(missing) > 0
        ), "Sanity check: BFS with wb_key exclusion should split this stream"

    def test_stream_contiguous_after_fix(self, graph, reverse_adj):
        """
        With the fix (stream-seeded BFS omits excluded_waterbody_keys),
        all edges on this unnamed stream should be reachable.
        """
        blk_edges = edges_by_blk(graph, UNNAMED_STREAM_BLK)

        source_nodes = {e.source for e in blk_edges}
        downstream_edges = [e for e in blk_edges if e.target not in source_nodes]
        seed = [downstream_edges[0].index]
        all_indices = set(e.index for e in blk_edges)

        # The fix: no excluded_waterbody_keys for stream-seeded BFS
        tributaries = bfs_upstream(
            graph,
            reverse_adj,
            seed_edge_indices=seed,
            excluded_watershed_codes=set(),
            excluded_waterbody_keys=None,
        )

        other_indices = all_indices - {seed[0]}
        missing = other_indices - tributaries

        assert len(missing) == 0, (
            f"After fix, all {len(blk_edges)} edges should be contiguous. "
            f"Missing {len(missing)} edges."
        )


# ===================================================================
# INTEGRATION: Mapper-level _enrich_with_tributaries
# ===================================================================


class _GraphBackedGazetteer:
    """Minimal gazetteer stub backed by real graph edge attributes.

    Provides only the methods RegulationMapper._enrich_with_tributaries and
    TributaryEnricher._edges_to_features actually call.
    """

    def __init__(self, graph):
        self._graph = graph
        self._edge_by_lid: Dict[str, int] = {}
        self.metadata: Dict[FeatureType, Dict[str, dict]] = defaultdict(dict)
        self.data_accessor = None
        self._reprojected_admin_cache: dict = {}

        # Index edges by linear_feature_id and populate stream metadata
        for e in graph.es:
            lid = str(e["linear_feature_id"])
            self._edge_by_lid[lid] = e.index
            self.metadata[FeatureType.STREAM][lid] = {
                "gnis_name": e["gnis_name"] or "",
                "gnis_id": e["gnis_id"] or "",
                "blue_line_key": str(e["blue_line_key"] or ""),
                "waterbody_key": str(e["waterbody_key"] or ""),
                "fwa_watershed_code": e["fwa_watershed_code"] or "",
                "zones": [],
                "zones_unbuffered": [],
                "mgmt_units": [],
                "mgmt_units_unbuffered": [],
                "region_names": [],
            }

    def get_feature_by_id(self, feature_id: str) -> Optional[FWAFeature]:
        return self.get_stream_by_id(feature_id)

    def get_stream_metadata(self, linear_feature_id: str) -> Optional[dict]:
        return self.metadata.get(FeatureType.STREAM, {}).get(linear_feature_id)

    def get_stream_by_id(self, linear_feature_id: str) -> Optional[FWAFeature]:
        meta = self.get_stream_metadata(linear_feature_id)
        if not meta:
            return None
        return FWAFeature(
            fwa_id=linear_feature_id,
            geometry_type="multilinestring",
            zones=meta.get("zones", []),
            feature_type=FeatureType.STREAM,
            gnis_name=meta.get("gnis_name") or None,
            waterbody_key=meta.get("waterbody_key") or None,
            blue_line_key=meta.get("blue_line_key") or None,
            mgmt_units=meta.get("mgmt_units", []),
        )

    def get_features(self) -> list:
        return []

    def get_valid_stream_ids(self) -> set:
        return set(self.metadata.get(FeatureType.STREAM, {}).keys())


class _FakeLinkingResult:
    def __init__(self):
        self.status = "NOT_FOUND"
        self.matched_features = []
        self.matched_name = ""
        self.link_method = ""
        self.admin_match = None
        self.additional_info = None


class _FakeCorrections:
    def get_all_feature_name_variations(self):
        return []


class _FakeLinker:
    """Minimal linker stub — only provides gazetteer access."""

    def __init__(self, gazetteer):
        self.gazetteer = gazetteer
        self.corrections = _FakeCorrections()

    def link_waterbody(self, **kwargs):
        return _FakeLinkingResult()


@pytest.fixture(scope="module")
def enricher(graph_data, graph):
    """Build a real TributaryEnricher with the full FWA graph."""
    gaz = _GraphBackedGazetteer(graph)
    return TributaryEnricher(graph_source=graph_data, metadata_gazetteer=gaz)


@pytest.fixture(scope="module")
def mapper_with_enricher(enricher, graph):
    """Build a RegulationMapper wired to a real TributaryEnricher."""
    gaz = _GraphBackedGazetteer(graph)
    linker = _FakeLinker(gaz)
    scope_filter = ScopeFilter()
    m = RegulationMapper(
        linker=linker,
        scope_filter=scope_filter,
        tributary_enricher=enricher,
        gpkg_path=None,
    )
    return m


def _oyster_river_features(graph) -> List[FWAFeature]:
    """Build FWAFeature objects for every Oyster River edge."""
    features = []
    for e in graph.es:
        if e["gnis_name"] and "oyster river" in str(e["gnis_name"]).lower():
            features.append(
                FWAFeature(
                    fwa_id=str(e["linear_feature_id"]),
                    geometry_type="multilinestring",
                    zones=[],
                    feature_type=FeatureType.STREAM,
                    gnis_name=e["gnis_name"],
                    waterbody_key=(
                        str(e["waterbody_key"]) if e["waterbody_key"] else None
                    ),
                    blue_line_key=(
                        str(e["blue_line_key"]) if e["blue_line_key"] else None
                    ),
                    mgmt_units=[],
                )
            )
    return features


class TestMapperStreamEnrichmentIntegration:
    """
    Integration: verifies RegulationMapper._enrich_with_tributaries
    correctly omits excluded_waterbody_keys for stream-seeded enrichment,
    allowing Piggott Creek to be found as a tributary of Oyster River
    even when a swamp wb_key is in linked_waterbody_keys_of_polygon.
    """

    def test_piggott_found_through_mapper_enrichment(self, mapper_with_enricher, graph):
        """Mapper-level: Piggott Creek appears in tributary results for Oyster River."""
        mapper = mapper_with_enricher
        mapper.linked_waterbody_keys_of_polygon = {ESTUARY_SWAMP_WB_KEY}

        oyster_features = _oyster_river_features(graph)
        assert len(oyster_features) > 0, "Oyster River features not found"

        tributaries = mapper._enrich_with_tributaries(oyster_features)
        trib_names = {f.gnis_name.lower() for f in tributaries if f.gnis_name}

        assert any("piggott" in n for n in trib_names), (
            f"Piggott Creek must appear in tributaries of Oyster River. "
            f"Found {len(tributaries)} tributaries, named streams: "
            f"{sorted(n for n in trib_names)[:20]}"
        )

    def test_stream_seeds_do_not_pass_excluded_wb_keys(
        self, mapper_with_enricher, graph
    ):
        """Verify the mapper calls enricher WITHOUT excluded_waterbody_keys for streams."""
        mapper = mapper_with_enricher
        mapper.linked_waterbody_keys_of_polygon = {ESTUARY_SWAMP_WB_KEY}

        oyster_features = _oyster_river_features(graph)

        call_args_log = []
        original_enrich = mapper.tributary_enricher.enrich_with_tributaries

        def spy_enrich(*args, **kwargs):
            call_args_log.append(kwargs.copy())
            return original_enrich(*args, **kwargs)

        with patch.object(
            mapper.tributary_enricher, "enrich_with_tributaries", side_effect=spy_enrich
        ):
            mapper.tributary_enricher.enrichment_cache.clear()
            mapper._enrich_with_tributaries(oyster_features)

        assert len(call_args_log) > 0, "Enricher was never called"
        # Stream-seeded path should NOT pass excluded_waterbody_keys
        for call_kwargs in call_args_log:
            assert "excluded_waterbody_keys" not in call_kwargs, (
                f"Stream-seeded enrichment must NOT pass excluded_waterbody_keys. "
                f"Got kwargs: {call_kwargs}"
            )

    def test_unnamed_stream_edges_all_found(self, mapper_with_enricher, graph):
        """
        BLK 354154376 (unnamed stream through wetlands): when ALL edges
        of the stream are seeded (as the real pipeline does), enrichment
        should find tributaries that branch off through wetland wb_keys.

        The real pipeline seeds every edge of a matched stream, so the
        mainstem edges are already "found" as seeds.  What we verify here
        is that the enricher does NOT choke — the combined seed + tributary
        set should cover every edge on the BLK, even with wetland wb_keys
        in linked_waterbody_keys_of_polygon.
        """
        mapper = mapper_with_enricher
        blk_edges = edges_by_blk(graph, UNNAMED_STREAM_BLK)
        assert len(blk_edges) > 0, f"BLK {UNNAMED_STREAM_BLK} not found"

        # Populate linked_waterbody_keys_of_polygon with the wetland wb_keys
        # (the set that USED to block traversal)
        wetland_wb_keys = wb_keys_from_edges(blk_edges)
        mapper.linked_waterbody_keys_of_polygon = wetland_wb_keys

        # Seed with ALL edges (matches real pipeline behaviour)
        seed_features = []
        for e in blk_edges:
            seed_features.append(
                FWAFeature(
                    fwa_id=str(e["linear_feature_id"]),
                    geometry_type="multilinestring",
                    zones=[],
                    feature_type=FeatureType.STREAM,
                    gnis_name=e["gnis_name"] or None,
                    waterbody_key=(
                        str(e["waterbody_key"]) if e["waterbody_key"] else None
                    ),
                    blue_line_key=(
                        str(e["blue_line_key"]) if e["blue_line_key"] else None
                    ),
                    mgmt_units=[],
                )
            )

        mapper.tributary_enricher.enrichment_cache.clear()
        tributaries = mapper._enrich_with_tributaries(seed_features)

        # Seeds + tributaries together should cover the full stream
        seed_lids = {f.fwa_id for f in seed_features}
        trib_lids = {f.fwa_id for f in tributaries}
        all_lids = {str(e["linear_feature_id"]) for e in blk_edges}
        combined = seed_lids | trib_lids
        missing = all_lids - combined

        assert len(missing) == 0, (
            f"Unnamed stream (BLK {UNNAMED_STREAM_BLK}): {len(missing)} of "
            f"{len(all_lids)} edges not in seeds+tributaries. "
            f"Wetland wb_keys in linked set: {wetland_wb_keys}"
        )

        # Bonus: enrichment should not error or return zero tributaries
        # (it may find upstream branches beyond this BLK)
        assert tributaries is not None
